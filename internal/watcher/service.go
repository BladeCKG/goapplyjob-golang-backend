package watcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

var (
	lastmodPattern     = regexp.MustCompile(`(?is)<lastmod>\s*([^<]+?)\s*</lastmod>`)
	urlOpenPattern     = regexp.MustCompile(`(?is)<url(?:\s|>)`)
	urlBlockPattern    = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)
	urlSetClosePattern = regexp.MustCompile(`(?is)</urlset\s*>`)
)

type Config struct {
	Enabled         bool
	URL             string
	IntervalMinutes float64
	SampleKB        int
	TimeoutSeconds  float64
}

type FetchSampleFunc func() ([]byte, error)
type FetchFullFunc func() ([]byte, error)

type Service struct {
	Config      Config
	DB          *database.DB
	FetchSample FetchSampleFunc
	FetchFull   FetchFullFunc
	status      map[string]any
}

func New(config Config, db *database.DB) *Service {
	svc := &Service{Config: config, DB: db}
	svc.status = map[string]any{
		"enabled":                     config.Enabled,
		"url":                         config.URL,
		"interval_minutes":            config.IntervalMinutes,
		"sample_kb":                   config.SampleKB,
		"running":                     false,
		"last_check_at":               nil,
		"last_change_at":              nil,
		"last_sample_hash":            nil,
		"last_error":                  nil,
		"last_overlap_bytes":          0,
		"last_delta_source":           nil,
		"last_delta_size":             0,
		"last_new_sample_lastmod":     nil,
		"last_previous_first_lastmod": nil,
		"last_delta_payload_id":       nil,
	}
	svc.FetchSample = func() ([]byte, error) { return nil, errors.New("fetch sample not configured") }
	svc.FetchFull = func() ([]byte, error) { return nil, errors.New("fetch full not configured") }
	return svc
}

func (s *Service) Status() map[string]any {
	out := map[string]any{}
	for k, v := range s.status {
		out[k] = v
	}
	return out
}

func (s *Service) setStatus(values map[string]any) {
	for k, v := range values {
		s.status[k] = v
	}
}

func (s *Service) RunForever(runOnce bool) error {
	if !s.Config.Enabled {
		s.setStatus(map[string]any{"last_error": nil})
		return nil
	}
	if strings.TrimSpace(s.Config.URL) == "" {
		s.setStatus(map[string]any{"last_error": "WATCH_URL is not set"})
		return nil
	}

	s.setStatus(map[string]any{"running": true})
	defer s.setStatus(map[string]any{"running": false})

	for {
		if err := s.RunOnce(); err != nil {
			return err
		}
		if runOnce {
			return nil
		}
		sleepSeconds := s.Config.IntervalMinutes * 60
		if sleepSeconds < 1 {
			sleepSeconds = 1
		}
		time.Sleep(time.Duration(sleepSeconds * float64(time.Second)))
	}
}

func (s *Service) RunOnce() error {
	sample, err := s.FetchSample()
	if err != nil {
		s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		return err
	}

	currentHash := sha256Hex(sample)
	previousHash, previousFirstLastmod, _ := s.loadState(context.Background())
	currentFirstLastmod := s.ExtractFirstLastmod(sample)

	s.setStatus(map[string]any{
		"last_check_at":    utcNowISO(),
		"last_sample_hash": currentHash,
		"last_error":       nil,
	})

	if currentHash == previousHash {
		_ = s.saveState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))
		s.setStatus(map[string]any{"last_overlap_bytes": len(sample)})
		return nil
	}

	newSampleLastmod := s.ExtractLastLastmod(sample)
	previousDT := s.parseLastmod(previousFirstLastmod)
	sampleLastDT := s.parseLastmod(newSampleLastmod)
	hasCompleteSampleBlocks := urlBlockPattern.Find(sample) != nil
	useSampleDelta := previousFirstLastmod != "" &&
		!previousDT.IsZero() &&
		!sampleLastDT.IsZero() &&
		(sampleLastDT.Before(previousDT) || sampleLastDT.Equal(previousDT)) &&
		hasCompleteSampleBlocks

	var fullData []byte
	deltaData := sample
	deltaSource := "full_no_previous_lastmod"
	overlapBytes := 0
	if useSampleDelta {
		deltaData = s.DeltaNewerThanLastmod(sample, previousFirstLastmod)
		deltaSource = "sample_lastmod_window"
		overlapBytes = max(len(sample)-len(deltaData), 0)
		if len(deltaData) == 0 {
			_ = s.saveState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))
			s.setStatus(map[string]any{
				"last_change_at":              utcNowISO(),
				"last_overlap_bytes":          overlapBytes,
				"last_delta_size":             0,
				"last_new_sample_lastmod":     emptyToNil(newSampleLastmod),
				"last_previous_first_lastmod": emptyToNil(previousFirstLastmod),
				"last_delta_payload_id":       nil,
			})
			return nil
		}
	} else {
		fullData, err = s.FetchFull()
		if err != nil {
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
			return err
		}
		deltaData = fullData
		if previousFirstLastmod != "" {
			deltaData = s.DeltaNewerThanLastmod(fullData, previousFirstLastmod)
			overlapBytes = max(len(fullData)-len(deltaData), 0)
			deltaSource = "full_lastmod_window"
		}
	}

	if len(fullData) > 0 && currentFirstLastmod == "" {
		currentFirstLastmod = s.ExtractFirstLastmod(fullData)
	}
	_ = s.saveState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))

	var payloadID any
	if len(deltaData) > 0 {
		saved, err := s.saveDeltaPayload(context.Background(), string(deltaData))
		if err != nil {
			return err
		}
		payloadID = saved
	}

	s.setStatus(map[string]any{
		"last_change_at":              utcNowISO(),
		"last_overlap_bytes":          overlapBytes,
		"last_delta_source":           deltaSource,
		"last_delta_size":             len(deltaData),
		"last_new_sample_lastmod":     emptyToNil(newSampleLastmod),
		"last_previous_first_lastmod": emptyToNil(previousFirstLastmod),
		"last_delta_payload_id":       payloadID,
	})
	return nil
}

func (s *Service) loadState(ctx context.Context) (string, string, error) {
	if s.DB == nil {
		return "", "", nil
	}
	var sampleHash, firstLastmod string
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT COALESCE(sample_hash, ''), COALESCE(first_lastmod, '')
		 FROM watcher_states
		 WHERE source_url = ?
		 LIMIT 1`,
		s.Config.URL,
	).Scan(&sampleHash, &firstLastmod)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	return sampleHash, firstLastmod, nil
}

func (s *Service) saveState(ctx context.Context, sampleHash, firstLastmod string) error {
	if s.DB == nil {
		return nil
	}
	_, err := s.DB.SQL.ExecContext(
		ctx,
		`INSERT INTO watcher_states (source_url, sample_hash, first_lastmod, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(source_url) DO UPDATE SET
		   sample_hash = excluded.sample_hash,
		   first_lastmod = excluded.first_lastmod,
		   updated_at = excluded.updated_at`,
		s.Config.URL,
		sampleHash,
		emptyToNil(firstLastmod),
		utcNowISO(),
	)
	return err
}

func (s *Service) inferFileExtension() string {
	parsed, err := url.Parse(s.Config.URL)
	if err != nil {
		return ".xml"
	}
	ext := strings.ToLower(parsed.Path)
	if ext == "" {
		return ".xml"
	}
	return ext
}

func (s *Service) saveDeltaPayload(ctx context.Context, bodyText string) (int64, error) {
	if s.DB == nil {
		return 0, nil
	}
	result, err := s.DB.SQL.ExecContext(
		ctx,
		`INSERT INTO watcher_payloads (source_url, payload_type, body_text, created_at)
		 VALUES (?, 'delta_xml', ?, ?)`,
		s.Config.URL,
		bodyText,
		utcNowISO(),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Service) ExtractFirstLastmod(data []byte) string {
	match := lastmodPattern.FindSubmatch(data)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func (s *Service) ExtractLastLastmod(data []byte) string {
	matches := lastmodPattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return ""
	}
	return strings.TrimSpace(string(matches[len(matches)-1][1]))
}

func (s *Service) parseLastmod(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}

func (s *Service) DeltaNewerThanLastmod(fullData []byte, previousFirstLastmod string) []byte {
	previousDT := s.parseLastmod(previousFirstLastmod)
	if previousDT.IsZero() {
		return fullData
	}

	blocks := make([][]byte, 0)
	for _, match := range urlBlockPattern.FindAll(fullData, -1) {
		blockLastmod := s.ExtractFirstLastmod(match)
		blockDT := s.parseLastmod(blockLastmod)
		if blockDT.IsZero() {
			continue
		}
		if blockDT.After(previousDT) {
			blocks = append(blocks, bytes.Clone(match))
		} else {
			break
		}
	}
	if len(blocks) == 0 {
		return []byte{}
	}

	firstURL := urlOpenPattern.FindIndex(fullData)
	if firstURL == nil {
		return bytes.Join(blocks, nil)
	}
	suffix := []byte{}
	if match := urlSetClosePattern.Find(fullData); len(match) > 0 {
		suffix = match
	}

	output := make([]byte, 0, len(fullData))
	output = append(output, fullData[:firstURL[0]]...)
	output = append(output, bytes.Join(blocks, nil)...)
	output = append(output, suffix...)
	return output
}

func utcNowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func emptyToNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
