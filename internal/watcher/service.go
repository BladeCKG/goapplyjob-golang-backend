package watcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/builtin"
)

const (
	sourceName       = "remoterocketship"
	sourceBuiltin    = "builtin"
	payloadTypeDelta = "delta"
	payloadTypeXML   = "delta_xml"
)

var (
	lastmodPattern     = regexp.MustCompile(`(?is)<lastmod>\s*([^<]+?)\s*</lastmod>`)
	urlOpenPattern     = regexp.MustCompile(`(?is)<url(?:\s|>)`)
	urlBlockPattern    = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)
	urlSetClosePattern = regexp.MustCompile(`(?is)</urlset\s*>`)
)

type Config struct {
	Enabled              bool
	URL                  string
	IntervalMinutes      float64
	SampleKB             int
	TimeoutSeconds       float64
	BuiltinBaseURL       string
	BuiltinMaxPage       int
	BuiltinPagesPerCycle int
	EnabledSources       map[string]struct{}
}

type FetchSampleFunc func() ([]byte, error)
type FetchFullFunc func() ([]byte, error)

type Service struct {
	Config      Config
	DB          *database.DB
	FetchSample FetchSampleFunc
	FetchFull   FetchFullFunc
	FetchText   func(string) (string, error)
	status      map[string]any
}

func New(config Config, db *database.DB) *Service {
	svc := &Service{Config: config, DB: db}
	svc.status = map[string]any{
		"enabled":                     config.Enabled,
		"url":                         config.URL,
		"interval_minutes":            config.IntervalMinutes,
		"sample_kb":                   config.SampleKB,
		"enabled_sources":             sortedSourceNames(config.EnabledSources),
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
	svc.FetchText = func(string) (string, error) { return "", errors.New("fetch text not configured") }
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
	if strings.TrimSpace(s.Config.URL) == "" && strings.TrimSpace(s.Config.BuiltinBaseURL) == "" {
		s.setStatus(map[string]any{"last_error": "No source configured"})
		return nil
	}
	if len(s.Config.EnabledSources) == 0 {
		s.setStatus(map[string]any{"last_error": "No source enabled"})
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
	if strings.TrimSpace(s.Config.URL) != "" && s.isSourceEnabled(sourceName) {
		if err := s.runOnceRemoteRocketship(); err != nil {
			return err
		}
	}
	if strings.TrimSpace(s.Config.BuiltinBaseURL) != "" && s.isSourceEnabled(sourceBuiltin) {
		if err := s.runOnceBuiltin(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) runOnceRemoteRocketship() error {
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

func (s *Service) runOnceBuiltin() error {
	statePayload, err := s.loadStatePayload(sourceBuiltin)
	if err != nil {
		return err
	}
	nextPage := intFromAny(statePayload["next_page"], s.Config.BuiltinMaxPage)
	if nextPage <= 0 {
		nextPage = s.Config.BuiltinMaxPage
	}
	lastJobURL, _ := statePayload["last_job_url"].(string)
	lastPostDate, _ := statePayload["last_post_date"].(string)
	lastPostDateDT := parseISOTime(lastPostDate)
	currentPage := nextPage
	pagesScanned := 0
	payloadsCreated := 0
	firstSeenMarkerUpdated := false
	phase1BoundaryMatched := false

	if (lastJobURL != "" || lastPostDateDT != nil) && currentPage < s.Config.BuiltinMaxPage {
		probePage := currentPage + 1
		for probePage <= s.Config.BuiltinMaxPage && pagesScanned < s.Config.BuiltinPagesPerCycle {
			pageURL := strings.ReplaceAll(s.Config.BuiltinBaseURL, "{page}", strconv.Itoa(probePage))
			htmlText, err := s.FetchText(pageURL)
			if err != nil {
				return err
			}
			pagesScanned++
			if strings.Contains(htmlText, "No job results") {
				break
			}
			listings := builtin.ExtractJobListings(htmlText)
			if len(listings) == 0 {
				break
			}
			if _, err := s.saveDeltaPayloadForSource(sourceBuiltin, pageURL, payloadTypeDelta, mustMarshalJSON(listings)); err != nil {
				return err
			}
			payloadsCreated++
			if containsListingURL(listings, lastJobURL) || allListingsOlderThan(listings, lastPostDateDT) {
				phase1BoundaryMatched = true
				break
			}
			probePage++
		}
	}

	skipPhase2UntilBoundary := !phase1BoundaryMatched && (lastJobURL != "" || lastPostDateDT != nil)
	for currentPage >= 1 && pagesScanned < s.Config.BuiltinPagesPerCycle {
		pageURL := strings.ReplaceAll(s.Config.BuiltinBaseURL, "{page}", strconv.Itoa(currentPage))
		htmlText, err := s.FetchText(pageURL)
		if err != nil {
			return err
		}
		pagesScanned++
		if strings.Contains(htmlText, "No job results") {
			currentPage--
			continue
		}
		listings := builtin.ExtractJobListings(htmlText)
		if skipPhase2UntilBoundary && len(listings) > 0 {
			boundaryHit := containsListingURL(listings, lastJobURL) || allListingsOlderThan(listings, lastPostDateDT)
			if boundaryHit {
				skipPhase2UntilBoundary = false
			}
			currentPage--
			continue
		}
		if len(listings) > 0 {
			if _, err := s.saveDeltaPayloadForSource(sourceBuiltin, pageURL, payloadTypeDelta, mustMarshalJSON(listings)); err != nil {
				return err
			}
			payloadsCreated++
			if !firstSeenMarkerUpdated {
				if firstURL, ok := listings[0]["url"].(string); ok {
					lastJobURL = firstURL
				}
				if firstPostDate, ok := listings[0]["post_date"].(string); ok {
					lastPostDate = firstPostDate
				}
				firstSeenMarkerUpdated = true
			}
		}
		currentPage--
	}

	nextSavedPage := currentPage
	if nextSavedPage < 1 {
		nextSavedPage = 1
	}
	return s.saveStatePayload(sourceBuiltin, map[string]any{
		"next_page":                   nextSavedPage,
		"last_post_date":              valueOrNil(lastPostDate),
		"last_job_url":                valueOrNil(lastJobURL),
		"last_scan_at":                utcNowISO(),
		"pages_scanned_last_cycle":    pagesScanned,
		"payloads_created_last_cycle": payloadsCreated,
	})
}

func (s *Service) loadState(ctx context.Context) (string, string, error) {
	if s.DB == nil {
		return "", "", nil
	}
	var sampleHash, firstLastmod string
	var stateJSON sql.NullString
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT COALESCE(state_json, '')
		 FROM watcher_states
		 WHERE source = ?
		 LIMIT 1`,
		sourceName,
	).Scan(&stateJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
		payload := map[string]any{}
		if err := json.Unmarshal([]byte(stateJSON.String), &payload); err == nil {
			sampleHash, _ = payload["sample_hash"].(string)
			firstLastmod, _ = payload["first_lastmod"].(string)
		}
	}
	return sampleHash, firstLastmod, nil
}

func (s *Service) saveState(ctx context.Context, sampleHash, firstLastmod string) error {
	if s.DB == nil {
		return nil
	}
	_, err := s.DB.SQL.ExecContext(
		ctx,
		`INSERT INTO watcher_states (source, state_json, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(source) DO UPDATE SET
		   state_json = excluded.state_json,
		   updated_at = excluded.updated_at`,
		sourceName,
		mustMarshalJSON(map[string]any{
			"source_url":    s.Config.URL,
			"sample_hash":   sampleHash,
			"first_lastmod": emptyToNil(firstLastmod),
		}),
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
		`INSERT INTO watcher_payloads (source, source_url, payload_type, body_text, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		sourceName,
		s.Config.URL,
		payloadTypeXML,
		bodyText,
		utcNowISO(),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Service) saveDeltaPayloadForSource(source, sourceURL, payloadType, bodyText string) (int64, error) {
	if s.DB == nil {
		return 0, nil
	}
	result, err := s.DB.SQL.ExecContext(
		context.Background(),
		`INSERT INTO watcher_payloads (source, source_url, payload_type, body_text, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		source,
		sourceURL,
		payloadType,
		bodyText,
		utcNowISO(),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Service) loadStatePayload(source string) (map[string]any, error) {
	var stateJSON sql.NullString
	err := s.DB.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(state_json, '') FROM watcher_states WHERE source = ? LIMIT 1`, source).Scan(&stateJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	payload := map[string]any{}
	if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
		_ = json.Unmarshal([]byte(stateJSON.String), &payload)
	}
	return payload, nil
}

func (s *Service) saveStatePayload(source string, payload map[string]any) error {
	_, err := s.DB.SQL.ExecContext(context.Background(),
		`INSERT INTO watcher_states (source, state_json, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(source) DO UPDATE SET state_json = excluded.state_json, updated_at = excluded.updated_at`,
		source,
		mustMarshalJSON(payload),
		utcNowISO(),
	)
	return err
}

func mustMarshalJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func parseISOTime(value string) *time.Time {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05", value); err == nil {
		utc := parsed.UTC()
		return &utc
	}
	return nil
}

func intFromAny(value any, fallback int) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return fallback
	}
}

func containsListingURL(listings []map[string]any, targetURL string) bool {
	if strings.TrimSpace(targetURL) == "" {
		return false
	}
	for _, listing := range listings {
		if urlValue, _ := listing["url"].(string); urlValue == targetURL {
			return true
		}
	}
	return false
}

func allListingsOlderThan(listings []map[string]any, marker *time.Time) bool {
	if marker == nil {
		return false
	}
	foundAny := false
	for _, listing := range listings {
		postDate, _ := listing["post_date"].(string)
		listingDT := parseISOTime(postDate)
		if listingDT == nil {
			continue
		}
		foundAny = true
		if !listingDT.Before(*marker) {
			return false
		}
	}
	return foundAny
}

func valueOrNil(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
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

func (s *Service) isSourceEnabled(source string) bool {
	if len(s.Config.EnabledSources) == 0 {
		return false
	}
	_, ok := s.Config.EnabledSources[strings.ToLower(strings.TrimSpace(source))]
	return ok
}

func sortedSourceNames(values map[string]struct{}) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
