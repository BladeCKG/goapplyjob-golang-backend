package watcher

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
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
	StateFile       string
	OutputDir       string
}

type FetchSampleFunc func() ([]byte, error)
type FetchFullFunc func() ([]byte, error)

type Service struct {
	Config      Config
	FetchSample FetchSampleFunc
	FetchFull   FetchFullFunc
	status      map[string]any
}

func New(config Config) *Service {
	svc := &Service{Config: config}
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
		"last_full_file":              nil,
		"last_full_size":              nil,
		"last_overlap_bytes":          0,
		"last_delta_file":             nil,
		"last_delta_source":           nil,
		"last_delta_size":             0,
		"last_new_sample_lastmod":     nil,
		"last_previous_first_lastmod": nil,
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

func (s *Service) RunOnce() error {
	sample, err := s.FetchSample()
	if err != nil {
		s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		return err
	}

	currentHash := sha256Hex(sample)
	state, _ := s.loadState()
	previousHash, _ := state["sample_hash"].(string)
	previousFirstLastmod, _ := state["first_lastmod"].(string)
	currentFirstLastmod := s.ExtractFirstLastmod(sample)

	s.setStatus(map[string]any{
		"last_check_at":    utcNowISO(),
		"last_sample_hash": currentHash,
		"last_error":       nil,
	})

	if currentHash == previousHash {
		_ = s.saveState(currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))
		s.setStatus(map[string]any{"last_overlap_bytes": len(sample)})
		return nil
	}

	fullData, err := s.FetchFull()
	if err != nil {
		s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		return err
	}

	ext := s.inferFileExtension()
	previousFullData, _ := s.loadLatestFullData(ext)
	newSampleLastmod := s.ExtractLastLastmod(sample)
	previousSource := "sample"
	if previousFirstLastmod == "" && len(previousFullData) > 0 {
		previousFirstLastmod = s.ExtractFirstLastmod(previousFullData)
		previousSource = "full"
	}

	deltaData := fullData
	deltaSource := "full_no_previous_lastmod"
	overlapBytes := 0
	if previousFirstLastmod != "" {
		deltaData = s.DeltaNewerThanLastmod(fullData, previousFirstLastmod)
		overlapBytes = max(len(fullData)-len(deltaData), 0)
		deltaSource = "lastmod_value_" + previousSource
	}

	if currentFirstLastmod == "" {
		currentFirstLastmod = s.ExtractFirstLastmod(fullData)
	}
	_ = s.saveState(currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))

	var deltaFile any
	if len(deltaData) > 0 {
		saved, err := s.saveLatestDelta(deltaData)
		if err != nil {
			return err
		}
		deltaFile = saved
	}
	fullFile, err := s.saveFullData(fullData, ext)
	if err != nil {
		return err
	}

	s.setStatus(map[string]any{
		"last_change_at":              utcNowISO(),
		"last_full_file":              fullFile,
		"last_full_size":              len(fullData),
		"last_overlap_bytes":          overlapBytes,
		"last_delta_file":             deltaFile,
		"last_delta_source":           deltaSource,
		"last_delta_size":             len(deltaData),
		"last_new_sample_lastmod":     emptyToNil(newSampleLastmod),
		"last_previous_first_lastmod": emptyToNil(previousFirstLastmod),
	})
	return nil
}

func (s *Service) loadState() (map[string]any, error) {
	raw, err := os.ReadFile(s.Config.StateFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	if strings.TrimSpace(string(raw)) == "" {
		return map[string]any{}, nil
	}
	data := map[string]any{}
	if err := json.Unmarshal(raw, &data); err != nil {
		return map[string]any{}, nil
	}
	return data, nil
}

func (s *Service) saveState(sampleHash, firstLastmod string) error {
	payload, err := json.MarshalIndent(map[string]any{
		"sample_hash":   sampleHash,
		"first_lastmod": emptyToNil(firstLastmod),
		"updated_at":    utcNowISO(),
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.Config.StateFile), 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.Config.StateFile, payload, 0o644)
}

func (s *Service) inferFileExtension() string {
	parsed, err := url.Parse(s.Config.URL)
	if err != nil {
		return ".xml"
	}
	ext := strings.ToLower(filepath.Ext(parsed.Path))
	if ext == "" {
		return ".xml"
	}
	return ext
}

func (s *Service) saveLatestDelta(content []byte) (string, error) {
	if err := os.MkdirAll(s.Config.OutputDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(s.Config.OutputDir, "latest_delta_"+utcNowCompact()+".xml")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Service) saveFullData(content []byte, ext string) (string, error) {
	if err := os.MkdirAll(s.Config.OutputDir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(s.Config.OutputDir, "latest"+ext)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Service) loadLatestFullData(ext string) ([]byte, error) {
	path := filepath.Join(s.Config.OutputDir, "latest"+ext)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return raw, nil
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

func utcNowCompact() string {
	return time.Now().UTC().Format("20060102T150405Z")
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
