package raw

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

const statusNotFound = 404
const statusTooManyRequests = 429
const (
	sourceRemoteRocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
	builtinRemovedText     = "sorry, this job was removed"
)

type ReadHTMLFunc func(string) (string, int, error)
type ParseHTMLFunc func(string) (map[string]any, error)

type Service struct {
	DB        *database.DB
	ReadHTML  ReadHTMLFunc
	ParseHTML ParseHTMLFunc
	Status    StatusFunc
}

var scriptJSONBlockPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)

func New(db *database.DB) *Service {
	return &Service{
		DB: db,
		ReadHTML: func(string) (string, int, error) {
			return "", 0, errors.New("read html not configured")
		},
		ParseHTML: func(html string) (map[string]any, error) {
			blocks := scriptJSONBlockPattern.FindAllStringSubmatch(html, -1)
			if len(blocks) == 0 {
				return map[string]any{}, nil
			}
			lastBlock := strings.TrimSpace(blocks[len(blocks)-1][1])
			if lastBlock == "" {
				return map[string]any{}, nil
			}
			var data map[string]any
			if err := json.Unmarshal([]byte(lastBlock), &data); err != nil {
				return map[string]any{}, nil
			}
			props, _ := data["props"].(map[string]any)
			pageProps, _ := props["pageProps"].(map[string]any)
			jobData, _ := pageProps["jobOpening"].(map[string]any)
			if jobData == nil {
				return map[string]any{}, nil
			}
			return jobData, nil
		},
	}
}

func toTargetJobURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	pathParts := strings.FieldsFunc(parsed.Path, func(r rune) bool { return r == '/' })
	if len(pathParts) >= 2 && pathParts[1] == "company" {
		pathParts = pathParts[1:]
	}
	trailingSlash := ""
	if strings.HasSuffix(parsed.Path, "/") {
		trailingSlash = "/"
	}
	parsed.Path = "/"
	if len(pathParts) > 0 {
		parsed.Path = "/" + strings.Join(pathParts, "/") + trailingSlash
	}
	return parsed.String()
}

func toTargetJobURLForSource(source, rawURL string) string {
	if plugin, ok := plugins.Get(source); ok && plugin.ToTargetJobURL != nil {
		return plugin.ToTargetJobURL(rawURL)
	}
	return toTargetJobURL(rawURL)
}

func parseHTMLForSource(source, html, sourceURL string) map[string]any {
	if plugin, ok := plugins.Get(source); ok && plugin.ParseRawHTML != nil {
		return plugin.ParseRawHTML(html, sourceURL)
	}
	parser := New(nil).ParseHTML
	payload, _ := parser(html)
	if payload == nil {
		return map[string]any{}
	}
	return payload
}

func isRemovedBuiltinJobHTML(source, html string) bool {
	if strings.ToLower(strings.TrimSpace(source)) != sourceBuiltin {
		return false
	}
	return strings.Contains(strings.ToLower(html), builtinRemovedText)
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	var rows *sql.Rows
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		rows, err = s.DB.SQL.QueryContext(ctx, `SELECT id, url, COALESCE(source, '') FROM raw_us_jobs WHERE is_ready = false AND is_skippable = false ORDER BY post_date DESC, id DESC LIMIT ?`, batchSize)
		if err == nil {
			break
		}
		if !isTransientDBError(err) || attempt == 2 {
			return 0, err
		}
		time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
	}
	defer rows.Close()

	type rawJob struct {
		id     int64
		url    string
		source string
	}
	jobs := make([]rawJob, 0, batchSize)
	for rows.Next() {
		var job rawJob
		if err := rows.Scan(&job.id, &job.url, &job.source); err != nil {
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	log.Printf("raw-us-job-worker picked_unready_jobs=%d", len(jobs))

	processed := 0
	throttledSources := map[string]struct{}{}
	for _, job := range jobs {
		if _, throttled := throttledSources[strings.TrimSpace(strings.ToLower(job.source))]; throttled {
			log.Printf("raw-us-job-worker source_throttled_skip job_id=%d source=%s reason=prior_429_in_cycle", job.id, job.source)
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET retry_count = retry_count + 1 WHERE id = ?`, job.id)
				return err
			}); err != nil {
				return processed, err
			}
			processed++
			continue
		}
		targetURL := toTargetJobURLForSource(job.source, job.url)
		log.Printf("raw-us-job-worker fetch_start job_id=%d source=%s target_url=%s", job.id, job.source, targetURL)
		html, statusCode, err := s.ReadHTML(targetURL)
		if err != nil {
			return processed, err
		}
		switch {
		case statusCode == statusNotFound:
			log.Printf("raw-us-job-worker fetch_result job_id=%d status=404", job.id)
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_skippable = true, retry_count = retry_count + 1 WHERE id = ?`, job.id)
				return err
			}); err != nil {
				return processed, err
			}
		case statusCode == statusTooManyRequests:
			log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s status=429 retry_later", job.id, job.source)
			throttledSources[strings.TrimSpace(strings.ToLower(job.source))] = struct{}{}
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET retry_count = retry_count + 1 WHERE id = ?`, job.id)
				return err
			}); err != nil {
				return processed, err
			}
		case isRemovedBuiltinJobHTML(job.source, html):
			log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s detected_builtin_removed_job", job.id, job.source)
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = true, is_skippable = true, raw_json = NULL, retry_count = retry_count + 1 WHERE id = ?`, job.id)
				return err
			}); err != nil {
				return processed, err
			}
		case strings.TrimSpace(html) == "":
			log.Printf("raw-us-job-worker fetch_result job_id=%d status=%d empty_html_or_error", job.id, statusCode)
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET retry_count = retry_count + 1 WHERE id = ?`, job.id)
				return err
			}); err != nil {
				return processed, err
			}
		default:
			log.Printf("raw-us-job-worker parse_start job_id=%d source=%s", job.id, job.source)
			payload, err := s.ParseHTML(html)
			if err != nil {
				return processed, err
			}
			if source := strings.TrimSpace(job.source); source != "" && source != sourceRemoteRocketship {
				payload = parseHTMLForSource(source, html, job.url)
			}
			if payload == nil {
				payload = map[string]any{}
			}
			if skipRetry, _ := payload["_skip_for_retry"].(bool); skipRetry {
				log.Printf("raw-us-job-worker parse_retry_later job_id=%d source=%s reason=%v", job.id, job.source, payload["_skip_reason"])
				if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
					_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET retry_count = retry_count + 1 WHERE id = ?`, job.id)
					return err
				}); err != nil {
					return processed, err
				}
				processed++
				continue
			}
			if _, ok := payload["url"]; !ok {
				payload["url"] = job.url
			}
			log.Printf("raw-us-job-worker parse_done job_id=%d parsed_keys=%d", job.id, len(payload))
			rawJSON, err := json.Marshal(payload)
			if err != nil {
				return processed, err
			}
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = true, raw_json = ? WHERE id = ?`, string(rawJSON), job.id)
				return err
			}); err != nil {
				return processed, err
			}
		}
		processed++
	}
	return processed, nil
}

var _ = sql.ErrNoRows

func isTransientDBError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"database is locked",
		"database table is locked",
		"connection is closed",
		"connection already closed",
		"server closed the connection unexpectedly",
		"terminating connection",
		"connection not open",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
