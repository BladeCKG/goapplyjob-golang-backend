package raw

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

const (
	statusNotFound        = 404
	statusGone            = 410
	statusTooManyRequests = 429
)

const (
	sourceRemoteRocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
	sourceDailyRemote      = "dailyremote"
	builtinRemovedText     = "sorry, this job was removed"
	dailyRemoteGoneText    = "job no longer available"
)

type (
	ReadHTMLFunc func(string) (string, int, error)
)

type Service struct {
	DB             *database.DB
	ReadHTML       ReadHTMLFunc
	Status         StatusFunc
	EnabledSources map[string]struct{}
	Config         Config
}

type Config struct {
	BatchSize             int
	PollSeconds           int
	RunOnce               bool
	ErrorBackoffSeconds   int
	RetentionDays         int
	RetentionCleanupBatch int
}

var scriptJSONBlockPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)

func New(cfg Config, db *database.DB) *Service {
	return &Service{
		DB:     db,
		Config: cfg,
		ReadHTML: func(string) (string, int, error) {
			return "", 0, errors.New("read html not configured")
		},
	}
}

func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batchSize := s.Config.BatchSize
	if batchSize < 1 {
		batchSize = 100
	}
	retentionDays := s.Config.RetentionDays
	if retentionDays < 1 {
		retentionDays = 365
	}
	retentionCleanupBatch := s.Config.RetentionCleanupBatch
	if retentionCleanupBatch < 1 {
		retentionCleanupBatch = 1
	}
	deletedRaw, deletedParsed, cleanupErr := s.CleanupOldRawJobs(ctx, retentionDays, retentionCleanupBatch)
	if cleanupErr != nil {
		return 0, cleanupErr
	}
	if deletedRaw > 0 || deletedParsed > 0 {
		log.Printf("raw-us-job-worker cleanup_done raw_jobs=%d parsed_jobs=%d", deletedRaw, deletedParsed)
	}
	return s.ProcessPending(ctx, batchSize)
}

func (s *Service) RunForever() error {
	pollSeconds := s.Config.PollSeconds
	if pollSeconds < 1 {
		pollSeconds = 5
	}
	errorBackoffSeconds := s.Config.ErrorBackoffSeconds
	if errorBackoffSeconds < 1 {
		errorBackoffSeconds = 1
	}
	for {
		processed, err := s.RunOnce(context.Background())
		if err != nil {
			log.Printf("raw-us-job-worker cycle_failed error=%v", err)
			if s.Config.RunOnce {
				return err
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
			continue
		}
		if s.Config.RunOnce {
			log.Printf("raw-us-job-worker run-once completed processed=%d", processed)
			return nil
		}
		time.Sleep(time.Duration(pollSeconds) * time.Second)
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

	return map[string]any{}
}

func isRemovedBuiltinJobHTML(source, html string) bool {
	if source != sourceBuiltin {
		return false
	}
	return strings.Contains(strings.ToLower(html), builtinRemovedText)
}

func isDailyRemoteGoneJobHTML(source, html string) bool {
	if source != sourceDailyRemote {
		return false
	}
	return strings.Contains(strings.ToLower(html), dailyRemoteGoneText)
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	if len(s.EnabledSources) == 0 {
		log.Printf("raw-us-job-worker picked_unready_jobs=0")
		return 0, nil
	}
	var rows *sql.Rows
	var err error
	query := `SELECT id, url, COALESCE(source, ''), post_date, COALESCE(extra_json, '')
	            FROM raw_us_jobs
	           WHERE is_ready = false
	             AND is_skippable = false
	             AND source = ANY(?::text[])`
	names := make([]string, 0, len(s.EnabledSources))
	for name := range s.EnabledSources {
		names = append(names, name)
	}
	sort.Strings(names)
	query += ` ORDER BY post_date DESC, id DESC LIMIT ?`
	for attempt := 0; attempt < 3; attempt++ {
		rows, err = s.DB.SQL.QueryContext(ctx, query, names, batchSize)
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
		id        int64
		url       string
		source    string
		postDate  time.Time
		extraJSON string
	}
	jobs := make([]rawJob, 0, batchSize)
	for rows.Next() {
		var job rawJob
		if err := rows.Scan(&job.id, &job.url, &job.source, &job.postDate, &job.extraJSON); err != nil {
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
		clearParsed := func(jobID int64) error {
			return database.RetryLocked(8, 50*time.Millisecond, func() error {
				var parsedJobID int64
				err := s.DB.SQL.QueryRowContext(ctx, `SELECT id FROM parsed_jobs WHERE raw_us_job_id = ? LIMIT 1`, jobID).Scan(&parsedJobID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil {
					if _, execErr := s.DB.SQL.ExecContext(ctx, `DELETE FROM user_job_actions WHERE parsed_job_id = ?`, parsedJobID); execErr != nil {
						return execErr
					}
					if _, execErr := s.DB.SQL.ExecContext(ctx, `DELETE FROM parsed_jobs WHERE id = ?`, parsedJobID); execErr != nil {
						return execErr
					}
				}
				return nil
			})
		}
		setRetry := func(jobID int64) error {
			if err := clearParsed(jobID); err != nil {
				return err
			}
			return database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(
					ctx,
					`UPDATE raw_us_jobs
					 SET is_ready = false,
					     is_skippable = false,
					     is_parsed = false,
					     raw_json = NULL,
					     retry_count = retry_count + 1
					 WHERE id = ?`,
					jobID,
				)
				return err
			})
		}
		if _, throttled := throttledSources[job.source]; throttled {
			log.Printf("raw-us-job-worker source_throttled_skip job_id=%d source=%s reason=prior_429_in_cycle", job.id, job.source)
			if err := setRetry(job.id); err != nil {
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

		setSkippable := func(job_id int64) error {
			if err := clearParsed(job_id); err != nil {
				return err
			}
			return database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(
					ctx,
					`UPDATE raw_us_jobs
					 SET is_ready = true,
					     is_skippable = true,
					     is_parsed = false,
					     raw_json = NULL,
					     extra_json = NULL
					 WHERE id = ?`,
					job_id,
				)
				return err
			})
		}
		switch {
		case isRemovedBuiltinJobHTML(job.source, html):
			log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s detected_builtin_removed_job", job.id, job.source)
			if err := setSkippable(job.id); err != nil {
				return processed, err
			}
		case statusCode == statusGone && isDailyRemoteGoneJobHTML(job.source, html):
			log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s status=410 detected_no_longer_available", job.id, job.source)
			if err := setSkippable(job.id); err != nil {
				return processed, err
			}
		case statusCode == statusNotFound:
			log.Printf("raw-us-job-worker fetch_result job_id=%d status=404", job.id)
			if err := setSkippable(job.id); err != nil {
				return processed, err
			}
		case statusCode == statusTooManyRequests:
			log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s status=429 retry_later", job.id, job.source)
			throttledSources[job.source] = struct{}{}
			if err := setRetry(job.id); err != nil {
				return processed, err
			}
		case statusCode < 200 || statusCode >= 300:
			log.Printf("raw-us-job-worker fetch_result job_id=%d status=%d empty_html_or_error", job.id, statusCode)
			if err := setRetry(job.id); err != nil {
				return processed, err
			}
		case strings.TrimSpace(html) == "":
			log.Printf("raw-us-job-worker fetch_result job_id=%d status=%d empty_html_or_error", job.id, statusCode)
			if err := setRetry(job.id); err != nil {
				return processed, err
			}
		default:
			log.Printf("raw-us-job-worker parse_start job_id=%d source=%s", job.id, job.source)
			payload := parseHTMLForSource(job.source, html, job.url)
			if skipRetry, _ := payload["_skip_for_retry"].(bool); skipRetry {
				log.Printf("raw-us-job-worker parse_retry_later job_id=%d source=%s reason=%v", job.id, job.source, payload["_skip_reason"])
				if err := setRetry(job.id); err != nil {
					return processed, err
				}
				processed++
				continue
			}
			if skipNonUS, _ := payload["_skip_for_non_us"].(bool); skipNonUS {
				log.Printf("raw-us-job-worker parse_skipped_non_us job_id=%d source=%s", job.id, job.source)
				if err := setSkippable(job.id); err != nil {
					return processed, err
				}
				processed++
				continue
			}

			extraPayload := parseExtraJSON(job.extraJSON)
			if len(extraPayload) > 0 {
				for key, value := range extraPayload {
					if _, exists := payload[key]; !exists {
						payload[key] = value
					}
				}
			}
			log.Printf("raw-us-job-worker parse_done job_id=%d parsed_keys=%d", job.id, len(payload))
			rawJSON, err := json.Marshal(payload)
			if err != nil {
				return processed, err
			}
			if err := database.RetryLocked(8, 50*time.Millisecond, func() error {
				_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = true, raw_json = ?, extra_json = NULL WHERE id = ?`, string(rawJSON), job.id)
				return err
			}); err != nil {
				return processed, err
			}
		}
		processed++
	}
	return processed, nil
}

func (s *Service) CleanupOldRawJobs(ctx context.Context, retentionDays, cleanupBatchSize int) (int64, int64, error) {
	if retentionDays <= 0 {
		return 0, 0, nil
	}
	if cleanupBatchSize <= 0 {
		cleanupBatchSize = 5000
	}
	cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	var deletedRaw int64
	var deletedParsed int64
	err := database.RetryLocked(8, 50*time.Millisecond, func() error {
		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT id FROM raw_us_jobs WHERE post_date < ? ORDER BY post_date ASC, id ASC LIMIT ?`, cutoff.Format(time.RFC3339Nano), cleanupBatchSize)
		if err != nil {
			return err
		}
		rawIDs := make([]int64, 0, cleanupBatchSize)
		for rows.Next() {
			var id int64
			if scanErr := rows.Scan(&id); scanErr != nil {
				_ = rows.Close()
				return scanErr
			}
			rawIDs = append(rawIDs, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()
		if len(rawIDs) == 0 {
			return tx.Commit()
		}
		parsedResult, err := tx.ExecContext(ctx, `DELETE FROM parsed_jobs WHERE raw_us_job_id = ANY(?::bigint[])`, rawIDs)
		if err != nil {
			return err
		}
		rawResult, err := tx.ExecContext(ctx, `DELETE FROM raw_us_jobs WHERE id = ANY(?::bigint[])`, rawIDs)
		if err != nil {
			return err
		}
		if affected, err := parsedResult.RowsAffected(); err == nil {
			deletedParsed = affected
		}
		if affected, err := rawResult.RowsAffected(); err == nil {
			deletedRaw = affected
		}
		return tx.Commit()
	})
	if err != nil {
		return 0, 0, err
	}
	return deletedRaw, deletedParsed, nil
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

func parseExtraJSON(value string) map[string]any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil || payload == nil {
		return map[string]any{}
	}
	return payload
}
