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
	"sync"
	"sync/atomic"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
)

const (
	statusNotFound        = 404
	statusGone            = 410
	statusTooManyRequests = 429
	defaultFetchTimeout   = 45 * time.Second
)

const (
	sourceRemoteRocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
	sourceDailyRemote      = "dailyremote"
	builtinRemovedText     = "sorry, this job was removed"
	dailyRemoteGoneText    = "job no longer available"
)

type (
	ReadHTMLFunc          func(context.Context, string) (string, int, error)
	ReadHTMLForSourceFunc func(context.Context, string, string) (string, int, error)
)

type Service struct {
	DB                *database.DB
	ReadHTML          ReadHTMLFunc
	ReadHTMLForSource ReadHTMLForSourceFunc
	Status            StatusFunc
	EnabledSources    map[string]struct{}
	Config            Config
}

type Config struct {
	BatchSize             int
	PollSeconds           int
	RunOnce               bool
	ErrorBackoffSeconds   int
	FetchTimeoutSeconds   int
	RetentionDays         int
	RetentionCleanupBatch int
	WorkerCount           int
}

var scriptJSONBlockPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)

func New(cfg Config, db *database.DB) *Service {
	return &Service{
		DB:     db,
		Config: cfg,
		ReadHTML: func(context.Context, string) (string, int, error) {
			return "", 0, errors.New("read html not configured")
		},
		ReadHTMLForSource: func(context.Context, string, string) (string, int, error) {
			return "", 0, errors.New("read html not configured")
		},
	}
}

func (s *Service) readHTMLForSource(ctx context.Context, source, targetURL string) (string, int, error) {
	if s.ReadHTMLForSource != nil {
		return s.ReadHTMLForSource(ctx, source, targetURL)
	}
	return s.ReadHTML(ctx, targetURL)
}

func (s *Service) fetchTimeout() time.Duration {
	if s.Config.FetchTimeoutSeconds > 0 {
		return time.Duration(s.Config.FetchTimeoutSeconds) * time.Second
	}
	return defaultFetchTimeout
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
	baseQuery := `SELECT id, url, COALESCE(source, ''), post_date, COALESCE(extra_json, '')
	            FROM raw_us_jobs
	           WHERE is_ready = false
	             AND is_skippable = false
	             AND source = ANY(?::text[])`
	names := make([]string, 0, len(s.EnabledSources))
	for name := range s.EnabledSources {
		names = append(names, name)
	}
	sort.Strings(names)
	zeroRetryQuery := baseQuery + ` AND retry_count = 0 ORDER BY post_date DESC, id DESC LIMIT ?`
	minRetryQuery := baseQuery + ` ORDER BY retry_count ASC, post_date DESC, id DESC LIMIT ?`

	queryWithRetry := func(query string, args ...any) (*sql.Rows, error) {
		var qErr error
		for attempt := 0; attempt < 3; attempt++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rows, qErr = s.DB.SQL.QueryContext(ctx, query, args...)
			if qErr == nil {
				return rows, nil
			}
			if !isTransientDBError(qErr) || attempt == 2 {
				return nil, qErr
			}
			delay := time.Duration(50*(1<<attempt)) * time.Millisecond
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		return nil, qErr
	}

	rows, err = queryWithRetry(zeroRetryQuery, names, batchSize)
	if err != nil {
		return 0, err
	}

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
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	if len(jobs) == 0 {
		rows, err = queryWithRetry(minRetryQuery, names, batchSize)
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			var job rawJob
			if err := rows.Scan(&job.id, &job.url, &job.source, &job.postDate, &job.extraJSON); err != nil {
				rows.Close()
				return 0, err
			}
			jobs = append(jobs, job)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, err
		}
		rows.Close()
	}

	log.Printf("raw-us-job-worker picked_unready_jobs=%d", len(jobs))

	workerCount := s.Config.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}
	if workerCount == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var processed int64
	var firstErr atomic.Value
	var errOnce sync.Once
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr.Store(err)
			cancel()
		})
	}

	throttledSources := map[string]struct{}{}
	var throttledMu sync.Mutex

	jobCh := make(chan rawJob)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if err := ctx.Err(); err != nil {
					return
				}
				clearParsed := func(jobID int64) error {
					return database.RetryLockedWithContext(ctx, 8, 50*time.Millisecond, func() error {
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
					return database.RetryLockedWithContext(ctx, 8, 50*time.Millisecond, func() error {
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
				throttledMu.Lock()
				_, throttled := throttledSources[job.source]
				throttledMu.Unlock()
				if throttled {
					log.Printf("raw-us-job-worker source_throttled_skip job_id=%d source=%s reason=prior_429_in_cycle", job.id, job.source)
					if err := setRetry(job.id); err != nil {
						reportErr(err)
						return
					}
					atomic.AddInt64(&processed, 1)
					continue
				}
				targetURL := toTargetJobURLForSource(job.source, job.url)
				log.Printf("raw-us-job-worker fetch_start job_id=%d source=%s target_url=%s", job.id, job.source, targetURL)
				fetchCtx, cancelFetch := context.WithTimeout(ctx, s.fetchTimeout())
				html, statusCode, err := s.readHTMLForSource(fetchCtx, job.source, targetURL)
				cancelFetch()
				if err != nil {
					if ctx.Err() != nil {
						reportErr(ctx.Err())
						return
					}
					log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s retry_later error=%v", job.id, job.source, err)
					if err := setRetry(job.id); err != nil {
						reportErr(err)
						return
					}
					atomic.AddInt64(&processed, 1)
					continue
				}

				setSkippable := func(job_id int64) error {
					if err := clearParsed(job_id); err != nil {
						return err
					}
					return database.RetryLockedWithContext(ctx, 8, 50*time.Millisecond, func() error {
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
						reportErr(err)
						return
					}
				case statusCode == statusGone && isDailyRemoteGoneJobHTML(job.source, html):
					log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s status=410 detected_no_longer_available", job.id, job.source)
					if err := setSkippable(job.id); err != nil {
						reportErr(err)
						return
					}
				case statusCode == statusNotFound:
					log.Printf("raw-us-job-worker fetch_result job_id=%d status=404", job.id)
					if err := setSkippable(job.id); err != nil {
						reportErr(err)
						return
					}
				case statusCode == statusTooManyRequests:
					log.Printf("raw-us-job-worker fetch_result job_id=%d source=%s status=429 retry_later", job.id, job.source)
					throttledMu.Lock()
					throttledSources[job.source] = struct{}{}
					throttledMu.Unlock()
					if err := setRetry(job.id); err != nil {
						reportErr(err)
						return
					}
				case statusCode < 200 || statusCode >= 300:
					log.Printf("raw-us-job-worker fetch_result job_id=%d status=%d empty_html_or_error", job.id, statusCode)
					if err := setRetry(job.id); err != nil {
						reportErr(err)
						return
					}
				case strings.TrimSpace(html) == "":
					log.Printf("raw-us-job-worker fetch_result job_id=%d status=%d empty_html_or_error", job.id, statusCode)
					if err := setRetry(job.id); err != nil {
						reportErr(err)
						return
					}
				default:
					log.Printf("raw-us-job-worker parse_start job_id=%d source=%s", job.id, job.source)
					payload := parseHTMLForSource(job.source, html, job.url)
					if skipRetry, _ := payload["_skip_for_retry"].(bool); skipRetry {
						log.Printf("raw-us-job-worker parse_retry_later job_id=%d source=%s reason=%v", job.id, job.source, payload["_skip_reason"])
						if err := setRetry(job.id); err != nil {
							reportErr(err)
							return
						}
						atomic.AddInt64(&processed, 1)
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
						reportErr(err)
						return
					}
					if err := database.RetryLockedWithContext(ctx, 8, 50*time.Millisecond, func() error {
						_, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = true, raw_json = ?, extra_json = NULL WHERE id = ?`, string(rawJSON), job.id)
						return err
					}); err != nil {
						reportErr(err)
						return
					}
				}
				atomic.AddInt64(&processed, 1)
			}
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return int(atomic.LoadInt64(&processed)), err.(error)
	}
	return int(atomic.LoadInt64(&processed)), nil
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
	err := database.RetryLockedWithContext(ctx, 8, 50*time.Millisecond, func() error {
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
