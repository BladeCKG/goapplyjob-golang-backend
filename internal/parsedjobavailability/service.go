package parsedjobavailability

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/constants"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	workerStateLastCheckedParsedJobIDKey = "last_checked_parsed_job_id"
	defaultBatchSize                     = 200
	defaultWorkerCount                   = 4
	defaultFetchTimeout                  = 30 * time.Second
	WorkerLogPrefix                      = "job-availability-worker"
)

type ReadHTMLForSourceFunc func(context.Context, string, string) (string, int, error)

type Config struct {
	BatchSize           int
	PollSeconds         float64
	RunOnce             bool
	ErrorBackoffSeconds int
	WorkerCount         int
	FetchTimeoutSeconds int
}

type Service struct {
	DB                *database.DB
	ReadHTMLForSource ReadHTMLForSourceFunc
	EnabledSources    map[string]struct{}
	Config            Config
}

type parsedJobRow struct {
	id     int64
	source string
	url    string
}

type processResult struct {
	id     int64
	closed bool
	err    error
}

func New(cfg Config, db *database.DB) *Service {
	return &Service{
		DB:     db,
		Config: cfg,
	}
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
			log.Printf(WorkerLogPrefix+" cycle_failed error=%v", err)
			if s.Config.RunOnce {
				return err
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
			continue
		}
		if s.Config.RunOnce {
			log.Printf(WorkerLogPrefix+" run-once completed processed=%d", processed)
			return nil
		}
		time.Sleep(time.Duration(pollSeconds * float64(time.Second)))
	}
}

func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batchSize := s.Config.BatchSize
	if batchSize < 1 {
		batchSize = defaultBatchSize
	}
	return s.ProcessPending(ctx, batchSize)
}

func (s *Service) fetchTimeout() time.Duration {
	if s.Config.FetchTimeoutSeconds < 1 {
		return defaultFetchTimeout
	}
	return time.Duration(s.Config.FetchTimeoutSeconds) * time.Second
}

func (s *Service) loadLastParsedJobID(ctx context.Context) (int64, error) {
	var rawState sql.NullString
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT COALESCE(state::text, '')
		   FROM worker_states
		  WHERE worker_name = ?
		  LIMIT 1`,
		constants.WorkerNameParsedAvailability,
	).Scan(&rawState)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !rawState.Valid || rawState.String == "" {
		return 0, nil
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(rawState.String), &state); err != nil {
		return 0, err
	}
	value, ok := state[workerStateLastCheckedParsedJobIDKey].(float64)
	if !ok {
		return 0, nil
	}
	return int64(value), nil
}

func (s *Service) saveLastParsedJobID(ctx context.Context, lastParsedJobID int64) error {
	stateJSON, err := json.Marshal(map[string]any{
		workerStateLastCheckedParsedJobIDKey: lastParsedJobID,
	})
	if err != nil {
		return err
	}
	_, err = s.DB.SQL.ExecContext(
		ctx,
		`INSERT INTO worker_states (worker_name, state, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(worker_name) DO UPDATE SET
		   state = excluded.state,
		   updated_at = excluded.updated_at`,
		constants.WorkerNameParsedAvailability,
		string(stateJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func buildSourceInClause(sources map[string]struct{}) (string, []any) {
	values := make([]string, 0, len(sources))
	for source := range sources {
		values = append(values, source)
	}
	sort.Strings(values)
	if len(values) == 0 {
		return "", nil
	}
	placeholders := make([]string, 0, len(values))
	args := make([]any, 0, len(values))
	for _, value := range values {
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}
	return strings.Join(placeholders, ", "), args
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if len(s.EnabledSources) == 0 {
		log.Printf(WorkerLogPrefix + " batch_done rows=0 processed=0")
		return 0, nil
	}
	if s.ReadHTMLForSource == nil {
		return 0, errors.New("read html for source not configured")
	}
	lastParsedJobID, err := s.loadLastParsedJobID(ctx)
	if err != nil {
		return 0, err
	}
	sourceInClause, sourceArgs := buildSourceInClause(s.EnabledSources)
	queryArgs := make([]any, 0, len(sourceArgs)+2)
	queryArgs = append(queryArgs, lastParsedJobID)
	queryArgs = append(queryArgs, sourceArgs...)
	queryArgs = append(queryArgs, batchSize)
	rows, err := s.DB.SQL.QueryContext(
		ctx,
		`SELECT p.id, r.source, r.url
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.id > ?
		    AND p.date_deleted IS NULL
		    AND r.source IN (`+sourceInClause+`)
		  ORDER BY p.id ASC
		  LIMIT ?`,
		queryArgs...,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	jobs := make([]parsedJobRow, 0, batchSize)
	for rows.Next() {
		var row parsedJobRow
		if err := rows.Scan(&row.id, &row.source, &row.url); err != nil {
			return 0, err
		}
		jobs = append(jobs, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		if lastParsedJobID > 0 {
			if err := s.saveLastParsedJobID(ctx, 0); err != nil {
				return 0, err
			}
		}
		log.Printf(WorkerLogPrefix + " batch_done rows=0 processed=0")
		return 0, nil
	}

	workerCount := s.Config.WorkerCount
	if workerCount < 1 {
		workerCount = defaultWorkerCount
	}
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}

	results := make([]processResult, len(jobs))
	workCh := make(chan int, len(jobs))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case idx, ok := <-workCh:
					if !ok {
						return
					}
					results[idx] = s.processRow(ctx, jobs[idx])
				}
			}
		}()
	}
	for idx := range jobs {
		select {
		case <-ctx.Done():
			close(workCh)
			wg.Wait()
			return 0, ctx.Err()
		case workCh <- idx:
		}
	}
	close(workCh)
	wg.Wait()

	processed := 0
	for _, res := range results {
		if res.err != nil {
			return processed, res.err
		}
		if err := s.saveLastParsedJobID(ctx, res.id); err != nil {
			return processed, err
		}
		processed++
	}
	log.Printf(WorkerLogPrefix+" batch_done rows=%d processed=%d", len(jobs), processed)
	return processed, nil
}

func (s *Service) processRow(ctx context.Context, row parsedJobRow) processResult {
	if err := ctx.Err(); err != nil {
		return processResult{id: row.id, err: err}
	}
	plugin, ok := plugins.Get(row.source)
	if !ok || plugin.IsJobClosed == nil || row.url == "" {
		return processResult{id: row.id}
	}
	log.Printf(WorkerLogPrefix+" check_start parsed_job_id=%d source=%s url=%q", row.id, row.source, row.url)
	// Bound each availability fetch independently so one stalled site cannot pin
	// all workers until the broader workerchain step timeout fires.
	fetchCtx, cancel := context.WithTimeout(ctx, s.fetchTimeout())
	defer cancel()
	bodyText, statusCode, err := s.ReadHTMLForSource(fetchCtx, row.source, row.url)
	if err != nil {
		log.Printf(WorkerLogPrefix+" check_failed parsed_job_id=%d source=%s error=%v", row.id, row.source, err)
		return processResult{id: row.id, err: err}
	}
	closed := plugin.IsJobClosed(statusCode, bodyText, row.url)
	if !closed {
		log.Printf(WorkerLogPrefix+" check_open parsed_job_id=%d source=%s status=%d", row.id, row.source, statusCode)
		return processResult{id: row.id}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.DB.SQL.ExecContext(
		ctx,
		`UPDATE parsed_jobs
		    SET date_deleted = COALESCE(date_deleted, ?),
		        updated_at = ?
		  WHERE id = ?`,
		now,
		now,
		row.id,
	)
	if err != nil {
		return processResult{id: row.id, err: err}
	}
	log.Printf(WorkerLogPrefix+" check_closed parsed_job_id=%d source=%s status=%d", row.id, row.source, statusCode)
	return processResult{id: row.id, closed: true}
}
