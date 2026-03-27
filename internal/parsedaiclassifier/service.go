package parsedaiclassifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"goapplyjob-golang-backend/internal/constants"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/normalize/techstacknorm"
	"goapplyjob-golang-backend/internal/sources/plugins"
	"log"
	"strings"
	"time"
)

const (
	workerStateLastClassifiedParsedJobIDKey = "last_classified_parsed_job_id"
	defaultBatchSizeParsedJobAIClassifier   = 25
)

type ClassifyFunc func(context.Context, string, string, string) (string, string, []string, error)

type Service struct {
	DB             *database.DB
	Classify       ClassifyFunc
	EnabledSources map[string]struct{}
	Config         Config
}

type Config struct {
	BatchSize           int
	PollSeconds         float64
	RunOnce             bool
	ErrorBackoffSeconds int
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func New(cfg Config, db *database.DB) *Service {
	return &Service{
		DB:     db,
		Config: cfg,
	}
}

func (s *Service) RunOnce(ctx context.Context) (int, error) {
	batchSize := s.Config.BatchSize
	if batchSize < 1 {
		batchSize = defaultBatchSizeParsedJobAIClassifier
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
			log.Printf("parsed-job-ai-classifier-worker cycle_failed error=%v", err)
			if s.Config.RunOnce {
				return err
			}
			time.Sleep(time.Duration(errorBackoffSeconds) * time.Second)
			continue
		}
		if s.Config.RunOnce {
			log.Printf("parsed-job-ai-classifier-worker run-once completed processed=%d", processed)
			return nil
		}
		time.Sleep(time.Duration(pollSeconds * float64(time.Second)))
	}
}

func (s *Service) classify(ctx context.Context, roleRequirements, roleTitle, roleDescription string) (string, string, []string, error) {
	if s.Classify != nil {
		return s.Classify(ctx, roleRequirements, roleTitle, roleDescription)
	}
	return s.SuggestCategoryWithTechStack(ctx, roleRequirements, roleTitle, roleDescription, nil, false)
}

func (s *Service) SuggestCategoryWithTechStack(ctx context.Context, roleRequirements, roleTitle, roleDescription string, techStack any, overrideTechStack bool) (string, string, []string, error) {
	normalizedTechStack := techstacknorm.Normalize(techStack)
	categorizedTitle := ""
	categorizedFunction := ""

	if len(normalizedTechStack) == 0 || overrideTechStack {
		allowedCategories, categoryFunctions, err := s.loadAllowedJobCategoriesAndFunctions(ctx)
		if err != nil {
			return "", "", nil, err
		}
		if shouldUseGroqClassification(roleTitle) {
			groqCategory, groqRequiredSkills, err := classifyJobTitleWithGroqSync(
				roleTitle,
				groqClassifierDescription(roleDescription, roleRequirements),
				allowedCategories,
			)
			if err != nil {
				return "", "", nil, err
			}
			if groqCategory != "" {
				categorizedTitle = groqCategory
				categorizedFunction = categoryFunctions[groqCategory]
				log.Printf(
					"parsed-job-ai-classifier-worker groq_inferred role_title=%q category=%q function=%q required_skills_len=%d",
					roleTitle,
					categorizedTitle,
					categorizedFunction,
					len(groqRequiredSkills),
				)
				normalizedTechStack = techstacknorm.Normalize(groqRequiredSkills)
			}
		} else {
			groqCategory, err := classifyJobCategoryWithGroqSync(roleTitle, allowedCategories)
			if err != nil {
				return "", "", nil, err
			}
			if groqCategory != "" {
				categorizedTitle = groqCategory
				categorizedFunction = categoryFunctions[groqCategory]
				log.Printf(
					"parsed-job-ai-classifier-worker groq_inferred role_title=%q category=%q function=%q",
					roleTitle,
					categorizedTitle,
					categorizedFunction,
				)
			}
		}
	}

	return categorizedTitle, categorizedFunction, normalizedTechStack, nil
}

func (s *Service) loadLastParsedJobID(ctx context.Context) (int64, error) {
	var rawState sql.NullString
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT COALESCE(state::text, '')
		   FROM worker_states
		  WHERE worker_name = ?
		  LIMIT 1`,
		constants.WorkerNameParsedAIClassifier,
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
	switch value := state[workerStateLastClassifiedParsedJobIDKey].(type) {
	case float64:
		return int64(value), nil
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	default:
		return 0, nil
	}
}

func (s *Service) saveLastParsedJobID(ctx context.Context, lastParsedJobID int64) error {
	stateJSON, err := json.Marshal(map[string]any{
		workerStateLastClassifiedParsedJobIDKey: lastParsedJobID,
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
		constants.WorkerNameParsedAIClassifier,
		string(stateJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func parseTechStackJSON(raw sql.NullString) []string {
	if !raw.Valid || raw.String == "" {
		return nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw.String), &values); err != nil {
		return nil
	}
	return values
}

func buildSourceInClause(sources map[string]struct{}) (string, []any) {
	values := make([]string, 0, len(sources))
	for source := range sources {
		values = append(values, source)
	}
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
		log.Printf("parsed-job-ai-classifier-worker batch_done rows=0 processed=0")
		return 0, nil
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
		`SELECT p.id,
		        r.source,
		        p.role_title,
		        p.role_description,
		        p.role_requirements,
		        p.tech_stack::text
		   FROM parsed_jobs p
		   JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		  WHERE p.id > ?
		    AND r.source IN (`+sourceInClause+`)
		  ORDER BY p.id ASC
		  LIMIT ?`,
		queryArgs...,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type parsedJobRow struct {
		id               int64
		source           string
		roleTitle        sql.NullString
		roleDescription  sql.NullString
		roleRequirements sql.NullString
		techStackJSON    sql.NullString
	}

	jobs := make([]parsedJobRow, 0, batchSize)
	for rows.Next() {
		var row parsedJobRow
		if err := rows.Scan(
			&row.id,
			&row.source,
			&row.roleTitle,
			&row.roleDescription,
			&row.roleRequirements,
			&row.techStackJSON,
		); err != nil {
			return 0, err
		}
		jobs = append(jobs, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		log.Printf("parsed-job-ai-classifier-worker batch_done rows=0 processed=0")
		return 0, nil
	}

	processed := 0
	for _, row := range jobs {
		if err := ctx.Err(); err != nil {
			return processed, err
		}

		plugin, ok := plugins.Get(row.source)
		shouldSkip := !ok ||
			!plugin.InferCategories ||
			len(parseTechStackJSON(row.techStackJSON)) > 0
		if shouldSkip {
			if err := s.saveLastParsedJobID(ctx, row.id); err != nil {
				return processed, err
			}
			processed++
			continue
		}

		log.Printf("parsed-job-ai-classifier-worker classify_start parsed_job_id=%d source=%s role_title=%q", row.id, row.source, nullStringValue(row.roleTitle))
		categorizedTitle, categorizedFunction, normalizedTechStack, err := s.classify(
			ctx,
			nullStringValue(row.roleRequirements),
			nullStringValue(row.roleTitle),
			nullStringValue(row.roleDescription),
		)
		if err != nil {
			log.Printf("parsed-job-ai-classifier-worker classify_failed parsed_job_id=%d source=%s error=%v", row.id, row.source, err)
			return processed, err
		}
		if categorizedTitle == "" {
			if err := s.saveLastParsedJobID(ctx, row.id); err != nil {
				return processed, err
			}
			log.Printf("parsed-job-ai-classifier-worker classify_skipped_empty parsed_job_id=%d source=%s", row.id, row.source)
			processed++
			continue
		}

		techStackJSON, err := json.Marshal(normalizedTechStack)
		if err != nil {
			return processed, err
		}
		_, err = s.DB.SQL.ExecContext(
			ctx,
			`UPDATE parsed_jobs
			    SET categorized_job_title = ?,
			        categorized_job_function = ?,
			        tech_stack = ?::jsonb,
			        updated_at = ?
			  WHERE id = ?`,
			categorizedTitle,
			nilIfEmpty(categorizedFunction),
			string(techStackJSON),
			time.Now().UTC().Format(time.RFC3339Nano),
			row.id,
		)
		if err != nil {
			return processed, err
		}
		if err := s.saveLastParsedJobID(ctx, row.id); err != nil {
			return processed, err
		}
		log.Printf(
			"parsed-job-ai-classifier-worker classify_done parsed_job_id=%d source=%s category=%q function=%q tech_stack_len=%d",
			row.id,
			row.source,
			categorizedTitle,
			categorizedFunction,
			len(normalizedTechStack),
		)
		processed++
	}

	log.Printf("parsed-job-ai-classifier-worker batch_done rows=%d processed=%d", len(jobs), processed)
	return processed, nil
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
