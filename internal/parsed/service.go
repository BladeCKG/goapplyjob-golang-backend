package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

type Service struct {
	DB *database.DB
}

func New(db *database.DB) *Service { return &Service{DB: db} }

func parseDT(value any) *time.Time {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return &parsed
	}
	return nil
}

func normalizeDT(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func isSourceOlderThanPostDate(sourceCreatedAt, postDate *time.Time) bool {
	source := normalizeDT(sourceCreatedAt)
	post := normalizeDT(postDate)
	if source == nil || post == nil {
		return false
	}
	return source.Before(*post)
}

func parseDBDatetime(value string) (*time.Time, error) {
	if value == "" {
		return nil, errors.New("empty")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return &parsed, nil
	}
	return nil, errors.New("invalid datetime")
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rows, err := s.DB.SQL.QueryContext(ctx, `SELECT id, post_date, raw_json FROM raw_us_jobs WHERE is_ready = 1 AND is_parsed = 0 ORDER BY id ASC LIMIT ?`, batchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type rawRow struct {
		id       int64
		postDate string
		rawJSON  sql.NullString
	}
	pending := make([]rawRow, 0, batchSize)
	for rows.Next() {
		var row rawRow
		if err := rows.Scan(&row.id, &row.postDate, &row.rawJSON); err != nil {
			return 0, err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	processed := 0
	for _, row := range pending {
		postDate, err := parseDBDatetime(row.postDate)
		if err != nil {
			continue
		}
		payload := map[string]any{}
		if !row.rawJSON.Valid || row.rawJSON.String == "" {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			continue
		}
		if err := json.Unmarshal([]byte(row.rawJSON.String), &payload); err != nil {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			continue
		}
		sourceCreatedAt := parseDT(payload["created_at"])
		if isSourceOlderThanPostDate(sourceCreatedAt, postDate) {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = 0, raw_json = NULL, is_parsed = 0 WHERE id = ?`, row.id); err != nil {
				return processed, err
			}
			processed++
			continue
		}

		if _, err := s.DB.SQL.ExecContext(
			ctx,
			`INSERT INTO parsed_jobs (raw_us_job_id, created_at_source, url, categorized_job_title, role_title, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(raw_us_job_id) DO UPDATE SET
			   created_at_source = excluded.created_at_source,
			   url = excluded.url,
			   categorized_job_title = excluded.categorized_job_title,
			   role_title = excluded.role_title,
			   updated_at = excluded.updated_at`,
			row.id,
			formatNullableTime(sourceCreatedAt),
			stringFromPayload(payload["url"]),
			stringFromPayload(payload["categorizedJobTitle"]),
			stringFromPayload(payload["roleTitle"]),
			time.Now().UTC().Format(time.RFC3339Nano),
		); err != nil {
			return processed, err
		}
		if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_parsed = 1 WHERE id = ?`, row.id); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func formatNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func stringFromPayload(value any) any {
	text, ok := value.(string)
	if !ok || text == "" {
		return nil
	}
	return text
}
