package raw

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"goapplyjob-golang-backend/internal/database"
	rr "goapplyjob-golang-backend/internal/sources/remoterocketship"
)

const statusNotFound = 404

type ReadHTMLFunc func(string) (string, int, error)
type ParseHTMLFunc func(string) (map[string]any, error)

type Service struct {
	DB        *database.DB
	ReadHTML  ReadHTMLFunc
	ParseHTML ParseHTMLFunc
	Status    StatusFunc
}

func New(db *database.DB) *Service {
	return &Service{
		DB: db,
		ReadHTML: func(string) (string, int, error) {
			return "", 0, errors.New("read html not configured")
		},
		ParseHTML: func(html string) (map[string]any, error) {
			return rr.ParseHTML(html), nil
		},
	}
}

func toTargetJobURL(rawURL string) string {
	return rr.ToTargetJobURL(rawURL)
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rows, err := s.DB.SQL.QueryContext(ctx, `SELECT id, url FROM raw_us_jobs WHERE is_ready = 0 AND is_skippable = 0 ORDER BY id DESC LIMIT ?`, batchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type rawJob struct {
		id  int64
		url string
	}
	jobs := make([]rawJob, 0, batchSize)
	for rows.Next() {
		var job rawJob
		if err := rows.Scan(&job.id, &job.url); err != nil {
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	processed := 0
	for _, job := range jobs {
		targetURL := toTargetJobURL(job.url)
		html, statusCode, err := s.ReadHTML(targetURL)
		if err != nil {
			return processed, err
		}
		switch {
		case statusCode == statusNotFound:
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_skippable = 1, retry_count = retry_count + 1 WHERE id = ?`, job.id); err != nil {
				return processed, err
			}
		case strings.TrimSpace(html) == "":
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET retry_count = retry_count + 1 WHERE id = ?`, job.id); err != nil {
				return processed, err
			}
		default:
			payload, err := s.ParseHTML(html)
			if err != nil {
				return processed, err
			}
			if payload == nil {
				payload = map[string]any{}
			}
			if _, ok := payload["url"]; !ok {
				payload["url"] = job.url
			}
			rawJSON, err := json.Marshal(payload)
			if err != nil {
				return processed, err
			}
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = 1, raw_json = ? WHERE id = ?`, string(rawJSON), job.id); err != nil {
				return processed, err
			}
		}
		processed++
	}
	return processed, nil
}

var _ = sql.ErrNoRows
