package raw

import (
	"context"
	"net/http"
)

type StatusFunc func(string) (int, error)

func defaultStatusFunc(url string) (int, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed {
		getResp, err := http.Get(url)
		if err != nil {
			return 0, err
		}
		defer getResp.Body.Close()
		return getResp.StatusCode, nil
	}
	return resp.StatusCode, nil
}

func (s *Service) RecheckSkippable(ctx context.Context, batchSize int) (int, int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	statusFn := s.statusFunc()
	checkedTotal := 0
	clearedTotal := 0
	var lastID int64
	for {
		rows, err := s.DB.SQL.QueryContext(ctx, `SELECT id, url FROM raw_us_jobs WHERE is_skippable = 1 AND id > ? ORDER BY id ASC LIMIT ?`, lastID, batchSize)
		if err != nil {
			return checkedTotal, clearedTotal, err
		}
		type rawJob struct {
			id  int64
			url string
		}
		jobs := make([]rawJob, 0, batchSize)
		for rows.Next() {
			var job rawJob
			if err := rows.Scan(&job.id, &job.url); err != nil {
				rows.Close()
				return checkedTotal, clearedTotal, err
			}
			jobs = append(jobs, job)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return checkedTotal, clearedTotal, err
		}
		rows.Close()
		if len(jobs) == 0 {
			break
		}

		clearedIDs := make([]int64, 0, len(jobs))
		for _, job := range jobs {
			lastID = job.id
			checkedTotal++
			statusCode, err := statusFn(toTargetJobURL(job.url))
			if err != nil {
				continue
			}
			if statusCode != statusNotFound && statusCode >= 200 && statusCode <= 399 {
				clearedIDs = append(clearedIDs, job.id)
			}
		}
		for _, jobID := range clearedIDs {
			if _, err := s.DB.SQL.ExecContext(ctx, `UPDATE raw_us_jobs SET is_skippable = 0, is_ready = 0 WHERE id = ?`, jobID); err != nil {
				return checkedTotal, clearedTotal, err
			}
			clearedTotal++
		}
	}
	return checkedTotal, clearedTotal, nil
}

func (s *Service) statusFunc() StatusFunc {
	if s.Status != nil {
		return s.Status
	}
	return defaultStatusFunc
}
