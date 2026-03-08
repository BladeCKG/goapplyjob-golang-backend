package parsed

import (
	"context"
	"database/sql"
)

func (s *Service) ResetStaleParsed(ctx context.Context, batchSize int) (int, int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	checkedCount := 0
	staleCount := 0
	var maxIDCursor any

	for {
		query := `SELECT r.id, r.post_date, p.id, p.created_at_source
			FROM raw_us_jobs r
			INNER JOIN parsed_jobs p ON p.raw_us_job_id = r.id
			WHERE r.is_parsed = true`
		args := []any{}
		if maxIDCursor != nil {
			query += ` AND r.id < ?`
			args = append(args, maxIDCursor)
		}
		query += ` ORDER BY r.id DESC LIMIT ?`
		args = append(args, batchSize)

		rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
		if err != nil {
			return checkedCount, staleCount, err
		}

		type pair struct {
			rawID           int64
			postDate        string
			parsedID        int64
			createdAtSource sql.NullString
		}
		pairs := make([]pair, 0, batchSize)
		for rows.Next() {
			var row pair
			if err := rows.Scan(&row.rawID, &row.postDate, &row.parsedID, &row.createdAtSource); err != nil {
				rows.Close()
				return checkedCount, staleCount, err
			}
			pairs = append(pairs, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return checkedCount, staleCount, err
		}
		rows.Close()

		if len(pairs) == 0 {
			break
		}

		maxIDCursor = pairs[len(pairs)-1].rawID
		checkedCount += len(pairs)

		for _, row := range pairs {
			postDate, err := parseDBDatetime(row.postDate)
			if err != nil {
				continue
			}
			sourceCreatedAt := parseDT(row.createdAtSource.String)
			if !isSourceOlderThanPostDate(sourceCreatedAt, postDate) {
				continue
			}

			tx, err := s.DB.SQL.BeginTx(ctx, nil)
			if err != nil {
				return checkedCount, staleCount, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM parsed_jobs WHERE id = ?`, row.parsedID); err != nil {
				tx.Rollback()
				return checkedCount, staleCount, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE raw_us_jobs SET is_ready = false, raw_json = NULL, is_parsed = false WHERE id = ?`, row.rawID); err != nil {
				tx.Rollback()
				return checkedCount, staleCount, err
			}
			if err := tx.Commit(); err != nil {
				return checkedCount, staleCount, err
			}
			staleCount++
		}
	}

	return checkedCount, staleCount, nil
}
