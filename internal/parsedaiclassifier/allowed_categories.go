package parsedaiclassifier

import (
	"context"
	"database/sql"
	"log"
	"sync"
)

var categoryCache = struct {
	mu        sync.RWMutex
	items     []string
	functions map[string]string
}{}

func SetCachedGroqCategorizedJobTitles(titles []string, functions map[string]string) {
	categoryCache.mu.Lock()
	categoryCache.items = append([]string(nil), titles...)
	categoryCache.functions = functions
	categoryCache.mu.Unlock()
}

func containsCaseSensitive(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (s *Service) loadAllowedJobCategoriesForGroq(ctx context.Context) ([]string, error) {
	categories, _, err := s.loadAllowedJobCategoriesAndFunctions(ctx)
	return categories, err
}

func (s *Service) loadAllowedJobCategoriesAndFunctions(ctx context.Context) ([]string, map[string]string, error) {
	categoryCache.mu.RLock()
	cached := append([]string(nil), categoryCache.items...)
	cachedFunctions := categoryCache.functions
	categoryCache.mu.RUnlock()
	if len(cached) > 0 {
		if !containsCaseSensitive(cached, "Blank") {
			cached = append(cached, "Blank")
		}
		return cached, cachedFunctions, nil
	}
	log.Printf("parsed-job-worker groq_category_cache_empty_fallback=db")

	rows, err := s.DB.SQL.QueryContext(
		ctx,
		`SELECT categorized_job_title, categorized_job_function
		   FROM parsed_jobs
		  WHERE categorized_job_title IS NOT NULL
		    AND categorized_job_function IS NOT NULL
		  GROUP BY categorized_job_title, categorized_job_function`,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 128)
	functions := map[string]string{}
	for rows.Next() {
		var title sql.NullString
		var function sql.NullString
		if scanErr := rows.Scan(&title, &function); scanErr != nil {
			return nil, nil, scanErr
		}
		titleString := title.String
		out = append(out, titleString)
		functions[titleString] = function.String
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if !containsCaseSensitive(out, "Blank") {
		out = append(out, "Blank")
	}

	categoryCache.mu.Lock()
	categoryCache.items = append([]string(nil), out...)
	categoryCache.functions = functions
	categoryCache.mu.Unlock()

	return out, functions, nil
}

func groqClassifierDescription(roleDescription, roleRequirements any) string {
	description, _ := roleDescription.(string)
	requirements, _ := roleRequirements.(string)
	if requirements == "" {
		return description
	}
	if description == "" {
		return requirements
	}
	return description + "\n\nRole requirements:\n" + requirements
}
