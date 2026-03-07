package parsed

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
)

const (
	sourceRemoteRocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
)

var seniorityTokens = map[string]struct{}{
	"senior": {}, "sr": {}, "junior": {}, "jr": {}, "lead": {}, "principal": {}, "staff": {}, "entry": {}, "mid": {}, "expert": {}, "leader": {}, "level": {},
}

var genericCategoryMatchTokens = map[string]struct{}{
	"accountant": {}, "administrator": {}, "engineer": {}, "developer": {}, "manager": {}, "specialist": {}, "consultant": {}, "analyst": {}, "architect": {}, "designer": {}, "director": {}, "producer": {}, "writer": {}, "support": {}, "operations": {}, "web": {}, "remote": {}, "lead": {}, "staff": {},
}

var normalizationReplacements = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{pattern: regexp.MustCompile(`\bdev[\s\-]*ops\b`), replacement: "devops"},
	{pattern: regexp.MustCompile(`\bdev\b`), replacement: "developer"},
	{pattern: regexp.MustCompile(`\bbdr\b`), replacement: "business development representative"},
	{pattern: regexp.MustCompile(`\bsdr\b`), replacement: "sales development representative"},
	{pattern: regexp.MustCompile(`\bswe\b`), replacement: "software engineer"},
	{pattern: regexp.MustCompile(`\bvp\b`), replacement: "vice president"},
	{pattern: regexp.MustCompile(`\bta\b`), replacement: "talent acquisition"},
	{pattern: regexp.MustCompile(`\bhr\b`), replacement: "human resources"},
	{pattern: regexp.MustCompile(`\btalent acquisition\b`), replacement: "recruitment human resources"},
	{pattern: regexp.MustCompile(`\bcpg\b`), replacement: "consumer packaged goods"},
}

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

func normalizeTextForMatching(value string) string {
	normalized := strings.ToLower(value)
	for _, replacement := range normalizationReplacements {
		normalized = replacement.pattern.ReplaceAllString(normalized, replacement.replacement)
	}
	return normalized
}

func tokenizeRoleTitleForSimilarity(roleTitle string) map[string]struct{} {
	rawTokens := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(roleTitle), -1)
	out := map[string]struct{}{}
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out[token] = struct{}{}
	}
	return out
}

func tokenizeTextForSequence(value string) []string {
	rawTokens := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(rawTokens))
	for _, token := range rawTokens {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out = append(out, token)
	}
	return out
}

func normalizeRoleTitleForExactMatch(value string) string {
	return strings.Join(tokenizeTextForSequence(value), " ")
}

func jaccardSimilarity(left, right map[string]struct{}) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	intersection := 0
	union := map[string]struct{}{}
	for token := range left {
		union[token] = struct{}{}
		if _, ok := right[token]; ok {
			intersection++
		}
	}
	for token := range right {
		union[token] = struct{}{}
	}
	if len(union) == 0 {
		return 0
	}
	return float64(intersection) / float64(len(union))
}

func orderedTokenMatchScore(roleTitle, categoryTitle string) float64 {
	roleTokens := orderedTokens(roleTitle)
	categoryTokens := orderedTokens(categoryTitle)
	if len(roleTokens) == 0 || len(categoryTokens) == 0 {
		return 0
	}
	matched := 0
	idx := 0
	for _, categoryToken := range categoryTokens {
		for idx < len(roleTokens) {
			if roleTokens[idx] == categoryToken {
				matched++
				idx++
				break
			}
			idx++
		}
	}
	return float64(matched) / float64(len(categoryTokens))
}

func orderedTokens(value string) []string {
	raw := regexp.MustCompile(`[^a-z0-9]+`).Split(normalizeTextForMatching(value), -1)
	out := make([]string, 0, len(raw))
	for _, token := range raw {
		if len(token) <= 1 {
			continue
		}
		if _, ok := seniorityTokens[token]; ok {
			continue
		}
		out = append(out, token)
	}
	return out
}

func (s *Service) findSimilarRemoteCategories(ctx context.Context, roleTitle string) (string, string, error) {
	sourceTokens := tokenizeRoleTitleForSimilarity(roleTitle)
	if len(sourceTokens) == 0 {
		return "", "", nil
	}
	sourceSequenceTokens := tokenizeTextForSequence(roleTitle)
	sourceExactTitle := normalizeRoleTitleForExactMatch(roleTitle)
	prioritizedTokens := make([]string, 0, len(sourceSequenceTokens))
	seenTokens := map[string]struct{}{}
	for _, token := range sourceSequenceTokens {
		if _, seen := seenTokens[token]; seen {
			continue
		}
		seenTokens[token] = struct{}{}
		prioritizedTokens = append(prioritizedTokens, token)
	}
	sort.SliceStable(prioritizedTokens, func(i, j int) bool {
		leftGeneric := isGenericCategoryToken(prioritizedTokens[i])
		rightGeneric := isGenericCategoryToken(prioritizedTokens[j])
		if leftGeneric != rightGeneric {
			return !leftGeneric
		}
		return len(prioritizedTokens[i]) > len(prioritizedTokens[j])
	})

	if sourceExactTitle != "" {
		title, function, err := s.findExactNormalizedCategoryMatch(ctx, sourceExactTitle)
		if err != nil {
			return "", "", err
		}
		if title != "" {
			return title, function, nil
		}
	}

	query := `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE r.source = ? AND p.role_title IS NOT NULL AND p.categorized_job_title IS NOT NULL`
	args := []any{sourceRemoteRocketship}
	if len(prioritizedTokens) > 0 {
		conditions := make([]string, 0, min(len(prioritizedTokens), 5))
		for _, token := range prioritizedTokens[:min(len(prioritizedTokens), 5)] {
			conditions = append(conditions, `(LOWER(p.role_title) LIKE ? OR LOWER(p.categorized_job_title) LIKE ? OR LOWER(COALESCE(p.categorized_job_function, '')) LIKE ?)`)
			like := "%" + token + "%"
			args = append(args, like, like, like)
		}
		query += " AND (" + strings.Join(conditions, " OR ") + ")"
	}
	query += " ORDER BY p.updated_at DESC, p.id DESC LIMIT 1000"
	rows, err := s.DB.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()
	bestScore := 0.0
	bestTitle := ""
	bestFunction := ""
	for rows.Next() {
		var candidateRoleTitle, candidateTitle sql.NullString
		var candidateFunction sql.NullString
		if err := rows.Scan(&candidateRoleTitle, &candidateTitle, &candidateFunction); err != nil {
			return "", "", err
		}
		score := jaccardSimilarity(sourceTokens, tokenizeRoleTitleForSimilarity(candidateRoleTitle.String))
		titleTokens := orderedTokens(candidateTitle.String)
		score += orderedTokenMatchScore(roleTitle, candidateTitle.String)
		score += 0.1 * float64(len(titleTokens))
		if normalizeRoleTitleForExactMatch(candidateRoleTitle.String) == sourceExactTitle {
			score += 0.5
		}
		if strings.EqualFold(candidateTitle.String, "Engineer") || strings.EqualFold(candidateTitle.String, "Manager") {
			score -= 0.35
		}
		if score > bestScore {
			bestScore = score
			bestTitle = candidateTitle.String
			bestFunction = candidateFunction.String
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}
	if bestScore < 0.5 {
		return "", "", nil
	}
	return bestTitle, bestFunction, nil
}

func (s *Service) findExactNormalizedCategoryMatch(ctx context.Context, normalizedRoleTitle string) (string, string, error) {
	rows, err := s.DB.SQL.QueryContext(ctx, `SELECT p.role_title, p.categorized_job_title, p.categorized_job_function
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
		WHERE r.source = ? AND p.role_title IS NOT NULL AND p.categorized_job_title IS NOT NULL
		ORDER BY p.updated_at DESC, p.id DESC`, sourceRemoteRocketship)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	for rows.Next() {
		var roleTitle, title, function sql.NullString
		if err := rows.Scan(&roleTitle, &title, &function); err != nil {
			return "", "", err
		}
		if roleTitle.String == "" || title.String == "" {
			continue
		}
		if normalizeRoleTitleForExactMatch(roleTitle.String) == normalizedRoleTitle {
			return title.String, function.String, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}
	return "", "", nil
}

func isGenericCategoryToken(token string) bool {
	_, ok := genericCategoryMatchTokens[token]
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *Service) ProcessPending(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	rows, err := s.DB.SQL.QueryContext(ctx, `SELECT id, raw_json, COALESCE(source, '') FROM raw_us_jobs WHERE is_ready = 1 AND is_parsed = 0 ORDER BY post_date DESC, id DESC LIMIT ?`, batchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type rawRow struct {
		id      int64
		rawJSON sql.NullString
		source  string
	}
	pending := make([]rawRow, 0, batchSize)
	for rows.Next() {
		var row rawRow
		if err := rows.Scan(&row.id, &row.rawJSON, &row.source); err != nil {
			return 0, err
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	processed := 0
	for _, row := range pending {
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
		categorizedTitle := stringFromPayload(payload["categorizedJobTitle"])
		categorizedFunction := stringFromPayload(payload["categorizedJobFunction"])
		if row.source == sourceBuiltin && categorizedTitle == nil {
			inferredTitle, inferredFunction, err := s.findSimilarRemoteCategories(ctx, stringValue(payload["roleTitle"]))
			if err != nil {
				return processed, err
			}
			categorizedTitle = stringFromPayload(inferredTitle)
			categorizedFunction = stringFromPayload(inferredFunction)
		}
		if _, err := s.DB.SQL.ExecContext(
			ctx,
			`INSERT INTO parsed_jobs (raw_us_job_id, external_job_id, created_at_source, url, categorized_job_title, categorized_job_function, role_title, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(raw_us_job_id) DO UPDATE SET
			   external_job_id = excluded.external_job_id,
			   created_at_source = excluded.created_at_source,
			   url = excluded.url,
			   categorized_job_title = excluded.categorized_job_title,
			   categorized_job_function = excluded.categorized_job_function,
			   role_title = excluded.role_title,
			   updated_at = excluded.updated_at`,
			row.id,
			stringFromPayload(payload["id"]),
			formatNullableTime(sourceCreatedAt),
			stringFromPayload(payload["url"]),
			categorizedTitle,
			categorizedFunction,
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
	switch item := value.(type) {
	case string:
		if strings.TrimSpace(item) == "" {
			return nil
		}
		return strings.TrimSpace(item)
	case float64:
		return strings.TrimSpace(strconv.FormatInt(int64(item), 10))
	case int:
		return strconv.Itoa(item)
	case int64:
		return strconv.FormatInt(item, 10)
	default:
		return nil
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
