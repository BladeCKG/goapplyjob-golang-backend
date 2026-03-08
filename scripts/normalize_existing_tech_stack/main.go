package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
)

var techStackAliases = map[string]string{
	"nodejs":                   "Node.js",
	"node.js":                  "Node.js",
	"node js":                  "Node.js",
	"reactjs":                  "React",
	"react.js":                 "React",
	"nextjs":                   "Next.js",
	"next.js":                  "Next.js",
	"vuejs":                    "Vue.js",
	"vue.js":                   "Vue.js",
	"angularjs":                "AngularJS",
	"javascript":               "JavaScript",
	"typescript":               "TypeScript",
	"c#":                       "C#",
	"csharp":                   "C#",
	"c++":                      "C++",
	"cplusplus":                "C++",
	"golang":                   "Go",
	"postgres":                 "PostgreSQL",
	"postgresql":               "PostgreSQL",
	"mongodb":                  "MongoDB",
	"graphql":                  "GraphQL",
	"graph ql":                 "GraphQL",
	"rest api":                 "REST API",
	"restful api":              "REST API",
	"rest apis":                "REST API",
	"restful apis":             "REST API",
	"apis":                     "API",
	"aws":                      "AWS",
	"gcp":                      "GCP",
	"azure":                    "Azure",
	".net":                     ".NET",
	"dotnet":                   ".NET",
	"asp.net":                  "ASP.NET",
	"asp.net core":             "ASP.NET Core",
	"grpc":                     "gRPC",
	"json":                     "JSON",
	"xml":                      "XML",
	"html":                     "HTML",
	"html5":                    "HTML5",
	"css":                      "CSS",
	"css3":                     "CSS3",
	"scss":                     "SCSS",
	"sass":                     "Sass",
	"sql":                      "SQL",
	"nosql":                    "NoSQL",
	"no-sql":                   "NoSQL",
	"etl":                      "ETL",
	"elt":                      "ELT",
	"etl/elt":                  "ETL/ELT",
	"ci/cd":                    "CI/CD",
	"cicd":                     "CI/CD",
	"iac":                      "IaC",
	"infrastructure as code":   "Infrastructure as Code",
	"k8s":                      "Kubernetes",
	"kubernetes (k8s)":         "Kubernetes",
	"tailwindcss":              "Tailwind CSS",
	"tailwind css":             "Tailwind CSS",
	"google tag manager (gtm)": "Google Tag Manager",
	"google tag manager":       "Google Tag Manager",
	"gtm":                      "Google Tag Manager",
	"sfdc":                     "Salesforce",
	"sfdc crm":                 "Salesforce",
	"salesforce.com":           "Salesforce",
	"salesforce crm":           "Salesforce",
}

var techStackDropValues = map[string]struct{}{
	"n/a": {}, "na": {}, "none": {}, "null": {}, "unknown": {}, "tbd": {},
}

func main() {
	_ = config.LoadDotEnvIfExists(".env")
	sourcesCSV := flag.String("sources", "builtin,workable", "optional comma-separated sources (example: builtin,workable,hiringcafe)")
	dryRun := flag.Bool("dry-run", false, "preview only; do not write updates")
	batchSize := flag.Int("batch-size", 500, "commit every N updates")
	flag.Parse()

	db, err := database.Open(config.Getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sources := splitSources(*sourcesCSV)
	scanned, updated, err := run(context.Background(), db.SQL, sources, *dryRun, max(*batchSize, 1))
	if err != nil {
		log.Fatal(err)
	}
	mode := "APPLIED"
	if *dryRun {
		mode = "DRY-RUN"
	}
	label := sources
	if len(label) == 0 {
		label = []string{"<all>"}
	}
	fmt.Printf("[%s] scanned=%d updated=%d sources=%v\n", mode, scanned, updated, label)
}

func run(ctx context.Context, db *database.SQLConn, sources []string, dryRun bool, batchSize int) (int, int, error) {
	query := `SELECT p.id, p.tech_stack
		FROM parsed_jobs p
		JOIN raw_us_jobs r ON r.id = p.raw_us_job_id`
	args := make([]any, 0, len(sources))
	if len(sources) > 0 {
		placeholders := make([]string, 0, len(sources))
		for _, source := range sources {
			placeholders = append(placeholders, "?")
			args = append(args, source)
		}
		query += ` WHERE r.source IN (` + strings.Join(placeholders, ", ") + `)`
	}
	query += ` ORDER BY p.id ASC`

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	scanned := 0
	updated := 0
	for rows.Next() {
		var id int64
		var rawTech sql.NullString
		if err := rows.Scan(&id, &rawTech); err != nil {
			return scanned, updated, err
		}
		scanned++
		current := parseTechStack(rawTech)
		next := normalizeTechStack(current)
		currentJSON, _ := json.Marshal(current)
		nextJSON, _ := json.Marshal(next)
		if string(currentJSON) == string(nextJSON) {
			continue
		}
		updated++
		if dryRun {
			continue
		}
		var nextValue any
		if len(next) > 0 {
			nextValue = string(nextJSON)
		}
		if _, err := db.ExecContext(ctx, `UPDATE parsed_jobs SET tech_stack = ?, updated_at = ? WHERE id = ?`, nextValue, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return scanned, updated, err
		}
		if updated%batchSize == 0 {
			// no-op checkpoint for parity with batched scripts
		}
	}
	if err := rows.Err(); err != nil {
		return scanned, updated, err
	}
	return scanned, updated, nil
}

func parseTechStack(value sql.NullString) []string {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	var list []string
	if err := json.Unmarshal([]byte(value.String), &list); err == nil {
		return list
	}
	var anyList []any
	if err := json.Unmarshal([]byte(value.String), &anyList); err != nil {
		return nil
	}
	out := make([]string, 0, len(anyList))
	for _, item := range anyList {
		text, _ := item.(string)
		if strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	return out
}

func normalizeTechStackValue(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.Trim(normalized, "\"'")
	normalized = regexpReplace(`\([^)]*\)`, normalized, "")
	if strings.Contains(normalized, "(") && !strings.Contains(normalized, ")") {
		normalized = strings.SplitN(normalized, "(", 2)[0]
	}
	normalized = strings.ReplaceAll(normalized, ")", "")
	normalized = strings.ReplaceAll(normalized, "]", "")
	normalized = strings.ReplaceAll(normalized, "}", "")
	normalized = regexpReplace(`\s*/\s*`, normalized, "/")
	normalized = regexpReplace(`\s*-\s*`, normalized, "-")
	normalized = regexpReplace(`[;,:]+$`, normalized, "")
	normalized = regexpReplace(`\s+`, normalized, " ")
	normalized = strings.Trim(normalized, " .-_/")
	if normalized == "" {
		return ""
	}
	lowered := strings.ToLower(normalized)
	if _, ok := techStackDropValues[lowered]; ok {
		return ""
	}
	if alias, ok := techStackAliases[lowered]; ok {
		return alias
	}
	return normalized
}

func normalizeTechStack(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		next := normalizeTechStackValue(value)
		if next == "" {
			continue
		}
		key := strings.ToLower(next)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, next)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func splitSources(csv string) []string {
	out := []string{}
	for _, part := range strings.Split(csv, ",") {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func regexpReplace(pattern, value, replacement string) string {
	re := regexpCache(pattern)
	return re.ReplaceAllString(value, replacement)
}

var reCache = map[string]*regexp.Regexp{}

func regexpCache(pattern string) *regexp.Regexp {
	if existing, ok := reCache[pattern]; ok {
		return existing
	}
	compiled := regexp.MustCompile(pattern)
	reCache[pattern] = compiled
	return compiled
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
