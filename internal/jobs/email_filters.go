package jobs

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/locationnorm"

	"github.com/jackc/pgx/v5/pgtype"
)

type LastJobFiltersPayload struct {
	JobCategories   []string `json:"jobCategories"`
	JobFunctions    []string `json:"jobFunctions"`
	JobTitles       []string `json:"jobTitles"`
	Company         string   `json:"company"`
	USStates        []string `json:"usStates"`
	Countries       []string `json:"countries"`
	EmploymentTypes []string `json:"employmentTypes"`
	TechStacks      []string `json:"techStacks"`
	PostDate        string   `json:"postDate"`
	PostDateCutoff  string   `json:"postDateCutoffIso"`
	SalaryMin       *float64 `json:"salaryMin"`
	Seniorities     []string `json:"seniorities"`
	UserJobAction   string   `json:"userJobAction"`
}

func BuildJobsWhereSQLForEmailFilters(payload LastJobFiltersPayload) (string, []any) {
	input := listingFilterInput{
		JobCategories:          []string{},
		JobFunctions:           []string{},
		TitleTokenGroups:       [][]string{},
		TitleTokenGroupsJSON:   []byte("[]"),
		USStates:               []string{},
		Countries:              []string{},
		LocationPatterns:       []string{},
		TechStacks:             []string{},
		EmploymentTypePatterns: []string{},
	}

	for _, category := range uniqueStrings(payload.JobCategories) {
		if trimmed := strings.TrimSpace(category); trimmed != "" {
			input.JobCategories = append(input.JobCategories, trimmed)
		}
	}
	for _, fn := range uniqueStrings(payload.JobFunctions) {
		if trimmed := strings.TrimSpace(fn); trimmed != "" {
			input.JobFunctions = append(input.JobFunctions, trimmed)
		}
	}

	titleTokenGroups := make([][]string, 0, len(payload.JobTitles))
	for _, title := range uniqueStrings(payload.JobTitles) {
		normalizedTitle := strings.TrimSpace(title)
		if normalizedTitle == "" {
			continue
		}
		if tokens := uniqueStrings(tokenizeTitleSearchText(normalizedTitle)); len(tokens) > 0 {
			titleTokenGroups = append(titleTokenGroups, tokens)
		}
	}
	input.TitleTokenGroups = titleTokenGroups
	if payload, err := json.Marshal(titleTokenGroups); err == nil {
		input.TitleTokenGroupsJSON = payload
	}
	input.HasTitleFilters = len(input.JobCategories) > 0 || len(input.JobFunctions) > 0 || len(titleTokenGroups) > 0

	for _, state := range uniqueStrings(payload.USStates) {
		if normalized := locationnorm.NormalizeUSStateName(state); normalized != "" {
			input.USStates = append(input.USStates, normalized)
		}
	}
	for _, country := range uniqueStrings(payload.Countries) {
		if trimmed := strings.TrimSpace(country); trimmed != "" {
			input.Countries = append(input.Countries, trimmed)
		}
	}
	input.HasStructuredLocation = len(input.USStates) > 0 || len(input.Countries) > 0

	input.CompanyFilter = strings.ToLower(strings.TrimSpace(payload.Company))

	for _, tech := range uniqueStrings(payload.TechStacks) {
		if trimmed := strings.TrimSpace(tech); trimmed != "" {
			input.TechStacks = append(input.TechStacks, trimmed)
		}
	}
	for _, employment := range uniqueStrings(payload.EmploymentTypes) {
		if trimmed := strings.TrimSpace(employment); trimmed != "" {
			input.EmploymentTypePatterns = append(input.EmploymentTypePatterns, "%"+trimmed+"%")
		}
	}

	if payload.PostDateCutoff != "" {
		if cutoff, ok := parsePostDateFrom(payload.PostDateCutoff); ok {
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: cutoff, Valid: true}
		}
	} else if postDate := strings.ToLower(strings.TrimSpace(payload.PostDate)); postDate != "" {
		nowUTC := time.Now().UTC()
		todayStart := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)
		thisWeekStart := todayStart.AddDate(0, 0, -((int(todayStart.Weekday()) + 6) % 7))
		thisMonthStart := time.Date(nowUTC.Year(), nowUTC.Month(), 1, 0, 0, 0, 0, time.UTC)
		lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
		lastWeekStart := thisWeekStart.AddDate(0, 0, -7)

		switch postDate {
		case "today":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: todayStart, Valid: true}
		case "yesterday":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: todayStart.AddDate(0, 0, -1), Valid: true}
			input.HasCreatedTo = true
			input.CreatedTo = pgtype.Timestamptz{Time: todayStart, Valid: true}
		case "this_week":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: thisWeekStart, Valid: true}
		case "previous_week":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: lastWeekStart, Valid: true}
			input.HasCreatedTo = true
			input.CreatedTo = pgtype.Timestamptz{Time: thisWeekStart, Valid: true}
		case "this_month":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: thisMonthStart, Valid: true}
		case "previous_month":
			input.HasCreatedFrom = true
			input.CreatedFrom = pgtype.Timestamptz{Time: lastMonthStart, Valid: true}
			input.HasCreatedTo = true
			input.CreatedTo = pgtype.Timestamptz{Time: thisMonthStart, Valid: true}
		default:
			if window, ok := postDateWindows[postDate]; ok {
				input.HasCreatedFrom = true
				input.CreatedFrom = pgtype.Timestamptz{Time: nowUTC.Add(-window), Valid: true}
			}
		}
	}

	if payload.SalaryMin != nil && *payload.SalaryMin > 0 {
		input.HasMinSalary = true
		input.MinSalary = *payload.SalaryMin
	}

	for _, seniority := range uniqueStrings(payload.Seniorities) {
		switch strings.ToLower(strings.TrimSpace(seniority)) {
		case "entry":
			input.SeniorityEntry = true
		case "junior":
			input.SeniorityJunior = true
		case "mid":
			input.SeniorityMid = true
		case "senior":
			input.SenioritySenior = true
		case "lead":
			input.SeniorityLead = true
		}
	}
	input.HasSeniority = input.SeniorityEntry || input.SeniorityJunior || input.SeniorityMid || input.SenioritySenior || input.SeniorityLead

	return buildJobsWhereSQL(input)
}

func BuildEmailJobsQuery(payload LastJobFiltersPayload, limit int) (string, []any) {
	whereSQL, whereArgs := BuildJobsWhereSQLForEmailFilters(payload)
	b := sqlArgsBuilder{args: append([]any{}, whereArgs...)}

	sqlText := `SELECT p.role_title, c.name, p.url, p.slug, p.created_at_source
		FROM parsed_jobs p
		LEFT JOIN parsed_companies c ON c.id = p.company_id`
	if whereSQL != "" {
		sqlText += " WHERE " + whereSQL
	}
	sqlText += " ORDER BY p.created_at_source DESC NULLS LAST"
	sqlText += fmt.Sprintf(" LIMIT %s::int", b.add(limit))
	return sqlText, b.args
}
