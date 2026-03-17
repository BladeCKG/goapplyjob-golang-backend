package jobs

import (
	"encoding/json"
	"fmt"
	"goapplyjob-golang-backend/internal/locationnorm"
	"strings"
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
		JobCategories:        []string{},
		JobFunctions:         []string{},
		TitleTokenGroups:     [][]string{},
		TitleTokenGroupsJSON: []byte("[]"),
		USStates:             []string{},
		Countries:            []string{},
		LocationPatterns:     []string{},
		TechStacks:           []string{},
		EmploymentTypes:      []string{},
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
			input.EmploymentTypes = append(input.EmploymentTypes, "%"+trimmed+"%")
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

func BuildEmailJobsQuery(payload LastJobFiltersPayload, userID int64, limit int) (string, []any) {
	whereSQL, whereArgs := BuildJobsWhereSQLForEmailFilters(payload)
	b := sqlArgsBuilder{args: append([]any{}, whereArgs...)}

	sqlText := `SELECT p.role_title, c.name, c.profile_pic_url, p.url, p.slug, p.created_at_source, p.categorized_job_title, p.categorized_job_function, p.salary_human_text
		FROM parsed_jobs p
		LEFT JOIN parsed_companies c ON c.id = p.company_id`
	clauses := []string{}
	if whereSQL != "" {
		clauses = append(clauses, whereSQL)
	}
	userIDAppliedPh := b.add(userID)
	userIDHiddenPh := b.add(userID)
	clauses = append(clauses, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_applied = true)", userIDAppliedPh))
	clauses = append(clauses, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM user_job_actions uja WHERE uja.user_id = %s::bigint AND uja.parsed_job_id = p.id AND uja.is_hidden = true)", userIDHiddenPh))
	if len(clauses) > 0 {
		sqlText += " WHERE " + strings.Join(clauses, " AND ")
	}
	sqlText += " ORDER BY p.created_at_source DESC NULLS LAST"
	sqlText += fmt.Sprintf(" LIMIT %s::int", b.add(limit))
	return sqlText, b.args
}
