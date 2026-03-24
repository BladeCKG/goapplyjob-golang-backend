package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"log"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

var slugSuffixPattern = regexp.MustCompile(`^(?P<base>.+)-(?P<num>\d+)$`)

const (
	externalCompanyIDPrefix = "gaj("
	externalCompanyIDSuffix = ")gaj"
)

type companyRow struct {
	ID                          int64
	ExternalCompanyID           sql.NullString
	Name                        sql.NullString
	Slug                        sql.NullString
	Tagline                     sql.NullString
	FoundedYear                 sql.NullString
	HomePageURL                 sql.NullString
	LinkedInURL                 sql.NullString
	SponsorsH1B                 sql.NullBool
	SponsorsUKSkilledWorkerVisa sql.NullBool
	EmployeeRange               sql.NullString
	ProfilePicURL               sql.NullString
	TaglineBrazil               sql.NullString
	TaglineFrance               sql.NullString
	TaglineGermany              sql.NullString
	ChatGPTDescription          sql.NullString
	LinkedInDescription         sql.NullString
	ChatGPTDescriptionBrazil    sql.NullString
	ChatGPTDescriptionFrance    sql.NullString
	ChatGPTDescriptionGermany   sql.NullString
	LinkedInDescriptionBrazil   sql.NullString
	LinkedInDescriptionFrance   sql.NullString
	LinkedInDescriptionGermany  sql.NullString
	FundingData                 sql.NullString
	ChatGPTIndustries           sql.NullString
	IndustrySpecialities        sql.NullString
	IndustrySpecialitiesBrazil  sql.NullString
	IndustrySpecialitiesFrance  sql.NullString
	IndustrySpecialitiesGermany sql.NullString
}

type mergeStats struct {
	mergedGroups    int
	mergedCompanies int
	reassignedJobs  int
	slugCleaned     int
}

func main() {
	dryRun := flag.Bool("dry-run", false, "preview changes without committing")
	limitGroups := flag.Int("limit-groups", 1000, "limit number of merge groups to process")
	flag.Parse()

	_ = config.LoadDotEnvIfExists(".env")
	cfg := config.Load()
	db, err := database.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	rows, err := db.SQL.QueryContext(
		ctx,
		`SELECT id, external_company_id, name, slug, tagline, founded_year, home_page_url, linkedin_url,
		        sponsors_h1b, sponsors_uk_skilled_worker_visa, employee_range, profile_pic_url,
		        tagline_brazil, tagline_france, tagline_germany,
		        chatgpt_description, linkedin_description,
		        chatgpt_description_brazil, chatgpt_description_france, chatgpt_description_germany,
		        linkedin_description_brazil, linkedin_description_france, linkedin_description_germany,
		        funding_data::text, chatgpt_industries::text, industry_specialities::text,
		        industry_specialities_brazil::text, industry_specialities_france::text, industry_specialities_germany::text
		   FROM parsed_companies`,
	)
	if err != nil {
		log.Fatalf("query companies: %v", err)
	}
	defer rows.Close()

	companiesByID := map[int64]companyRow{}
	activeCompanyIDs := map[int64]struct{}{}
	linkedinGroups := map[string][]int64{}
	nameSlugHostGroups := map[string][]int64{}
	for rows.Next() {
		var row companyRow
		if err := rows.Scan(
			&row.ID, &row.ExternalCompanyID, &row.Name, &row.Slug, &row.Tagline, &row.FoundedYear, &row.HomePageURL, &row.LinkedInURL,
			&row.SponsorsH1B, &row.SponsorsUKSkilledWorkerVisa, &row.EmployeeRange, &row.ProfilePicURL,
			&row.TaglineBrazil, &row.TaglineFrance, &row.TaglineGermany,
			&row.ChatGPTDescription, &row.LinkedInDescription,
			&row.ChatGPTDescriptionBrazil, &row.ChatGPTDescriptionFrance, &row.ChatGPTDescriptionGermany,
			&row.LinkedInDescriptionBrazil, &row.LinkedInDescriptionFrance, &row.LinkedInDescriptionGermany,
			&row.FundingData, &row.ChatGPTIndustries, &row.IndustrySpecialities,
			&row.IndustrySpecialitiesBrazil, &row.IndustrySpecialitiesFrance, &row.IndustrySpecialitiesGermany,
		); err != nil {
			log.Fatalf("scan company: %v", err)
		}
		companiesByID[row.ID] = row
		activeCompanyIDs[row.ID] = struct{}{}

		if linkedinKey := normalizedLinkedInIdentity(row.LinkedInURL.String); linkedinKey != "" {
			linkedinGroups[linkedinKey] = append(linkedinGroups[linkedinKey], row.ID)
		}
		if nameSlugHostKey := normalizedNameSlugHostKey(row); nameSlugHostKey != "" {
			nameSlugHostGroups[nameSlugHostKey] = append(nameSlugHostGroups[nameSlugHostKey], row.ID)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error: %v", err)
	}

	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	totalStats := mergeStats{}
	groupLimitRemaining := *limitGroups
	linkedinStats := processMergePass(ctx, tx, "linkedin", linkedinGroups, companiesByID, activeCompanyIDs, &groupLimitRemaining)
	totalStats.mergedGroups += linkedinStats.mergedGroups
	totalStats.mergedCompanies += linkedinStats.mergedCompanies
	totalStats.reassignedJobs += linkedinStats.reassignedJobs
	totalStats.slugCleaned += linkedinStats.slugCleaned

	nameSlugHostStats := processMergePass(ctx, tx, "name_slug_homepage", nameSlugHostGroups, companiesByID, activeCompanyIDs, &groupLimitRemaining)
	totalStats.mergedGroups += nameSlugHostStats.mergedGroups
	totalStats.mergedCompanies += nameSlugHostStats.mergedCompanies
	totalStats.reassignedJobs += nameSlugHostStats.reassignedJobs
	totalStats.slugCleaned += nameSlugHostStats.slugCleaned

	mode := "APPLIED"
	if *dryRun {
		if err := tx.Rollback(); err != nil {
			log.Fatalf("rollback: %v", err)
		}
		mode = "DRY-RUN"
	} else {
		if err := tx.Commit(); err != nil {
			log.Fatalf("commit: %v", err)
		}
	}

	fmt.Printf(
		"[%s] merged_groups=%d merged_companies=%d reassigned_jobs=%d slug_cleaned=%d\n",
		mode,
		totalStats.mergedGroups,
		totalStats.mergedCompanies,
		totalStats.reassignedJobs,
		totalStats.slugCleaned,
	)
}

func processMergePass(
	ctx context.Context,
	tx *database.Tx,
	keyLabel string,
	groups map[string][]int64,
	companiesByID map[int64]companyRow,
	activeCompanyIDs map[int64]struct{},
	limitRemaining *int,
) mergeStats {
	stats := mergeStats{}
	candidateKeys := make([]string, 0, len(groups))
	for key, ids := range groups {
		if len(filterActiveCompanyIDs(ids, activeCompanyIDs)) > 1 {
			candidateKeys = append(candidateKeys, key)
		}
	}
	sort.Strings(candidateKeys)

	for _, key := range candidateKeys {
		if limitRemaining != nil && *limitRemaining == 0 {
			break
		}

		activeIDs := filterActiveCompanyIDs(groups[key], activeCompanyIDs)
		if len(activeIDs) <= 1 {
			continue
		}

		items := make([]companyRow, 0, len(activeIDs))
		for _, id := range activeIDs {
			row, ok := companiesByID[id]
			if !ok {
				continue
			}
			items = append(items, row)
		}
		if len(items) <= 1 {
			continue
		}

		sort.Slice(items, func(i, j int) bool {
			leftScore := completenessScore(items[i])
			rightScore := completenessScore(items[j])
			if leftScore != rightScore {
				return leftScore > rightScore
			}
			return items[i].ID < items[j].ID
		})

		canonical := items[0]
		duplicates := items[1:]
		if len(duplicates) == 0 {
			continue
		}

		mergedFields := mergeCompanyFields(&canonical, duplicates)
		cleanedSlug := cleanSlugNumericSuffix(canonical.Slug.String)
		if cleanedSlug != "" && cleanedSlug != strings.TrimSpace(canonical.Slug.String) {
			canonical.Slug = sql.NullString{String: cleanedSlug, Valid: true}
		}

		if len(mergedFields) > 0 || cleanedSlug != "" && cleanedSlug != strings.TrimSpace(items[0].Slug.String) {
			if err := updateCanonicalCompany(ctx, tx, canonical); err != nil {
				log.Fatalf("update canonical company_id=%d: %v", canonical.ID, err)
			}
			if len(mergedFields) > 0 {
				log.Printf("merge_fields canonical_id=%d fields=%v", canonical.ID, mergedFields)
			}
			if cleanedSlug != "" && cleanedSlug != strings.TrimSpace(items[0].Slug.String) {
				stats.slugCleaned++
			}
		}

		companiesByID[canonical.ID] = canonical
		dupIDs := make([]int64, 0, len(duplicates))
		for _, dup := range duplicates {
			dupIDs = append(dupIDs, dup.ID)
		}
		log.Printf("merge by=%s key=%s canonical_id=%d duplicate_ids=%v", keyLabel, key, canonical.ID, dupIDs)

		for _, dup := range duplicates {
			updateResult, err := tx.ExecContext(ctx, `UPDATE parsed_jobs SET company_id = ? WHERE company_id = ?`, canonical.ID, dup.ID)
			if err != nil {
				log.Fatalf("reassign jobs duplicate_company_id=%d: %v", dup.ID, err)
			}
			if affected, _ := updateResult.RowsAffected(); affected > 0 {
				stats.reassignedJobs += int(affected)
			}

			deleteResult, err := tx.ExecContext(ctx, `DELETE FROM parsed_companies WHERE id = ?`, dup.ID)
			if err != nil {
				log.Fatalf("delete duplicate_company_id=%d: %v", dup.ID, err)
			}
			if affected, _ := deleteResult.RowsAffected(); affected > 0 {
				stats.mergedCompanies += int(affected)
			}

			delete(activeCompanyIDs, dup.ID)
			delete(companiesByID, dup.ID)
		}

		stats.mergedGroups++
		if limitRemaining != nil && *limitRemaining > 0 {
			*limitRemaining--
		}
	}

	return stats
}

func updateCanonicalCompany(ctx context.Context, tx *database.Tx, canonical companyRow) error {
	_, err := tx.ExecContext(
		ctx,
		`UPDATE parsed_companies
		    SET external_company_id = ?, name = ?, slug = ?, tagline = ?, founded_year = ?, home_page_url = ?, linkedin_url = ?,
		        sponsors_h1b = ?, sponsors_uk_skilled_worker_visa = ?, employee_range = ?, profile_pic_url = ?,
		        tagline_brazil = ?, tagline_france = ?, tagline_germany = ?,
		        chatgpt_description = ?, linkedin_description = ?,
		        chatgpt_description_brazil = ?, chatgpt_description_france = ?, chatgpt_description_germany = ?,
		        linkedin_description_brazil = ?, linkedin_description_france = ?, linkedin_description_germany = ?,
		        funding_data = ?, chatgpt_industries = ?, industry_specialities = ?,
		        industry_specialities_brazil = ?, industry_specialities_france = ?, industry_specialities_germany = ?,
		        updated_at = NOW()
		  WHERE id = ?`,
		nullStringValue(canonical.ExternalCompanyID),
		nullStringValue(canonical.Name),
		nullStringValue(canonical.Slug),
		nullStringValue(canonical.Tagline),
		nullStringValue(canonical.FoundedYear),
		nullStringValue(canonical.HomePageURL),
		nullStringValue(canonical.LinkedInURL),
		nullBoolValue(canonical.SponsorsH1B),
		nullBoolValue(canonical.SponsorsUKSkilledWorkerVisa),
		nullStringValue(canonical.EmployeeRange),
		nullStringValue(canonical.ProfilePicURL),
		nullStringValue(canonical.TaglineBrazil),
		nullStringValue(canonical.TaglineFrance),
		nullStringValue(canonical.TaglineGermany),
		nullStringValue(canonical.ChatGPTDescription),
		nullStringValue(canonical.LinkedInDescription),
		nullStringValue(canonical.ChatGPTDescriptionBrazil),
		nullStringValue(canonical.ChatGPTDescriptionFrance),
		nullStringValue(canonical.ChatGPTDescriptionGermany),
		nullStringValue(canonical.LinkedInDescriptionBrazil),
		nullStringValue(canonical.LinkedInDescriptionFrance),
		nullStringValue(canonical.LinkedInDescriptionGermany),
		nullJSONTextValue(canonical.FundingData),
		nullJSONTextValue(canonical.ChatGPTIndustries),
		nullJSONTextValue(canonical.IndustrySpecialities),
		nullJSONTextValue(canonical.IndustrySpecialitiesBrazil),
		nullJSONTextValue(canonical.IndustrySpecialitiesFrance),
		nullJSONTextValue(canonical.IndustrySpecialitiesGermany),
		canonical.ID,
	)
	return err
}

func filterActiveCompanyIDs(ids []int64, activeCompanyIDs map[int64]struct{}) []int64 {
	filtered := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := activeCompanyIDs[id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func normalizedLinkedInIdentity(raw string) string {
	trimmed := normalizeString(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Hostname() == "" {
		parsed, err = url.Parse("https://" + trimmed)
		if err != nil {
			return ""
		}
	}
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "."))
	host = strings.TrimPrefix(host, "www.")
	if !strings.Contains(host, "linkedin.com") {
		return ""
	}
	path := regexp.MustCompile(`/+`).ReplaceAllString(parsed.Path, "/")
	path = strings.Trim(strings.ToLower(path), "/")
	if path == "" {
		return host
	}
	return host + "/" + path
}

func normalizedHomePageHost(raw string) string {
	trimmed := normalizeString(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Hostname() == "" {
		parsed, err = url.Parse("https://" + trimmed)
		if err != nil {
			return ""
		}
	}
	host := strings.ToLower(strings.Trim(parsed.Hostname(), "."))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func normalizedNameKey(raw string) string {
	trimmed := strings.ToLower(normalizeString(raw))
	if trimmed == "" {
		return ""
	}
	replacer := strings.NewReplacer("&", " and ")
	trimmed = replacer.Replace(trimmed)
	var builder strings.Builder
	lastDash := false
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func normalizedSlugKey(raw string) string {
	return strings.ToLower(normalizeString(raw))
}

func normalizedNameSlugHostKey(row companyRow) string {
	nameKey := normalizedNameKey(row.Name.String)
	slugKey := normalizedSlugKey(row.Slug.String)
	hostKey := normalizedHomePageHost(row.HomePageURL.String)
	if nameKey == "" || slugKey == "" || hostKey == "" {
		return ""
	}
	return nameKey + "|" + slugKey + "|" + hostKey
}

func cleanSlugNumericSuffix(raw string) string {
	trimmed := normalizeString(raw)
	if trimmed == "" {
		return ""
	}
	match := slugSuffixPattern.FindStringSubmatch(trimmed)
	if len(match) == 0 {
		return trimmed
	}
	baseIndex := slugSuffixPattern.SubexpIndex("base")
	if baseIndex <= 0 || baseIndex >= len(match) {
		return trimmed
	}
	base := strings.Trim(strings.TrimSpace(match[baseIndex]), "- ")
	if base == "" {
		return trimmed
	}
	return base
}

func normalizeString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	switch strings.ToLower(trimmed) {
	case "null", "none", "nil":
		return ""
	default:
		return trimmed
	}
}

func completenessScore(row companyRow) int {
	score := 0
	if populatedString(row.Name) {
		score++
	}
	if populatedString(row.Slug) {
		score++
	}
	if populatedString(row.Tagline) {
		score++
	}
	if populatedString(row.FoundedYear) {
		score++
	}
	if populatedString(row.HomePageURL) {
		score++
	}
	if populatedString(row.LinkedInURL) {
		score++
	}
	if row.SponsorsH1B.Valid {
		score++
	}
	if row.SponsorsUKSkilledWorkerVisa.Valid {
		score++
	}
	if populatedString(row.EmployeeRange) {
		score++
	}
	if populatedString(row.ProfilePicURL) {
		score++
	}
	if populatedString(row.TaglineBrazil) {
		score++
	}
	if populatedString(row.TaglineFrance) {
		score++
	}
	if populatedString(row.TaglineGermany) {
		score++
	}
	if populatedString(row.ChatGPTDescription) {
		score++
	}
	if populatedString(row.LinkedInDescription) {
		score++
	}
	if populatedString(row.ChatGPTDescriptionBrazil) {
		score++
	}
	if populatedString(row.ChatGPTDescriptionFrance) {
		score++
	}
	if populatedString(row.ChatGPTDescriptionGermany) {
		score++
	}
	if populatedString(row.LinkedInDescriptionBrazil) {
		score++
	}
	if populatedString(row.LinkedInDescriptionFrance) {
		score++
	}
	if populatedString(row.LinkedInDescriptionGermany) {
		score++
	}
	if populatedJSONText(row.FundingData) {
		score++
	}
	if populatedJSONText(row.ChatGPTIndustries) {
		score++
	}
	if populatedJSONText(row.IndustrySpecialities) {
		score++
	}
	if populatedJSONText(row.IndustrySpecialitiesBrazil) {
		score++
	}
	if populatedJSONText(row.IndustrySpecialitiesFrance) {
		score++
	}
	if populatedJSONText(row.IndustrySpecialitiesGermany) {
		score++
	}
	return score
}

func mergeCompanyFields(winner *companyRow, losers []companyRow) []string {
	merged := []string{}
	mergedExternalIDs := mergeExternalCompanyIDs(winner.ExternalCompanyID, losers)
	if mergedExternalIDs.Valid && mergedExternalIDs.String != winner.ExternalCompanyID.String {
		winner.ExternalCompanyID = mergedExternalIDs
		merged = append(merged, "external_company_id")
	}
	fillString := func(name string, dest *sql.NullString, getter func(companyRow) sql.NullString) {
		if populatedString(*dest) {
			return
		}
		for _, loser := range losers {
			candidate := getter(loser)
			if populatedString(candidate) {
				*dest = candidate
				merged = append(merged, name)
				return
			}
		}
	}
	fillBool := func(name string, dest *sql.NullBool, getter func(companyRow) sql.NullBool) {
		if dest.Valid {
			return
		}
		for _, loser := range losers {
			candidate := getter(loser)
			if candidate.Valid {
				*dest = candidate
				merged = append(merged, name)
				return
			}
		}
	}
	fillJSON := func(name string, dest *sql.NullString, getter func(companyRow) sql.NullString) {
		if populatedJSONText(*dest) {
			return
		}
		for _, loser := range losers {
			candidate := getter(loser)
			if populatedJSONText(candidate) {
				*dest = candidate
				merged = append(merged, name)
				return
			}
		}
	}

	fillString("name", &winner.Name, func(c companyRow) sql.NullString { return c.Name })
	fillString("slug", &winner.Slug, func(c companyRow) sql.NullString { return c.Slug })
	fillString("tagline", &winner.Tagline, func(c companyRow) sql.NullString { return c.Tagline })
	fillString("founded_year", &winner.FoundedYear, func(c companyRow) sql.NullString { return c.FoundedYear })
	fillString("home_page_url", &winner.HomePageURL, func(c companyRow) sql.NullString { return c.HomePageURL })
	fillString("linkedin_url", &winner.LinkedInURL, func(c companyRow) sql.NullString { return c.LinkedInURL })
	fillBool("sponsors_h1b", &winner.SponsorsH1B, func(c companyRow) sql.NullBool { return c.SponsorsH1B })
	fillBool("sponsors_uk_skilled_worker_visa", &winner.SponsorsUKSkilledWorkerVisa, func(c companyRow) sql.NullBool { return c.SponsorsUKSkilledWorkerVisa })
	fillString("employee_range", &winner.EmployeeRange, func(c companyRow) sql.NullString { return c.EmployeeRange })
	fillString("profile_pic_url", &winner.ProfilePicURL, func(c companyRow) sql.NullString { return c.ProfilePicURL })
	fillString("tagline_brazil", &winner.TaglineBrazil, func(c companyRow) sql.NullString { return c.TaglineBrazil })
	fillString("tagline_france", &winner.TaglineFrance, func(c companyRow) sql.NullString { return c.TaglineFrance })
	fillString("tagline_germany", &winner.TaglineGermany, func(c companyRow) sql.NullString { return c.TaglineGermany })
	fillString("chatgpt_description", &winner.ChatGPTDescription, func(c companyRow) sql.NullString { return c.ChatGPTDescription })
	fillString("linkedin_description", &winner.LinkedInDescription, func(c companyRow) sql.NullString { return c.LinkedInDescription })
	fillString("chatgpt_description_brazil", &winner.ChatGPTDescriptionBrazil, func(c companyRow) sql.NullString { return c.ChatGPTDescriptionBrazil })
	fillString("chatgpt_description_france", &winner.ChatGPTDescriptionFrance, func(c companyRow) sql.NullString { return c.ChatGPTDescriptionFrance })
	fillString("chatgpt_description_germany", &winner.ChatGPTDescriptionGermany, func(c companyRow) sql.NullString { return c.ChatGPTDescriptionGermany })
	fillString("linkedin_description_brazil", &winner.LinkedInDescriptionBrazil, func(c companyRow) sql.NullString { return c.LinkedInDescriptionBrazil })
	fillString("linkedin_description_france", &winner.LinkedInDescriptionFrance, func(c companyRow) sql.NullString { return c.LinkedInDescriptionFrance })
	fillString("linkedin_description_germany", &winner.LinkedInDescriptionGermany, func(c companyRow) sql.NullString { return c.LinkedInDescriptionGermany })
	fillJSON("funding_data", &winner.FundingData, func(c companyRow) sql.NullString { return c.FundingData })
	fillJSON("chatgpt_industries", &winner.ChatGPTIndustries, func(c companyRow) sql.NullString { return c.ChatGPTIndustries })
	fillJSON("industry_specialities", &winner.IndustrySpecialities, func(c companyRow) sql.NullString { return c.IndustrySpecialities })
	fillJSON("industry_specialities_brazil", &winner.IndustrySpecialitiesBrazil, func(c companyRow) sql.NullString { return c.IndustrySpecialitiesBrazil })
	fillJSON("industry_specialities_france", &winner.IndustrySpecialitiesFrance, func(c companyRow) sql.NullString { return c.IndustrySpecialitiesFrance })
	fillJSON("industry_specialities_germany", &winner.IndustrySpecialitiesGermany, func(c companyRow) sql.NullString { return c.IndustrySpecialitiesGermany })

	return merged
}

func mergeExternalCompanyIDs(current sql.NullString, losers []companyRow) sql.NullString {
	ordered := make([]string, 0, len(losers)+1)
	seen := map[string]struct{}{}
	appendParts := func(raw string) {
		for _, part := range strings.Split(raw, ",") {
			normalized := normalizeExternalCompanyIDToken(part)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			ordered = append(ordered, normalized)
		}
	}

	appendParts(current.String)
	for _, loser := range losers {
		appendParts(loser.ExternalCompanyID.String)
	}

	if len(ordered) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: strings.Join(ordered, ","), Valid: true}
}

func normalizeExternalCompanyIDToken(raw string) string {
	normalized := normalizeString(strings.TrimSpace(raw))
	normalized = strings.TrimPrefix(normalized, externalCompanyIDPrefix)
	normalized = strings.TrimSuffix(normalized, externalCompanyIDSuffix)
	if normalized == "" {
		return ""
	}
	return externalCompanyIDPrefix + normalized + externalCompanyIDSuffix
}

func populatedString(value sql.NullString) bool {
	return normalizeString(value.String) != ""
}

func populatedJSONText(value sql.NullString) bool {
	if !value.Valid {
		return false
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" || trimmed == "null" {
		return false
	}
	if trimmed == "{}" || trimmed == "[]" {
		return false
	}
	var anyValue any
	if err := json.Unmarshal([]byte(trimmed), &anyValue); err == nil {
		switch typed := anyValue.(type) {
		case []any:
			return len(typed) > 0
		case map[string]any:
			return len(typed) > 0
		}
	}
	return true
}

func nullStringValue(value sql.NullString) any {
	if !populatedString(value) {
		return nil
	}
	return normalizeString(value.String)
}

func nullBoolValue(value sql.NullBool) any {
	if !value.Valid {
		return nil
	}
	return value.Bool
}

func nullJSONTextValue(value sql.NullString) any {
	if !populatedJSONText(value) {
		return nil
	}
	return strings.TrimSpace(value.String)
}
