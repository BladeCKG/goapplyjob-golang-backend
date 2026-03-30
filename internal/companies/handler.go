package companies

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	gensqlc "goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
)

type Handler struct {
	cfg  config.Config
	db   *database.DB
	auth *auth.Handler
	q    *gensqlc.Queries
}

type companySitemapItem struct {
	Slug              string  `json:"slug"`
	Name              *string `json:"name"`
	LatestJobPostedAt *string `json:"latest_job_posted_at"`
}

type companyListItem struct {
	Id                 int64    `json:"id"`
	Slug               string   `json:"slug"`
	Name               *string  `json:"name"`
	Tagline            *string  `json:"tagline"`
	Description        *string  `json:"description"`
	ProfilePicURL      *string  `json:"profile_pic_url"`
	HomePageURL        *string  `json:"home_page_url"`
	LinkedInURL        *string  `json:"linkedin_url"`
	EmployeeRange      *string  `json:"employee_range"`
	FoundedYear        *string  `json:"founded_year"`
	Industries         []string `json:"industries"`
	TotalJobs          int64    `json:"total_jobs"`
	LatestJobPostedAt  *string  `json:"latest_job_posted_at"`
}

type companyProfileItem struct {
	ID                 int64    `json:"id"`
	Slug               string   `json:"slug"`
	Name               *string  `json:"name"`
	Tagline            *string  `json:"tagline"`
	ProfilePicURL      *string  `json:"profile_pic_url"`
	HomePageURL        *string  `json:"home_page_url"`
	LinkedInURL        *string  `json:"linkedin_url"`
	EmployeeRange      *string  `json:"employee_range"`
	FoundedYear        *string  `json:"founded_year"`
	SponsorsH1B        *bool    `json:"sponsors_h1b"`
	Industries         []string `json:"industries"`
	TotalJobs          int64    `json:"total_jobs"`
	LatestJobPostedAt  *string  `json:"latest_job_posted_at"`
}

func pgTextString(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func NewHandler(cfg config.Config, db *database.DB, authHandler *auth.Handler) *Handler {
	return &Handler{
		cfg:  cfg,
		db:   db,
		auth: authHandler,
		q:    gensqlc.New(db.PGX),
	}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/companies", h.listCompanies)
	router.GET("/companies/sitemap", h.companiesSitemap)
	router.GET("/companies/count", h.companiesCount)
	router.GET("/companies/:companySlug", h.companyProfile)
}

func parseDelimitedQuery(value, sep string) []string {
	if value == "" {
		return nil
	}
	items := []string{}
	for _, item := range strings.Split(value, sep) {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func normalizeEmployeeRangeToken(value string) string {
	normalized := strings.TrimSpace(strings.ToLower(value))
	if normalized == "" {
		return ""
	}
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "-", ",")
	return normalized
}

func (h *Handler) listCompanies(c *gin.Context) {
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 20), 1)
	if perPage > 2000 {
		perPage = 2000
	}
	offset := (page - 1) * perPage

	nameFilter := strings.ToLower(strings.TrimSpace(c.Query("name")))
	industryFilter := strings.ToLower(strings.TrimSpace(c.Query("industry")))
	rangeTokensRaw := parseDelimitedQuery(c.Query("employee_ranges"), "|")
	rangeTokens := make([]string, 0, len(rangeTokensRaw))
	for _, token := range rangeTokensRaw {
		normalized := normalizeEmployeeRangeToken(token)
		if normalized != "" {
			rangeTokens = append(rangeTokens, normalized)
		}
	}

	where := []string{"pc.slug IS NOT NULL", "trim(pc.slug) <> ''"}
	args := []any{}
	argIndex := 1
	if nameFilter != "" {
		where = append(where, fmt.Sprintf("(lower(trim(pc.name)) LIKE $%d)", argIndex))
		args = append(args, "%"+nameFilter+"%")
		argIndex++
	}
	if industryFilter != "" {
		where = append(where, fmt.Sprintf(`EXISTS (
				SELECT 1
				FROM jsonb_array_elements_text(
					CASE
						WHEN jsonb_typeof(pc.industries::jsonb) = 'array' THEN pc.industries::jsonb
						ELSE '[]'::jsonb
					END
				) AS ind(value)
				WHERE lower(ind.value) = $%d
			)`, argIndex))
		args = append(args, industryFilter)
		argIndex++
	}
	if len(rangeTokens) > 0 {
		where = append(where, fmt.Sprintf("(replace(lower(coalesce(pc.employee_range, '')), '-', ',') = ANY($%d) OR lower(coalesce(pc.employee_range, '')) = ANY($%d))", argIndex, argIndex))
		args = append(args, rangeTokens)
		argIndex++
	}
	whereClause := strings.Join(where, " AND ")

	totalQuery := fmt.Sprintf(`
		SELECT count(*) FROM (
			SELECT pc.id
			FROM parsed_companies pc
			JOIN parsed_jobs pj ON pj.company_id = pc.id
			WHERE %s
			GROUP BY pc.id
		) sub`, whereClause)
	var totalCount int64
	if err := h.db.PGX.QueryRow(c.Request.Context(), totalQuery, args...).Scan(&totalCount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies"})
		return
	}

	limitArg := argIndex
	offsetArg := argIndex + 1
	args = append(args, int32(perPage), int32(offset))
	listQuery := fmt.Sprintf(`
		SELECT
				pc.id,
				pc.slug,
		       pc.name,
		       pc.tagline,
		       pc.chatgpt_description,
		       pc.linkedin_description,
		       pc.profile_pic_url,
		       pc.home_page_url,
		       pc.linkedin_url,
		       pc.employee_range,
		       pc.founded_year,
		       pc.industries::jsonb,
		       count(pj.id) AS total_jobs,
		       max(pj.created_at_source) AS latest_job_posted_at
		FROM parsed_companies pc
		JOIN parsed_jobs pj ON pj.company_id = pc.id
		WHERE %s
		GROUP BY pc.id, pc.slug, pc.name, pc.tagline, pc.chatgpt_description, pc.linkedin_description, pc.profile_pic_url, pc.home_page_url, pc.linkedin_url, pc.employee_range, pc.founded_year, pc.industries::jsonb
		ORDER BY max(pj.created_at_source) DESC, pc.id DESC
		LIMIT $%d OFFSET $%d`, whereClause, limitArg, offsetArg)

	rows, err := h.db.PGX.Query(c.Request.Context(), listQuery, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies"})
		return
	}
	defer rows.Close()

	items := []companyListItem{}
	for rows.Next() {
		var companyId int64
		var slug pgtype.Text
		var name pgtype.Text
		var tagline pgtype.Text
		var chatgptDescription pgtype.Text
		var linkedinDescription pgtype.Text
		var profilePic pgtype.Text
		var homePage pgtype.Text
		var linkedin pgtype.Text
		var employeeRange pgtype.Text
		var foundedYear pgtype.Text
		var industries []byte
		var totalJobs int64
		var latestJob pgtype.Timestamptz
		if err := rows.Scan(&companyId, &slug, &name, &tagline, &chatgptDescription, &linkedinDescription, &profilePic, &homePage, &linkedin, &employeeRange, &foundedYear, &industries, &totalJobs, &latestJob); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies"})
			return
		}
		slugValue := strings.TrimSpace(pgTextString(slug))
		if slugValue == "" {
			continue
		}
		industryValues := []string{}
		if len(industries) > 0 {
			_ = json.Unmarshal(industries, &industryValues)
		}
		description := pgTextPtr(chatgptDescription)
		if description == nil {
			description = pgTextPtr(linkedinDescription)
		}
		items = append(items, companyListItem{
			Id:                 companyId,
			Slug:               slugValue,
			Name:               pgTextPtr(name),
			Tagline:            pgTextPtr(tagline),
			Description:        description,
			ProfilePicURL:      pgTextPtr(profilePic),
			HomePageURL:        pgTextPtr(homePage),
			LinkedInURL:        pgTextPtr(linkedin),
			EmployeeRange:      pgTextPtr(employeeRange),
			FoundedYear:        pgTextPtr(foundedYear),
			Industries:         industryValues,
			TotalJobs:          totalJobs,
			LatestJobPostedAt:  timestamptzStringPtr(latestJob),
		})
	}
	if rows.Err() != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    totalCount,
		"items":    items,
	})
}

func (h *Handler) companiesSitemap(c *gin.Context) {
	page := max(parseIntDefault(c.Query("page"), 1), 1)
	perPage := max(parseIntDefault(c.Query("per_page"), 500), 1)
	if perPage > 50000 {
		perPage = 50000
	}
	offset := (page - 1) * perPage
	totalCount, err := h.q.CountCompaniesWithJobsForSitemap(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	rows, err := h.q.ListCompanySitemapPage(c.Request.Context(), gensqlc.ListCompanySitemapPageParams{
		Limit:  int32(perPage),
		Offset: int32(offset),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies sitemap"})
		return
	}
	items := []companySitemapItem{}
	for _, row := range rows {
		slugValue := strings.TrimSpace(pgTextString(row.Slug))
		if slugValue == "" {
			continue
		}
		items = append(items, companySitemapItem{
			Slug:              slugValue,
			Name:              pgTextPtr(row.Name),
			LatestJobPostedAt: timestamptzStringPtr(row.LatestJobPostedAt),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"page":     page,
		"per_page": perPage,
		"total":    totalCount,
		"items":    items,
	})
}

func (h *Handler) companiesCount(c *gin.Context) {
	totalCount, err := h.q.CountCompaniesWithJobsForSitemap(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load companies count"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"total": totalCount})
}

func (h *Handler) companyProfile(c *gin.Context) {
	slug := strings.ToLower(strings.TrimSpace(c.Param("companySlug")))
	if slug == "" {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	row, err := h.q.GetCompanyProfileBySlug(c.Request.Context(), pgtype.Text{String: slug, Valid: true})
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Company not found"})
		return
	}
	item := companyProfileItem{
		ID:            int64(row.ID),
		Slug:          strings.TrimSpace(pgTextString(row.Slug)),
		Name:          pgTextPtr(row.Name),
		Tagline:       pgTextPtr(row.Tagline),
		ProfilePicURL: pgTextPtr(row.ProfilePicUrl),
		HomePageURL:   pgTextPtr(row.HomePageUrl),
		LinkedInURL:   pgTextPtr(row.LinkedinUrl),
		EmployeeRange: pgTextPtr(row.EmployeeRange),
		FoundedYear:   pgTextPtr(row.FoundedYear),
		SponsorsH1B:   pgBoolPtr(row.SponsorsH1b),
	}
	if len(row.Industries) > 0 {
		_ = json.Unmarshal(row.Industries, &item.Industries)
	}
	stats, err := h.q.GetCompanyProfileStats(c.Request.Context(), pgtype.Int4{Int32: row.ID, Valid: true})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to load company profile"})
		return
	}
	item.TotalJobs = stats.TotalJobs
	item.LatestJobPostedAt = timestamptzStringPtr(stats.LatestJobPostedAt)
	c.JSON(http.StatusOK, item)
}

func pgTextPtr(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func pgBoolPtr(value pgtype.Bool) *bool {
	if !value.Valid {
		return nil
	}
	return &value.Bool
}

func timestamptzStringPtr(value pgtype.Timestamptz) *string {
	if !value.Valid {
		return nil
	}
	iso := value.Time.UTC().Format(time.RFC3339Nano)
	return &iso
}

func parseIntDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
