package email

import (
	"fmt"
	"html"
	"strings"
	"time"
)

type MarketingJob struct {
	Title    string
	Company  string
	URL      string
	PostedAt string
}

type MarketingEmailData struct {
	SiteName       string
	SiteURL        string
	SiteLogoURL    string
	FirstName      string
	BrowseJobsURL  string
	ManagePrefsURL string
	UnsubscribeURL string
	DailyNewJobs   int
	WeeklyNewJobs  int
	Jobs           []MarketingJob
}

func (s *Service) BuildMarketingEmailHTML(data MarketingEmailData) string {
	templateBody, err := templates.ReadFile("templates/marketing_email_light.html")
	if err != nil {
		return "<html><body><h2>" + data.SiteName + "</h2><p>Browse jobs at " + data.BrowseJobsURL + "</p></body></html>"
	}
	firstName := strings.TrimSpace(data.FirstName)
	if firstName == "" {
		firstName = "there"
	}
	jobsBlock := buildJobsBlockHTML(data.Jobs, true)
	replacer := strings.NewReplacer(
		"__SITE_LOGO_URL__", html.EscapeString(data.SiteLogoURL),
		"__SITE_URL__", html.EscapeString(data.SiteURL),
		"__FIRST_NAME__", html.EscapeString(firstName),
		"__DAILY_NEW_SOFTWARE_ENGINEER_JOBS__", fmt.Sprintf("%d", data.DailyNewJobs),
		"__WEEKLY_NEW_SOFTWARE_ENGINEER_JOBS__", fmt.Sprintf("%d", data.WeeklyNewJobs),
		"__JOBS_BLOCK__", jobsBlock,
		"__BROWSE_JOBS_URL__", html.EscapeString(data.BrowseJobsURL),
		"__MANAGE_PREFERENCES_URL__", html.EscapeString(data.ManagePrefsURL),
		"__UNSUBSCRIBE_URL__", html.EscapeString(data.UnsubscribeURL),
	)
	return replacer.Replace(string(templateBody))
}

func (s *Service) BuildMarketingEmailText(data MarketingEmailData) string {
	firstName := strings.TrimSpace(data.FirstName)
	if firstName == "" {
		firstName = "there"
	}
	lines := []string{
		"Hi " + firstName + ",",
		"",
		"Here are some new US remote jobs for you:",
		"",
	}
	for _, job := range data.Jobs {
		line := "- " + job.Title
		if job.Company != "" {
			line += " @ " + job.Company
		}
		if job.URL != "" {
			line += " (" + job.URL + ")"
		}
		lines = append(lines, line)
	}
	lines = append(lines, "", "Browse more jobs: "+data.BrowseJobsURL)
	return strings.Join(lines, "\r\n")
}

func (s *Service) SendMarketingEmail(toEmail string, data MarketingEmailData) error {
	subject := data.SiteName + " - new US remote jobs for you"
	if strings.TrimSpace(data.SiteName) == "" {
		subject = "GoApplyJob - new US remote jobs for you"
	}
	htmlContent := s.BuildMarketingEmailHTML(data)
	textContent := s.BuildMarketingEmailText(data)
	return s.SendEmail(toEmail, subject, textContent, htmlContent)
}

func buildJobsBlockHTML(jobs []MarketingJob, lightTheme bool) string {
	if len(jobs) == 0 {
		if lightTheme {
			return `<p style="margin:0 0 12px;font-size:14px;line-height:1.7;color:#64748b;">No matching jobs yet. Check the latest listings to stay ahead.</p>`
		}
		return `<p style="margin:0 0 12px;font-size:14px;line-height:1.7;color:#cbd5e1;">No matching jobs yet. Check the latest listings to stay ahead.</p>`
	}
	var builder strings.Builder
	for _, job := range jobs {
		title := html.EscapeString(strings.TrimSpace(job.Title))
		if title == "" {
			title = "New role"
		}
		company := html.EscapeString(strings.TrimSpace(job.Company))
		posted := strings.TrimSpace(job.PostedAt)
		if posted != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, posted); err == nil {
				posted = parsed.Format("Jan 2, 2006")
			}
			posted = html.EscapeString(posted)
		}
		jobURL := html.EscapeString(strings.TrimSpace(job.URL))
		if lightTheme {
			builder.WriteString(`<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:12px;background:#ffffff;margin-bottom:12px;">`)
			builder.WriteString(`<tr><td style="padding:14px 16px;">`)
			if jobURL != "" {
				builder.WriteString(`<a href="` + jobURL + `" style="color:#0284c7;text-decoration:none;font-size:15px;font-weight:700;">` + title + `</a>`)
			} else {
				builder.WriteString(`<div style="color:#0284c7;font-size:15px;font-weight:700;">` + title + `</div>`)
			}
			if company != "" {
				builder.WriteString(`<div style="margin-top:6px;font-size:13px;color:#475569;">` + company + `</div>`)
			}
			if posted != "" {
				builder.WriteString(`<div style="margin-top:4px;font-size:12px;color:#64748b;">Posted ` + posted + `</div>`)
			}
			builder.WriteString(`</td></tr></table>`)
			continue
		}
		builder.WriteString(`<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #1f2937;border-radius:12px;background:#0b1220;margin-bottom:12px;">`)
		builder.WriteString(`<tr><td style="padding:14px 16px;">`)
		if jobURL != "" {
			builder.WriteString(`<a href="` + jobURL + `" style="color:#67e8f9;text-decoration:none;font-size:15px;font-weight:700;">` + title + `</a>`)
		} else {
			builder.WriteString(`<div style="color:#67e8f9;font-size:15px;font-weight:700;">` + title + `</div>`)
		}
		if company != "" {
			builder.WriteString(`<div style="margin-top:6px;font-size:13px;color:#cbd5e1;">` + company + `</div>`)
		}
		if posted != "" {
			builder.WriteString(`<div style="margin-top:4px;font-size:12px;color:#94a3b8;">Posted ` + posted + `</div>`)
		}
		builder.WriteString(`</td></tr></table>`)
	}
	return builder.String()
}
