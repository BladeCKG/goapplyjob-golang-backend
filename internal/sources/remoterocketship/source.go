package remoterocketship

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/locationnorm"
)

const (
	Source      = "remoterocketship"
	PayloadType = "delta_xml"
)

var (
	lastmodPattern     = regexp.MustCompile(`(?is)<lastmod>\s*([^<]+?)\s*</lastmod>`)
	urlOpenPattern     = regexp.MustCompile(`(?is)<url(?:\s|>)`)
	urlBlockPattern    = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)
	urlSetClosePattern = regexp.MustCompile(`(?is)</urlset\s*>`)
	jsonBlockPattern   = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)
)

type SitemapRow struct {
	URL      string
	PostDate time.Time
}

type xmlURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func ToTargetJobURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	pathParts := strings.FieldsFunc(parsed.Path, func(r rune) bool { return r == '/' })
	if len(pathParts) >= 2 && pathParts[1] == "company" {
		pathParts = pathParts[1:]
	}
	trailingSlash := ""
	if strings.HasSuffix(parsed.Path, "/") {
		trailingSlash = "/"
	}
	parsed.Path = "/"
	if len(pathParts) > 0 {
		parsed.Path = "/" + strings.Join(pathParts, "/") + trailingSlash
	}
	return parsed.String()
}

func ParseRawHTML(htmlText, sourceUrl string) map[string]any {
	blocks := jsonBlockPattern.FindAllStringSubmatch(htmlText, -1)
	if len(blocks) == 0 {
		return map[string]any{}
	}
	lastBlock := strings.TrimSpace(blocks[len(blocks)-1][1])
	if lastBlock == "" {
		return map[string]any{}
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(lastBlock), &data); err != nil {
		return map[string]any{}
	}
	props, _ := data["props"].(map[string]any)
	pageProps, _ := props["pageProps"].(map[string]any)
	jobData, _ := pageProps["jobOpening"].(map[string]any)
	if jobData == nil {
		return map[string]any{}
	}
	jobData["locationCountries"] = []string{}
	if country := normalizeCountryToken(stringValue(jobData["location"])); country != "" {
		jobData["locationCountries"] = []string{country}
	}
	jobData["url"] = sourceUrl
	return jobData
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func normalizeCountryToken(value string) string {
	return locationnorm.NormalizeCountryName(value, true)
}

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	blocks := urlBlockPattern.FindAllString(bodyText, -1)
	rows := make([]map[string]any, 0, len(blocks))
	skipped := 0
	for _, block := range blocks {
		var row xmlURL
		if err := xml.Unmarshal([]byte(block), &row); err != nil {
			skipped++
			continue
		}
		postDate, err := normalizeTime(row.LastMod)
		if err != nil || strings.TrimSpace(row.Loc) == "" {
			skipped++
			continue
		}
		rows = append(rows, map[string]any{
			"url":       strings.TrimSpace(row.Loc),
			"post_date": postDate,
		})
	}
	return rows, skipped
}

func SerializeImportRows(rows []map[string]any) string {
	type pair struct {
		url      string
		postDate time.Time
	}
	ordered := make([]pair, 0, len(rows))
	for _, row := range rows {
		urlValue, _ := row["url"].(string)
		postDate, _ := row["post_date"].(time.Time)
		if urlValue == "" || postDate.IsZero() {
			continue
		}
		ordered = append(ordered, pair{url: urlValue, postDate: postDate})
	}
	var parts []string
	parts = append(parts, `<?xml version="1.0" encoding="UTF-8"?>`)
	parts = append(parts, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`)
	for _, row := range ordered {
		parts = append(parts, "  <url>")
		parts = append(parts, "    <loc>"+escapeXML(row.url)+"</loc>")
		parts = append(parts, "    <lastmod>"+row.postDate.UTC().Format(time.RFC3339Nano)+"</lastmod>")
		parts = append(parts, "  </url>")
	}
	parts = append(parts, `</urlset>`)
	return strings.Join(parts, "\n") + "\n"
}

func ExtractFirstLastmod(data []byte) string {
	match := lastmodPattern.FindSubmatch(data)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func ExtractLastLastmod(data []byte) string {
	matches := lastmodPattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return ""
	}
	return strings.TrimSpace(string(matches[len(matches)-1][1]))
}

func DeltaNewerThanLastmod(fullData []byte, previousFirstLastmod string) []byte {
	previousDT, err := normalizeTime(previousFirstLastmod)
	if err != nil {
		return fullData
	}
	blocks := make([][]byte, 0)
	for _, match := range urlBlockPattern.FindAll(fullData, -1) {
		blockLastmod := ExtractFirstLastmod(match)
		blockDT, err := normalizeTime(blockLastmod)
		if err != nil {
			continue
		}
		if blockDT.After(previousDT) {
			blocks = append(blocks, []byte(match))
		} else {
			break
		}
	}
	if len(blocks) == 0 {
		return []byte{}
	}
	firstURL := urlOpenPattern.FindIndex(fullData)
	if firstURL == nil {
		return []byte(strings.Join(byteBlocksToStrings(blocks), ""))
	}
	suffix := []byte{}
	if match := urlSetClosePattern.Find(fullData); len(match) > 0 {
		suffix = match
	}
	output := make([]byte, 0, len(fullData))
	output = append(output, fullData[:firstURL[0]]...)
	for _, block := range blocks {
		output = append(output, block...)
	}
	output = append(output, suffix...)
	return output
}

func parseISO(value string) string {
	if parsed, err := normalizeTime(value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func normalizeTime(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(value)); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02", strings.TrimSpace(value)); err == nil {
		dateOnly := parsed.UTC()
		now := time.Now().UTC()
		if dateOnly.Year() == now.Year() && dateOnly.YearDay() == now.YearDay() {
			return now, nil
		}
		return dateOnly, nil
	}
	return time.Time{}, errors.New("invalid time format")
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", `'`, "&apos;")
	return replacer.Replace(html.UnescapeString(value))
}

func byteBlocksToStrings(blocks [][]byte) []string {
	out := make([]string, 0, len(blocks))
	for _, block := range blocks {
		out = append(out, string(block))
	}
	return out
}
