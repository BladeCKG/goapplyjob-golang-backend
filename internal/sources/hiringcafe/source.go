package hiringcafe

import (
	"encoding/json"
	"encoding/xml"
	"regexp"
	"strings"

	"goapplyjob-golang-backend/internal/sources/remoterocketship"
)

const (
	Source      = "hiringcafe"
	PayloadType = "delta_xml"
)

var (
	nextDataPattern = regexp.MustCompile(`(?is)<script[^>]*id=['"]__NEXT_DATA__['"][^>]*>(.*?)</script>`)
)

type sitemapIndex struct {
	Sitemaps []struct {
		Loc string `xml:"loc"`
	} `xml:"sitemap"`
}

func ToTargetJobURL(rawURL string) string {
	return rawURL
}

func ParseRawHTML(htmlText, sourceURL string) map[string]any {
	match := nextDataPattern.FindStringSubmatch(htmlText)
	if len(match) < 2 {
		return map[string]any{"url": sourceURL}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(match[1])), &payload); err != nil {
		return map[string]any{"url": sourceURL}
	}
	props, _ := payload["props"].(map[string]any)
	pageProps, _ := props["pageProps"].(map[string]any)
	job, _ := pageProps["job"].(map[string]any)
	if job == nil {
		return map[string]any{"url": sourceURL}
	}
	out := map[string]any{
		"url": sourceURL,
	}
	if id, ok := job["id"]; ok {
		out["id"] = id
	}
	if title, ok := job["title"]; ok {
		out["roleTitle"] = title
	}
	return out
}

func ParseImportRows(bodyText string) ([]map[string]any, int) {
	return remoterocketship.ParseImportRows(bodyText)
}

func SerializeImportRows(rows []map[string]any) string {
	return remoterocketship.SerializeImportRows(rows)
}

func ExtractTitleSitemapRows(indexXML string) []string {
	var parsed sitemapIndex
	if err := xml.Unmarshal([]byte(indexXML), &parsed); err != nil {
		return nil
	}
	out := make([]string, 0, len(parsed.Sitemaps))
	for _, sitemap := range parsed.Sitemaps {
		loc := strings.TrimSpace(sitemap.Loc)
		if loc == "" {
			continue
		}
		if !strings.Contains(loc, "/sitemaps/sitemap-titles-") {
			continue
		}
		out = append(out, loc)
	}
	return out
}
