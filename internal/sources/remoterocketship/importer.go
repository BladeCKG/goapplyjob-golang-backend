package remoterocketship

import (
	"encoding/xml"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	xmlDecl             = `<?xml version="1.0" encoding="UTF-8"?>`
	namespaceURLSetOpen = `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`
	rootCloseTag        = `</urlset>`
)

var sitemapURLBlockPattern = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)

type SitemapRow struct {
	URL      string
	PostDate time.Time
}

type xmlURL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func ExtractCompleteURLBlocks(xmlText string) []string {
	return sitemapURLBlockPattern.FindAllString(xmlText, -1)
}

func ExtractRowFromURLBlock(block string) (string, string, bool) {
	var row xmlURL
	if err := xml.Unmarshal([]byte(block), &row); err != nil {
		return "", "", false
	}
	loc := strings.TrimSpace(row.Loc)
	lastmod := strings.TrimSpace(row.LastMod)
	if loc == "" || lastmod == "" {
		return "", "", false
	}
	return loc, lastmod, true
}

func IterSitemapRowsText(xmlText string) [][2]string {
	blocks := ExtractCompleteURLBlocks(xmlText)
	rows := make([][2]string, 0, len(blocks))
	for _, block := range blocks {
		loc, lastmod, ok := ExtractRowFromURLBlock(block)
		if ok {
			rows = append(rows, [2]string{loc, lastmod})
		}
	}
	return rows
}

func ParseRowsFromXMLText(xmlText string) ([]SitemapRow, int) {
	rows := IterSitemapRowsText(xmlText)
	parsed := make([]SitemapRow, 0, len(rows))
	skippedInvalid := 0
	for _, row := range rows {
		postDate, err := normalizeDBDatetime(row[1])
		if err != nil {
			skippedInvalid++
			continue
		}
		parsed = append(parsed, SitemapRow{URL: row[0], PostDate: postDate})
	}
	return parsed, skippedInvalid
}

func RowsListToXML(rows []SitemapRow) string {
	parts := []string{xmlDecl, namespaceURLSetOpen}
	for _, row := range rows {
		parts = append(parts,
			"  <url>",
			"    <loc>"+escapeXML(row.URL)+"</loc>",
			"    <lastmod>"+row.PostDate.Format(time.RFC3339)+"</lastmod>",
			"  </url>",
		)
	}
	parts = append(parts, rootCloseTag)
	return strings.Join(parts, "\n") + "\n"
}

func RowsMapToXML(rows map[string]time.Time) string {
	type pair struct {
		url      string
		postDate time.Time
	}
	ordered := make([]pair, 0, len(rows))
	for url, postDate := range rows {
		ordered = append(ordered, pair{url: url, postDate: postDate})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].postDate.After(ordered[j].postDate) })
	parts := []string{xmlDecl, namespaceURLSetOpen}
	for _, row := range ordered {
		parts = append(parts,
			"  <url>",
			"    <loc>"+escapeXML(row.url)+"</loc>",
			"    <lastmod>"+row.postDate.Format(time.RFC3339)+"</lastmod>",
			"  </url>",
		)
	}
	parts = append(parts, rootCloseTag)
	return strings.Join(parts, "\n") + "\n"
}

func normalizeDBDatetime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("empty")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339, value)
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", `'`, "&apos;")
	return replacer.Replace(value)
}
