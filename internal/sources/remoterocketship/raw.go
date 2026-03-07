package remoterocketship

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
)

var scriptJSONBlockPattern = regexp.MustCompile(`(?is)<script[^>]*type=['"]application/json['"][^>]*>(.*?)</script>`)

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

func ParseHTML(html string) map[string]any {
	blocks := scriptJSONBlockPattern.FindAllStringSubmatch(html, -1)
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
	return jobData
}
