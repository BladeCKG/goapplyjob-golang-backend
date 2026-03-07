package builtin

import (
	"encoding/json"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var publishedDateByJobIDRegex = regexp.MustCompile(`(?is)\{[^{}]*['"]id['"]\s*:\s*(\d+)[^{}]*['"]published_date['"]\s*:\s*['"]([^'"]+)['"][^{}]*\}`)

func ExtractJobListings(htmlText string) []map[string]any {
	collectionPage, itemList := findItemListLD(htmlText)
	publishedDatesByJobID := extractPublishedDatesByJobID(htmlText)
	_ = collectionPage

	items, _ := itemList["itemListElement"].([]any)
	if len(items) == 0 {
		return []map[string]any{}
	}

	parsed := make([]map[string]any, 0, len(items))
	for _, item := range items {
		node, _ := item.(map[string]any)
		if node == nil {
			continue
		}
		jobURL := stringValue(node["url"])
		externalJobID := extractExternalJobID(jobURL, "")
		postDate := ""
		if externalJobIDInt, ok := externalJobID.(int); ok {
			postDate = publishedDatesByJobID[externalJobIDInt]
		}
		normalizedPostDate := normalizePostDate(postDate)
		parsed = append(parsed, map[string]any{
			"url":          valueOrNil(jobURL),
			"post_date":    valueOrNil(firstNonEmpty(normalizedPostDate, time.Now().UTC().Format(time.RFC3339Nano))),
			"is_ready":     false,
			"is_skippable": false,
			"is_parsed":    false,
			"retry_count":  0,
			"raw_json":     nil,
		})
	}
	return parsed
}

func findItemListLD(htmlText string) (map[string]any, map[string]any) {
	collectionPage := map[string]any{}
	itemList := map[string]any{}
	for _, match := range scriptLDPattern.FindAllStringSubmatch(htmlText, -1) {
		raw := strings.TrimSpace(match[1])
		if raw == "" {
			continue
		}
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		nodes := flattenLDNodes(payload)
		for _, node := range nodes {
			switch stringValue(node["@type"]) {
			case "CollectionPage":
				collectionPage = node
			case "ItemList":
				itemList = node
			}
		}
	}
	return collectionPage, itemList
}

func flattenLDNodes(payload any) []map[string]any {
	switch item := payload.(type) {
	case []any:
		out := make([]map[string]any, 0, len(item))
		for _, entry := range item {
			if node, ok := entry.(map[string]any); ok {
				out = append(out, node)
			}
		}
		return out
	case map[string]any:
		graph, _ := item["@graph"].([]any)
		if len(graph) == 0 {
			return []map[string]any{item}
		}
		out := make([]map[string]any, 0, len(graph))
		for _, entry := range graph {
			if node, ok := entry.(map[string]any); ok {
				out = append(out, node)
			}
		}
		return out
	default:
		return nil
	}
}

func extractPublishedDatesByJobID(htmlText string) map[int]string {
	decoded := html.UnescapeString(htmlText)
	match := regexp.MustCompile(`(?is)logBuiltinTrackEvent\(\s*['"]job_board_view['"],\s*\{.*?['"]jobs['"]\s*:\s*\[(.*?)\]\s*,\s*['"]filters['"]`).FindStringSubmatch(decoded)
	if len(match) < 2 {
		return map[int]string{}
	}
	out := map[int]string{}
	for _, item := range publishedDateByJobIDRegex.FindAllStringSubmatch(match[1], -1) {
		id, err := strconv.Atoi(item[1])
		if err != nil {
			continue
		}
		value := strings.TrimSpace(item[2])
		if value != "" {
			out[id] = value
		}
	}
	return out
}

func normalizePostDate(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05", value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return value
}
