package locationnorm

import (
	_ "embed"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
)

//go:embed location-normalization.json
var datasetJSON []byte

type dataset struct {
	Countries struct {
		ByAlpha2 map[string]string `json:"by_alpha2"`
		ByAlpha3 map[string]string `json:"by_alpha3"`
		ByName   map[string]string `json:"by_name"`
		Aliases  map[string]string `json:"aliases"`
	} `json:"countries"`
	USStates struct {
		ByAbbreviation map[string]string `json:"by_abbreviation"`
		ByName         map[string]string `json:"by_name"`
	} `json:"us_states"`
}

var (
	loadOnce sync.Once
	data     dataset
)

func load() {
	loadOnce.Do(func() {
		_ = json.Unmarshal(datasetJSON, &data)
	})
}

func normalizeKey(value string) string {
	re := regexp.MustCompile(`[^A-Z0-9]+`)
	return strings.TrimSpace(re.ReplaceAllString(strings.ToUpper(value), " "))
}

func title(value string) string {
	parts := strings.Fields(strings.ToLower(strings.TrimSpace(value)))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}

func NormalizeCountryName(value string, allowFallbackTitleCase bool) string {
	normalized := strings.TrimSpace(regexp.MustCompile(`[\s_]+`).ReplaceAllString(value, " "))
	if normalized == "" {
		return ""
	}
	load()
	key := normalizeKey(strings.ReplaceAll(normalized, ".", " "))
	compact := strings.ReplaceAll(key, " ", "")

	if alias := data.Countries.Aliases[key]; alias != "" {
		return alias
	}
	if byAlpha2 := data.Countries.ByAlpha2[compact]; byAlpha2 != "" {
		return byAlpha2
	}
	if byAlpha3 := data.Countries.ByAlpha3[compact]; byAlpha3 != "" {
		return byAlpha3
	}
	if byName := data.Countries.ByName[key]; byName != "" {
		return byName
	}

	alphaCode := regexp.MustCompile(`[^A-Z]`).ReplaceAllString(key, "")
	if len(alphaCode) >= 2 && len(alphaCode) <= 3 && alphaCode == compact {
		return ""
	}

	if allowFallbackTitleCase {
		return title(normalized)
	}
	return ""
}

func NormalizeUSStateName(value string) string {
	normalized := strings.TrimSpace(regexp.MustCompile(`[\s_]+`).ReplaceAllString(value, " "))
	if normalized == "" {
		return ""
	}
	load()
	key := normalizeKey(normalized)
	compact := strings.ReplaceAll(key, " ", "")
	if byAbbreviation := data.USStates.ByAbbreviation[compact]; byAbbreviation != "" {
		return byAbbreviation
	}
	if byName := data.USStates.ByName[key]; byName != "" {
		return byName
	}
	return title(normalized)
}
