package locationnorm

import (
	_ "embed"
	"encoding/json"
	"regexp"
	"sort"
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
	Regions struct {
		ByName  map[string][]string `json:"by_name"`
		Aliases map[string]string   `json:"aliases"`
	} `json:"regions"`
	USStates struct {
		ByAbbreviation map[string]string `json:"by_abbreviation"`
		ByName         map[string]string `json:"by_name"`
	} `json:"us_states"`
}

var (
	loadOnce sync.Once
	data     dataset
)

const (
	regionKeyWorldwide = "WORLDWIDE"
)

var preservedRegionTokens = map[string]struct{}{
	"APAC": {},
	"EMEA": {},
	"LATAM": {},
	"MENA": {},
	"USA": {},
	"US":   {},
}

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
	parts := strings.Fields(strings.ToLower(value))
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}

func NormalizeCountryName(value string) string {
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
	if region := NormalizeRegionName(normalized); region != "" {
		return region
	}

	alphaCode := regexp.MustCompile(`[^A-Z]`).ReplaceAllString(key, "")
	if len(alphaCode) >= 2 && len(alphaCode) <= 3 && alphaCode == compact {
		return ""
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

func USStateNames() []string {
	load()
	seen := map[string]string{}
	for _, value := range data.USStates.ByName {
		cleaned := value
		if cleaned == "" {
			continue
		}
		normalized := strings.ToLower(cleaned)
		if _, ok := seen[normalized]; !ok {
			seen[normalized] = cleaned
		}
	}
	out := make([]string, 0, len(seen))
	for _, name := range seen {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func CountryNames() []string {
	load()
	seen := map[string]string{}
	for _, value := range data.Countries.ByName {
		cleaned := value
		if cleaned == "" {
			continue
		}
		normalized := strings.ToLower(cleaned)
		if _, ok := seen[normalized]; !ok {
			seen[normalized] = cleaned
		}
	}
	out := make([]string, 0, len(seen))
	for _, name := range seen {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func NormalizeRegionName(value string) string {
	load()
	key := normalizeKey(value)
	if key == "" {
		return ""
	}
	if alias := data.Regions.Aliases[key]; alias != "" {
		key = alias
	}
	if _, ok := data.Regions.ByName[key]; !ok && key != regionKeyWorldwide {
		return ""
	}
	return regionDisplayName(key)
}

func RegionCountries(value string) []string {
	load()
	key := normalizeKey(value)
	if key == "" {
		return nil
	}
	if alias := data.Regions.Aliases[key]; alias != "" {
		key = alias
	}
	if key == regionKeyWorldwide {
		return CountryNames()
	}
	rawCountries := data.Regions.ByName[key]
	if len(rawCountries) == 0 {
		return nil
	}
	out := make([]string, 0, len(rawCountries))
	seen := map[string]struct{}{}
	for _, rawCountry := range rawCountries {
		country := NormalizeCountryName(rawCountry)
		if country == "" {
			continue
		}
		normalized := strings.ToLower(country)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, country)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func RegionNamesForCountry(value string) []string {
	load()
	country := NormalizeCountryName(value)
	if country == "" {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	addRegion := func(regionKey string) {
		display := regionDisplayName(regionKey)
		if display == "" {
			return
		}
		normalized := strings.ToLower(display)
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		out = append(out, display)
	}
	for regionKey, rawCountries := range data.Regions.ByName {
		if regionKey == regionKeyWorldwide {
			continue
		}
		for _, rawCountry := range rawCountries {
			if NormalizeCountryName(rawCountry) == country {
				addRegion(regionKey)
				break
			}
		}
	}
	addRegion(regionKeyWorldwide)
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func RegionParentNames(value string) []string {
	load()
	key := normalizeKey(value)
	if key == "" {
		return nil
	}
	if alias := data.Regions.Aliases[key]; alias != "" {
		key = alias
	}
	childCountries := RegionCountries(key)
	if len(childCountries) == 0 {
		return nil
	}
	childSet := map[string]struct{}{}
	for _, country := range childCountries {
		childSet[strings.ToLower(country)] = struct{}{}
	}
	out := []string{}
	seen := map[string]struct{}{}
	addRegion := func(regionKey string) {
		display := regionDisplayName(regionKey)
		if display == "" {
			return
		}
		normalized := strings.ToLower(display)
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		out = append(out, display)
	}
	for regionKey := range data.Regions.ByName {
		if regionKey == key || regionKey == regionKeyWorldwide {
			continue
		}
		parentCountries := RegionCountries(regionKey)
		if len(parentCountries) < len(childCountries) {
			continue
		}
		parentSet := map[string]struct{}{}
		for _, country := range parentCountries {
			parentSet[strings.ToLower(country)] = struct{}{}
		}
		isParent := true
		for country := range childSet {
			if _, ok := parentSet[country]; !ok {
				isParent = false
				break
			}
		}
		if isParent {
			addRegion(regionKey)
		}
	}
	if key != regionKeyWorldwide {
		addRegion(regionKeyWorldwide)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func regionDisplayName(key string) string {
	if key == "" {
		return ""
	}
	if !strings.Contains(key, " ") && len(key) <= 5 {
		return key
	}
	parts := strings.Fields(strings.ToLower(key))
	rawParts := strings.Fields(key)
	for i := range parts {
		if i < len(rawParts) {
			if _, ok := preservedRegionTokens[rawParts[i]]; ok {
				parts[i] = rawParts[i]
				continue
			}
		}
		if i < len(rawParts) && len(rawParts[i]) <= 5 && rawParts[i] == strings.ToUpper(rawParts[i]) && len(rawParts) == 1 {
			parts[i] = rawParts[i]
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}
