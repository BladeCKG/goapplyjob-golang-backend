package techstack

import (
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type catalogEntry struct {
	Canonical     string   `json:"canonical"`
	Aliases       []string `json:"aliases"`
	CaseSensitive bool     `json:"caseSensitive"`
}

type matcherEntry struct {
	canonical string
	patterns  []*regexp.Regexp
}

type matchCandidate struct {
	start     int
	end       int
	canonical string
}

//go:embed catalog.json
var catalogJSON []byte

var (
	catalogMu         sync.RWMutex
	defaultMatchers   []matcherEntry
	defaultCanonicals map[string]string
	matchersByURL     = map[string][]matcherEntry{}
	canonicalsByURL   = map[string]map[string]string{}
	catalogETagByURL  = map[string]string{}
)

type Extractor struct {
	catalogURL string
}

func NewExtractor(catalogURL string) Extractor {
	return Extractor{catalogURL: strings.TrimSpace(catalogURL)}
}

func (e Extractor) Extract(text string) []string {
	matchers := getMatchers(e.catalogURL)
	if strings.TrimSpace(text) == "" {
		return []string{}
	}
	candidates := make([]matchCandidate, 0, len(matchers))
	for _, entry := range matchers {
		var best *matchCandidate
		for _, pattern := range entry.patterns {
			match := pattern.FindStringIndex(text)
			if match != nil {
				aliasStart, aliasEnd := trimBoundaryMatch(text, match[0], match[1])
				candidate := matchCandidate{
					start:     aliasStart,
					end:       aliasEnd,
					canonical: entry.canonical,
				}
				if best == nil || candidate.start < best.start || (candidate.start == best.start && (candidate.end-candidate.start) > (best.end-best.start)) {
					best = &candidate
				}
			}
		}
		if best != nil {
			candidates = append(candidates, *best)
		}
	}
	if len(candidates) == 0 {
		return []string{}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].start == candidates[j].start {
			return (candidates[i].end - candidates[i].start) > (candidates[j].end - candidates[j].start)
		}
		return candidates[i].start < candidates[j].start
	})

	results := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	lastCoveredEnd := -1
	for _, candidate := range candidates {
		if candidate.start < lastCoveredEnd {
			continue
		}
		if _, ok := seen[candidate.canonical]; ok {
			continue
		}
		seen[candidate.canonical] = struct{}{}
		results = append(results, candidate.canonical)
		lastCoveredEnd = candidate.end
	}
	return results
}

func (e Extractor) ExtractDescriptionRequirements(description, requirements string) []string {
	switch {
	case description == "" && requirements == "":
		return []string{}
	case description == "":
		return e.Extract(requirements)
	case requirements == "":
		return e.Extract(description)
	default:
		return e.Extract(description + "\n\n" + requirements)
	}
}

func (e Extractor) ExactCanonical(value string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "", false
	}
	canonicals := getCanonicals(e.catalogURL)
	canonical, ok := canonicals[normalized]
	return canonical, ok
}

func getMatchers(catalogURL string) []matcherEntry {
	if catalogURL == "" {
		catalogMu.RLock()
		if defaultMatchers != nil && defaultCanonicals != nil {
			matchers := defaultMatchers
			catalogMu.RUnlock()
			return matchers
		}
		catalogMu.RUnlock()

		loaded, canonicals, err := buildCatalog(catalogJSON)
		if err != nil {
			return []matcherEntry{}
		}

		catalogMu.Lock()
		defaultMatchers = loaded
		defaultCanonicals = canonicals
		catalogMu.Unlock()
		return loaded
	}

	catalogMu.RLock()
	prevETag := catalogETagByURL[catalogURL]
	catalogMu.RUnlock()

	req, err := http.NewRequest(http.MethodGet, catalogURL, nil)
	if err != nil {
		return []matcherEntry{}
	}
	if prevETag != "" {
		req.Header.Set("If-None-Match", prevETag)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return []matcherEntry{}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		catalogMu.RLock()
		matchers := matchersByURL[catalogURL]
		catalogMu.RUnlock()
		if matchers == nil {
			return []matcherEntry{}
		}
		return matchers
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return []matcherEntry{}
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return []matcherEntry{}
	}
	loaded, canonicals, err := buildCatalog(raw)
	if err != nil {
		return []matcherEntry{}
	}

	catalogMu.Lock()
	matchersByURL[catalogURL] = loaded
	canonicalsByURL[catalogURL] = canonicals
	catalogETagByURL[catalogURL] = resp.Header.Get("ETag")
	catalogMu.Unlock()
	return loaded
}

func getCanonicals(catalogURL string) map[string]string {
	_ = getMatchers(catalogURL)
	catalogMu.RLock()
	defer catalogMu.RUnlock()
	if catalogURL == "" {
		if defaultCanonicals == nil {
			return map[string]string{}
		}
		return defaultCanonicals
	}
	if canonicals := canonicalsByURL[catalogURL]; canonicals != nil {
		return canonicals
	}
	return map[string]string{}
}

func buildCatalog(raw []byte) ([]matcherEntry, map[string]string, error) {
	var entries []catalogEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, nil, err
	}

	loaded := make([]matcherEntry, 0, len(entries))
	canonicals := make(map[string]string, len(entries))
	for _, entry := range entries {
		canonical := strings.TrimSpace(entry.Canonical)
		if canonical == "" {
			continue
		}
		canonicals[strings.ToLower(canonical)] = canonical
		aliasSet := map[string]struct{}{}
		for _, alias := range entry.Aliases {
			trimmed := strings.TrimSpace(alias)
			if trimmed != "" {
				aliasSet[trimmed] = struct{}{}
			}
		}
		if len(aliasSet) == 0 {
			continue
		}
		aliases := make([]string, 0, len(aliasSet))
		for alias := range aliasSet {
			aliases = append(aliases, alias)
		}
		sort.SliceStable(aliases, func(i, j int) bool {
			if len(aliases[i]) == len(aliases[j]) {
				return aliases[i] < aliases[j]
			}
			return len(aliases[i]) > len(aliases[j])
		})

		patterns := make([]*regexp.Regexp, 0, len(aliases))
		for _, alias := range aliases {
			patterns = append(patterns, regexp.MustCompile(buildAliasPattern(alias, entry.CaseSensitive)))
		}
		loaded = append(loaded, matcherEntry{
			canonical: canonical,
			patterns:  patterns,
		})
	}
	return loaded, canonicals, nil
}

func buildAliasPattern(alias string, caseSensitive bool) string {
	prefix := ""
	if !caseSensitive {
		prefix = `(?i)`
	}
	return prefix + `(^|[^[:alnum:]])` + regexp.QuoteMeta(alias) + `($|[^[:alnum:]])`
}

func trimBoundaryMatch(text string, start, end int) (int, int) {
	for start < end {
		r := rune(text[start])
		if isAlphaNumericByte(r) {
			break
		}
		start++
	}
	for end > start {
		r := rune(text[end-1])
		if isAlphaNumericByte(r) || isTechSymbolByte(r) {
			break
		}
		end--
	}
	return start, end
}

func isAlphaNumericByte(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isTechSymbolByte(r rune) bool {
	switch r {
	case '+', '#', '.', '-':
		return true
	default:
		return false
	}
}
