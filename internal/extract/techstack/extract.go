package techstack

import (
	_ "embed"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"sync"
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
	loadCatalogOnce sync.Once
	matchers        []matcherEntry
)

func Extract(text string) []string {
	loadCatalogOnce.Do(loadCatalog)
	if strings.TrimSpace(text) == "" {
		return []string{}
	}
	candidates := make([]matchCandidate, 0, len(matchers))
	for _, entry := range matchers {
		var best *matchCandidate
		for _, pattern := range entry.patterns {
			match := pattern.FindStringIndex(text)
			if match != nil {
				candidate := matchCandidate{
					start:     match[0],
					end:       match[1],
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

func ExtractDescriptionRequirements(description, requirements string) []string {
	switch {
	case description == "" && requirements == "":
		return []string{}
	case description == "":
		return Extract(requirements)
	case requirements == "":
		return Extract(description)
	default:
		return Extract(description + "\n\n" + requirements)
	}
}

func loadCatalog() {
	var entries []catalogEntry
	if err := json.Unmarshal(catalogJSON, &entries); err != nil {
		matchers = []matcherEntry{}
		return
	}

	loaded := make([]matcherEntry, 0, len(entries))
	for _, entry := range entries {
		canonical := strings.TrimSpace(entry.Canonical)
		if canonical == "" {
			continue
		}
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
	matchers = loaded
}

func buildAliasPattern(alias string, caseSensitive bool) string {
	prefix := ""
	if !caseSensitive {
		prefix = `(?i)`
	}
	return prefix + `(^|[^[:alnum:]])` + regexp.QuoteMeta(alias) + `($|[^[:alnum:]])`
}
