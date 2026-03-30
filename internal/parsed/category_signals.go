package parsed

import (
	_ "embed"
	"encoding/json"
	"sync"
)

//go:embed category_signal_tokens.json
var categorySignalTokensJSON []byte

type categorySignalEntry struct {
	Category string                `json:"category"`
	Tokens   []categorySignalToken `json:"tokens"`
}

type categorySignalToken struct {
	Token  string  `json:"token"`
	Weight float64 `json:"weight"`
}

type categorySignalTerms struct {
	tokens map[string]float64
}

var (
	loadCategorySignalsOnce sync.Once
	categorySignalCatalog   map[string]categorySignalTerms
)

func loadCategorySignals() {
	var entries []categorySignalEntry
	if err := json.Unmarshal(categorySignalTokensJSON, &entries); err != nil {
		panic(err)
	}

	categorySignalCatalog = map[string]categorySignalTerms{}
	for _, entry := range entries {
		categoryKey := normalizeRoleTitleForExactMatch(entry.Category)
		if categoryKey == "" {
			continue
		}
		tokens := map[string]float64{}
		for _, token := range entry.Tokens {
			normalized := normalizeRoleTitleForExactMatch(token.Token)
			if normalized == "" {
				continue
			}
			for _, unit := range tokenizeTextForSequence(normalized) {
				if token.Weight > tokens[unit] {
					tokens[unit] = token.Weight
				}
			}
		}

		categorySignalCatalog[categoryKey] = categorySignalTerms{
			tokens: tokens,
		}
	}
}

func getCategorySignalCatalog() map[string]categorySignalTerms {
	loadCategorySignalsOnce.Do(loadCategorySignals)
	return categorySignalCatalog
}

func categorySignalWeight(sourceNormalizedTitle, candidateCategoryTitle, candidateCategoryFunction string) float64 {
	catalog := getCategorySignalCatalog()
	if len(catalog) == 0 || sourceNormalizedTitle == "" {
		return 0
	}

	weight := 0.0
	sourceTokens := map[string]struct{}{}
	for _, token := range tokenizeTextForSequence(sourceNormalizedTitle) {
		sourceTokens[token] = struct{}{}
	}
	candidateKeys := []string{
		normalizeRoleTitleForExactMatch(candidateCategoryTitle),
		normalizeRoleTitleForExactMatch(candidateCategoryFunction),
	}

	for _, candidateKey := range candidateKeys {
		if candidateKey == "" {
			continue
		}
		terms, ok := catalog[candidateKey]
		if !ok {
			continue
		}
		for token, tokenWeight := range terms.tokens {
			if _, ok := sourceTokens[token]; ok {
				weight += tokenWeight
			}
		}
	}

	return weight
}
