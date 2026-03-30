package parsed

import (
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

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

//go:embed category_signal_tokens.json
var categorySignalTokensJSON []byte

var (
	categorySignalCatalogMu      sync.RWMutex
	categorySignalCatalogDefault map[string]categorySignalTerms
)

func buildCategorySignalCatalog(raw []byte) (map[string]categorySignalTerms, error) {
	var entries []categorySignalEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}

	catalog := map[string]categorySignalTerms{}
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

		catalog[categoryKey] = categorySignalTerms{
			tokens: tokens,
		}
	}
	return catalog, nil
}

func getCategorySignalCatalog(url string) map[string]categorySignalTerms {
	if url == "" {
		categorySignalCatalogMu.RLock()
		if categorySignalCatalogDefault != nil {
			catalog := categorySignalCatalogDefault
			categorySignalCatalogMu.RUnlock()
			return catalog
		}
		categorySignalCatalogMu.RUnlock()

		catalog, err := buildCategorySignalCatalog(categorySignalTokensJSON)
		if err != nil {
			panic(err)
		}

		categorySignalCatalogMu.Lock()
		categorySignalCatalogDefault = catalog
		categorySignalCatalogMu.Unlock()
		return catalog
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic("failed to fetch category signal tokens: " + resp.Status)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	catalog, err := buildCategorySignalCatalog(raw)
	if err != nil {
		panic(err)
	}
	return catalog
}

func categorySignalWeightFromCatalog(catalog map[string]categorySignalTerms, sourceNormalizedTitle, candidateCategoryTitle, candidateCategoryFunction string) float64 {
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
