package parsed

import (
	_ "embed"
	"encoding/json"
	"io"
	"log"
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
		categoryKey := entry.Category
		tokens := map[string]float64{}
		for _, token := range entry.Tokens {
			if token.Weight > tokens[token.Token] {
				tokens[token.Token] = token.Weight
			}
		}

		catalog[categoryKey] = categorySignalTerms{
			tokens: tokens,
		}
	}
	return catalog, nil
}

func getCategorySignalCatalog(url string) map[string]categorySignalTerms {
	defaultCatalog := getDefaultCategorySignalCatalog()
	if url == "" {
		return defaultCatalog
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Printf("parsed category signals remote_load_failed url=%q error=%v; falling back to embedded catalog", url, err)
		return defaultCatalog
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("parsed category signals remote_load_failed url=%q error=%v; falling back to embedded catalog", url, err)
		return defaultCatalog
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("parsed category signals remote_load_failed url=%q status=%s; falling back to embedded catalog", url, resp.Status)
		return defaultCatalog
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("parsed category signals remote_load_failed url=%q error=%v; falling back to embedded catalog", url, err)
		return defaultCatalog
	}
	catalog, err := buildCategorySignalCatalog(raw)
	if err != nil {
		log.Printf("parsed category signals remote_load_failed url=%q error=%v; falling back to embedded catalog", url, err)
		return defaultCatalog
	}
	log.Printf("parsed category signals remote_load_succeeded url=%q categories=%d", url, len(catalog))
	return catalog
}

func getDefaultCategorySignalCatalog() map[string]categorySignalTerms {
	categorySignalCatalogMu.RLock()
	if categorySignalCatalogDefault != nil {
		catalog := categorySignalCatalogDefault
		categorySignalCatalogMu.RUnlock()
		return catalog
	}
	categorySignalCatalogMu.RUnlock()

	catalog, err := buildCategorySignalCatalog(categorySignalTokensJSON)
	if err != nil {
		log.Printf("parsed category signals embedded_load_failed error=%v", err)
		return map[string]categorySignalTerms{}
	}
	log.Printf("parsed category signals embedded_load_succeeded categories=%d", len(catalog))

	categorySignalCatalogMu.Lock()
	if categorySignalCatalogDefault == nil {
		categorySignalCatalogDefault = catalog
	} else {
		catalog = categorySignalCatalogDefault
	}
	categorySignalCatalogMu.Unlock()
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
		candidateCategoryTitle,
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
