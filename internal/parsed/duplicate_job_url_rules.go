package parsed

import (
	_ "embed"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type duplicateJobURLRulesDocument struct {
	Version int                   `json:"version"`
	Rules   []duplicateJobURLRule `json:"rules"`
}

type duplicateJobURLRule struct {
	Name             string                        `json:"name"`
	Description      string                        `json:"description"`
	HostSuffixes     []string                      `json:"hostSuffixes"`
	AllowWithoutHost bool                          `json:"allowWithoutHost"`
	Signatures       []duplicateJobURLSignatureDef `json:"signatures"`
}

type duplicateJobURLSignatureDef struct {
	Name  string                   `json:"name"`
	Parts []duplicateJobURLPartDef `json:"parts"`
}

type duplicateJobURLPartDef struct {
	Type    string `json:"type"`
	Key     string `json:"key"`
	Pattern string `json:"pattern"`
	Group   int    `json:"group"`
	Label   string `json:"label"`
}

type duplicateJobURLRuleSet struct {
	rules []compiledDuplicateJobURLRule
}

type compiledDuplicateJobURLRule struct {
	name             string
	hostSuffixes     []string
	allowWithoutHost bool
	signatures       []compiledDuplicateJobURLSignatureDef
}

type compiledDuplicateJobURLSignatureDef struct {
	name  string
	parts []compiledDuplicateJobURLPartDef
}

type compiledDuplicateJobURLPartDef struct {
	partType string
	key      string
	label    string
	group    int
	regex    *regexp.Regexp
}

type duplicateJobURLSignature struct {
	key            string
	prefilterTerms []string
}

//go:embed duplicate_job_url_rules.json
var duplicateJobURLRulesJSON []byte

var (
	duplicateJobURLRulesMu      sync.RWMutex
	duplicateJobURLRulesDefault *duplicateJobURLRuleSet
)

func buildDuplicateJobURLRuleSet(raw []byte) (*duplicateJobURLRuleSet, error) {
	var document duplicateJobURLRulesDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}

	compiled := make([]compiledDuplicateJobURLRule, 0, len(document.Rules))
	for _, rule := range document.Rules {
		if strings.TrimSpace(rule.Name) == "" {
			continue
		}
		signatures := make([]compiledDuplicateJobURLSignatureDef, 0, len(rule.Signatures))
		for _, signature := range rule.Signatures {
			if strings.TrimSpace(signature.Name) == "" || len(signature.Parts) == 0 {
				continue
			}
			parts := make([]compiledDuplicateJobURLPartDef, 0, len(signature.Parts))
			validSignature := true
			for _, part := range signature.Parts {
				partType := strings.ToLower(strings.TrimSpace(part.Type))
				label := strings.TrimSpace(part.Label)
				if label == "" {
					label = strings.TrimSpace(part.Key)
				}
				if label == "" {
					label = partType
				}
				compiledPart := compiledDuplicateJobURLPartDef{
					partType: partType,
					key:      strings.ToLower(strings.TrimSpace(part.Key)),
					label:    strings.ToLower(label),
					group:    part.Group,
				}
				switch partType {
				case "query_param":
					if compiledPart.key == "" {
						validSignature = false
					}
				case "host_label":
					if compiledPart.key == "" {
						validSignature = false
					}
				case "full_host":
					// no extra validation needed
				case "path_regex":
					if strings.TrimSpace(part.Pattern) == "" {
						validSignature = false
						break
					}
					re, err := regexp.Compile(part.Pattern)
					if err != nil {
						return nil, err
					}
					compiledPart.regex = re
				default:
					validSignature = false
				}
				if !validSignature {
					break
				}
				parts = append(parts, compiledPart)
			}
			if !validSignature || len(parts) == 0 {
				continue
			}
			signatures = append(signatures, compiledDuplicateJobURLSignatureDef{
				name:  strings.ToLower(strings.TrimSpace(signature.Name)),
				parts: parts,
			})
		}
		if len(signatures) == 0 {
			continue
		}
		hostSuffixes := make([]string, 0, len(rule.HostSuffixes))
		for _, suffix := range rule.HostSuffixes {
			suffix = strings.ToLower(strings.TrimSpace(suffix))
			suffix = strings.TrimPrefix(suffix, ".")
			if suffix == "" {
				continue
			}
			hostSuffixes = append(hostSuffixes, suffix)
		}
		compiled = append(compiled, compiledDuplicateJobURLRule{
			name:             strings.ToLower(strings.TrimSpace(rule.Name)),
			hostSuffixes:     hostSuffixes,
			allowWithoutHost: rule.AllowWithoutHost,
			signatures:       signatures,
		})
	}

	return &duplicateJobURLRuleSet{rules: compiled}, nil
}

func getDuplicateJobURLRuleSet(url string) *duplicateJobURLRuleSet {
	defaultRules := getDefaultDuplicateJobURLRuleSet()
	if url == "" {
		return defaultRules
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Printf("parsed duplicate url rules remote_load_failed url=%q error=%v; falling back to embedded rules", url, err)
		return defaultRules
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("parsed duplicate url rules remote_load_failed url=%q error=%v; falling back to embedded rules", url, err)
		return defaultRules
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("parsed duplicate url rules remote_load_failed url=%q status=%s; falling back to embedded rules", url, resp.Status)
		return defaultRules
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("parsed duplicate url rules remote_load_failed url=%q error=%v; falling back to embedded rules", url, err)
		return defaultRules
	}
	rules, err := buildDuplicateJobURLRuleSet(raw)
	if err != nil {
		log.Printf("parsed duplicate url rules remote_load_failed url=%q error=%v; falling back to embedded rules", url, err)
		return defaultRules
	}
	log.Printf("parsed duplicate url rules remote_load_succeeded url=%q rules=%d", url, len(rules.rules))
	return rules
}

func getDefaultDuplicateJobURLRuleSet() *duplicateJobURLRuleSet {
	duplicateJobURLRulesMu.RLock()
	if duplicateJobURLRulesDefault != nil {
		rules := duplicateJobURLRulesDefault
		duplicateJobURLRulesMu.RUnlock()
		return rules
	}
	duplicateJobURLRulesMu.RUnlock()

	rules, err := buildDuplicateJobURLRuleSet(duplicateJobURLRulesJSON)
	if err != nil {
		log.Printf("parsed duplicate url rules embedded_load_failed error=%v", err)
		return &duplicateJobURLRuleSet{}
	}
	log.Printf("parsed duplicate url rules embedded_load_succeeded rules=%d", len(rules.rules))

	duplicateJobURLRulesMu.Lock()
	if duplicateJobURLRulesDefault == nil {
		duplicateJobURLRulesDefault = rules
	} else {
		rules = duplicateJobURLRulesDefault
	}
	duplicateJobURLRulesMu.Unlock()
	return rules
}

func hostMatchesSuffix(host, suffix string) bool {
	if host == "" || suffix == "" {
		return false
	}
	return host == suffix || strings.HasSuffix(host, "."+suffix)
}

func findQueryParamValue(values url.Values, key string) string {
	for existingKey, candidateValues := range values {
		if !strings.EqualFold(existingKey, key) || len(candidateValues) == 0 {
			continue
		}
		for _, value := range candidateValues {
			value = strings.TrimSpace(value)
			if value != "" {
				return strings.ToLower(value)
			}
		}
	}
	return ""
}

func extractDuplicateJobURLSignatures(rawURL string, ruleSet *duplicateJobURLRuleSet) []duplicateJobURLSignature {
	if ruleSet == nil || len(ruleSet.rules) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" || isEmailApplyTarget(trimmed) {
		return nil
	}
	parsedURL, err := url.Parse(trimmed)
	if err != nil {
		return nil
	}
	host := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(parsedURL.Hostname()), "www."))
	hostParts := strings.Split(host, ".")
	path := strings.ToLower(regexp.MustCompile(`/+`).ReplaceAllString(parsedURL.EscapedPath(), "/"))
	if path == "" {
		path = "/"
	}
	queryValues := parsedURL.Query()

	seen := map[string]struct{}{}
	out := make([]duplicateJobURLSignature, 0, 4)
	for _, rule := range ruleSet.rules {
		hostMatched := false
		matchedSuffix := ""
		for _, suffix := range rule.hostSuffixes {
			if hostMatchesSuffix(host, suffix) {
				hostMatched = true
				matchedSuffix = suffix
				break
			}
		}
		if !hostMatched && !rule.allowWithoutHost {
			continue
		}
		for _, signature := range rule.signatures {
			values := make([]string, 0, len(signature.parts))
			prefilterTerms := make([]string, 0, len(signature.parts)+1)
			if matchedSuffix != "" {
				prefilterTerms = append(prefilterTerms, matchedSuffix)
			}
			valid := true
			for _, part := range signature.parts {
				switch part.partType {
				case "query_param":
					value := findQueryParamValue(queryValues, part.key)
					if value == "" {
						valid = false
						break
					}
					values = append(values, part.label+"="+value)
					prefilterTerms = append(prefilterTerms, strings.ToLower(url.QueryEscape(part.key))+"="+strings.ToLower(url.QueryEscape(value)))
				case "host_label":
					index, err := strconv.Atoi(part.key)
					if err != nil || index < 0 || index >= len(hostParts) {
						valid = false
						break
					}
					value := strings.ToLower(strings.TrimSpace(hostParts[index]))
					if value == "" {
						valid = false
						break
					}
					values = append(values, part.label+"="+value)
					prefilterTerms = append(prefilterTerms, value)
				case "full_host":
					if host == "" {
						valid = false
						break
					}
					values = append(values, part.label+"="+host)
					prefilterTerms = append(prefilterTerms, host)
				case "path_regex":
					matches := part.regex.FindStringSubmatch(path)
					if len(matches) == 0 {
						valid = false
						break
					}
					groupIndex := part.group
					if groupIndex <= 0 {
						groupIndex = 1
					}
					if groupIndex >= len(matches) {
						valid = false
						break
					}
					value := strings.ToLower(strings.TrimSpace(matches[groupIndex]))
					if value == "" {
						valid = false
						break
					}
					values = append(values, part.label+"="+value)
					prefilterTerms = append(prefilterTerms, value)
				default:
					valid = false
					break
				}
			}
			if !valid || len(values) == 0 {
				continue
			}
			signatureKey := rule.name + ":" + strings.Join(values, "|")
			if _, ok := seen[signatureKey]; ok {
				continue
			}
			seen[signatureKey] = struct{}{}
			out = append(out, duplicateJobURLSignature{
				key:            signatureKey,
				prefilterTerms: prefilterTerms,
			})
		}
	}
	return out
}
