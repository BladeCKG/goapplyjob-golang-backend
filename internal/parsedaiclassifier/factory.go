package parsedaiclassifier

import (
	providercommon "goapplyjob-golang-backend/internal/parsedaiclassifier/providers/common"
	"strings"
)

const defaultAIClassifierProvider = "groq"

type Factory struct {
	cfg Config
}

func NewFactory(cfg Config) *Factory {
	return &Factory{cfg: cfg}
}

func normalizeClassifierProvider(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return "auto"
	case "groq":
		return "groq"
	case "ollama":
		return "ollama"
	case "cerebras":
		return "cerebras"
	case "openai":
		return "openai"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func normalizeClassifierProviderList(raw string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, token := range strings.Split(raw, ",") {
		value := normalizeClassifierProvider(token)
		if value == "" || value == "auto" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (f *Factory) ResolveProviders() []string {
	if list := normalizeClassifierProviderList(f.cfg.Providers); len(list) > 0 {
		return list
	}
	provider := normalizeClassifierProvider(f.cfg.Provider)
	if provider != "" && provider != "auto" {
		return []string{provider}
	}

	out := []string{}
	if len(providercommon.CollectProviderKeys(f.cfg.OpenAIAPIKey, f.cfg.OpenAIAPIKeys, false)) > 0 &&
		len(providercommon.CollectProviderModels(f.cfg.OpenAIModel, f.cfg.OpenAIModels)) > 0 {
		out = append(out, "openai")
	}
	if len(providercommon.CollectProviderKeys(f.cfg.CerebrasAPIKey, f.cfg.CerebrasAPIKeys, false)) > 0 {
		out = append(out, "cerebras")
	}
	if f.cfg.OllamaConfigured &&
		len(providercommon.CollectProviderModels(f.cfg.OllamaModel, f.cfg.OllamaModels)) > 0 {
		out = append(out, "ollama")
	}
	if len(providercommon.CollectProviderKeys(f.cfg.GroqAPIKey, f.cfg.GroqAPIKeys, false)) > 0 {
		out = append(out, "groq")
	}
	if len(out) == 0 {
		return []string{defaultAIClassifierProvider}
	}
	return out
}
