package common

import (
	"strings"
	"sync"
)

type rotationState struct {
	keys []string
	next int
}

var keyRings = struct {
	mu    sync.Mutex
	items map[string]rotationState
}{
	items: map[string]rotationState{},
}

func EqualStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func CollectProviderKeys(single, multi string, allowEmpty bool) []string {
	keys := make([]string, 0, 8)
	seen := map[string]struct{}{}

	if strings.TrimSpace(multi) != "" {
		for _, part := range strings.Split(multi, ",") {
			key := strings.TrimSpace(part)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}

	if value := strings.TrimSpace(single); value != "" {
		if _, exists := seen[value]; !exists {
			keys = append(keys, value)
		}
	}

	if len(keys) == 0 && allowEmpty {
		return []string{""}
	}
	return keys
}

func CollectProviderModels(primary, extraCSV string, defaults ...string) []string {
	models := make([]string, 0, 1+len(defaults))
	seen := map[string]struct{}{}

	if model := strings.TrimSpace(primary); model != "" {
		models = append(models, model)
		seen[model] = struct{}{}
	}
	if strings.TrimSpace(extraCSV) != "" {
		for _, part := range strings.Split(extraCSV, ",") {
			model := strings.TrimSpace(part)
			if model == "" {
				continue
			}
			if _, exists := seen[model]; exists {
				continue
			}
			seen[model] = struct{}{}
			models = append(models, model)
		}
	}
	for _, candidate := range defaults {
		model := strings.TrimSpace(candidate)
		if model == "" {
			continue
		}
		if _, exists := seen[model]; exists {
			continue
		}
		seen[model] = struct{}{}
		models = append(models, model)
	}
	return models
}

func KeyRingStart(provider string, keys []string) int {
	keyRings.mu.Lock()
	defer keyRings.mu.Unlock()

	state := keyRings.items[provider]
	if !EqualStringSlices(state.keys, keys) {
		state.keys = append([]string(nil), keys...)
		state.next = 0
	}
	if len(keys) == 0 {
		state.next = 0
		keyRings.items[provider] = state
		return 0
	}
	if state.next < 0 || state.next >= len(keys) {
		state.next = 0
	}
	keyRings.items[provider] = state
	return state.next
}

func KeyRingSetNext(provider string, keys []string, next int) {
	keyRings.mu.Lock()
	defer keyRings.mu.Unlock()

	state := keyRings.items[provider]
	if !EqualStringSlices(state.keys, keys) {
		return
	}
	if len(keys) == 0 {
		state.next = 0
		keyRings.items[provider] = state
		return
	}
	if next < 0 {
		next = 0
	}
	state.next = next % len(keys)
	keyRings.items[provider] = state
}

func ResetKeyRingForTest(provider string) {
	keyRings.mu.Lock()
	delete(keyRings.items, provider)
	keyRings.mu.Unlock()
}
