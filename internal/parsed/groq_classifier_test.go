package parsed

import (
	"reflect"
	"testing"
)

func TestExtractGroqClassificationEnforcesEnumAndDedupeSkills(t *testing.T) {
	allowed := []string{"Software Engineer", "Data Engineer", "Blank"}
	content := `{"job_category":"Software Engineer","required_skills":["Go","go","PostgreSQL",""]}`

	category, skills := extractGroqClassification(content, allowed)
	if category != "Software Engineer" {
		t.Fatalf("expected Software Engineer, got %q", category)
	}
	wantSkills := []string{"Go", "PostgreSQL"}
	if !reflect.DeepEqual(skills, wantSkills) {
		t.Fatalf("expected skills %v, got %v", wantSkills, skills)
	}
}

func TestExtractGroqClassificationFallsBackToBlank(t *testing.T) {
	allowed := []string{"Software Engineer", "Blank"}
	content := `{"job_category":"Invalid Category","required_skills":["Go"]}`

	category, skills := extractGroqClassification(content, allowed)
	if category != "" {
		t.Fatalf("expected blank fallback (empty result), got %q", category)
	}
	wantSkills := []string{"Go"}
	if !reflect.DeepEqual(skills, wantSkills) {
		t.Fatalf("expected skills %v, got %v", wantSkills, skills)
	}
}

func TestCollectGroqAPIKeysOrderAndDedup(t *testing.T) {
	t.Setenv(envGroqAPIKeys, "k1, k2, k1")
	t.Setenv(envGroqAPIKey, "k2")

	keys := collectGroqAPIKeys()
	want := []string{"k1", "k2"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("expected %v, got %v", want, keys)
	}
}
