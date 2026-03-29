package main

import (
	"reflect"
	"testing"
)

func TestNormalizeExactCanonical(t *testing.T) {
	exactCanonicals = map[string]string{
		"go":      "Go",
		"angular": "Angular",
	}

	got, ok := normalizeExactCanonical("Go")
	if !ok || got != "Go" {
		t.Fatalf("normalizeExactCanonical mismatch got=%q ok=%v", got, ok)
	}

	got, ok = normalizeExactCanonical("angular")
	if !ok || got != "Angular" {
		t.Fatalf("normalizeExactCanonical mismatch got=%q ok=%v", got, ok)
	}

	got, ok = normalizeExactCanonical("angular js")
	if ok || got != "" {
		t.Fatalf("normalizeExactCanonical unexpected match got=%q ok=%v", got, ok)
	}
}

func TestParseJSONArrayStrings(t *testing.T) {
	got := parseJSONArrayStrings(`["angular","ReactJS","unknown"]`)
	want := []string{"angular", "ReactJS", "unknown"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseJSONArrayStrings mismatch got=%#v want=%#v", got, want)
	}
}

func TestNormalizationRule(t *testing.T) {
	exactCanonicals = map[string]string{
		"go": "Go",
	}
	before := parseJSONArrayStrings(`["angular","ReactJS","unknown","postgres","Go","Golang"]`)
	after := normalizeTechStack(before)
	want := []string{"Angular", "React", "PostgreSQL", "Go"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}

func TestNormalizationDropsUnmatchedValues(t *testing.T) {
	exactCanonicals = map[string]string{
		"go": "Go",
	}
	before := parseJSONArrayStrings(`["Go","unknown"]`)
	after := normalizeTechStack(before)
	want := []string{"Go"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}

func TestNormalizationAllowsAliasInsideLine(t *testing.T) {
	exactCanonicals = map[string]string{
		"go": "Go",
	}
	before := parseJSONArrayStrings(`["Go","angular js","postgres sql","unknown"]`)
	after := normalizeTechStack(before)
	want := []string{"Go", "Angular", "PostgreSQL"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}
