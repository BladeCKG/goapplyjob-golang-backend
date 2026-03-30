package main

import (
	"reflect"
	"testing"

	"goapplyjob-golang-backend/internal/extract/techstack"
)

func TestExactCanonical(t *testing.T) {
	extractor := techstack.NewExtractor("")

	got, ok := extractor.ExactCanonical("Go")
	if !ok || got != "Go" {
		t.Fatalf("ExactCanonical mismatch got=%q ok=%v", got, ok)
	}

	got, ok = extractor.ExactCanonical("angular")
	if !ok || got != "Angular" {
		t.Fatalf("ExactCanonical mismatch got=%q ok=%v", got, ok)
	}

	got, ok = extractor.ExactCanonical("angular js")
	if ok || got != "" {
		t.Fatalf("ExactCanonical unexpected match got=%q ok=%v", got, ok)
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
	extractor := techstack.NewExtractor("")
	before := parseJSONArrayStrings(`["angular","ReactJS","unknown","postgres","Go","Golang"]`)
	after := normalizeTechStack(extractor, before)
	want := []string{"Angular", "React", "PostgreSQL", "Go"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}

func TestNormalizationDropsUnmatchedValues(t *testing.T) {
	extractor := techstack.NewExtractor("")
	before := parseJSONArrayStrings(`["Go","unknown"]`)
	after := normalizeTechStack(extractor, before)
	want := []string{"Go"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}

func TestNormalizationAllowsAliasInsideLine(t *testing.T) {
	extractor := techstack.NewExtractor("")
	before := parseJSONArrayStrings(`["Go","angular js","postgres sql","unknown"]`)
	after := normalizeTechStack(extractor, before)
	want := []string{"Go", "Angular", "PostgreSQL"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}
