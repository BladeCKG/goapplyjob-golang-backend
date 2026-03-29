package main

import (
	"reflect"
	"testing"
)

func TestParseJSONArrayStrings(t *testing.T) {
	got := parseJSONArrayStrings(`["angular","ReactJS","unknown"]`)
	want := []string{"angular", "ReactJS", "unknown"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseJSONArrayStrings mismatch got=%#v want=%#v", got, want)
	}
}

func TestNormalizationRule(t *testing.T) {
	before := parseJSONArrayStrings(`["angular","ReactJS","unknown","postgres","go","Golang"]`)
	after := normalizeTechStack(before)
	want := []string{"Angular", "React", "PostgreSQL", "Go"}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("normalizeTechStack mismatch got=%#v want=%#v", after, want)
	}
}
