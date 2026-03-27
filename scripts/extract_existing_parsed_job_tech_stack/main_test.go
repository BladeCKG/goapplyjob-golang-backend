package main

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestExtractForRowReturnsNormalizedTechStackForEligibleRow(t *testing.T) {
	row := parsedJobRow{
		Source:              "dailyremote",
		RoleDescription:     sql.NullString{String: "Build services with Node.js and PostgreSQL.", Valid: true},
		RoleRequirements:    sql.NullString{String: "Docker and Kubernetes experience required.", Valid: true},
		CategorizedFunction: sql.NullString{String: "Software Engineer", Valid: true},
	}

	got, ok := extractForRow(row)
	if !ok {
		t.Fatal("expected row to be eligible")
	}
	want := []string{"Node.js", "PostgreSQL", "Docker", "Kubernetes"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected tech stack got=%#v want=%#v", got, want)
	}
}

func TestExtractForRowSkipsExistingTechStack(t *testing.T) {
	row := parsedJobRow{
		Source:              "dailyremote",
		RoleDescription:     sql.NullString{String: "Build services with Node.js.", Valid: true},
		CategorizedFunction: sql.NullString{String: "Software Engineer", Valid: true},
		TechStackJSON:       sql.NullString{String: `["Go"]`, Valid: true},
	}

	got, ok := extractForRow(row)
	if ok || got != nil {
		t.Fatalf("expected row to be skipped, got=%#v ok=%t", got, ok)
	}
}

