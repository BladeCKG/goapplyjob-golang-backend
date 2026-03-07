package config

import "testing"

func TestNormalizeDatabaseURL(t *testing.T) {
	t.Run("keeps sqlite dsn unchanged", func(t *testing.T) {
		input := "file:page_extract.db?_foreign_keys=on"
		if got := normalizeDatabaseURL(input); got != input {
			t.Fatalf("normalizeDatabaseURL()=%q want %q", got, input)
		}
	})

	t.Run("normalizes postgres scheme", func(t *testing.T) {
		input := "postgres://user:pass@example.com:5432/app?sslmode=require"
		want := "postgresql://user:pass@example.com:5432/app?sslmode=require"
		if got := normalizeDatabaseURL(input); got != want {
			t.Fatalf("normalizeDatabaseURL()=%q want %q", got, want)
		}
	})
}
