package locationnorm

import "testing"

func TestNormalizeCountryNameSupportsCompactAliases(t *testing.T) {
	tests := map[string]string{
		"vietnam":             "Viet Nam",
		"elsalvador":          "El Salvador",
		"unitedkingdom":       "United Kingdom",
		"unitedarabemirates":  "United Arab Emirates",
		"southkorea":          "Korea, Republic Of",
		"northkorea":          "Korea, Democratic People's Republic Of",
		"czechia":             "Czech Republic",
		"newzealand":          "New Zealand",
		"costarica":           "Costa Rica",
	}

	for input, want := range tests {
		if got := NormalizeCountryName(input, true); got != want {
			t.Fatalf("NormalizeCountryName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeCountryNameSupportsPlainAliases(t *testing.T) {
	tests := map[string]string{
		"Russia":    "Russian Federation",
		"Laos":      "Lao People's Democratic Republic",
		"Moldova":   "Moldova, Republic Of",
		"Taiwan":    "Taiwan, Province Of China",
		"Venezuela": "Venezuela, Bolivarian Republic Of",
	}

	for input, want := range tests {
		if got := NormalizeCountryName(input, true); got != want {
			t.Fatalf("NormalizeCountryName(%q) = %q, want %q", input, got, want)
		}
	}
}
