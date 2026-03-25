package locationnorm

import "testing"

const (
	testRegionAPAC      = "APAC"
	testRegionAnywhere  = "Anywhere"
	testRegionEMEA      = "EMEA"
	testRegionEurope    = "Europe"
	testRegionGermany   = "Germany"
	testRegionUSATZ     = "USA timezones"
	testRegionWorldwide = "Worldwide"
)

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
		if got := NormalizeCountryName(input); got != want {
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
		if got := NormalizeCountryName(input); got != want {
			t.Fatalf("NormalizeCountryName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeCountryNameSupportsRegionTerms(t *testing.T) {
	tests := map[string]string{
		testRegionAPAC:      "Asia Pacific",
		testRegionEMEA:      testRegionEMEA,
		testRegionAnywhere:  "Worldwide",
		testRegionUSATZ:     "USA Timezones",
		testRegionWorldwide: "Worldwide",
	}

	for input, want := range tests {
		if got := NormalizeCountryName(input); got != want {
			t.Fatalf("NormalizeCountryName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeRegionNameSupportsAliases(t *testing.T) {
	tests := map[string]string{
		testRegionAPAC:      "Asia Pacific",
		testRegionEMEA:      testRegionEMEA,
		testRegionAnywhere:  "Worldwide",
		testRegionUSATZ:     "USA Timezones",
		testRegionWorldwide: "Worldwide",
	}

	for input, want := range tests {
		if got := NormalizeRegionName(input); got != want {
			t.Fatalf("NormalizeRegionName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRegionCountriesSupportsAliasesAndWorldwide(t *testing.T) {
	apac := RegionCountries(testRegionAPAC)
	for _, expected := range []string{"Australia", "Japan", "India"} {
		if !containsString(apac, expected) {
			t.Fatalf("RegionCountries(%s) missing %q in %v", testRegionAPAC, expected, apac)
		}
	}

	worldwide := RegionCountries(testRegionWorldwide)
	for _, expected := range []string{"United States", "Germany", "Japan", "Brazil", "South Africa"} {
		if !containsString(worldwide, expected) {
			t.Fatalf("RegionCountries(%s) missing %q in %v", testRegionWorldwide, expected, worldwide)
		}
	}

	anywhere := RegionCountries(testRegionAnywhere)
	for _, expected := range []string{"United States", "Germany", "Japan"} {
		if !containsString(anywhere, expected) {
			t.Fatalf("RegionCountries(%s) missing %q in %v", testRegionAnywhere, expected, anywhere)
		}
	}

	usaTimezones := RegionCountries(testRegionUSATZ)
	for _, expected := range []string{"United States"} {
		if !containsString(usaTimezones, expected) {
			t.Fatalf("RegionCountries(%s) missing %q in %v", testRegionUSATZ, expected, usaTimezones)
		}
	}
}

func TestRegionNamesForCountryIncludesBroaderRegions(t *testing.T) {
	got := RegionNamesForCountry(testRegionGermany)

	for _, expected := range []string{
		testRegionEurope,
		testRegionEMEA,
		testRegionWorldwide,
	} {
		if !containsString(got, expected) {
			t.Fatalf("RegionNamesForCountry(%s) missing %q in %v", testRegionGermany, expected, got)
		}
	}
}

func TestRegionParentNamesIncludesBroaderRegionsOnly(t *testing.T) {
	got := RegionParentNames(testRegionEurope)

	for _, expected := range []string{
		testRegionEMEA,
		testRegionWorldwide,
	} {
		if !containsString(got, expected) {
			t.Fatalf("RegionParentNames(%s) missing %q in %v", testRegionEurope, expected, got)
		}
	}
	for _, unexpected := range []string{testRegionGermany, "France"} {
		if containsString(got, unexpected) {
			t.Fatalf("RegionParentNames(%s) unexpectedly contained %q in %v", testRegionEurope, unexpected, got)
		}
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
