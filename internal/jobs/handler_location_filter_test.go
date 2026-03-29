package jobs

import "testing"

const (
	testCountryFilterGermany      = "Germany"
	testCountryFilterEurope       = "Europe"
	testCountryFilterNorthAmerica = "North America"
	testCountryFilterOceania      = "Oceania"
	testCountryFilterWorldwide    = "Worldwide"
	testCountryFilterAmericas     = "Americas"
	testCountryFilterEMEA         = "EMEA"
	testUSStateFilterCalifornia   = "California"
)

func TestExpandCountryFilterTermsExpandsRegions(t *testing.T) {
	got := expandCountryFilterTerms([]string{
		testCountryFilterEurope,
		testCountryFilterNorthAmerica,
		testCountryFilterOceania,
		testCountryFilterWorldwide,
	})

	for _, expected := range []string{
		testCountryFilterEurope,
		testCountryFilterEMEA,
		testCountryFilterNorthAmerica,
		testCountryFilterAmericas,
		testCountryFilterOceania,
		"Asia Pacific",
		testCountryFilterWorldwide,
	} {
		if !containsString(got, expected) {
			t.Fatalf("expected %q in expanded values, got %v", expected, got)
		}
	}
	for _, unexpected := range []string{"Germany", "Canada", "Japan"} {
		if containsString(got, unexpected) {
			t.Fatalf("did not expect %q in expanded values, got %v", unexpected, got)
		}
	}
}

func TestExpandCountryFilterTermsExpandsAmericas(t *testing.T) {
	got := expandCountryFilterTerms([]string{testCountryFilterAmericas})

	for _, expected := range []string{testCountryFilterAmericas, testCountryFilterWorldwide} {
		if !containsString(got, expected) {
			t.Fatalf("expected %q in expanded values, got %v", expected, got)
		}
	}
	for _, unexpected := range []string{"Canada", "United States", "Brazil", "Argentina"} {
		if containsString(got, unexpected) {
			t.Fatalf("did not expect %q in expanded values, got %v", unexpected, got)
		}
	}
}

func TestExpandCountryFilterTermsExpandsCountryToRegions(t *testing.T) {
	got := expandCountryFilterTerms([]string{testCountryFilterGermany})

	for _, expected := range []string{
		testCountryFilterGermany,
		testCountryFilterEurope,
		testCountryFilterEMEA,
		testCountryFilterWorldwide,
	} {
		if !containsString(got, expected) {
			t.Fatalf("expected %q in expanded values, got %v", expected, got)
		}
	}
}

func TestBroadRegionTermsForUSStates(t *testing.T) {
	got := broadRegionTermsForUSStates([]string{testUSStateFilterCalifornia})
	for _, expected := range []string{testCountryFilterNorthAmerica, testCountryFilterAmericas, testCountryFilterWorldwide} {
		if !containsString(got, expected) {
			t.Fatalf("expected %q in expanded values, got %v", expected, got)
		}
	}
	if containsString(got, unitedStatesCountry) {
		t.Fatalf("did not expect %q in broad region values, got %v", unitedStatesCountry, got)
	}
}

func TestBuildLocationParentsMapNormalizesRegionCountryLabels(t *testing.T) {
	got := buildLocationParentsMap([][2][]string{
		{
			nil,
			[]string{"ASIA"},
		},
	})

	if _, ok := got["Asia"]; !ok {
		t.Fatalf("expected canonical %q key in location parents, got %v", "Asia", got)
	}
	if _, ok := got["ASIA"]; ok {
		t.Fatalf("did not expect raw %q key in location parents, got %v", "ASIA", got)
	}
}

func TestParseCSVQueryPreservesQuotedCommas(t *testing.T) {
	got := parseCSVQuery(`"Sudan, Republic of",Canada`)
	if len(got) != 2 {
		t.Fatalf("expected 2 values, got %v", got)
	}
	if got[0] != "Sudan, Republic of" {
		t.Fatalf("expected first value to preserve comma, got %q", got[0])
	}
	if got[1] != "Canada" {
		t.Fatalf("expected second value to be Canada, got %q", got[1])
	}
}
