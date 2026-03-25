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

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
