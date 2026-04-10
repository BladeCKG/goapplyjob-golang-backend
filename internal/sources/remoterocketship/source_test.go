package remoterocketship

import "testing"

func TestParseRawHTMLAlwaysSetsLocationCountries(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer","location":"United States"}}}}
	</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	values, ok := payload["locationCountries"].([]string)
	if !ok || len(values) != 1 || values[0] != "United States" {
		t.Fatalf("expected normalized locationCountries, got %#v", payload["locationCountries"])
	}
}

func TestParseRawHTMLSetsEmptyLocationCountriesWhenMissing(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer"}}}}
</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	values, ok := payload["locationCountries"].([]string)
	if !ok || len(values) != 0 {
		t.Fatalf("expected empty locationCountries slice, got %#v", payload["locationCountries"])
	}
}

func TestParseRawHTMLNormalizesNumericCompanyIDToString(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer","company":{"id":227383,"name":"CUSG"}}}}}
</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	company, _ := payload["company"].(map[string]any)
	if company["id"] != "remoterocketship_227383" {
		t.Fatalf("expected company.id string, got %#v", company["id"])
	}
}

func TestParseRawHTMLMapsCompanyIndustryToIndustries(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer","company":{"id":227383,"name":"CUSG","industry":"Wellness and Fitness Services"}}}}}
</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	company, _ := payload["company"].(map[string]any)
	industries, _ := company["industries"].([]string)
	if len(industries) != 1 || industries[0] != "Wellness and Fitness Services" {
		t.Fatalf("expected company.industries, got %#v", company["industries"])
	}
	if _, exists := company["industry"]; exists {
		t.Fatalf("expected company.industry to be removed, got %#v", company["industry"])
	}
}

func TestParseRawHTMLInfersCurrencySymbolFromCode(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer","salaryRange":{"currencyCode":"usd","currencySymbol":""}}}}}
</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	salaryRange, _ := payload["salaryRange"].(map[string]any)
	if salaryRange["currencyCode"] != "USD" {
		t.Fatalf("expected normalized currencyCode, got %#v", salaryRange["currencyCode"])
	}
	if salaryRange["currencySymbol"] != "$" {
		t.Fatalf("expected inferred currencySymbol, got %#v", salaryRange["currencySymbol"])
	}
}

func TestParseRawHTMLInfersCurrencyCodeWhenSymbolContainsCode(t *testing.T) {
	htmlText := `
<html>
<body>
<script type="application/json">
{"props":{"pageProps":{"jobOpening":{"title":"Engineer","salaryRange":{"currencyCode":"","currencySymbol":"usd"}}}}}
</script>
</body>
</html>`
	payload, err := ParseRawHTML(htmlText, "")
	if err != nil {
		t.Fatalf("ParseRawHTML failed: %v", err)
	}
	salaryRange, _ := payload["salaryRange"].(map[string]any)
	if salaryRange["currencyCode"] != "USD" {
		t.Fatalf("expected inferred currencyCode, got %#v", salaryRange["currencyCode"])
	}
	if salaryRange["currencySymbol"] != "$" {
		t.Fatalf("expected normalized currencySymbol, got %#v", salaryRange["currencySymbol"])
	}
}
