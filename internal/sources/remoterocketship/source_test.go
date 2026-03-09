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
	payload := ParseRawHTML(htmlText, "")
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
	payload := ParseRawHTML(htmlText, "")
	values, ok := payload["locationCountries"].([]string)
	if !ok || len(values) != 0 {
		t.Fatalf("expected empty locationCountries slice, got %#v", payload["locationCountries"])
	}
}
