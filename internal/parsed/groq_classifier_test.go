package parsed

import "testing"

func TestCleanGroqDescriptionStripsHTML(t *testing.T) {
	raw := "  <p>Hello&nbsp;<b>World</b></p>\n<div>Line&nbsp;Two</div> "
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Hello World Line Two" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionHandlesEmpty(t *testing.T) {
	if cleanGroqDescription("") != "" {
		t.Fatalf("expected empty result for empty input")
	}
}
