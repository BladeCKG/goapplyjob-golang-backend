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

func TestCleanGroqDescriptionCollapsesWhitespace(t *testing.T) {
	raw := "Line one\n\n\tLine two   Line   three"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Line one Line two Line three" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionUnescapesEntities(t *testing.T) {
	raw := "AT&amp;T &lt;span&gt;Rock&amp;Roll&lt;/span&gt; &quot;Quoted&quot;"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "AT&T Rock&Roll \"Quoted\"" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanGroqDescriptionStripsNestedTags(t *testing.T) {
	raw := "<div><h2>Role</h2><ul><li>Item 1</li><li>Item 2</li></ul></div>"
	cleaned := cleanGroqDescription(raw)
	if cleaned != "Role Item 1 Item 2" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}
