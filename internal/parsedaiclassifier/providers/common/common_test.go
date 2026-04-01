package common

import "testing"

func TestCleanDescriptionStripsHTML(t *testing.T) {
	raw := "  <p>Hello&nbsp;<b>World</b></p>\n<div>Line&nbsp;Two</div> "
	cleaned := CleanDescription(raw)
	if cleaned != "Hello World Line Two" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanDescriptionHandlesEmpty(t *testing.T) {
	if CleanDescription("") != "" {
		t.Fatalf("expected empty result for empty input")
	}
}

func TestCleanDescriptionCollapsesWhitespace(t *testing.T) {
	raw := "Line one\n\n\tLine two   Line   three"
	cleaned := CleanDescription(raw)
	if cleaned != "Line one Line two Line three" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanDescriptionUnescapesEntities(t *testing.T) {
	raw := "AT&amp;T &lt;span&gt;Rock&amp;Roll&lt;/span&gt; &quot;Quoted&quot;"
	cleaned := CleanDescription(raw)
	if cleaned != "AT&T Rock&Roll \"Quoted\"" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}

func TestCleanDescriptionStripsNestedTags(t *testing.T) {
	raw := "<div><h2>Role</h2><ul><li>Item 1</li><li>Item 2</li></ul></div>"
	cleaned := CleanDescription(raw)
	if cleaned != "Role Item 1 Item 2" {
		t.Fatalf("unexpected cleaned description: %q", cleaned)
	}
}
