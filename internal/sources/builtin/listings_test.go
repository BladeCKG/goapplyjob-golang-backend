package builtin

import "testing"

func TestExtractJobListingsReturnsRawJobRowsWithNormalizedPostDate(t *testing.T) {
	htmlText := `
<html>
  <head>
    <script type="application/ld+json">
      {
        "@graph": [
          {"@type":"CollectionPage","url":"https://builtin.com/jobs"},
          {
            "@type":"ItemList",
            "itemListElement":[
              {
                "@type":"ListItem",
                "position":1,
                "url":"https://builtin.com/job/platform-engineer/12345",
                "name":"Platform Engineer",
                "description":"Build systems."
              }
            ]
          }
        ]
      }
    </script>
  </head>
  <body>
    <script>
      logBuiltinTrackEvent('job_board_view', {'jobs':[{'id':12345,'published_date':'2026-02-17T04:30:12'}],'filters':{}});
    </script>
  </body>
</html>`

	items := ExtractJobListings(htmlText)
	if len(items) != 1 {
		t.Fatalf("expected one listing, got %d", len(items))
	}
	if items[0]["post_date"] != "2026-02-17T04:30:12Z" {
		t.Fatalf("expected normalized post_date, got %#v", items[0]["post_date"])
	}
	if items[0]["is_ready"] != false || items[0]["raw_json"] != nil {
		t.Fatalf("expected raw import row shape, got %#v", items[0])
	}
}
