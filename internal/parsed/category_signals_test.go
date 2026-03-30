package parsed

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func resetCategorySignalCatalogForTest() {
	categorySignalCatalogMu.Lock()
	categorySignalCatalogDefault = nil
	categorySignalCatalogMu.Unlock()
}

func TestCategorySignalCatalogReloadsFromExternalURL(t *testing.T) {
	t.Cleanup(resetCategorySignalCatalogForTest)

	version := "v1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch version {
		case "v1":
			w.Header().Set("ETag", `"v1"`)
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[
  {
    "category": "Engineer",
    "tokens": [
      {"token": "alpha", "weight": 0.5}
    ]
  }
]`))
		case "v2":
			w.Header().Set("ETag", `"v2"`)
			if r.Header.Get("If-None-Match") == `"v2"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[
  {
    "category": "Engineer",
    "tokens": [
      {"token": "beta", "weight": 0.8}
    ]
  }
]`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	resetCategorySignalCatalogForTest()

	if score := categorySignalWeightFromCatalog(getCategorySignalCatalog(server.URL), "alpha", "Engineer", "Engineering"); score != 0.5 {
		t.Fatalf("expected initial external score 0.5, got %v", score)
	}

	version = "v2"

	if score := categorySignalWeightFromCatalog(getCategorySignalCatalog(server.URL), "alpha", "Engineer", "Engineering"); score != 0 {
		t.Fatalf("expected old external token to stop matching after reload, got %v", score)
	}
	if score := categorySignalWeightFromCatalog(getCategorySignalCatalog(server.URL), "beta", "Engineer", "Engineering"); score != 0.8 {
		t.Fatalf("expected reloaded external score 0.8, got %v", score)
	}
}
