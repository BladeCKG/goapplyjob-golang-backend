package plugins

import "testing"

func TestRegistryIncludesWorkable(t *testing.T) {
	plugin, ok := Get("workable")
	if !ok || plugin.Source != "workable" {
		t.Fatalf("expected workable plugin, got %#v ok=%v", plugin, ok)
	}
}
