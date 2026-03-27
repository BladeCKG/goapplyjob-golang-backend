package techstacknorm

import "testing"

func TestNormalizeReturnsEmptySliceForUnsupportedInput(t *testing.T) {
	values := Normalize(nil)
	if values == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(values) != 0 {
		t.Fatalf("expected empty slice, got %#v", values)
	}
}

