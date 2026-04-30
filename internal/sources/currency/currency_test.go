package currency

import "testing"

func TestSymbolForCodeSupportsAdditionalCurrencies(t *testing.T) {
	if got := SymbolForCode("CRC"); got != "₡" {
		t.Fatalf("expected CRC symbol, got %#v", got)
	}
	if got := SymbolForCode("PKR"); got != "₨" {
		t.Fatalf("expected PKR symbol, got %#v", got)
	}
	if got := SymbolForCode("crc"); got != "₡" {
		t.Fatalf("expected lowercase CRC to normalize, got %#v", got)
	}
}

func TestExtractSalaryAmountsDoesNotTreatFollowingWordsAsSuffix(t *testing.T) {
	got := ExtractSalaryAmounts("minimum: $86,000 midpoint: $114,000 maximum: $142,000")
	if len(got) != 3 {
		t.Fatalf("expected 3 amounts, got %#v", got)
	}
	if got[0] != 86000 || got[1] != 114000 || got[2] != 142000 {
		t.Fatalf("unexpected extracted amounts %#v", got)
	}
}

func TestExtractSalaryAmountsStillSupportsCompactSuffixes(t *testing.T) {
	got := ExtractSalaryAmounts("$160k - $180k")
	if len(got) != 2 {
		t.Fatalf("expected 2 amounts, got %#v", got)
	}
	if got[0] != 160000 || got[1] != 180000 {
		t.Fatalf("unexpected extracted amounts %#v", got)
	}
}
