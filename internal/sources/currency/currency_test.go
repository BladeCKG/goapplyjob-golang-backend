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
