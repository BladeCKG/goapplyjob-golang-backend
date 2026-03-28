package currency

import (
	"regexp"
	"strings"
)

var SymbolToCode = map[string]string{
	"$":   "USD",
	"€":   "EUR",
	"£":   "GBP",
	"₹":   "INR",
	"¥":   "JPY",
	"₩":   "KRW",
	"₽":   "RUB",
	"₺":   "TRY",
	"₫":   "VND",
	"₪":   "ILS",
	"CHF": "CHF",
	"฿":   "THB",
	"₱":   "PHP",
	"₦":   "NGN",
	"C$":  "CAD",
	"A$":  "AUD",
	"NZ$": "NZD",
	"S$":  "SGD",
	"HK$": "HKD",
	"R$":  "BRL",
	"MX$": "MXN",
}

var CodeToSymbol = map[string]string{
	"USD": "$",
	"EUR": "€",
	"GBP": "£",
	"CAD": "C$",
	"AUD": "A$",
	"NZD": "NZ$",
	"SGD": "S$",
	"HKD": "HK$",
	"INR": "₹",
	"JPY": "¥",
	"CNY": "¥",
	"RMB": "¥",
	"CHF": "CHF",
	"SEK": "SEK",
	"NOK": "NOK",
	"DKK": "DKK",
	"BRL": "R$",
	"MXN": "MX$",
	"ZAR": "ZAR",
	"PLN": "PLN",
	"CZK": "CZK",
	"HUF": "HUF",
	"RON": "RON",
	"KRW": "₩",
	"RUB": "₽",
	"TRY": "₺",
	"VND": "₫",
	"ILS": "₪",
	"THB": "฿",
	"PHP": "₱",
	"NGN": "₦",
}

var (
	SalaryNumberPattern = regexp.MustCompile(`([€£¥₹₩₽₺₫₪฿₱₦$]|\b(?:C\$|A\$|NZ\$|S\$|HK\$|R\$|MX\$)\b)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?`)
	SalaryHintPattern   = regexp.MustCompile(`(?:[$€£¥₹₩₽₺₫₪฿₱₦]|\b(?:c\$|a\$|nz\$|s\$|hk\$|r\$|mx\$)\b|\b(?:usd|eur|gbp|cad|aud|nzd|sgd|hkd|inr|jpy|cny|rmb|chf|sek|nok|dkk|brl|mxn|zar|pln|czk|huf|ron|krw|rub|try|vnd|ils|thb|php|ngn|salary|compensation|hour|hr|day|week|month|year|annual|annum|monthly|weekly|daily)\b|/[a-z]+)`)
)

func DetectCurrency(txt string) (code string, symbol string, ok bool) {
	txtUpper := strings.ToUpper(txt)

	// Prefer explicit currency code first
	for c, s := range CodeToSymbol {
		if strings.Contains(txtUpper, c) {
			return c, s, true
		}
	}

	// Then detect by symbol, preferring longer symbols first
	longestCode := ""
	longestSymbol := ""

	for c, s := range CodeToSymbol {
		if strings.Contains(txt, s) && len(s) > len(longestSymbol) {
			longestCode = c
			longestSymbol = s
		}
	}

	if longestCode != "" {
		return longestCode, longestSymbol, true
	}

	return "", "", false
}

func Detect(text, symbol string) (string, string) {
	trimmedSymbol := strings.TrimSpace(symbol)
	if trimmedSymbol != "" {
		if code, ok := SymbolToCode[trimmedSymbol]; ok {
			return code, trimmedSymbol
		}
	}

	lowered := strings.ToLower(text)
	switch {
	case strings.Contains(lowered, "c$") || strings.Contains(lowered, "ca$"):
		return "CAD", "C$"
	case strings.Contains(lowered, "a$") || strings.Contains(lowered, "au$"):
		return "AUD", "A$"
	case strings.Contains(lowered, "nz$"):
		return "NZD", "NZ$"
	case strings.Contains(lowered, "s$"):
		return "SGD", "S$"
	case strings.Contains(lowered, "hk$"):
		return "HKD", "HK$"
	case strings.Contains(lowered, "r$"):
		return "BRL", "R$"
	case strings.Contains(lowered, "mx$"):
		return "MXN", "MX$"
	}

	for sym, code := range SymbolToCode {
		if strings.Contains(text, sym) {
			return code, sym
		}
	}

	for _, token := range SplitTokens(text) {
		if symbol, ok := CodeToSymbol[token]; ok {
			return token, symbol
		}
	}

	return "USD", firstNonEmpty(trimmedSymbol, CodeToSymbol["USD"])
}

func SymbolForCode(code string) string {
	normalized := strings.ToUpper(code)
	if normalized == "" {
		return ""
	}
	return CodeToSymbol[normalized]
}

func SplitTokens(value string) []string {
	upper := strings.ToUpper(value)
	tokens := []string{}
	var buf strings.Builder
	flush := func() {
		if buf.Len() == 0 {
			return
		}
		tokens = append(tokens, buf.String())
		buf.Reset()
	}
	for _, r := range upper {
		if r >= 'A' && r <= 'Z' {
			buf.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return tokens
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
