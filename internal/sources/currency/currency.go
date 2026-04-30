package currency

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
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
	"NT$": "TWD",
	"₡":   "CRC",
	"₨":   "PKR",
	"₭":   "LAK",
	"₮":   "MNT",
	"₴":   "UAH",
	"₸":   "KZT",
	"₼":   "AZN",
	"₾":   "GEL",
	"₿":   "BTC",
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
	"ARS": "ARS",
	"BOB": "BOB",
	"CLP": "CLP",
	"COP": "COP",
	"CRC": "₡",
	"DOP": "DOP",
	"GTQ": "GTQ",
	"HNL": "HNL",
	"NIO": "NIO",
	"PAB": "PAB",
	"PEN": "PEN",
	"PYG": "PYG",
	"UYU": "UYU",
	"VES": "VES",
	"AED": "AED",
	"SAR": "SAR",
	"QAR": "QAR",
	"KWD": "KWD",
	"BHD": "BHD",
	"OMR": "OMR",
	"JOD": "JOD",
	"EGP": "EGP",
	"MAD": "MAD",
	"TND": "TND",
	"ZAR": "ZAR",
	"PLN": "PLN",
	"CZK": "CZK",
	"HUF": "HUF",
	"RON": "RON",
	"BGN": "BGN",
	"HRK": "HRK",
	"RSD": "RSD",
	"ISK": "ISK",
	"UAH": "₴",
	"KZT": "₸",
	"AZN": "₼",
	"GEL": "₾",
	"KRW": "₩",
	"RUB": "₽",
	"TRY": "₺",
	"VND": "₫",
	"ILS": "₪",
	"THB": "฿",
	"PHP": "₱",
	"NGN": "₦",
	"KES": "KES",
	"GHS": "GHS",
	"UGX": "UGX",
	"TZS": "TZS",
	"ZMW": "ZMW",
	"BWP": "BWP",
	"MUR": "MUR",
	"XOF": "XOF",
	"XAF": "XAF",
	"ETB": "ETB",
	"PKR": "₨",
	"BDT": "৳",
	"LKR": "Rs",
	"NPR": "Rs",
	"IDR": "IDR",
	"MYR": "MYR",
	"LAK": "₭",
	"MMK": "MMK",
	"KHR": "KHR",
	"MNT": "₮",
	"TWD": "NT$",
	"FJD": "FJD",
	"PGK": "PGK",
	"WST": "WST",
	"TOP": "TOP",
	"BTC": "₿",
}

var (
	SalaryNumberPattern = regexp.MustCompile(`([$€£₹¥₩₽₺₫₪฿₱₦₡₨₭₮₴₸₼₾₿]|\b(?:C\$|A\$|NZ\$|S\$|HK\$|R\$|MX\$|NT\$)\b)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?`)
	SalaryHintPattern   = regexp.MustCompile(`(?:[$€£₹¥₩₽₺₫₪฿₱₦₡₨₭₮₴₸₼₾₿]|\b(?:c\$|a\$|nz\$|s\$|hk\$|r\$|mx\$|nt\$)\b|\b(?:usd|eur|gbp|cad|aud|nzd|sgd|hkd|inr|jpy|cny|rmb|chf|sek|nok|dkk|brl|mxn|ars|bob|clp|cop|crc|dop|gtq|hnl|nio|pab|pen|pyg|uyu|ves|aed|sar|qar|kwd|bhd|omr|jod|egp|mad|tnd|zar|pln|czk|huf|ron|bgn|hrk|rsd|isk|uah|kzt|azn|gel|krw|rub|try|vnd|ils|thb|php|ngn|kes|ghs|ugx|tzs|zmw|bwp|mur|xof|xaf|etb|pkr|bdt|lkr|npr|idr|myr|lak|mmk|khr|mnt|twd|fjd|pgk|wst|top|btc|salary|compensation|hour|hr|day|week|month|year|annual|annum|monthly|weekly|daily)\b|/[a-z]+)`)
)

func DetectCurrency(txt string) (code string, symbol string, ok bool) {
	txtUpper := strings.ToUpper(txt)

	for c, s := range CodeToSymbol {
		if strings.Contains(txtUpper, c) {
			return c, s, true
		}
	}

	longestCode := ""
	longestSymbol := ""
	for c, s := range CodeToSymbol {
		if s == "" {
			continue
		}
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
	case strings.Contains(lowered, "nt$"):
		return "TWD", "NT$"
	}

	for sym, code := range SymbolToCode {
		if strings.Contains(text, sym) {
			return code, sym
		}
	}

	for _, token := range SplitTokens(text) {
		if sym, ok := CodeToSymbol[token]; ok {
			return token, sym
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

func ExtractSalaryAmounts(text string) []float64 {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	matches := SalaryNumberPattern.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return nil
	}
	amounts := make([]float64, 0, len(matches))
	for _, match := range matches {
		if len(match) < 8 || match[4] < 0 || match[5] < 0 {
			continue
		}
		numberText := text[match[4]:match[5]]
		suffix := ""
		if match[6] >= 0 && match[7] >= 0 {
			candidateSuffix := text[match[6]:match[7]]
			if suffixIsStandalone(text, match[7]) {
				suffix = candidateSuffix
			}
		}
		amount := ParseSalaryAmount(numberText, suffix)
		if amount > 0 {
			amounts = append(amounts, amount)
		}
	}
	if len(amounts) == 0 {
		return nil
	}
	return amounts
}

func ParseSalaryAmount(numberText, suffix string) float64 {
	clean := strings.TrimSpace(strings.ReplaceAll(numberText, ",", ""))
	if clean == "" {
		return 0
	}
	value, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(suffix)) {
	case "k":
		value *= 1000
	case "m":
		value *= 1000000
	}
	return value
}

func suffixIsStandalone(text string, suffixEnd int) bool {
	if suffixEnd < 0 || suffixEnd >= len(text) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(text[suffixEnd:])
	if r == utf8.RuneError {
		return true
	}
	return !unicode.IsLetter(r) && !unicode.IsDigit(r)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
