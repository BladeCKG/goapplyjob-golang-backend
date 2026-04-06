package useragent

import (
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
)

//go:embed browsers.json
var browserJSON []byte

// Agent represents a browser profile.
type Agent struct {
	Headers      http.Header
	CipherSuites []uint16
	Browser      string
}

// Config allows for filtering user agents.
type Config struct {
	Browser  string
	Platform string
	Desktop  bool
	Mobile   bool
	Custom   string
}

type browserData struct {
	Headers     map[string]map[string]string              `json:"headers"`
	CipherSuite map[string][]string                       `json:"cipherSuite"`
	UserAgents  map[string]map[string]map[string][]string `json:"user_agents"`
}

var tlsCipherMap = map[string]uint16{
	// TLS 1.3
	"TLS_AES_128_GCM_SHA256":       tls.TLS_AES_128_GCM_SHA256,
	"TLS_AES_256_GCM_SHA384":       tls.TLS_AES_256_GCM_SHA384,
	"TLS_CHACHA20_POLY1305_SHA256": tls.TLS_CHACHA20_POLY1305_SHA256,
	// ECDHE
	"ECDHE-ECDSA-AES128-GCM-SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-RSA-AES128-GCM-SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-ECDSA-AES256-GCM-SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-RSA-AES256-GCM-SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-ECDSA-CHACHA20-POLY1305": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	"ECDHE-RSA-CHACHA20-POLY1305":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	"ECDHE-RSA-AES128-SHA":          tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	"ECDHE-RSA-AES256-SHA":          tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	"ECDHE-ECDSA-AES128-SHA":        tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	"ECDHE-ECDSA-AES256-SHA":        tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	// DHE
	"DHE-RSA-AES128-SHA": tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"DHE-RSA-AES256-SHA": tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	// Legacy
	"AES128-GCM-SHA256": tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"AES256-GCM-SHA384": tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	"AES128-SHA":        tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"AES256-SHA":        tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	"DES-CBC3-SHA":      tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
}

// New creates a new User-Agent based on the provided config.
func New(cfg Config) (*Agent, error) {
	if len(browserJSON) == 0 {
		return nil, fmt.Errorf("browsers.json was not embedded")
	}

	var browsers browserData
	if err := json.Unmarshal(browserJSON, &browsers); err != nil {
		return nil, fmt.Errorf("could not parse browsers.json: %w", err)
	}

	if cfg.Custom != "" {
		return &Agent{
			Headers: http.Header{"User-Agent": []string{cfg.Custom}},
			// Provide a generic but strong cipher suite for custom UAs
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_CHACHA20_POLY1305_SHA256,
			},
		}, nil
	}

	// Default to all device types if none specified
	if !cfg.Desktop && !cfg.Mobile {
		cfg.Desktop = true
		cfg.Mobile = true
	}

	var candidates []string
	if cfg.Browser == "" {
		candidates = []string{"chrome", "firefox"}
	} else {
		candidates = []string{cfg.Browser}
	}

	browser := candidates[rand.Intn(len(candidates))]

	// Filter user agents based on config
	var availableUAs []string
	if cfg.Desktop {
		addFilteredUAs(&availableUAs, browsers.UserAgents["desktop"], cfg.Platform, browser)
	}
	if cfg.Mobile {
		addFilteredUAs(&availableUAs, browsers.UserAgents["mobile"], cfg.Platform, browser)
	}

	if len(availableUAs) == 0 {
		return nil, fmt.Errorf("no user agents match the specified criteria (Browser: %s, Platform: %s, Desktop: %t, Mobile: %t)", cfg.Browser, cfg.Platform, cfg.Desktop, cfg.Mobile)
	}

	ua := availableUAs[rand.Intn(len(availableUAs))]

	headers := make(http.Header)
	for key, value := range browsers.Headers[browser] {
		headers.Set(key, value)
	}
	headers.Set("User-Agent", ua)

	var ciphers []uint16
	for _, name := range browsers.CipherSuite[browser] {
		if val, ok := tlsCipherMap[strings.ToUpper(name)]; ok {
			ciphers = append(ciphers, val)
		}
	}

	return &Agent{
		Headers:      headers,
		CipherSuites: ciphers,
		Browser:      browser,
	}, nil
}

func addFilteredUAs(target *[]string, source map[string]map[string][]string, platform, browser string) {
	if platform != "" {
		if p, ok := source[platform]; ok {
			if uas, ok := p[browser]; ok {
				*target = append(*target, uas...)
			}
		}
	} else {
		for _, p := range source {
			if uas, ok := p[browser]; ok {
				*target = append(*target, uas...)
			}
		}
	}
}
