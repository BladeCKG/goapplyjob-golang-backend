package stealth

import (
	"math/rand"
	"net/http"
	"time"
)

// Options configures the stealth mode.
type Options struct {
	Enabled          bool
	MinDelay         time.Duration
	MaxDelay         time.Duration
	HumanLikeDelays  bool
	RandomizeHeaders bool
	BrowserQuirks    bool
}

// Mode handles applying stealth techniques.
type Mode struct {
	opts            Options
	requestCount    int
	lastRequestTime time.Time
}

// New creates a new StealthMode instance.
func New(opts Options) *Mode {
	if !opts.Enabled {
		return &Mode{opts: opts}
	}

	// Set reasonable defaults if not provided
	if opts.MinDelay == 0 {
		opts.MinDelay = 500 * time.Millisecond
	}
	if opts.MaxDelay == 0 {
		opts.MaxDelay = 2 * time.Second
	}

	return &Mode{opts: opts}
}

// Apply applies all configured stealth techniques to a request.
func (s *Mode) Apply(req *http.Request, browser string) {
	if !s.opts.Enabled {
		return
	}

	s.applyDelay()

	if s.opts.RandomizeHeaders {
		s.randomizeHeaders(req.Header)
	}

	if s.opts.BrowserQuirks {
		s.applyBrowserQuirks(req.Header, browser)
	}

	s.requestCount++
	s.lastRequestTime = time.Now()
}

func (s *Mode) applyDelay() {
	if s.requestCount == 0 {
		return
	}
	if s.opts.HumanLikeDelays {
		delay := s.opts.MinDelay + time.Duration(rand.Int63n(int64(s.opts.MaxDelay-s.opts.MinDelay)))
		time.Sleep(delay)
	}
}

func (s *Mode) randomizeHeaders(h http.Header) {
	acceptLangs := []string{"en-US,en;q=0.9", "en-GB,en;q=0.8", "en-CA,en;q=0.7"}
	if h.Get("Accept-Language") == "" {
		h.Set("Accept-Language", acceptLangs[rand.Intn(len(acceptLangs))])
	}
}

func (s *Mode) applyBrowserQuirks(h http.Header, browser string) {
	// The exact order of headers is difficult to control in Go's net/http,
	// but we can ensure browser-specific headers are present.
	var quirks map[string]string
	switch browser {
	case "chrome":
		quirks = map[string]string{
			"sec-ch-ua":          `"Not_A Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"`,
			"sec-ch-ua-mobile":   "?0",
			"sec-ch-ua-platform": `"Windows"`,
			"Sec-Fetch-Site":     "none",
			"Sec-Fetch-Mode":     "navigate",
			"Sec-Fetch-User":     "?1",
			"Sec-Fetch-Dest":     "document",
		}
	case "firefox":
		quirks = map[string]string{
			"Upgrade-Insecure-Requests": "1",
		}
	}

	for key, value := range quirks {
		if h.Get(key) == "" {
			h.Set(key, value)
		}
	}
}
