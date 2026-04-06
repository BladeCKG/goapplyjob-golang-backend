package proxy

import (
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"
)

// Strategy defines the proxy rotation strategy.
type Strategy string

const (
	Sequential Strategy = "sequential"
	Random     Strategy = "random"
	Smart      Strategy = "smart" // Not yet implemented, defaults to random
)

// ProxyStat holds statistics for a single proxy.
type ProxyStat struct {
	Success  int
	Failure  int
	LastUsed time.Time
}

// Manager handles proxy rotation and temporary banning.
type Manager struct {
	mu            sync.Mutex
	proxies       []*url.URL
	strategy      Strategy
	currentIndex  int
	bannedProxies map[string]time.Time
	proxyStats    map[string]*ProxyStat
	banTime       time.Duration
}

// NewManager creates a new proxy manager.
func NewManager(proxyURLs []string, strategy Strategy, banTime time.Duration) (*Manager, error) {
	var proxies []*url.URL
	for _, p := range proxyURLs {
		parsed, err := url.Parse(p)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL '%s': %w", p, err)
		}
		proxies = append(proxies, parsed)
	}

	if strategy == "" {
		strategy = Sequential
	}

	return &Manager{
		proxies:       proxies,
		strategy:      strategy,
		bannedProxies: make(map[string]time.Time),
		proxyStats:    make(map[string]*ProxyStat),
		banTime:       banTime,
	}, nil
}

// GetProxy selects a proxy based on the configured strategy.
func (m *Manager) GetProxy() (*url.URL, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.proxies) == 0 {
		return nil, nil // No proxies configured
	}

	available := m.getAvailableProxies()
	if len(available) == 0 {
		return nil, fmt.Errorf("all proxies are currently banned")
	}

	var chosen *url.URL
	switch m.strategy {
	case Random:
		chosen = available[rand.Intn(len(available))]
	case Sequential:
		chosen = available[m.currentIndex%len(available)]
		m.currentIndex++
	case Smart:
		// Smart strategy: pick proxy with best success rate, least used.
		// For now, let's keep it simple and just do random.
		// A full implementation would require more complex scoring.
		chosen = available[rand.Intn(len(available))]
	default:
		return nil, fmt.Errorf("unknown proxy strategy: %s", m.strategy)
	}

	if _, ok := m.proxyStats[chosen.String()]; !ok {
		m.proxyStats[chosen.String()] = &ProxyStat{}
	}
	m.proxyStats[chosen.String()].LastUsed = time.Now()

	return chosen, nil
}

// ReportSuccess marks a proxy as successful.
func (m *Manager) ReportSuccess(proxy *url.URL) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pStr := proxy.String()
	delete(m.bannedProxies, pStr)
	if stat, ok := m.proxyStats[pStr]; ok {
		stat.Success++
	}
}

// ReportFailure marks a proxy as failed and bans it for the configured duration.
func (m *Manager) ReportFailure(proxy *url.URL) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pStr := proxy.String()
	m.bannedProxies[pStr] = time.Now()
	if stat, ok := m.proxyStats[pStr]; ok {
		stat.Failure++
	}
}

func (m *Manager) getAvailableProxies() []*url.URL {
	var available []*url.URL
	now := time.Now()
	for _, p := range m.proxies {
		if banTime, ok := m.bannedProxies[p.String()]; !ok || now.Sub(banTime) > m.banTime {
			available = append(available, p)
		}
	}
	return available
}
