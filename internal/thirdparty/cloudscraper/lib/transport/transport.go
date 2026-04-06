package transport

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

type CipherSuiteTransport struct {
	*http.Transport
}

func NewTransport() *CipherSuiteTransport {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{},
	}
	return &CipherSuiteTransport{tr}
}

func (t *CipherSuiteTransport) SetCipherSuites(suites []uint16) {
	t.Transport.TLSClientConfig.CipherSuites = suites
	t.Transport.TLSClientConfig.MinVersion = tls.VersionTLS12
}
