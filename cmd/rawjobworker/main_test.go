package main

import (
	"context"
	"testing"
	"time"
)

func TestMakeReadHTMLWith429RetrySwallowsNetworkError(t *testing.T) {
	readHTML := makeReadHTMLWith429Retry(0, 0)
	html, status, err := readHTML(context.Background(), "http://127.0.0.1:1/unreachable")
	if err == nil {
		if status != -1 {
			t.Fatalf("expected status -1 for network error, got %d", status)
		}
		if html != "" {
			t.Fatalf("expected empty html on network error, got %q", html)
		}
	}
}

func TestMakeReadHTMLWith429RetryDoesNotErrorOnReadFailurePath(t *testing.T) {
	readHTML := makeReadHTMLWith429Retry(1, 1*time.Millisecond)
	_, _, _ = readHTML(context.Background(), "http://127.0.0.1:1/unreachable")
}
