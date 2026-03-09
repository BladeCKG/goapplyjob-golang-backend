package main

import (
	"testing"
	"time"
)

func TestMakeReadHTMLWith429RetrySwallowsNetworkError(t *testing.T) {
	readHTML := makeReadHTMLWith429Retry(0, 0)
	html, status, err := readHTML("http://127.0.0.1:1/unreachable")
	if err != nil {
		t.Fatalf("expected network errors to be converted to retry outcome, got error=%v", err)
	}
	if status != -1 {
		t.Fatalf("expected status -1 for network error, got %d", status)
	}
	if html != "" {
		t.Fatalf("expected empty html on network error, got %q", html)
	}
}

func TestMakeReadHTMLWith429RetryDoesNotErrorOnReadFailurePath(t *testing.T) {
	readHTML := makeReadHTMLWith429Retry(1, 1*time.Millisecond)
	_, _, _ = readHTML("http://127.0.0.1:1/unreachable")
}
