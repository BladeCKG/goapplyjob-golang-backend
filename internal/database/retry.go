package database

import (
	"errors"
	"strings"
	"time"
)

func IsLockedError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked")
}

func RetryLocked(attempts int, baseDelay time.Duration, op func() error) error {
	if attempts < 0 {
		attempts = 0
	}
	if baseDelay <= 0 {
		baseDelay = 50 * time.Millisecond
	}
	var lastErr error
	for attempt := 0; attempt <= attempts; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsLockedError(err) || attempt >= attempts {
			return err
		}
		time.Sleep(baseDelay * time.Duration(1<<attempt))
	}
	if lastErr == nil {
		lastErr = errors.New("retry locked failed")
	}
	return lastErr
}
