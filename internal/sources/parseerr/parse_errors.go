package parseerr

import "fmt"

type Error struct {
	Reason string
	Retry  bool
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason == "" {
		if e.Retry {
			return "parse retry requested"
		}
		return "parse skip requested"
	}
	return fmt.Sprintf("parse error: %s", e.Reason)
}

func Retry(reason string) error {
	return &Error{Reason: reason, Retry: true}
}

func Skip(reason string) error {
	return &Error{Reason: reason, Retry: false}
}

func IsRetry(err error) bool {
	if err == nil {
		return false
	}
	parseErr, ok := err.(*Error)
	return ok && parseErr.Retry
}

func Reason(err error) string {
	if err == nil {
		return ""
	}
	if parseErr, ok := err.(*Error); ok {
		return parseErr.Reason
	}
	return err.Error()
}
