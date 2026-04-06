package captcha

import "context"

// Solver defines the interface for a captcha solving service.
type Solver interface {
	Solve(ctx context.Context, captchaType, url, siteKey string) (string, error)
}
