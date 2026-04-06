package captcha

// Solver defines the interface for a captcha solving service.
type Solver interface {
	Solve(captchaType, url, siteKey string) (string, error)
}
