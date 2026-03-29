package dailyremote

import "strings"

func IsJobClosed(status int, bodyText string, _ string) bool {
	if status == 404 || status == 410 {
		return true
	}
	return strings.Contains(strings.ToLower(bodyText), "job no longer available")
}
