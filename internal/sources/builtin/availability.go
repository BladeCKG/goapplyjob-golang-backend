package builtin

import "strings"

func IsJobClosed(status int, bodyText string, _ string) bool {
	if status == 404 || status == 410 {
		return true
	}
	bodyText = strings.ToLower(bodyText)
	return strings.Contains(bodyText, "sorry, this job was removed")
}
