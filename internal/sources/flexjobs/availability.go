package flexjobs

func IsJobClosed(status int, bodyText string, _ string) bool {
	if status == 404 || status == 410 {
		return true
	}

	return false
}
