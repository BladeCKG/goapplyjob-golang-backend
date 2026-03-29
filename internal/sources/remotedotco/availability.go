package remotedotco

func IsJobClosed(status int, bodyText string, _ string) bool {
	return status == 404 || status == 410
}
