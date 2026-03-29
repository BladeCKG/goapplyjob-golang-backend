package remoterocketship

func IsJobClosed(status int, _ string, _ string) bool {
	return status == 404 || status == 410
}
