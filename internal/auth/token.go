package auth

import (
	"crypto/rand"
	"encoding/base64"
)

func randomToken() string {
	buf := make([]byte, 36)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}
