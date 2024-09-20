package pgutil

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

func randomHexString(n int) (string, error) {
	uuid := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, uuid); err != nil {
		return "", err
	}

	payload := make([]byte, n*2)
	hex.Encode(payload, uuid)
	return string(payload), nil
}
