package randutil

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// Hex returns a random hexadecimal string with 2*size characters.
func Hex(size int) string {
	if size <= 0 {
		size = 8
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(buf)
}
