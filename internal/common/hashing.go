package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func ChunkId(filename string, index int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", filename, index)))
	return hex.EncodeToString(h[:])
}
