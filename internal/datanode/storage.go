package datanode

import "os"

func WriteChunk(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}

func Readchunk(path string) ([]byte, error) {
	return os.ReadFile(path)
}
