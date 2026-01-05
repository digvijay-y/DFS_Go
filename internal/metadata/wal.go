package metadata

import (
	"encoding/json"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

type WALEntry struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func NewWAL(path string) *WAL {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &WAL{file: f}
}

func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = w.file.Write(append(b, '\n'))
	return err
}
