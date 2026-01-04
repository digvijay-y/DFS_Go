package metadata

import (
	"sync"
	"time"
)

type ChunkMetadata struct {
	ChunkId string
	Nodes   []string
}

type NodeStatus struct {
	Address  string
	Lastseen time.Time
}

type State struct {
	Nodes       map[string]NodeStatus
	Files       map[string]map[int]ChunkMetadata
	Mu          sync.RWMutex
	Replicating map[string]bool
}

func NewState() *State {
	return &State{
		Nodes:       make(map[string]NodeStatus),
		Files:       make(map[string]map[int]ChunkMetadata),
		Replicating: make(map[string]bool),
	}
}

func (c ChunkMetadata) IsHealthy(rf int) bool {
	return len(c.Nodes) >= rf
}
