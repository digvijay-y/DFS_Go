package metadata

import (
	"DFS_GO/internal/common"
	"encoding/json"
	"time"
)

func (s *Server) StartReplicationLoop() {
	go func() {
		for {
			time.Sleep(10 * time.Second)

			s.State.Mu.RLock()
			for fname, chunks := range s.State.Files {
				for idx, meta := range chunks {
					if len(meta.Nodes) < common.ReplicationFactor {
						go s.replicateChunk(fname, idx, meta)
					}
				}
			}
			s.State.Mu.RUnlock()
		}
	}()
}

func (s *Server) replicateChunk(
	filename string,
	chunkIndex int,
	meta ChunkMetadata,
) {

	// Step 1: pick source & target
	source := pickSource(meta.Nodes)
	if source == "" {
		return
	}

	target := pickTarget(s.State.Nodes, meta.Nodes)
	if target == "" {
		return // nowhere to replicate to
	}

	// Step 2: fetch chunk from source
	data, err := fetchChunk(source, meta.ChunkId)
	if err != nil {
		return
	}

	// Step 3: store chunk on target
	err = StoreChunk(target, meta.ChunkId, data)
	if err != nil {
		return
	}

	// Step 4: update metadata atomically
	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()

	// re-validate source and target
	if _, ok := s.State.Nodes[source]; !ok {
		return
	}
	if _, ok := s.State.Nodes[target]; !ok {
		return
	}

	// re-validate chunk still needs replication
	chunk := s.State.Files[filename][chunkIndex]
	if len(chunk.Nodes) >= common.ReplicationFactor {
		return
	}

	payload, err := json.Marshal(struct {
		Filename   string
		ChunkIndex int
		Node       string
	}{
		Filename:   filename,
		ChunkIndex: chunkIndex,
		Node:       target,
	})
	if err != nil {
		return
	}

	err = s.WAL.Append(WALEntry{
		Type: "ADD_REPLICA",
		Data: payload,
	})
	if err != nil {
		return
	}

	// now update metadata
	chunk.Nodes = append(chunk.Nodes, target)
	s.State.Files[filename][chunkIndex] = chunk

}
