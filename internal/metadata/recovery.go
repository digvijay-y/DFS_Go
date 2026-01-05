package metadata

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

type Snapshot struct {
	Files map[string]map[int]ChunkMetadata
}

func (s *Server) ReplayWAL(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var e WALEntry
		json.Unmarshal(scanner.Bytes(), &e)

		switch e.Type {
		case "REGISTER_NODE":
			var payload struct {
				NodeID  string `json:"node_id"`
				Address string `json:"address"`
			}
			if err := json.Unmarshal(e.Data, &payload); err != nil {
				continue
			}

			s.State.Nodes[payload.NodeID] = NodeStatus{
				Address:  payload.Address,
				Lastseen: time.Time{},
			}
		case "CREATE_FILE":
			var filename string
			if err := json.Unmarshal(e.Data, &filename); err != nil {
				continue
			}

			if _, ok := s.State.Files[filename]; !ok {
				s.State.Files[filename] = make(map[int]ChunkMetadata)
			}
		case "ALLOCATE_CHUNK":
			var payload struct {
				Filename   string   `json:"filename"`
				ChunkIndex int      `json:"chunkIndex"`
				ChunkId    string   `json:"chunkId"`
				Nodes      []string `json:"nodes"`
			}
			if err := json.Unmarshal(e.Data, &payload); err != nil {
				continue
			}

			if _, ok := s.State.Files[payload.Filename]; !ok {
				s.State.Files[payload.Filename] = make(map[int]ChunkMetadata)
			}

			s.State.Files[payload.Filename][payload.ChunkIndex] = ChunkMetadata{
				ChunkId: payload.ChunkId,
				Nodes:   payload.Nodes,
			}
		case "ADD_REPLICA":
			var payload struct {
				Filename   string `json:"filename"`
				ChunkIndex int    `json:"chunkIndex"`
				Node       string `json:"node"`
			}
			if err := json.Unmarshal(e.Data, &payload); err != nil {
				continue
			}

			chunk := s.State.Files[payload.Filename][payload.ChunkIndex]

			// avoid duplicates
			for _, n := range chunk.Nodes {
				if n == payload.Node {
					goto done
				}
			}

			chunk.Nodes = append(chunk.Nodes, payload.Node)
			s.State.Files[payload.Filename][payload.ChunkIndex] = chunk

		done:
		}
	}
}

func (s *Server) WriteSnapShot() {
	s.State.Mu.RLock()
	defer s.State.Mu.RUnlock()

	b, _ := json.Marshal(s.State.Files)
	os.WriteFile("metadata.snapshot", b, 0644)
}
