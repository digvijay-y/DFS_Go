package metadata

// Scanner
func (s *Server) FindUnderReplicatedChunks(rf int) []struct {
	Filename   string
	ChunkIndex int
	Meta       ChunkMetadata
} {
	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()

	var res []struct {
		Filename   string
		ChunkIndex int
		Meta       ChunkMetadata
	}

	for filename, chunks := range s.State.Files {
		for idx, meta := range chunks {
			if len(meta.Nodes) < rf {
				res = append(res, struct {
					Filename   string
					ChunkIndex int
					Meta       ChunkMetadata
				}{filename, idx, meta})
			}
		}
	}

	return res
}
