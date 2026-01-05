package metadata

func buildOrderedChunks(chunks map[int]ChunkMetadata) []ChunkMetadata {

	size := len(chunks)
	ordered := make([]ChunkMetadata, size)

	for idx, meta := range chunks {
		if idx < 0 || idx >= size {
			panic("invalid chunk index in metadata")
		}
		ordered[idx] = meta
	}

	return ordered
}
