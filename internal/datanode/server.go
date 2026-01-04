package datanode

import (
	pb "DFS_GO/internal/proto"
	"context"
	"path/filepath"
)

type Server struct {
	pb.UnimplementedDataNodeServiceServer
	DataDir string
}

func (s *Server) StoreChunk(ctx context.Context, c *pb.Chunk) (*pb.Ack, error) {
	path := filepath.Join(s.DataDir, c.ChunkId)
	err := WriteChunk(path, c.Data)

	return &pb.Ack{Ok: err == nil}, err
}

func (s *Server) GetChunk(ctx context.Context, req *pb.ChunkRequest) (*pb.Chunk, error) {
	path := filepath.Join(s.DataDir, req.ChunkId)
	data, err := Readchunk(path)
	if err != nil {
		return nil, err
	}

	return &pb.Chunk{
		ChunkId: req.ChunkId,
		Data:    data,
	}, nil
}
