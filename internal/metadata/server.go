package metadata

import (
	"DFS_GO/internal/common"
	pb "DFS_GO/internal/proto"
	"context"
	"fmt"
	"time"
)

type Server struct {
	pb.UnimplementedMetadataServiceServer
	State *State
}

func NewServer() *Server {
	return &Server{State: NewState()}
}

func (s *Server) RegisterNode(ctx context.Context, n *pb.NodeInfo) (*pb.Ack, error) {
	//register the Node
	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()
	s.State.Nodes[n.NodeId] = NodeStatus{
		Address:  n.Address,
		Lastseen: time.Now(),
	}
	return &pb.Ack{Ok: true}, nil
}

func (s *Server) CreateFile(ctx context.Context, req *pb.FileRequest) (*pb.FileMetadata, error) {
	// Intent to upload File
	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()

	if _, exists := s.State.Files[req.Filename]; exists {
		return nil, fmt.Errorf("file exists")
	}

	s.State.Files[req.Filename] = make(map[int]ChunkMetadata)

	return &pb.FileMetadata{
		Filename: req.Filename,
	}, nil
}

func (s *Server) AllocateChunk(
	ctx context.Context,
	req *pb.AllocateChunkRequest,
) (*pb.ChunkMetadata, error) {

	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()

	// Ensure file entry exists
	if _, ok := s.State.Files[req.Filename]; !ok {
		s.State.Files[req.Filename] = make(map[int]ChunkMetadata)
	}

	// If chunk already exists, return existing metadata (idempotent)
	if meta, ok := s.State.Files[req.Filename][int(req.ChunkIndex)]; ok {
		return &pb.ChunkMetadata{
			ChunkId: meta.ChunkId,
			Nodes:   meta.Nodes,
		}, nil
	}

	// Pick replica nodes (replication-aware)
	nodes := PickReplicaNodes(s.State.Nodes, common.ReplicationFactor)

	meta := ChunkMetadata{
		ChunkId: req.ChunkId,
		Nodes:   nodes,
	}

	// Store metadata indexed by chunk index
	s.State.Files[req.Filename][int(req.ChunkIndex)] = meta

	return &pb.ChunkMetadata{
		ChunkId: meta.ChunkId,
		Nodes:   meta.Nodes,
	}, nil
}

func (s *Server) GetFile(ctx context.Context, req *pb.FileRequest) (*pb.FileMetadata, error) {
	// Download
	s.State.Mu.RLock()
	defer s.State.Mu.RUnlock()

	chunksMap, ok := s.State.Files[req.Filename]

	if !ok {
		return nil, fmt.Errorf("File not Found: 404")
	}

	// Rebuild ordered slice from map
	ordered := make([]*pb.ChunkMetadata, len(chunksMap))
	for idx, meta := range chunksMap {
		ordered[idx] = &pb.ChunkMetadata{
			ChunkId: meta.ChunkId,
			Nodes:   meta.Nodes,
		}
	}

	return &pb.FileMetadata{
		Filename: req.Filename,
		Chunks:   ordered,
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, hb *pb.NodeHeartbeat) (*pb.Ack, error) {
	// If no Heartbeat its DEAD!
	s.State.Mu.Lock()
	defer s.State.Mu.Unlock()

	node, ok := s.State.Nodes[hb.NodeId]
	if !ok {
		return &pb.Ack{Ok: false}, nil
	}

	node.Lastseen = time.Now()
	s.State.Nodes[hb.NodeId] = node

	return &pb.Ack{Ok: true}, nil
}
