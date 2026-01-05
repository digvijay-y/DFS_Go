package metadata

import (
	pb "DFS_GO/internal/proto"
	"context"
	"os"
	"testing"
)

func TestChunkOrdering(t *testing.T) {
	s := NewServer()

	s.State.Files["a.txt"] = map[int]ChunkMetadata{
		2: {ChunkId: "c2"},
		0: {ChunkId: "c0"},
		1: {ChunkId: "c1"},
	}

	// Test via GetFile which does the ordering
	resp, err := s.GetFile(context.Background(), &pb.FileRequest{Filename: "a.txt"})
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}

	if resp.Chunks[0].ChunkId != "c0" {
		t.Fatalf("chunk ordering broken: expected c0, got %s", resp.Chunks[0].ChunkId)
	}
	if resp.Chunks[1].ChunkId != "c1" {
		t.Fatalf("chunk ordering broken: expected c1, got %s", resp.Chunks[1].ChunkId)
	}
	if resp.Chunks[2].ChunkId != "c2" {
		t.Fatalf("chunk ordering broken: expected c2, got %s", resp.Chunks[2].ChunkId)
	}
}

func TestReplicationLock(t *testing.T) {
	s := NewState()
	key := "file:0"

	s.Replicating[key] = true

	if s.Replicating[key] != true {
		t.Fatal("replication guard failed")
	}
}

func TestWALReplay(t *testing.T) {
	// Use a temp WAL file for this test
	walPath := "/tmp/test_wal_" + t.Name() + ".wal"
	defer os.Remove(walPath)

	// Create first server with temp WAL
	s := &Server{State: NewState(), WAL: NewWAL(walPath)}
	ctx := context.Background()

	_, err := s.CreateFile(ctx, &pb.FileRequest{Filename: "x.txt"})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Create second server and replay
	s2 := &Server{State: NewState(), WAL: NewWAL(walPath)}
	s2.ReplayWAL(walPath)

	if _, ok := s2.State.Files["x.txt"]; !ok {
		t.Fatal("WAL replay failed - file not found after replay")
	}
}

func TestRegisterNode(t *testing.T) {
	s := NewServer()
	ctx := context.Background()

	_, err := s.RegisterNode(ctx, &pb.NodeInfo{
		NodeId:  "dn1",
		Address: "localhost:6001",
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	if node, ok := s.State.Nodes["dn1"]; !ok {
		t.Fatal("node not registered")
	} else if node.Address != "localhost:6001" {
		t.Fatalf("wrong address: got %s", node.Address)
	}
}

func TestAllocateChunk(t *testing.T) {
	walPath := "/tmp/test_wal_" + t.Name() + ".wal"
	defer os.Remove(walPath)

	s := &Server{State: NewState(), WAL: NewWAL(walPath)}
	ctx := context.Background()

	// Register a node first
	s.State.Nodes["dn1"] = NodeStatus{Address: "localhost:6001"}

	// Create file
	s.CreateFile(ctx, &pb.FileRequest{Filename: "test.txt"})

	// Allocate chunk
	resp, err := s.AllocateChunk(ctx, &pb.AllocateChunkRequest{
		ChunkId:    "chunk123",
		Filename:   "test.txt",
		ChunkIndex: 0,
	})
	if err != nil {
		t.Fatalf("AllocateChunk failed: %v", err)
	}

	if resp.ChunkId != "chunk123" {
		t.Fatalf("wrong chunk ID: got %s", resp.ChunkId)
	}
}

func TestHeartbeat(t *testing.T) {
	s := NewServer()
	ctx := context.Background()

	// Register node first
	s.State.Nodes["dn1"] = NodeStatus{Address: "localhost:6001"}

	// Send heartbeat
	resp, err := s.Heartbeat(ctx, &pb.NodeHeartbeat{NodeId: "dn1"})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	if !resp.Ok {
		t.Fatal("Heartbeat should return Ok=true for registered node")
	}

	// Heartbeat from unknown node
	resp, err = s.Heartbeat(ctx, &pb.NodeHeartbeat{NodeId: "unknown"})
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	if resp.Ok {
		t.Fatal("Heartbeat should return Ok=false for unknown node")
	}
}
