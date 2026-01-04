package metadata

import (
	pb "DFS_GO/internal/proto"
	"context"
	"time"

	"google.golang.org/grpc"
)

func fetchChunk(addr, ChunkId string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	dn := pb.NewDataNodeServiceClient(conn)

	resp, err := dn.GetChunk(ctx, &pb.ChunkRequest{
		ChunkId: ChunkId,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func StoreChunk(addr, ChunkId string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	dn := pb.NewDataNodeServiceClient(conn)

	_, err = dn.StoreChunk(ctx, &pb.Chunk{
		ChunkId: ChunkId,
		Data:    data,
	})

	return err
}
