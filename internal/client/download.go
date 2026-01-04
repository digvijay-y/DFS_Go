package client

import (
	"context"
	"log"
	"sync"
	"time"

	pb "DFS_GO/internal/proto"

	"google.golang.org/grpc"
)

/* Parallel download:

Allocate result buffer
Spawn goroutine per chunk
each goroutine writes to its index
wait
assemble
*/

func Download(filename string, meta pb.MetadataServiceClient) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := meta.GetFile(ctx, &pb.FileRequest{Filename: filename})

	if err != nil {
		log.Printf("Failed to get file metadata: %v", err)
		return nil, err
	}

	chunks := make([][]byte, len(resp.Chunks))
	errs := make(chan error, len(resp.Chunks))

	var wg sync.WaitGroup

	for i, c := range resp.Chunks {
		wg.Add(1)

		go func(i int, c *pb.ChunkMetadata) {
			defer wg.Done()

			var lastErr error

			for _, addr := range c.Nodes {
				conn, err := grpc.Dial(
					addr,
					grpc.WithInsecure(),
					grpc.WithDefaultCallOptions(
						grpc.MaxCallSendMsgSize(16*1024*1024),
						grpc.MaxCallRecvMsgSize(16*1024*1024),
					),
				)
				if err != nil {
					lastErr = err
					continue
				}

				dn := pb.NewDataNodeServiceClient(conn)

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				chunk, err := dn.GetChunk(ctx, &pb.ChunkRequest{ChunkId: c.ChunkId})
				cancel()
				conn.Close()

				if err == nil {
					chunks[i] = chunk.Data
					return
				}

				lastErr = err
			}

			// All replicas failed
			errs <- lastErr
		}(i, c)
	}

	wg.Wait()

	close(errs)

	if len(errs) > 0 {
		return nil, <-errs
	}

	var result []byte

	for _, c := range chunks {
		result = append(result, c...)
	}

	return result, nil

}
