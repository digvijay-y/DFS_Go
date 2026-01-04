package client

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"DFS_GO/internal/common"
	pb "DFS_GO/internal/proto"

	"google.golang.org/grpc"
)

func Upload(filename string, meta pb.MetadataServiceClient) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	chunks := Chunk(data, 4*1024*1024)
	baseName := filepath.Base(filename)

	// Tell metadata server we intend to upload this file
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = meta.CreateFile(ctx, &pb.FileRequest{Filename: baseName})
	if err != nil {
		log.Printf("Warning: CreateFile failed (file may exist): %v", err)
	}

	// ----- CONCURRENCY CONTROL -----
	maxWorkers := 4
	sem := make(chan struct{}, maxWorkers)

	var wg sync.WaitGroup
	errs := make(chan error, len(chunks))

	for i, chunkData := range chunks {
		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(i int, c []byte) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			chunkId := common.ChunkId(baseName, i)

			// Ask metadata where to store this chunk
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			metaResp, err := meta.AllocateChunk(ctx, &pb.AllocateChunkRequest{
				ChunkId:    chunkId,
				Filename:   baseName,
				ChunkIndex: int32(i),
			})
			if err != nil {
				errs <- err
				return
			}

			// Send chunk to assigned DataNodes
			successful := 0
			var lastErr error

			for _, nodeAddr := range metaResp.Nodes {
				conn, err := grpc.Dial(
					nodeAddr,
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
				_, err = dn.StoreChunk(ctx, &pb.Chunk{
					ChunkId: chunkId,
					Data:    c,
				})
				cancel()
				conn.Close()

				if err == nil {
					successful++
				} else {
					lastErr = err
				}
			}

			// At least one replica must succeed
			if successful == 0 {
				errs <- lastErr
				return
			}

		}(i, chunkData)
	}

	wg.Wait()
	close(errs)

	if len(errs) > 0 {
		return <-errs
	}

	return nil
}
