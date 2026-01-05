package datanode

import (
	pb "DFS_GO/internal/proto"
	"context"
	"log"
	"time"
)

func StartHeartbeat(nodeId string, meta pb.MetadataServiceClient) {
	go func() {
		log.Printf("Starting heartbeat loop for node %s", nodeId)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

			_, err := meta.Heartbeat(ctx, &pb.NodeHeartbeat{
				NodeId: nodeId,
			})

			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			} else {
				log.Printf("Heartbeat sent successfully from node %s", nodeId)
			}

			cancel()
			time.Sleep(3 * time.Second)
		}
	}()
}
