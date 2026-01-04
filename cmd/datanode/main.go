package main

import (
	"DFS_GO/internal/datanode"
	pb "DFS_GO/internal/proto"
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	lis, err := net.Listen("tcp", ":6001")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("DataNode listens to port 6001")

	// Increase max message size to 16MB for large chunks
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)

	pb.RegisterDataNodeServiceServer(
		grpcServer,
		&datanode.Server{DataDir: "./data/dn1"},
	)

	conn, err := grpc.Dial("localhost:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)

	_, err = client.RegisterNode(context.Background(), &pb.NodeInfo{
		NodeId:  "dn1",
		Address: "localhost:6001",
	})
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	log.Println("DataNode registered with metadata server")

	// Start heartbeat loop
	datanode.StartHeartbeat("dn1", client)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Storage Server Error: %v", err)
	}
}
