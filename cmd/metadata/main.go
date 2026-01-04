package main

import (
	"DFS_GO/internal/metadata"
	pb "DFS_GO/internal/proto"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":5000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Metadata listens to port 5000")

	server := metadata.NewServer()
	server.StartCleanupLoop()

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server Failed: %v", err)
	}
}
