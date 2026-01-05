package main

import (
	"DFS_GO/internal/common"
	"DFS_GO/internal/metadata"
	pb "DFS_GO/internal/proto"
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config/metadata.yaml", "path to config file")
	flag.Parse()

	// Load configuration
	cfg, err := common.LoadMetadataConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Use config values
	lis, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Metadata server listening on %s", cfg.Address)

	// Create server with config (WAL path from config)
	server := metadata.NewServer()
	server.StartCleanupLoop()

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server Failed: %v", err)
	}
}
