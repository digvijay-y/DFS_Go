package main

import (
	"DFS_GO/internal/common"
	"DFS_GO/internal/datanode"
	pb "DFS_GO/internal/proto"
	"context"
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config/datanode.yaml", "path to config file")
	flag.Parse()

	// Load configuration
	cfg, err := common.LoadDataNodeConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Use config values
	lis, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("DataNode %s listening on %s", cfg.NodeID, cfg.Address)

	// Calculate max message size from config (convert MB to bytes)
	maxMsgSize := cfg.GRPC.MaxMsgMB * 1024 * 1024
	if maxMsgSize == 0 {
		maxMsgSize = 16 * 1024 * 1024 // default 16MB
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	pb.RegisterDataNodeServiceServer(
		grpcServer,
		&datanode.Server{DataDir: cfg.DataDir},
	)

	// Connect to metadata server
	conn, err := grpc.Dial(cfg.MetadataAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)

	// Register this node with metadata server
	// Build full address for registration (include host if needed)
	nodeAddress := cfg.Address
	if nodeAddress[0] == ':' {
		nodeAddress = "localhost" + nodeAddress
	}

	_, err = client.RegisterNode(context.Background(), &pb.NodeInfo{
		NodeId:  cfg.NodeID,
		Address: nodeAddress,
	})
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}

	log.Printf("DataNode %s registered with metadata server at %s", cfg.NodeID, cfg.MetadataAddress)

	// Start heartbeat loop
	datanode.StartHeartbeat(cfg.NodeID, client)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Storage Server Error: %v", err)
	}
}
