package main

import (
	"DFS_GO/internal/client"
	pb "DFS_GO/internal/proto"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: client <upload|download> <filename> [output_path]")
	}

	command := os.Args[1]
	filename := os.Args[2]

	// Connect to metadata server
	metaConn, err := grpc.Dial("localhost:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata server: %v", err)
	}
	defer metaConn.Close()

	metaClient := pb.NewMetadataServiceClient(metaConn)

	switch command {
	case "upload":
		log.Printf("Uploading file: %s", filename)
		if err := client.Upload(filename, metaClient); err != nil {
			log.Fatalf("Upload failed: %v", err)
		}
		log.Println("Upload complete!")
	case "download":
		outputPath := filename // default to same name
		if len(os.Args) >= 4 {
			outputPath = os.Args[3]
		}
		log.Printf("Downloading file: %s to %s", filename, outputPath)
		data, err := client.Download(filename, metaClient)
		if err != nil {
			log.Fatalf("Download failed: %v", err)
		}
		err = os.WriteFile(outputPath, data, 0644)
		if err != nil {
			log.Fatalf("Failed to write file: %v", err)
		}
		log.Printf("Download complete! Saved to %s", outputPath)
	default:
		log.Fatalf("Unknown command: %s. Use 'upload' or 'download'", command)
	}
}
