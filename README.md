# Distributed File System in Go

Welcome to the Distributed File System (DFS) project implemented in Go! This project aims to provide a robust, scalable, and fault-tolerant file storage system using Go programming language.

## Features

- **Scalability:** Easily scale out by adding more nodes.
- **Fault Tolerance:** Redundant storage to ensure data availability even if some nodes fail.
- **High Performance:** Optimized for high throughput and low latency.
- **Docker Support:** Easily deploy and manage using Docker containers.

## Prerequisites

- [Go](https://golang.org/doc/install) (version 1.18 or later)
- [Docker](https://docs.docker.com/get-docker/) (version 20.10 or later)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/distributed-file-system.git
cd distributed-file-system
```

### 2. Build the Project

Ensure you have Go installed, and build the project:

```bash

go build -o dfs ./cmd/dfs
```
### 3. Run Locally

To run the distributed file system locally:

```bash

./dfs --config config/local.yaml
```
Ensure you have a config/local.yaml file with appropriate configurations.
**Docker Deployment**

The project includes a Dockerfile to build and deploy the DFS as a Docker container.
Build the Docker Image

```bash

docker build -t distributed-file-system .
```
Run the Docker Container

```bash

docker run -d \
  --name dfs \
  -p 8080:8080 \
  distributed-file-system
```
    Port 8080: Exposed for accessing the DFS service.

### Docker Compose

You can also use Docker Compose to manage multi-container deployments. Ensure you have docker-compose.yml configured, then run:

```bash

docker-compose up
```
### Configuration

The system can be configured through YAML configuration files. Example configurations are available in the config directory.
Example Configuration

```yaml

# config/local.yaml
server:
  port: 8080
  storagePath: /data
  replicationFactor: 3
```
### Usage

After starting the DFS, you can interact with it via the provided REST API or through the CLI tools included in the repository.
REST API

Base URL: http://localhost:8080

    Upload File: POST /upload
    Download File: GET /download/{fileID}
    List Files: GET /files

### CLI Tool

Use the CLI tool for various operations. Run:

```bash

./dfs-cli --help
```
### Testing

To run tests, use:

```bash

go test ./...
```
### Contributing

We welcome contributions to improve the DFS project. Please refer to CONTRIBUTING.md for guidelines.
###License

This project is licensed under the MIT License.
### Contact

For any questions or support, please open an issue on the GitHub repository or contact yewaredigvijay@gmail.com.
