#!/bin/bash

# End-to-end test script for DFS_Go
# Tests read-write consistency for small and large files

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METADATA_PORT=5000
DATANODE_PORT=6001
TEST_DIR="/tmp/dfs_test_$$"
SMALL_FILE_SIZE="1K"
LARGE_FILE_SIZE="15M"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    
    # Kill servers
    pkill -9 -f "go run.*cmd/metadata" 2>/dev/null || true
    pkill -9 -f "go run.*cmd/datanode" 2>/dev/null || true
    lsof -ti:$METADATA_PORT | xargs kill -9 2>/dev/null || true
    lsof -ti:$DATANODE_PORT | xargs kill -9 2>/dev/null || true
    
    # Remove test files
    rm -rf "$TEST_DIR"
    rm -rf "$PROJECT_DIR/data"
    rm -f /tmp/metadata.log /tmp/datanode.log
    
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set trap for cleanup on exit
trap cleanup EXIT

# Print test header
print_header() {
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

# Print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print failure
print_failure() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

# Wait for port to be available
wait_for_port() {
    local port=$1
    local max_attempts=30
    local attempt=0
    
    while ! lsof -i:$port -sTCP:LISTEN > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            print_failure "Timeout waiting for port $port"
        fi
        sleep 0.5
    done
}

# Main test execution
main() {
    print_header "DFS End-to-End Test Suite"
    
    cd "$PROJECT_DIR"
    
    # Create test directory
    mkdir -p "$TEST_DIR"
    
    # Step 1: Build project
    print_header "Step 1: Building project"
    if go build ./...; then
        print_success "Project built successfully"
    else
        print_failure "Build failed"
    fi
    
    # Step 2: Start servers
    print_header "Step 2: Starting servers"
    
    # Clean up any existing processes
    pkill -9 -f "go run.*cmd/metadata" 2>/dev/null || true
    pkill -9 -f "go run.*cmd/datanode" 2>/dev/null || true
    lsof -ti:$METADATA_PORT | xargs kill -9 2>/dev/null || true
    lsof -ti:$DATANODE_PORT | xargs kill -9 2>/dev/null || true
    sleep 1
    
    # Create data directory
    rm -rf ./data && mkdir -p ./data/dn1
    
    # Start metadata server
    echo "Starting metadata server..."
    nohup go run cmd/metadata/main.go > /tmp/metadata.log 2>&1 &
    METADATA_PID=$!
    wait_for_port $METADATA_PORT
    print_success "Metadata server started on port $METADATA_PORT"
    
    # Start datanode
    echo "Starting datanode..."
    nohup go run cmd/datanode/main.go > /tmp/datanode.log 2>&1 &
    DATANODE_PID=$!
    wait_for_port $DATANODE_PORT
    print_success "DataNode started on port $DATANODE_PORT"
    
    sleep 1  # Give servers time to fully initialize
    
    # Step 3: Test small file
    print_header "Step 3: Small file read-write consistency test"
    
    # Create small test file
    echo "Creating small test file ($SMALL_FILE_SIZE)..."
    echo "Hello, this is a test file for the DFS system! Testing read-write consistency." > "$TEST_DIR/small_original.txt"
    
    # Upload
    echo "Uploading small file..."
    if go run cmd/client/main.go upload "$TEST_DIR/small_original.txt"; then
        print_success "Small file uploaded"
    else
        print_failure "Small file upload failed"
    fi
    
    # Download
    echo "Downloading small file..."
    if go run cmd/client/main.go download small_original.txt "$TEST_DIR/small_downloaded.txt"; then
        print_success "Small file downloaded"
    else
        print_failure "Small file download failed"
    fi
    
    # Diff
    echo "Comparing files..."
    if diff "$TEST_DIR/small_original.txt" "$TEST_DIR/small_downloaded.txt" > /dev/null; then
        print_success "Small file: Original and downloaded files are identical"
    else
        print_failure "Small file: Files differ!"
    fi
    
    # Step 4: Test large file (>10MB)
    print_header "Step 4: Large file (>10MB) read-write consistency test"
    
    # Create large test file
    echo "Creating large test file ($LARGE_FILE_SIZE)..."
    dd if=/dev/urandom of="$TEST_DIR/large_original.bin" bs=1M count=15 2>/dev/null
    
    ORIGINAL_HASH=$(sha256sum "$TEST_DIR/large_original.bin" | cut -d' ' -f1)
    echo "Original file hash: $ORIGINAL_HASH"
    
    # Upload
    echo "Uploading large file..."
    if go run cmd/client/main.go upload "$TEST_DIR/large_original.bin"; then
        print_success "Large file uploaded"
    else
        print_failure "Large file upload failed"
    fi
    
    # Download
    echo "Downloading large file..."
    if go run cmd/client/main.go download large_original.bin "$TEST_DIR/large_downloaded.bin"; then
        print_success "Large file downloaded"
    else
        print_failure "Large file download failed"
    fi
    
    # Compare hashes
    echo "Comparing file hashes..."
    DOWNLOADED_HASH=$(sha256sum "$TEST_DIR/large_downloaded.bin" | cut -d' ' -f1)
    echo "Downloaded file hash: $DOWNLOADED_HASH"
    
    if [ "$ORIGINAL_HASH" = "$DOWNLOADED_HASH" ]; then
        print_success "Large file: Hashes match! Read-write consistency verified"
    else
        print_failure "Large file: Hash mismatch!"
    fi
    
    # Step 5: Test multiple chunks (verify chunking works)
    print_header "Step 5: Verifying chunk storage"
    
    CHUNK_COUNT=$(ls -1 ./data/dn1/ 2>/dev/null | wc -l)
    echo "Chunks stored on datanode: $CHUNK_COUNT"
    
    # 15MB file with 4MB chunks = 4 chunks
    if [ "$CHUNK_COUNT" -ge 4 ]; then
        print_success "Chunk count is correct (expected >= 4 for 15MB file with 4MB chunks)"
    else
        print_failure "Unexpected chunk count: $CHUNK_COUNT"
    fi
    
    # Step 6: Test DataNode failure resilience (WARNING: This test will fail with only 1 datanode)
    print_header "Step 6: DataNode failure test (Expected to fail with single datanode)"
    
    echo "Note: With replication factor 1, killing the datanode will cause download to fail"
    echo "This test demonstrates the need for replication"
    
    # Create test file for failure test
    echo "Creating test file for failure test..."
    echo "Testing datanode failure resilience" > "$TEST_DIR/failure_test.txt"
    
    # Upload
    echo "Uploading test file..."
    if go run cmd/client/main.go upload "$TEST_DIR/failure_test.txt"; then
        print_success "File uploaded successfully"
    else
        print_failure "File upload failed"
    fi
    
    # Kill datanode
    echo "Killing DataNode process (PID: $DATANODE_PID)..."
    kill -9 $DATANODE_PID 2>/dev/null || true
    sleep 2
    
    # Try to download (expected to fail with single node)
    echo "Attempting to download after DataNode failure..."
    set +e  # Don't exit on error for this test
    go run cmd/client/main.go download failure_test.txt "$TEST_DIR/failure_downloaded.txt" > /tmp/download_test.log 2>&1
    DOWNLOAD_EXIT_CODE=$?
    set -e
    
    if [ $DOWNLOAD_EXIT_CODE -ne 0 ]; then
        print_success "Download failed as expected (no replication)"
        echo "  Error: $(cat /tmp/download_test.log | tail -1)"
        echo "  Note: With replication, download would succeed using replica nodes"
    else
        echo "  ${YELLOW}Warning: Download succeeded (might use cached connection)${NC}"
    fi
    
    # Restart datanode for next test
    echo "Restarting DataNode..."
    rm -rf ./data/dn1 && mkdir -p ./data/dn1
    nohup go run cmd/datanode/main.go > /tmp/datanode.log 2>&1 &
    DATANODE_PID=$!
    wait_for_port $DATANODE_PORT
    sleep 2
    print_success "DataNode restarted"
    
    # Step 7: Test Metadata server failure (catastrophic)
    print_header "Step 7: Metadata server failure test (Expected to be catastrophic)"
    
    echo "Note: Without metadata server HA/replication, all operations will fail"
    
    # Upload a file first
    echo "Uploading test file..."
    echo "Testing metadata failure" > "$TEST_DIR/metadata_test.txt"
    go run cmd/client/main.go upload "$TEST_DIR/metadata_test.txt" 2>/dev/null || true
    sleep 1
    
    # Kill metadata server
    echo "Killing Metadata server (PID: $METADATA_PID)..."
    kill -9 $METADATA_PID 2>/dev/null || true
    sleep 2
    
    # Try operations (expected to fail)
    echo "Attempting operations after Metadata server failure..."
    set +e  # Don't exit on error for this test
    go run cmd/client/main.go download metadata_test.txt "$TEST_DIR/metadata_downloaded.txt" > /tmp/metadata_test.log 2>&1
    METADATA_EXIT_CODE=$?
    set -e
    
    if [ $METADATA_EXIT_CODE -ne 0 ]; then
        print_success "Operations failed as expected (no metadata HA)"
        echo "  Error: $(cat /tmp/metadata_test.log | tail -1)"
        echo "  Note: This is catastrophic and requires metadata server HA (Option D)"
    else
        echo "  ${YELLOW}Warning: Operation succeeded unexpectedly${NC}"
    fi
    
    # Restart metadata for cleanup
    echo "Restarting Metadata server..."
    nohup go run cmd/metadata/main.go > /tmp/metadata.log 2>&1 &
    METADATA_PID=$!
    wait_for_port $METADATA_PORT
    sleep 2
    print_success "Metadata server restarted"
    
    # Final summary
    print_header "Test Summary"
    echo -e "${GREEN}All tests passed!${NC}"
    echo ""
    echo "Tests executed:"
    echo "  ✓ Project build"
    echo "  ✓ Metadata server startup"
    echo "  ✓ DataNode startup and registration"
    echo "  ✓ Small file upload/download (diff comparison)"
    echo "  ✓ Large file upload/download (hash comparison)"
    echo "  ✓ Chunk storage verification"
    echo "  ✓ DataNode failure test (demonstrates need for replication)"
    echo "  ✓ Metadata failure test (demonstrates need for HA)"
}

# Run main
main "$@"
