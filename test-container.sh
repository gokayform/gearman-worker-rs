#!/bin/bash

set -e

# Configuration
CONTAINER_NAME="gearman-test-server"
GEARMAN_PORT="4730"
IMAGE_NAME="ubuntu:22.04"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to clean up container
cleanup() {
    log "Cleaning up container..."
    podman rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

# Function to wait for gearman server to be ready
wait_for_gearman() {
    local max_attempts=60
    local attempt=1
    
    log "Waiting for Gearman server to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        # Check if gearmand process is running
        if podman exec "$CONTAINER_NAME" pgrep gearmand >/dev/null 2>&1; then
            log "Gearmand process is running, checking port..."
            # Check if port is listening
            if podman exec "$CONTAINER_NAME" nc -z localhost "$GEARMAN_PORT" 2>/dev/null; then
                log "Gearman server is ready on port $GEARMAN_PORT!"
                return 0
            fi
        fi
        
        # Show progress and some debugging info every 10 attempts
        if [ $((attempt % 10)) -eq 0 ]; then
            echo ""
            log "Attempt $attempt/$max_attempts - checking server status..."
            log "Gearmand process check:"
            podman exec "$CONTAINER_NAME" pgrep -l gearmand 2>/dev/null || echo "  No gearmand process found"
            log "Port check:"
            podman exec "$CONTAINER_NAME" netstat -ln 2>/dev/null | grep ":$GEARMAN_PORT" || echo "  Port $GEARMAN_PORT not listening"
            log "Recent container logs:"
            podman logs "$CONTAINER_NAME" 2>&1 | tail -5
        else
            echo -n "."
        fi
        
        sleep 1
        attempt=$((attempt + 1))
    done
    
    echo ""
    error "Gearman server failed to start within $max_attempts seconds"
    error "Final container logs:"
    podman logs "$CONTAINER_NAME" 2>&1 | tail -20
    return 1
}

# Function to check if podman is available
check_podman() {
    if ! command -v podman &> /dev/null; then
        error "Podman is not installed or not in PATH"
        error "Please install podman first: https://podman.io/getting-started/installation"
        exit 1
    fi
}

# Function to setup gearman container
setup_container() {
    log "Setting up Gearman test container..."
    
    # Remove existing container if it exists
    cleanup
    
    log "Creating container with Gearman server..."
    # Create and start container with gearman
    podman run -d \
        --name "$CONTAINER_NAME" \
        --publish "$GEARMAN_PORT:$GEARMAN_PORT" \
        "$IMAGE_NAME" \
        bash -c "
            echo 'Updating package lists...' && \
            apt-get update && \
            echo 'Installing packages...' && \
            apt-get install -y gearman-job-server gearman-tools netcat-openbsd && \
            echo 'Starting gearmand...' && \
            gearmand --listen=0.0.0.0 --port=$GEARMAN_PORT --verbose=INFO --log-file=/tmp/gearmand.log --daemon && \
            echo 'Gearmand started successfully' && \
            # Keep container running
            tail -f /dev/null
        "
    
    # Wait for container to be ready
    log "Waiting for container to start..."
    sleep 3
    
    # Check if container is running
    if ! podman ps --filter "name=$CONTAINER_NAME" --filter "status=running" | grep -q "$CONTAINER_NAME"; then
        error "Failed to start container"
        log "Container logs:"
        podman logs "$CONTAINER_NAME"
        exit 1
    fi
    
    log "Container is running, checking Gearman server..."
    
    # Show container logs for debugging
    log "Container startup logs:"
    podman logs "$CONTAINER_NAME" 2>&1 | head -20
    
    # Wait for gearman to be ready
    wait_for_gearman
}

# Function to run tests
run_tests() {
    log "Running Rust tests..."
    
    # Set environment variables for tests
    export GEARMAN_TEST_PORT="$GEARMAN_PORT"
    export GEARMAN_USE_CONTAINER="1"
    export GEARMAN_CONTAINER_NAME="$CONTAINER_NAME"
    
    # Run cargo test
    if cargo test -- --test-threads=1; then
        log "All tests passed!"
        return 0
    else
        error "Tests failed!"
        return 1
    fi
}

# Function to show container logs
show_logs() {
    log "Gearman server logs:"
    podman logs "$CONTAINER_NAME" 2>&1 | tail -20
}

# Main execution
main() {
    log "Starting Gearman container test environment..."
    
    # Check prerequisites
    check_podman
    
    # Setup trap for cleanup
    trap cleanup EXIT
    
    # Setup container
    setup_container
    
    # Show some logs
    show_logs
    
    # Run tests
    if run_tests; then
        log "Test run completed successfully!"
        exit 0
    else
        error "Test run failed!"
        show_logs
        exit 1
    fi
}

# Handle script arguments
case "${1:-}" in
    "cleanup")
        cleanup
        exit 0
        ;;
    "logs")
        podman logs "$CONTAINER_NAME"
        exit 0
        ;;
    "shell")
        podman exec -it "$CONTAINER_NAME" sh
        exit 0
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  (no args)  - Run full test suite with container"
        echo "  cleanup    - Remove test container"
        echo "  logs       - Show container logs"
        echo "  shell      - Open shell in container"
        echo "  help       - Show this help"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown command: $1"
        echo "Run '$0 help' for usage information"
        exit 1
        ;;
esac 