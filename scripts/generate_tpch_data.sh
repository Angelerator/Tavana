#!/bin/bash
# Generate TPC-H benchmark data using tpchgen-rs and upload to MinIO
# https://github.com/clflushopt/tpchgen-rs

set -e

# Configuration
MINIO_ALIAS="tavana-minio"
MINIO_URL="${MINIO_URL:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET="tavana-data"
DATA_DIR="data/tpch"
TPCHGEN_BIN=""  # Will be set by install_tpchgen

# Default scale factors (can be overridden via command line)
DEFAULT_SCALE_FACTORS="0.01 0.1 1 10 100"

# Parse command line arguments
SCALE_FACTORS="${1:-$DEFAULT_SCALE_FACTORS}"
SKIP_UPLOAD="${SKIP_UPLOAD:-false}"

usage() {
    echo "Usage: $0 [scale_factors] [options]"
    echo ""
    echo "Generate TPC-H benchmark data at various scale factors and upload to MinIO."
    echo ""
    echo "Arguments:"
    echo "  scale_factors    Space-separated list of scale factors (default: \"0.01 0.1 1 10 100\")"
    echo ""
    echo "Environment variables:"
    echo "  MINIO_URL        MinIO endpoint URL (default: http://localhost:9000)"
    echo "                   Use http://localhost:9002 for docker-compose setup"
    echo "  MINIO_ACCESS_KEY MinIO access key (default: minioadmin)"
    echo "  MINIO_SECRET_KEY MinIO secret key (default: minioadmin)"
    echo "  SKIP_UPLOAD      Skip uploading to MinIO (default: false)"
    echo ""
    echo "Examples:"
    echo "  $0                           # Generate all default scale factors"
    echo "  $0 \"0.01 0.1 1\"              # Generate only small scale factors"
    echo "  $0 \"1\"                       # Generate only SF=1"
    echo "  SKIP_UPLOAD=true $0 \"1\"      # Generate SF=1 without uploading"
    echo ""
    echo "Estimated data sizes:"
    echo "  SF 0.01  ~10 MB   (unit tests, quick validation)"
    echo "  SF 0.1   ~100 MB  (integration tests)"
    echo "  SF 1     ~1 GB    (standard benchmarking)"
    echo "  SF 10    ~10 GB   (performance testing)"
    echo "  SF 100   ~100 GB  (stress testing)"
    exit 1
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
fi

# Check if cargo is installed
check_cargo() {
    if ! command -v cargo &> /dev/null; then
        echo "Error: cargo is not installed. Please install Rust first:"
        echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        exit 1
    fi
}

# Install tpchgen-cli if not present
install_tpchgen() {
    # Check both possible binary names
    if command -v tpchgen-cli &> /dev/null; then
        TPCHGEN_BIN="tpchgen-cli"
        echo "✓ tpchgen-cli is already installed"
        $TPCHGEN_BIN --version
        return
    elif command -v tpchgen &> /dev/null; then
        TPCHGEN_BIN="tpchgen"
        echo "✓ tpchgen is already installed"
        $TPCHGEN_BIN --version
        return
    fi

    echo "Installing tpchgen-cli..."
    cargo install tpchgen-cli
    
    # Check which binary name was installed
    if command -v tpchgen-cli &> /dev/null; then
        TPCHGEN_BIN="tpchgen-cli"
        echo "✓ tpchgen-cli installed successfully"
        $TPCHGEN_BIN --version
    elif [[ -x "$HOME/.cargo/bin/tpchgen-cli" ]]; then
        TPCHGEN_BIN="$HOME/.cargo/bin/tpchgen-cli"
        echo "✓ tpchgen-cli installed successfully (using full path)"
        $TPCHGEN_BIN --version
    else
        echo "Error: tpchgen-cli installation failed"
        exit 1
    fi
}

# Generate TPC-H data for a given scale factor
generate_data() {
    local sf=$1
    local output_dir="${DATA_DIR}/sf_${sf}"
    
    echo ""
    echo "========================================"
    echo "Generating TPC-H data at Scale Factor $sf"
    echo "Output directory: $output_dir"
    echo "========================================"
    
    # Create output directory
    mkdir -p "$output_dir"
    
    # Generate data using tpchgen-cli
    # The tool generates all 8 TPC-H tables as Parquet files
    echo "Running: $TPCHGEN_BIN -s $sf -f parquet -o $output_dir"
    $TPCHGEN_BIN -s "$sf" -f parquet -o "$output_dir"
    
    # List generated files
    echo ""
    echo "Generated files:"
    ls -lh "$output_dir"/*.parquet 2>/dev/null || echo "No parquet files found"
    
    # Calculate total size
    local total_size
    total_size=$(du -sh "$output_dir" | cut -f1)
    echo "Total size for SF $sf: $total_size"
}

# Upload data to MinIO
upload_to_minio() {
    if [[ "$SKIP_UPLOAD" == "true" ]]; then
        echo ""
        echo "Skipping MinIO upload (SKIP_UPLOAD=true)"
        return
    fi
    
    echo ""
    echo "========================================"
    echo "Uploading TPC-H data to MinIO"
    echo "========================================"
    
    # Check if mc (MinIO client) is installed
    if ! command -v mc &> /dev/null; then
        echo "Warning: MinIO client (mc) is not installed. Skipping upload."
        echo "Install mc from: https://min.io/docs/minio/linux/reference/minio-mc.html"
        return
    fi
    
    # Configure MinIO alias
    echo "Setting up MinIO alias..."
    mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
    
    # Create bucket if not exists
    echo "Creating bucket if not exists..."
    mc mb --ignore-existing "$MINIO_ALIAS/$BUCKET"
    mc anonymous set download "$MINIO_ALIAS/$BUCKET"
    
    # Upload each scale factor directory
    for sf in $SCALE_FACTORS; do
        local source_dir="${DATA_DIR}/sf_${sf}"
        local dest_path="$MINIO_ALIAS/$BUCKET/tpch/sf_${sf}/"
        
        if [[ -d "$source_dir" ]]; then
            echo ""
            echo "Uploading SF $sf to $dest_path..."
            mc cp --recursive "$source_dir/" "$dest_path"
        else
            echo "Warning: Directory $source_dir does not exist, skipping"
        fi
    done
    
    # List uploaded data
    echo ""
    echo "Uploaded TPC-H data structure:"
    mc ls --recursive "$MINIO_ALIAS/$BUCKET/tpch/" | head -50
    
    echo ""
    echo "Data is available at: s3://$BUCKET/tpch/"
}

# Main execution
main() {
    echo "TPC-H Data Generator for Tavana"
    echo "================================"
    echo ""
    echo "Scale factors to generate: $SCALE_FACTORS"
    echo ""
    
    # Change to project root
    cd "$(dirname "$0")/.."
    
    # Check prerequisites
    check_cargo
    
    # Install tpchgen-cli
    install_tpchgen
    
    # Create data directory
    mkdir -p "$DATA_DIR"
    
    # Generate data for each scale factor
    for sf in $SCALE_FACTORS; do
        generate_data "$sf"
    done
    
    # Summary of generated data
    echo ""
    echo "========================================"
    echo "Generation Summary"
    echo "========================================"
    echo ""
    for sf in $SCALE_FACTORS; do
        local dir="${DATA_DIR}/sf_${sf}"
        if [[ -d "$dir" ]]; then
            local size
            size=$(du -sh "$dir" | cut -f1)
            local file_count
            file_count=$(ls -1 "$dir"/*.parquet 2>/dev/null | wc -l)
            echo "SF $sf: $size ($file_count tables)"
        fi
    done
    
    # Upload to MinIO
    upload_to_minio
    
    echo ""
    echo "========================================"
    echo "Done!"
    echo "========================================"
    echo ""
    echo "Generated data is in: $DATA_DIR/"
    echo ""
    echo "TPC-H tables generated:"
    echo "  - lineitem.parquet  (largest table, order line items)"
    echo "  - orders.parquet    (customer orders)"
    echo "  - customer.parquet  (customer information)"
    echo "  - part.parquet      (parts catalog)"
    echo "  - partsupp.parquet  (part suppliers)"
    echo "  - supplier.parquet  (supplier information)"
    echo "  - nation.parquet    (nations, 25 rows fixed)"
    echo "  - region.parquet    (regions, 5 rows fixed)"
    echo ""
    echo "To query the data via Tavana:"
    echo "  psql -h localhost -p 15432 -c \"SELECT count(*) FROM read_parquet('s3://tavana-data/tpch/sf_1/lineitem.parquet')\""
}

main "$@"
