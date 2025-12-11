#!/bin/bash
# Upload sample data to MinIO

set -e

MINIO_ALIAS="tavana-minio"
MINIO_URL="${MINIO_URL:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET="tavana-data"

echo "Setting up MinIO alias..."
mc alias set $MINIO_ALIAS $MINIO_URL $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

echo "Creating bucket if not exists..."
mc mb --ignore-existing $MINIO_ALIAS/$BUCKET

echo "Setting bucket policy to allow downloads..."
mc anonymous set download $MINIO_ALIAS/$BUCKET

# Check if Python and pyarrow are available
if command -v python3 &> /dev/null && python3 -c "import pyarrow" 2>/dev/null; then
    echo "Generating sample data..."
    cd "$(dirname "$0")/.."
    python3 scripts/generate_sample_data.py
    
    echo "Uploading sample data to MinIO..."
    mc cp data/sample.parquet $MINIO_ALIAS/$BUCKET/
    mc cp data/sample_small.parquet $MINIO_ALIAS/$BUCKET/
else
    echo "Python or pyarrow not available, creating minimal test data..."
    # Create a simple CSV that DuckDB can read
    mkdir -p data
    cat > data/sample.csv << 'EOF'
id,name,category,price,quantity,sale_date
1,Product_1,Electronics,299.99,5,2024-01-15
2,Product_2,Clothing,49.99,10,2024-02-20
3,Product_3,Food,12.99,25,2024-03-10
4,Product_4,Books,24.99,8,2024-04-05
5,Product_5,Home,149.99,3,2024-05-12
EOF
    
    echo "Uploading CSV data to MinIO..."
    mc cp data/sample.csv $MINIO_ALIAS/$BUCKET/
fi

echo "Listing bucket contents..."
mc ls $MINIO_ALIAS/$BUCKET/

echo "Done! Sample data is available at s3://$BUCKET/"

