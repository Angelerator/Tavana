#!/usr/bin/env python3
"""Generate sample Parquet data for Tavana testing."""

import pyarrow as pa
import pyarrow.parquet as pq
import random
from datetime import datetime, timedelta
import os

def generate_sample_data():
    """Generate sample sales data."""
    n_rows = 10000
    
    # Generate data
    ids = list(range(1, n_rows + 1))
    names = [f"Product_{i % 100}" for i in range(n_rows)]
    categories = [random.choice(["Electronics", "Clothing", "Food", "Books", "Home"]) for _ in range(n_rows)]
    prices = [round(random.uniform(10.0, 1000.0), 2) for _ in range(n_rows)]
    quantities = [random.randint(1, 100) for _ in range(n_rows)]
    
    base_date = datetime(2024, 1, 1)
    dates = [(base_date + timedelta(days=random.randint(0, 365))).isoformat() for _ in range(n_rows)]
    
    # Create Arrow table
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "name": pa.array(names, type=pa.string()),
        "category": pa.array(categories, type=pa.string()),
        "price": pa.array(prices, type=pa.float64()),
        "quantity": pa.array(quantities, type=pa.int32()),
        "sale_date": pa.array(dates, type=pa.string()),
    })
    
    return table

def main():
    # Create output directory
    os.makedirs("data", exist_ok=True)
    
    # Generate and save sample data
    print("Generating sample sales data...")
    table = generate_sample_data()
    
    output_path = "data/sample.parquet"
    pq.write_table(table, output_path, compression="snappy")
    print(f"Saved {len(table)} rows to {output_path}")
    
    # Generate a smaller dataset for testing
    small_table = table.slice(0, 100)
    small_output = "data/sample_small.parquet"
    pq.write_table(small_table, small_output, compression="snappy")
    print(f"Saved {len(small_table)} rows to {small_output}")
    
    print("\nTo upload to MinIO:")
    print("  mc alias set myminio http://localhost:9000 minioadmin minioadmin")
    print("  mc cp data/sample.parquet myminio/tavana-data/")
    print("  mc cp data/sample_small.parquet myminio/tavana-data/")

if __name__ == "__main__":
    main()

