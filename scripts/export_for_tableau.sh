#!/bin/bash
# Export Tavana query results to CSV for Tableau

QUERY="${1:-SELECT * FROM read_csv_auto('s3://tavana-data/test1/Databricks-tech-euw_prod-int_patent-patent.csv') LIMIT 1000}"
OUTPUT="${2:-exports/tableau_data.csv}"

mkdir -p exports
echo "Exporting query results to $OUTPUT..."
PGPASSWORD=tavana psql -h localhost -p 15432 -U tavana -d tavana -A -F',' -c "$QUERY" > "$OUTPUT"
echo "Done! $(wc -l < "$OUTPUT") rows exported."
echo ""
echo "In Tableau: Connect → To a File → Text file → Select: $PWD/$OUTPUT"
