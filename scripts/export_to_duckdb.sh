#!/bin/bash
# Export Tavana query results to a local DuckDB file for Tableau

OUTPUT_DB="${1:-exports/tableau.duckdb}"
QUERY="${2:-SELECT * FROM read_csv_auto('s3://tavana-data/test1/Databricks-tech-euw_prod-int_patent-patent.csv')}"
TABLE_NAME="${3:-data}"

mkdir -p exports

echo "Connecting to Tavana and exporting to local DuckDB..."
echo "Query: $QUERY"
echo "Output: $OUTPUT_DB"
echo "Table: $TABLE_NAME"
echo ""

# First, export to CSV via Tavana
PGPASSWORD=tavana psql -h localhost -p 15432 -U tavana -d tavana -A -F',' -c "$QUERY" > /tmp/tavana_export.csv

# Then import into local DuckDB
duckdb "$OUTPUT_DB" << DUCKQL
DROP TABLE IF EXISTS $TABLE_NAME;
CREATE TABLE $TABLE_NAME AS SELECT * FROM read_csv_auto('/tmp/tavana_export.csv');
SELECT count(*) as rows FROM $TABLE_NAME;
DUCKQL

rm /tmp/tavana_export.csv

echo ""
echo "Done! DuckDB file ready at: $PWD/$OUTPUT_DB"
echo ""
echo "In Tableau:"
echo "1. Quit Tableau"
echo "2. Launch from Terminal:"
echo "   /Applications/Tableau\ Desktop\ \(Apple\ silicon\)\ 2025.3.app/Contents/MacOS/Tableau -DDisableVerifyConnectorPluginSignature=true"
echo "3. Connect → More... → DuckDB by MotherDuck"
echo "4. Database file: $PWD/$OUTPUT_DB"
