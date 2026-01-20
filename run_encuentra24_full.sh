#!/bin/bash
# Run complete Encuentra24 scrape with date-based storage
# Usage: ./run_encuentra24_full.sh [max_pages] [container_id]

# Configuration
MAX_PAGES=${1:-0}  # Default: 0 (unlimited)
CONTAINER_ID=${2:-"a3305cd12fef"}  # Default container ID
DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="data/${DATE}"

echo "🚀 Starting Encuentra24 Full Scrape"
echo "📅 Date: ${DATE}"
echo "📄 Max Pages: ${MAX_PAGES} (0 = unlimited)"
echo "🐳 Container: ${CONTAINER_ID}"
echo ""

# Create output directory in container
docker exec "${CONTAINER_ID}" mkdir -p "${OUTPUT_DIR}"

# Run the spider
echo "⏳ Running spider..."
docker exec "${CONTAINER_ID}" scrapy crawl encuentra24 \
  -a max_pages="${MAX_PAGES}" \
  -a output_date="${DATE}" \
  -o "${OUTPUT_DIR}/encuentra24_${DATE}.json" \
  --loglevel=INFO

echo ""
echo "✅ Scrape completed!"
echo "📁 Output: ${OUTPUT_DIR}/encuentra24_${DATE}.json"
echo ""
echo "To view results:"
echo "  docker exec ${CONTAINER_ID} cat ${OUTPUT_DIR}/encuentra24_${DATE}.json | jq length"
