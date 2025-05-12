#!/bin/bash
# Script to run the API crawler locally for testing

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env"
    export $(grep -v '^#' .env | xargs)
fi

# Set defaults
SOURCE=${SOURCE:-chotot}
START_PAGE=${START_PAGE:-1}
END_PAGE=${END_PAGE:-3}
MAX_CONCURRENT=${MAX_CONCURRENT:-10}
INTERVAL=${INTERVAL:-3600}

# Handle command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --source)
        SOURCE="$2"
        shift # past argument
        shift # past value
        ;;
        --start-page)
        START_PAGE="$2"
        shift
        shift
        ;;
        --end-page)
        END_PAGE="$2"
        shift
        shift
        ;;
        --max-concurrent)
        MAX_CONCURRENT="$2"
        shift
        shift
        ;;
        --once)
        ONCE="--once"
        shift
        ;;
        --interval)
        INTERVAL="$2"
        shift
        shift
        ;;
        *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
    esac
done

echo "Starting API crawler with:"
echo "Source: $SOURCE"
echo "Pages: $START_PAGE to $END_PAGE"
echo "Max concurrent: $MAX_CONCURRENT"
echo "Run once: ${ONCE:=no}"

# Run the crawler with the provided arguments
if [ -n "$ONCE" ]; then
    python -m services.api_crawler.main --source $SOURCE --start-page $START_PAGE --end-page $END_PAGE --once
else
    python -m services.api_crawler.main --source $SOURCE --start-page $START_PAGE --end-page $END_PAGE --interval $INTERVAL
fi
