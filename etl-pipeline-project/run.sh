#!/bin/bash
# Run the ETL pipeline from the project root (required for relative paths to work)
cd "$(dirname "$0")"
java -jar target/etl-pipeline-1.0.jar
