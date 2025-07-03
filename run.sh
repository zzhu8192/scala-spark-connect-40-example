#!/usr/bin/env bash
set -euo pipefail

# Script to run the Spark Connect example job locally against a running Spark Connect server
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="${PROJECT_DIR}/target/sparkconnect-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f "${JAR_FILE}" ]; then
  JAR_FILE="${PROJECT_DIR}/target/sparkconnect-1.0.0-SNAPSHOT.jar"
fi

INPUT_CSV="${PROJECT_DIR}/src/main/resources/test.csv"
OUTPUT_DIR="${TMPDIR:-/tmp}/spark-connect-example-output"
SPARK_CONNECT_URL="${SPARK_CONNECT_URL:-sc://localhost:15002}"

if [ ! -f "${JAR_FILE}" ]; then
  echo "Jar file not found at ${JAR_FILE}."
  echo "Please build the project first: mvn clean package"
  exit 1
fi

echo "Connecting to Spark Connect Server at ${SPARK_CONNECT_URL}..."
echo "Cleaning previous output directory at ${OUTPUT_DIR}..."
rm -rf "${OUTPUT_DIR}"

if command -v spark-submit &> /dev/null; then
  SPARK_SUBMIT_CMD="spark-submit"
elif [ -n "${SPARK_HOME:-}" ] && [ -x "${SPARK_HOME}/bin/spark-submit" ]; then
  SPARK_SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit"
else
  echo "Notice: spark-submit not found on PATH. Attempting direct Java client execution..."
  if [ -n "${SPARK_HOME:-}" ]; then
    java -cp "${JAR_FILE}:${SPARK_HOME}/jars/*" com.uj.example.Example "file://${INPUT_CSV}" "${OUTPUT_DIR}"
    echo "Job finished successfully! Output written to ${OUTPUT_DIR}"
    exit 0
  else
    echo "Error: Neither spark-submit nor SPARK_HOME was found."
    echo "Ensure SPARK_HOME is set or start your Spark Connect server via \$SPARK_HOME/sbin/start-connect-server.sh"
    exit 1
  fi
fi

echo "Running Spark Connect job via ${SPARK_SUBMIT_CMD}..."
"${SPARK_SUBMIT_CMD}" \
  --remote "${SPARK_CONNECT_URL}" \
  --class com.uj.example.Example \
  "${JAR_FILE}" \
  "file://${INPUT_CSV}" \
  "${OUTPUT_DIR}"

echo "Job finished successfully! Output written to ${OUTPUT_DIR}"
