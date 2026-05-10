#!/bin/bash

set -euo pipefail

PROJECT_ROOT="/Users/rizkyzeinadam/Documents/Percobaan Tesis"
FLINK_JOB_DIR="${PROJECT_ROOT}/flink-jobs"

cd "$FLINK_JOB_DIR"
echo "[build_flink_job] Building Flink job jar with Maven container..."

docker run --rm \
  -v "$FLINK_JOB_DIR":/workspace \
  -w /workspace \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -q -DskipTests clean package

echo "[build_flink_job] Build complete: $FLINK_JOB_DIR/target/cic-flink-job.jar"
