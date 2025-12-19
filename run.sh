#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env" ]; then
    echo "Loading environment variables from .env..."
    source .env
else
    echo "ERROR: .env file not found. Please create it with your HF token."
    exit 1
fi

ML_DIR="$(pwd)/model-launch"
cd $ML_DIR

echo "Launching Nomic 1.5 embedding server..."
echo "Account: large-sc-2"
echo "Environment: $ML_DIR/serving/vllm.toml"
echo "User: $USER"

python "$ML_DIR/serving/submit_job.py" \
  --slurm-nodes 1 \
  --slurm-account large-sc-2 \
  --serving-framework vllm \
  --slurm-environment "$ML_DIR/serving/vllm.toml" \
  --framework-args "--model nomic-ai/nomic-embed-text-v1.5 \
    --host 0.0.0.0 \
    --port 8080 \
    --task embedding \
    --trust-remote-code \
    --served-model-name nomic-ai/nomic-embed-text-v1.5-${USER}"
