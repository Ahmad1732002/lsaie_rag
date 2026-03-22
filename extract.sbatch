#!/bin/bash
#SBATCH --job-name=warc_extract
#SBATCH --account=a-infra01-1
#SBATCH --partition=normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --time=02:00:00
#SBATCH --output=/iopsstor/scratch/cscs/ahfraij/lsaie_rag_output/logs/extract_%j.out
#SBATCH --error=/iopsstor/scratch/cscs/ahfraij/lsaie_rag_output/logs/extract_%j.err

set -eo pipefail

mkdir -p /iopsstor/scratch/cscs/ahfraij/lsaie_rag_output/logs

echo "[sbatch] running on $(hostname)"
echo "[sbatch] SLURM_JOB_ID: $SLURM_JOB_ID"

# Activate conda environment
source /users/ahfraij/miniconda3/etc/profile.d/conda.sh
conda activate lsaie-rag

cd /iopsstor/scratch/cscs/ahfraij/lsaie-rag

python extract_to_markdown.py \
    --warc-input-dir /capstor/store/cscs/swissai/infra01/admin_chat/ \
    --output-dir /iopsstor/scratch/cscs/ahfraij/lsaie_rag_output

echo "[sbatch] Done"
