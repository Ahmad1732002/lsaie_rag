#!/usr/bin/env python
import os
from contextlib import asynccontextmanager
from typing import List

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# vLLM server location (can be overridden with env var)
VLLM_BASE_URL = os.getenv("VLLM_BASE_URL", "http://127.0.0.1:8080")
VLLM_MODEL = os.getenv("VLLM_MODEL", "nomic-ai/nomic-embed-text-v1.5-andrpac")


class Chunk(BaseModel):
    chunk_id: str
    text: str


class ChunkEmbedding(BaseModel):
    chunk_id: str
    chunk_embedding: List[float]


vllm_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global vllm_client
    vllm_client = httpx.AsyncClient(base_url=VLLM_BASE_URL, timeout=300)
    try:
        yield
    finally:
        await vllm_client.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        r = await vllm_client.get("/health")
        return {"status": "ok", "vllm_upstream": r.status_code == 200}
    except Exception:
        return {"status": "ok", "vllm_upstream": False}


@app.post("/v1/embed-chunks")
async def embed_chunks(chunks: List[Chunk]):
    texts = [c.text for c in chunks]
    payload = {"model": VLLM_MODEL, "input": texts}

    r = await vllm_client.post("/v1/embeddings", json=payload)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code,
                            detail=f"vLLM error: {r.text}")

    body = r.json()
    data = body.get("data", [])
    if len(data) != len(chunks):
        raise HTTPException(status_code=500,
                            detail="Mismatched number of embeddings")

    out = []
    for chunk, item in zip(chunks, data):
        out.append({
            "chunk_id": chunk.chunk_id,
            "chunk_embedding": item["embedding"],
        })
    return out
