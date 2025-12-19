#!/usr/bin/env python3
import json
import argparse
import asyncio
import httpx


DEFAULT_URL = "http://nid006998:9000/v1/embed-chunks"


async def embed_chunks(input_path: str, output_path: str, url: str):
    """Reads input JSON, sends to embedding service, writes pretty JSON output."""
    
    # Load input
    with open(input_path, "r") as f:
        chunks = json.load(f)

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(url, json=chunks)

        if resp.status_code != 200:
            raise RuntimeError(f"Embedding service error {resp.status_code}: {resp.text}")

        embeddings = resp.json()

        if len(embeddings) != len(chunks):
            raise RuntimeError("Mismatch: number of embeddings != number of chunks")

    # Pretty print output
    with open(output_path, "w") as f:
        json.dump(embeddings, f, indent=2)

    print(f"Successfully wrote {len(embeddings)} embeddings to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Embedding wrapper for Clariden vLLM endpoint")

    parser.add_argument("--input", "-i", default="queries.json", help="Path to input JSON file")
    parser.add_argument("--output", "-o", default="embeddings.json", help="Output JSON file")
    parser.add_argument("--url", "-u", default=DEFAULT_URL, help="Embedding service URL")

    args = parser.parse_args()

    asyncio.run(embed_chunks(args.input, args.output, args.url))


if __name__ == "__main__":
    main()
