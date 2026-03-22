#!/usr/bin/env python3
"""
WARC → Markdown extraction (Steps 1+2 only, no Elasticsearch).

Output markdown files are then uploaded to OpenWebUI via the knowledge base API.

Usage (quick test on a few files):
    python extract_to_markdown.py \
        --warc-input-dir /iopsstor/scratch/cscs/rashitig/19945/ \
        --topics-excel-path data/2025-11-20_19945_topics.xlsx \
        --output-dir /iopsstor/scratch/cscs/ahfraij/lsaie_rag_output \
        --max-warc-files 3

Usage (full run):
    python extract_to_markdown.py \
        --warc-input-dir /iopsstor/scratch/cscs/rashitig/19945/ \
        --topics-excel-path data/2025-11-20_19945_topics.xlsx \
        --output-dir /iopsstor/scratch/cscs/ahfraij/lsaie_rag_output
"""
import argparse
import os
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from prep_warc_files import warc_to_html, warc_to_pdf
from combine_domains import combine_domains_by_timestamp
from html_combined_to_markdown import convert_html_combined_to_markdown
from pdf_combined_to_markdown import convert_pdf_combined_to_markdown


COLL = "19945"


def make_subset_dir(warc_input_dir: str, max_files: int, tmp_dir: str) -> str:
    """Create a temp dir with symlinks to the first N WARC files."""
    input_path = Path(warc_input_dir)
    warc_files = sorted(
        list(input_path.glob("*.warc")) + list(input_path.glob("*.warc.gz"))
    )[:max_files]

    if not warc_files:
        raise RuntimeError(f"No WARC files found in {warc_input_dir}")

    subset_dir = os.path.join(tmp_dir, "warc_subset")
    os.makedirs(subset_dir, exist_ok=True)

    for wf in warc_files:
        link = os.path.join(subset_dir, wf.name)
        if not os.path.exists(link):
            os.symlink(wf.resolve(), link)

    print(f"Quick test: using {len(warc_files)} WARC files out of {len(sorted(input_path.glob('*.warc*')))}")
    for wf in warc_files:
        print(f"  {wf.name}")
    return subset_dir


def main():
    parser = argparse.ArgumentParser(description="Extract WARC files to Markdown.")
    parser.add_argument("--warc-input-dir", required=True,
                        help="Directory containing WARC / WARC.GZ files.")
    parser.add_argument("--topics-excel-path", default=None,
                        help="Excel file to filter domains. If omitted, all domains are processed.")
    parser.add_argument("--output-dir",
                        default="/iopsstor/scratch/cscs/ahfraij/lsaie_rag_output",
                        help="Base output directory.")
    parser.add_argument("--max-warc-files", type=int, default=None,
                        help="Limit to N WARC files for quick testing. Omit for full run.")
    args = parser.parse_args()

    # If subset requested, create a temp dir with symlinks
    tmp_ctx = tempfile.TemporaryDirectory() if args.max_warc_files else None
    warc_dir = (
        make_subset_dir(args.warc_input_dir, args.max_warc_files, tmp_ctx.name)
        if args.max_warc_files
        else args.warc_input_dir
    )

    try:
        base      = args.output_dir
        html_raw  = os.path.join(base, "html_raw",      COLL)
        pdf_raw   = os.path.join(base, "pdf_raw",       COLL)
        html_comb = os.path.join(base, "html_combined", COLL)
        pdf_comb  = os.path.join(base, "pdf_combined",  COLL)
        markdown  = os.path.join(base, "markdown",      COLL)
        mappings  = os.path.join(base, "mappings",      COLL)

        timestamps = os.path.join(mappings, "timestamps.json")
        html_maps  = os.path.join(mappings, "domain_mappings.json")
        pdf_maps   = os.path.join(mappings, "pdf_domain_mappings.json")

        for d in [mappings, markdown, html_comb, pdf_comb]:
            os.makedirs(d, exist_ok=True)

        # Stage 1: extract HTML + PDF from WARCs in parallel
        print("\n=== STAGE 1: WARC extraction ===")
        with ThreadPoolExecutor(max_workers=2) as ex:
            hf = ex.submit(warc_to_html, warc_dir, html_raw)
            pf = ex.submit(warc_to_pdf,  warc_dir, pdf_raw)
            hf.result()
            pf.result()

        # Stage 2: combine by domain + convert to Markdown in parallel
        print("\n=== STAGE 2: Domain combination + Markdown conversion ===")

        def html_pipeline():
            combine_domains_by_timestamp(html_raw, html_comb, timestamps, args.topics_excel_path)
            convert_html_combined_to_markdown(html_comb, markdown, args.topics_excel_path, html_maps)

        def pdf_pipeline():
            combine_domains_by_timestamp(pdf_raw, pdf_comb, timestamps, args.topics_excel_path)
            convert_pdf_combined_to_markdown(pdf_comb, markdown, args.topics_excel_path, pdf_maps)

        with ThreadPoolExecutor(max_workers=2) as ex:
            hf = ex.submit(html_pipeline)
            pf = ex.submit(pdf_pipeline)
            hf.result()
            pf.result()

        print(f"\n=== DONE ===")
        print(f"Markdown files written to: {markdown}")
        print(f"Next: run upload_to_openwebui.py pointing to that directory")

    finally:
        if tmp_ctx:
            tmp_ctx.cleanup()


if __name__ == "__main__":
    main()
