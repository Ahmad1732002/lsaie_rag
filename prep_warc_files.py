"""
Given warc files create md and text files.
using: https://github.com/recrm/ArchiveTools#warc-extractorpy
"""
import subprocess
import os
import json
import re
import gzip
import pandas as pd
from bs4 import BeautifulSoup
from html_to_markdown import convert_to_markdown
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp


def process_single_warc_file(warc_file, content_type, output_dir_path):
    """
    Process a single WARC file to extract HTML or PDF content using warcio.

    Args:
        warc_file (Path): Path to the WARC file
        content_type (str): Content type to extract ('text/html' or 'pdf')
        output_dir_path (str): Output directory path

    Returns:
        tuple: (warc_filename, success, error_message)
    """
    from warcio.archiveiterator import ArchiveIterator
    from urllib.parse import urlparse, unquote

    warc_filename = warc_file.name
    done_marker = os.path.join(output_dir_path, f".{warc_filename}.done")

    # Resume: skip if already processed
    if os.path.exists(done_marker):
        return (warc_filename, True, None)

    try:
        with open(str(warc_file), 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type != 'response':
                    continue
                if record.http_headers is None:
                    continue

                # Skip non-2xx HTTP responses (404, 500, 301, etc.)
                status_code = record.http_headers.get_statuscode()
                if not status_code or not status_code.startswith('2'):
                    continue

                http_ct = record.http_headers.get_header('Content-Type', '')
                if content_type not in http_ct:
                    continue

                url = record.rec_headers.get_header('WARC-Target-URI', '')
                if not url or not url.startswith('http'):
                    continue

                try:
                    parsed = urlparse(unquote(url))
                    if not parsed.hostname:
                        continue

                    host = parsed.hostname.replace('www.', '', 1)

                    # Replicate warc_extractor.py path logic exactly so combine_domains.py works unchanged
                    url_path = parsed.path or '/'
                    index = url_path.rfind('/') + 1
                    filename = url_path[index:]
                    dir_path = url_path[:index]

                    if '.' not in filename:
                        dir_path = dir_path + filename
                        if not dir_path.endswith('/'):
                            dir_path += '/'
                        filename = 'index.html'

                    dir_path = dir_path.replace('.', '-')

                    full_dir = os.path.join(
                        output_dir_path,
                        f"{warc_filename}_{host}",
                        dir_path.lstrip('/')
                    )
                    os.makedirs(full_dir, exist_ok=True)

                    # Handle duplicate filenames
                    dot_idx = filename.rfind('.')
                    stem = filename[:dot_idx] if dot_idx >= 0 else filename
                    suffix = filename[dot_idx:] if dot_idx >= 0 else ''
                    out_path = os.path.join(full_dir, filename)
                    n = 0
                    while os.path.isfile(out_path):
                        n += 1
                        out_path = os.path.join(full_dir, f"{stem}({n}){suffix}")

                    content = record.content_stream().read()
                    with open(out_path, 'wb') as f:
                        f.write(content)

                except Exception:
                    continue

        # Mark this WARC as done for resume
        with open(done_marker, 'w') as f:
            f.write('done')
        return (warc_file.name, True, None)
    except Exception as e:
        return (warc_file.name, False, str(e))


def warc_to_html(input_dir_path: str, output_dir_path: str, max_workers=None):
    """
    Goes through the files in `input_dir_path`, finds all the warc (and warc.gz) files,
    extracts the html pages and saves them in the given `output_dir_path`.
    The hierarchy of directories is preserved for the html output.
    Processes WARC files in parallel for better performance.

    Args:
        input_dir_path (str): Path to the input directory.
        output_dir_path (str): Path to the output directory.
        max_workers (int, optional): Maximum number of parallel workers. If None, uses CPU count.
    """
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    # Find all WARC files
    input_path = Path(input_dir_path)
    warc_files = list(input_path.glob("*.warc")) + list(input_path.glob("*.warc.gz"))

    if not warc_files:
        print(f"No WARC files found in {input_dir_path}")
        return

    # Determine number of workers
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(warc_files))

    print(f"Processing {len(warc_files)} WARC files for HTML extraction using {max_workers} workers...")

    # Process WARC files in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_single_warc_file, warc_file, "text/html", output_dir_path)
            for warc_file in warc_files
        ]

        successful = 0
        failed = 0

        for future in tqdm(as_completed(futures), total=len(warc_files), desc="Extracting HTML", unit="file"):
            filename, success, error = future.result()
            if success:
                successful += 1
            else:
                failed += 1
                if error:
                    print(f"\nError processing {filename}: {error}")

    print(f"HTML extraction complete: {successful} successful, {failed} failed")


def warc_to_pdf(input_dir_path: str, output_dir_path: str, max_workers=None):
    """
    Goes through the files in `input_dir_path`, finds all the warc (and warc.gz) files,
    extracts the pdf files and saves them in a `output_dir_path/wp-content` folder.
    Processes WARC files in parallel for better performance.

    Args:
        input_dir_path (str): Path to the input directory.
        output_dir_path (str): Path to the output directory.
        max_workers (int, optional): Maximum number of parallel workers. If None, uses CPU count.
    """
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    # Find all WARC files
    input_path = Path(input_dir_path)
    warc_files = list(input_path.glob("*.warc")) + list(input_path.glob("*.warc.gz"))

    if not warc_files:
        print(f"No WARC files found in {input_dir_path}")
        return

    # Determine number of workers
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(warc_files))

    print(f"Processing {len(warc_files)} WARC files for PDF extraction using {max_workers} workers...")

    # Process WARC files in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_single_warc_file, warc_file, "pdf", output_dir_path)
            for warc_file in warc_files
        ]

        successful = 0
        failed = 0

        for future in tqdm(as_completed(futures), total=len(warc_files), desc="Extracting PDF", unit="file"):
            filename, success, error = future.result()
            if success:
                successful += 1
            else:
                failed += 1
                if error:
                    print(f"\nError processing {filename}: {error}")

    print(f"PDF extraction complete: {successful} successful, {failed} failed")