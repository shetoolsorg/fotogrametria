#!/usr/bin/env python3
"""
Batch script to process .tif files in a directory and send them to the
calculate_stats endpoint.

Usage:
    python batch_calculate_stats.py <directory> \
        --gpkg_file /path/to/file.gpkg \
        --host http://localhost:8000 \
        --token YOUR_API_TOKEN \
        [--plot_id_field uid] \
        [--force_store]

Filename format required:
    {organizacion}-{unidad_produccion}-{aammdd}-{gsd}-{indice}.tif
    Example: RHN-El_Venado-260116-10-NDVI.tif
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime

import requests


# ---------------------------------------------------------------------------
# Filename validation (mirrors parse_tif_filename in app.py)
# ---------------------------------------------------------------------------

def validate_tif_filename(filename: str) -> tuple[bool, str]:
    """
    Validate that a .tif filename follows the required format:
        {organizacion}-{unidad_produccion}-{aammdd}-{gsd}-{indice}.tif

    Returns (is_valid: bool, reason: str).
    """
    stem = Path(filename).stem
    parts = stem.split("-", 4)

    if len(parts) != 5:
        return False, (
            f"Expected 5 dash-separated parts, got {len(parts)}. "
            "Format: {{organizacion}}-{{unidad_produccion}}-{{aammdd}}-{{gsd}}-{{indice}}.tif"
        )

    _, _, raw_date, raw_gsd, _ = parts

    try:
        datetime.strptime(raw_date, "%y%m%d")
    except ValueError:
        return False, f"Date part '{raw_date}' is not a valid date in YYMMDD format."

    try:
        float(raw_gsd)
    except ValueError:
        return False, f"GSD part '{raw_gsd}' is not a valid number."

    return True, "OK"


# ---------------------------------------------------------------------------
# HTTP upload
# ---------------------------------------------------------------------------

def send_to_calculate_stats(
    host: str,
    token: str,
    tif_path: Path,
    gpkg_path: Path,
    plot_id_field: str,
    force_store: bool,
) -> dict:
    """
    Upload a single .tif + .gpkg pair to the calculate_stats endpoint.
    Returns the parsed JSON response.
    Raises requests.HTTPError on non-2xx responses.
    """
    url = f"{host.rstrip('/')}/calculate_stats"
    headers = {"Authorization": f"Bearer {token}"}

    with open(tif_path, "rb") as tif_fp, open(gpkg_path, "rb") as gpkg_fp:
        files = {
            "tif_file": (tif_path.name, tif_fp, "image/tiff"),
            "gpkg_file": (gpkg_path.name, gpkg_fp, "application/geopackage+sqlite3"),
        }
        data = {
            "plot_id_field": plot_id_field,
            "force_store": str(force_store).lower(),  # FastAPI reads "true"/"false"
        }
        response = requests.post(url, headers=headers, files=files, data=data)

    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Batch-send .tif files from a directory to the calculate_stats endpoint."
    )

    parser.add_argument(
        "directory",
        help="Directory containing .tif files to process.",
    )
    parser.add_argument(
        "--gpkg_file",
        required=True,
        help="Path to the .gpkg file used as the polygon source.",
    )
    parser.add_argument(
        "--host",
        required=True,
        help="Base URL of the API, e.g. http://localhost:8000",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Bearer token for API authentication.",
    )
    parser.add_argument(
        "--plot_id_field",
        default="uid",
        help="Field name used as the plot identifier (default: uid).",
    )
    parser.add_argument(
        "--force_store",
        action="store_true",
        default=False,
        help="If set, overwrite an existing stored .tif file.",
    )

    args = parser.parse_args()

    # --- Validate inputs ---
    directory = Path(args.directory)
    if not directory.is_dir():
        print(f"[ERROR] Directory not found: {directory}", file=sys.stderr)
        sys.exit(1)

    gpkg_path = Path(args.gpkg_file)
    if not gpkg_path.is_file():
        print(f"[ERROR] GPKG file not found: {gpkg_path}", file=sys.stderr)
        sys.exit(1)

    if not gpkg_path.suffix.lower() == ".gpkg":
        print(f"[ERROR] Expected a .gpkg file, got: {gpkg_path.name}", file=sys.stderr)
        sys.exit(1)

    # --- Collect .tif files ---
    tif_files = sorted(directory.glob("*.tif")) + sorted(directory.glob("*.TIF"))
    # Deduplicate (glob is case-sensitive on Linux, but just in case)
    seen = set()
    unique_tifs = []
    for t in tif_files:
        if t not in seen:
            seen.add(t)
            unique_tifs.append(t)

    if not unique_tifs:
        print(f"[INFO] No .tif files found in: {directory}")
        sys.exit(0)

    print(f"[INFO] Found {len(unique_tifs)} .tif file(s) in '{directory}'.")
    print(f"[INFO] Using GPKG: {gpkg_path}")
    print(f"[INFO] Endpoint: {args.host.rstrip('/')}/calculate_stats")
    print()

    # --- Process each file ---
    results = {"sent": 0, "skipped": 0, "errors": 0}

    for tif_path in unique_tifs:
        print(f"[FILE] {tif_path.name}")

        is_valid, reason = validate_tif_filename(tif_path.name)
        if not is_valid:
            print(f"  [SKIP] Invalid filename format — {reason}")
            results["skipped"] += 1
            continue

        try:
            response_data = send_to_calculate_stats(
                host=args.host,
                token=args.token,
                tif_path=tif_path,
                gpkg_path=gpkg_path,
                plot_id_field=args.plot_id_field,
                force_store=args.force_store,
            )
            print(f"  [OK]   {response_data}")
            results["sent"] += 1
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            body = exc.response.text if exc.response is not None else str(exc)
            print(f"  [ERROR] HTTP {status} — {body}")
            results["errors"] += 1
        except requests.RequestException as exc:
            print(f"  [ERROR] Request failed — {exc}")
            results["errors"] += 1

    # --- Summary ---
    print()
    print(
        f"[SUMMARY] Sent: {results['sent']} | "
        f"Skipped (invalid name): {results['skipped']} | "
        f"Errors: {results['errors']}"
    )

    if results["errors"] > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()