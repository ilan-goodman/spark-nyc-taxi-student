#!/usr/bin/env python3
import argparse
import os
import sys
import requests

YELLOW_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_file(url: str, dest_path: str, chunk=1024 * 1024):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk_bytes in r.iter_content(chunk_size=chunk):
                if chunk_bytes:
                    f.write(chunk_bytes)
    return dest_path

def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC Yellow Taxi data and zones.")
    parser.add_argument("--months", nargs="+", required=True, help="Months like 2023-01 2023-02")
    parser.add_argument("--outdir", default="data/raw", help="Output directory for downloads")
    parser.add_argument("--zones", action="store_true", help="Also download taxi zone lookup CSV")
    args = parser.parse_args()

    success = True
    for m in args.months:
        url = f"{YELLOW_BASE}/yellow_tripdata_{m}.parquet"
        dest = os.path.join(args.outdir, f"yellow_tripdata_{m}.parquet")
        print(f"Downloading {url} -> {dest}")
        try:
            download_file(url, dest)
        except Exception as e:
            print(f"ERROR downloading {url}: {e}", file=sys.stderr)
            success = False

    if args.zones:
        zones_dest = os.path.join(args.outdir, "taxi_zone_lookup.csv")
        print(f"Downloading {ZONES_URL} -> {zones_dest}")
        try:
            download_file(ZONES_URL, zones_dest)
        except Exception as e:
            print(f"ERROR downloading zones CSV: {e}", file=sys.stderr)
            success = False

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
