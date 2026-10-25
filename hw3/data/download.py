#!/usr/bin/env python3
"""Fetch a course-provided canonical gzip and verify its immutable digest."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen


SHA256 = "7056f47bf85d8217cb3e5b71a815e75f3917feddd4a9c3f2da29a42c586706ad"
DEFAULT_DESTINATION = Path(__file__).with_name("online-retail-ii-v1.csv.gz")


def digest(path: Path) -> str:
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest()


def copy_source(source: str, destination: Path) -> None:
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        with urlopen(source, timeout=120) as response, destination.open("wb") as target:
            shutil.copyfileobj(response, target)
        return
    path = Path(source)
    if not path.is_file():
        raise ValueError("--source must be a readable local file or http(s) URL")
    shutil.copyfile(path, destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="course-provided local path or https URL")
    parser.add_argument("--destination", type=Path, default=DEFAULT_DESTINATION)
    args = parser.parse_args()
    destination = args.destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if digest(destination) == SHA256:
            print(destination)
            return 0
        print("destination exists but its SHA-256 differs; remove it before retrying", file=sys.stderr)
        return 2
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        copy_source(args.source, temporary_path)
        actual = digest(temporary_path)
        if actual != SHA256:
            print("downloaded file SHA-256 differs from the HW3 contract", file=sys.stderr)
            return 2
        temporary_path.replace(destination)
        print(destination)
        return 0
    finally:
        temporary_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
