#!/bin/sh
set -eu
cd "$(dirname "$0")"
mkdir -p output
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
docker compose up -d minio redis
docker compose run --rm --build pipeline python check.py
