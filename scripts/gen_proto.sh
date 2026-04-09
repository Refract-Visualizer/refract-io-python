#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

uv run python -m grpc_tools.protoc \
  -I "$ROOT_DIR/proto/" \
  --python_out="$ROOT_DIR/src/refract_io/_proto/" \
  --grpc_python_out="$ROOT_DIR/src/refract_io/_proto/" \
  "$ROOT_DIR/proto/kvstream.proto"

# Fix absolute import to relative import in generated grpc file
sed -i '' 's/^import kvstream_pb2 as/from . import kvstream_pb2 as/' \
  "$ROOT_DIR/src/refract_io/_proto/kvstream_pb2_grpc.py"

echo "Proto stubs regenerated."
