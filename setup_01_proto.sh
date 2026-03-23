#!/bin/bash
# Download and compile 01 Exchange protobuf schema
set -e

echo "Downloading schema.proto from 01 Exchange..."
curl -o schema.proto https://zo-mainnet.n1.xyz/schema.proto

echo "Compiling to Python..."
python -m grpc_tools.protoc -I. --python_out=. schema.proto

echo "Done! schema_pb2.py generated."
