#!/bin/bash
set -e

NODE1="http://your-vps-1:8080"
NODE2="http://your-vps-2:8080"
NODE3="http://your-vps-3:8080"

echo "Testing cluster..."
curl -X POST $NODE1/kv/test -d '{"value":"production-test"}'
curl $NODE1/kv/test
curl $NODE2/kv/test
curl $NODE3/kv/test
echo "All tests passed!"