#!/bin/bash

# Test set operation
echo "Testing SET operation..."
curl -X POST http://localhost:8082/set \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "key=test_key&value=test_value"

echo -e "\n\nTesting GET operation..."
curl -X POST http://localhost:8082/get \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "key=test_key"

echo -e "\n\nTesting DELETE operation..."
curl -X POST http://localhost:8082/delete \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "key=test_key"

echo -e "\n\nVerifying deletion..."
curl -X POST http://localhost:8082/get \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "key=test_key" 