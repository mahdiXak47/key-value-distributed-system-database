#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting Key-Value Store Data Flow Test${NC}"
echo "----------------------------------------"

# 1. First, ensure the controller is running
echo -e "${GREEN}Step 1: Checking Controller Status${NC}"
curl -s http://localhost:8080/health || { echo "Controller is not running. Please start it first."; exit 1; }

# 2. Add a test node
echo -e "\n${GREEN}Step 2: Adding Test Node${NC}"
curl -X POST http://localhost:8080/node/add \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "name=test-node-1&address=http://localhost:9001"

# 3. Create a partition
echo -e "\n${GREEN}Step 3: Creating Partition${NC}"
curl -X POST http://localhost:8080/partition/add

# 4. Test SET operation
echo -e "\n${GREEN}Step 4: Testing SET Operation${NC}"
echo "Sending SET request for key 'test-key'..."
curl -X POST http://localhost:8080/set \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key", "value": "test-value"}'

# 5. Test GET operation
echo -e "\n${GREEN}Step 5: Testing GET Operation${NC}"
echo "Sending GET request for key 'test-key'..."
curl -X POST http://localhost:8080/get \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key"}'

# 6. Test DELETE operation
echo -e "\n${GREEN}Step 6: Testing DELETE Operation${NC}"
echo "Sending DELETE request for key 'test-key'..."
curl -X POST http://localhost:8080/delete \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key"}'

# 7. Verify deletion
echo -e "\n${GREEN}Step 7: Verifying Deletion${NC}"
echo "Sending GET request to verify deletion..."
curl -X POST http://localhost:8080/get \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key"}'

echo -e "\n${BLUE}Test Flow Completed${NC}"
echo "----------------------------------------" 