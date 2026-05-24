#!/bin/bash
curl -s -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: df254048a96e4459b52191b8a07ee528c5cc39dfe91af4a3d58a01944ae0c861" \
  -d '{"city_name": "Philadelphia", "country_or_region": "United States"}' | python3 -m json.tool
