#!/usr/bin/env python3
import time
import sys
import json

time.sleep(20) # Reduced for faster E2E

for line in sys.stdin:
    print(json.dumps({"key": "sleep", "value": "1"}))
