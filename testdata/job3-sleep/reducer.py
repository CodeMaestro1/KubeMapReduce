#!/usr/bin/env python3
import sys
import json

for line in sys.stdin:
    print(json.dumps({"key": "sleep", "value": "1"}))
