#!/usr/bin/env python3
"""Average Temperature Mapper — emits (city, temperature) pairs."""

import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    doc = json.loads(line)
    city = doc.get("city", "unknown")
    temp = doc.get("temperature", 0)
    print(json.dumps({"key": city, "value": str(temp)}))
