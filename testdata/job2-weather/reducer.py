#!/usr/bin/env python3
"""Average Temperature Reducer — computes average temp per city."""

import sys
import json
from collections import defaultdict

data = defaultdict(list)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc = json.loads(line)
        key = doc.get("key", "")
        val = float(doc.get("value", "0"))
        data[key].append(val)
    except (json.JSONDecodeError, ValueError):
        continue

for city in sorted(data.keys()):
    temps = data[city]
    avg = sum(temps) / len(temps)
    print(json.dumps({"key": city, "value": f"{avg:.1f} ({len(temps)} readings)"}))
