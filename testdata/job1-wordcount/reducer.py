#!/usr/bin/env python3
"""Word Count Reducer — sums counts per word."""

import sys
import json
from collections import defaultdict

counts = defaultdict(int)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc = json.loads(line)
        key = doc.get("key", "")
        val = int(doc.get("value", "0"))
        counts[key] += val
    except (json.JSONDecodeError, ValueError):
        continue

for word, total in sorted(counts.items(), key=lambda x: -x[1]):
    print(json.dumps({"key": word, "value": str(total)}))
