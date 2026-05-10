#!/usr/bin/env python3
"""Word Count Mapper — classic MapReduce example."""

import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc = json.loads(line)
        text = doc.get("text", "")
    except json.JSONDecodeError:
        text = line
    for word in text.split():
        cleaned = "".join(c.lower() for c in word if c.isalnum())
        if cleaned:
            print(json.dumps({"key": cleaned, "value": "1"}))
