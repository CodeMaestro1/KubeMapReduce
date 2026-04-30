"""
Distributed Grep mapper.

Reads JSONL records from stdin. Emits a record unchanged if its value field
matches the search pattern. Pattern is read from the GREP_PATTERN env var
(default: "love"). Matching is case-insensitive.
"""

import json
import os
import re
import sys

pattern = os.environ.get("GREP_PATTERN", "love")
regex = re.compile(pattern, re.IGNORECASE)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if regex.search(record.get("value", "")):
        sys.stdout.write(json.dumps(record) + "\n")
