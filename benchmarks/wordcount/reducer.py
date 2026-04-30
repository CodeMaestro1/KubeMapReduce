"""
WordCount reducer.

Reads SORTED JSONL records from stdin (sorted by key, guaranteed by framework).
Emits {"key": word, "value": "<total_count>"} per unique word.
"""

import json
import sys

current_key = None
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue

    key = record["key"]
    if key != current_key:
        if current_key is not None:
            sys.stdout.write(json.dumps({"key": current_key, "value": str(count)}) + "\n")
        current_key = key
        count = 0
    count += int(record["value"])

if current_key is not None:
    sys.stdout.write(json.dumps({"key": current_key, "value": str(count)}) + "\n")
