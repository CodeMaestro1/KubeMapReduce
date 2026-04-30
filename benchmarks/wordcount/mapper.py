"""
WordCount mapper.

Reads JSONL records from stdin ({"key": ..., "value": text}).
Emits one {"key": word, "value": "1"} per word in the value field.
"""

import json
import re
import sys

WORD_RE = re.compile(r"[a-zA-Z']+")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    text = record.get("value", "")
    for word in WORD_RE.findall(text.lower()):
        sys.stdout.write(json.dumps({"key": word, "value": "1"}) + "\n")
