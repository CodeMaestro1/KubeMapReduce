#!/usr/bin/env python3
import sys
import os
import random
import json

marker = "/tmp/mapper_attempt_marker"
first_attempt = not os.path.exists(marker)
if first_attempt:
    with open(marker, "w") as f:
        f.write("1")

if first_attempt:
    sys.stderr.write("Simulated transient failure (guaranteed first attempt)\n")
    sys.exit(1)

if random.random() < 0.5:
    sys.stderr.write("Simulated transient failure (50% subsequent)\n")
    sys.exit(1)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc = json.loads(line)
        if isinstance(doc, dict):
            text = doc.get("text", "")
        else:
            text = str(doc)
    except (json.JSONDecodeError, TypeError):
        text = line
    print(json.dumps({"key": "retry-test", "value": "1"}))
