"""
Distributed Grep reducer.

Identity reducer: passes all records through unchanged.
Grep has no aggregation step — the reducer simply collects matching lines.
"""

import sys

for line in sys.stdin:
    sys.stdout.write(line)
