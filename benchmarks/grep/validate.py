"""
Distributed Grep validation script.

Default mode: runs the pipeline locally against benchmarks/data/corpus.jsonl
with the given pattern and checks that results are correct.

Compare mode (--compare <file>): verifies cluster output matches local grep
output exactly.

Exit code: 0 = pass, 1 = fail.
"""

import argparse
import json
import os
import re
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCHMARKS_DIR = os.path.dirname(SCRIPT_DIR)
CORPUS = os.path.join(BENCHMARKS_DIR, "data", "corpus.jsonl")
MAPPER = os.path.join(SCRIPT_DIR, "mapper.py")
REDUCER = os.path.join(SCRIPT_DIR, "reducer.py")


def run_pipeline(corpus_path, pattern):
    """Run grep mapper -> reducer locally. Returns list of matching records."""
    env = os.environ.copy()
    env["GREP_PATTERN"] = pattern

    with open(corpus_path, encoding="utf-8") as f:
        mapper_result = subprocess.run(
            [sys.executable, MAPPER],
            stdin=f,
            capture_output=True,
            text=True,
            env=env,
        )
    if mapper_result.returncode != 0:
        print(f"FAIL: mapper exited {mapper_result.returncode}")
        print(mapper_result.stderr[:500])
        sys.exit(1)

    # Sort by key (consistent ordering for comparison)
    raw_records = [
        json.loads(l)
        for l in mapper_result.stdout.splitlines()
        if l.strip()
    ]
    raw_records.sort(key=lambda r: r["key"])
    sorted_input = "\n".join(json.dumps(r) for r in raw_records) + "\n"

    reducer_result = subprocess.run(
        [sys.executable, REDUCER],
        input=sorted_input,
        capture_output=True,
        text=True,
    )
    if reducer_result.returncode != 0:
        print(f"FAIL: reducer exited {reducer_result.returncode}")
        print(reducer_result.stderr[:500])
        sys.exit(1)

    return [
        json.loads(l)
        for l in reducer_result.stdout.splitlines()
        if l.strip()
    ]


def count_corpus_lines(corpus_path):
    with open(corpus_path, encoding="utf-8") as f:
        return sum(1 for l in f if l.strip())


def load_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def sanity_check(records, pattern, total_lines):
    failures = []
    regex = re.compile(pattern, re.IGNORECASE)

    if len(records) == 0:
        failures.append(f"no lines matched pattern '{pattern}' — expected at least 1")
        return failures

    if len(records) >= total_lines:
        failures.append(
            f"all {len(records)} lines matched '{pattern}' — pattern too broad"
        )

    non_matching = [
        r["value"] for r in records if not regex.search(r.get("value", ""))
    ]
    if non_matching:
        failures.append(
            f"{len(non_matching)} output record(s) do not match pattern "
            f"'{pattern}': e.g. {non_matching[0]!r}"
        )

    return failures


def compare_outputs(local_records, cluster_records):
    failures = []

    local_keys = {r["key"] for r in local_records}
    cluster_keys = {r["key"] for r in cluster_records}

    missing = local_keys - cluster_keys
    extra = cluster_keys - local_keys

    if missing:
        failures.append(
            f"{len(missing)} line(s) in local output but not in cluster output, "
            f"e.g.: {sorted(missing)[:3]}"
        )
    if extra:
        failures.append(
            f"{len(extra)} line(s) in cluster output but not in local output, "
            f"e.g.: {sorted(extra)[:3]}"
        )

    return failures


def main():
    parser = argparse.ArgumentParser(description="Validate Distributed Grep benchmark")
    parser.add_argument("--corpus", default=CORPUS, help="Path to corpus.jsonl")
    parser.add_argument("--pattern", default="love", help="Regex search pattern (default: love)")
    parser.add_argument("--compare", metavar="FILE", help="Cluster output JSONL to compare against")
    args = parser.parse_args()

    if not os.path.exists(args.corpus):
        print(f"FAIL: corpus not found at {args.corpus}")
        print("Run: python benchmarks/build_corpus.py")
        sys.exit(1)

    total_lines = count_corpus_lines(args.corpus)
    print(f"Running Grep pipeline locally (pattern='{args.pattern}') ...")
    print(f"  Corpus: {total_lines:,} lines")

    local_records = run_pipeline(args.corpus, args.pattern)
    match_pct = 100 * len(local_records) / total_lines if total_lines else 0
    print(f"  Matches: {len(local_records):,} lines ({match_pct:.1f}%)")
    if local_records:
        print(f"  Sample: {local_records[0]['value'][:80]!r}")

    failures = sanity_check(local_records, args.pattern, total_lines)

    if args.compare:
        print(f"\nComparing against cluster output: {args.compare}")
        cluster_records = load_jsonl(args.compare)
        print(f"  Cluster matches: {len(cluster_records):,} lines")
        failures += compare_outputs(local_records, cluster_records)

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("\nPASS")


if __name__ == "__main__":
    main()
