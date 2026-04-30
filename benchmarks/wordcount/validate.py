"""
WordCount validation script.

Default mode: runs the pipeline locally against benchmarks/data/corpus.jsonl
and checks that output is sane (top words are common English words, unique word
count is above a minimum threshold).

Compare mode (--compare <file>): runs locally and compares against a cluster
output file, verifying the top-50 words match.

Exit code: 0 = pass, 1 = fail.
"""

import argparse
import json
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCHMARKS_DIR = os.path.dirname(SCRIPT_DIR)
CORPUS = os.path.join(BENCHMARKS_DIR, "data", "corpus.jsonl")
MAPPER = os.path.join(SCRIPT_DIR, "mapper.py")
REDUCER = os.path.join(SCRIPT_DIR, "reducer.py")

EXPECTED_TOP_WORDS = {"the", "and", "of", "to", "a", "in", "that", "is", "it", "was"}
MIN_UNIQUE_WORDS = 5_000
MIN_TOP_WORD_OVERLAP = 3


def run_pipeline(corpus_path):
    """Run mapper -> sort -> reducer locally. Returns list of output records."""
    with open(corpus_path, encoding="utf-8") as f:
        mapper_result = subprocess.run(
            [sys.executable, MAPPER],
            stdin=f,
            capture_output=True,
            text=True,
        )
    if mapper_result.returncode != 0:
        print(f"FAIL: mapper exited {mapper_result.returncode}")
        print(mapper_result.stderr[:500])
        sys.exit(1)

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


def load_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def top_n_by_count(records, n=50):
    return sorted(records, key=lambda r: -int(r["value"]))[:n]


def sanity_check(records):
    failures = []

    unique = len(records)
    if unique < MIN_UNIQUE_WORDS:
        failures.append(f"unique word count {unique} < minimum {MIN_UNIQUE_WORDS}")

    if any(int(r["value"]) == 0 for r in records):
        failures.append("some words have count 0")

    top_words = {r["key"] for r in top_n_by_count(records, 10)}
    overlap = top_words & EXPECTED_TOP_WORDS
    if len(overlap) < MIN_TOP_WORD_OVERLAP:
        failures.append(
            f"top-10 words overlap with common English words: {overlap} "
            f"(want >= {MIN_TOP_WORD_OVERLAP})"
        )

    return failures


def compare_outputs(local_records, cluster_records):
    failures = []

    local_top = {r["key"]: int(r["value"]) for r in top_n_by_count(local_records, 50)}
    cluster_top = {r["key"]: int(r["value"]) for r in top_n_by_count(cluster_records, 50)}

    missing = set(local_top) - set(cluster_top)
    if missing:
        failures.append(f"top-50 words missing from cluster output: {sorted(missing)[:5]}...")

    for word in set(local_top) & set(cluster_top):
        local_count = local_top[word]
        cluster_count = cluster_top[word]
        tolerance = max(1, int(local_count * 0.01))
        if abs(local_count - cluster_count) > tolerance:
            failures.append(
                f"count mismatch for '{word}': local={local_count} cluster={cluster_count}"
            )

    return failures


def main():
    parser = argparse.ArgumentParser(description="Validate WordCount benchmark")
    parser.add_argument("--corpus", default=CORPUS, help="Path to corpus.jsonl")
    parser.add_argument("--compare", metavar="FILE", help="Cluster output JSONL to compare against")
    args = parser.parse_args()

    if not os.path.exists(args.corpus):
        print(f"FAIL: corpus not found at {args.corpus}")
        print("Run: python benchmarks/build_corpus.py")
        sys.exit(1)

    print(f"Running WordCount pipeline locally against {args.corpus} ...")
    local_records = run_pipeline(args.corpus)
    print(f"  {len(local_records):,} unique words found")

    top10 = top_n_by_count(local_records, 10)
    print(f"  Top 10 words: {', '.join(r['key'] + '=' + r['value'] for r in top10)}")

    failures = sanity_check(local_records)

    if args.compare:
        print(f"\nComparing against cluster output: {args.compare}")
        cluster_records = load_jsonl(args.compare)
        print(f"  {len(cluster_records):,} unique words in cluster output")
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
