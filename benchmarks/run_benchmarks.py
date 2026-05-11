import json
import os
import subprocess
import sys
import time
import argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCHMARKS_DIR = SCRIPT_DIR
CORPUS = os.path.join(BENCHMARKS_DIR, "data", "corpus.jsonl")
WC_MAPPER = os.path.join(BENCHMARKS_DIR, "wordcount", "mapper.py")
WC_REDUCER = os.path.join(BENCHMARKS_DIR, "wordcount", "reducer.py")
GREP_MAPPER = os.path.join(BENCHMARKS_DIR, "grep", "mapper.py")
GREP_REDUCER = os.path.join(BENCHMARKS_DIR, "grep", "reducer.py")

def run_local_benchmark(name, mapper, reducer, env=None):
    print(f"--- Running Local Benchmark: {name} ---")
    
    start_time = time.time()
    
    # Map Phase
    print("  Phase: Map")
    map_start = time.time()
    with open(CORPUS, "r", encoding="utf-8") as f:
        map_proc = subprocess.run(
            [sys.executable, mapper],
            stdin=f,
            capture_output=True,
            text=True,
            env=env or os.environ
        )
    map_end = time.time()
    if map_proc.returncode != 0:
        print(f"Error in Map: {map_proc.stderr}")
        return None
    
    # Shuffle/Sort Phase
    print("  Phase: Shuffle/Sort")
    shuffle_start = time.time()
    lines = map_proc.stdout.splitlines()
    records = [json.loads(l) for l in lines if l.strip()]
    records.sort(key=lambda r: r["key"])
    sorted_input = "\n".join(json.dumps(r) for r in records) + "\n"
    shuffle_end = time.time()
    
    # Reduce Phase
    print("  Phase: Reduce")
    reduce_start = time.time()
    reduce_proc = subprocess.run(
        [sys.executable, reducer],
        input=sorted_input,
        capture_output=True,
        text=True
    )
    reduce_end = time.time()
    if reduce_proc.returncode != 0:
        print(f"Error in Reduce: {reduce_proc.stderr}")
        return None
    
    total_end = time.time()
    
    results = {
        "name": name,
        "total_time": total_end - start_time,
        "map_time": map_end - map_start,
        "shuffle_time": shuffle_end - shuffle_start,
        "reduce_time": reduce_end - reduce_start,
        "output_count": len(reduce_proc.stdout.splitlines())
    }
    
    print(f"  Total Time: {results['total_time']:.2f}s")
    print(f"  Map: {results['map_time']:.2f}s, Shuffle: {results['shuffle_time']:.2f}s, Reduce: {results['reduce_time']:.2f}s")
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=os.path.join(BENCHMARKS_DIR, "results", "local_results.json"))
    args = parser.parse_args()
    
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    all_results = []
    
    # WordCount
    wc_res = run_local_benchmark("WordCount", WC_MAPPER, WC_REDUCER)
    if wc_res: all_results.append(wc_res)
    
    # Grep
    grep_patterns = ["love", "captain", "the"]
    for pattern in grep_patterns:
        env = os.environ.copy()
        env["GREP_PATTERN"] = pattern
        res = run_local_benchmark(f"Grep-{pattern}", GREP_MAPPER, GREP_REDUCER, env=env)
        if res: all_results.append(res)
    
    with open(args.output, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {args.output}")

if __name__ == "__main__":
    main()
