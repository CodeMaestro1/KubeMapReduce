# KubeMapReduce Benchmarks

This directory contains the benchmarks used to validate the performance and resilience of the KubeMapReduce system.

## Dataset Preparation

To generate the datasets used in these benchmarks, run the following commands:

### 1. Build the base corpus (16MB)
Requires the Gutenberg HTML files in `corpus/`.
```bash
python benchmarks/build_corpus.py
```
Output: `benchmarks/data/corpus.jsonl` (~77,000 lines, one per text line from each book).

Each record: `{"key": "pg1342:000042", "value": "It is a truth universally acknowledged"}`

The `benchmarks/data/` directory is gitignored. Re-run this command after cloning.

### 2. Build the Big Data corpus (128MB)
Requires `benchmarks/data/corpus.jsonl` to exist.
```bash
python -c "with open('benchmarks/data/corpus.jsonl', 'r') as f: content = f.read(); open('benchmarks/data/corpus_128mb.jsonl', 'w').write(content * 8)"
```

---

## WordCount

Counts word frequencies across the entire corpus.

**Mapper** (`wordcount/mapper.py`): tokenises each line into lowercase words, emits `{"key": word, "value": "1"}` per word.

**Combiner** (`wordcount/combiner.py`): local pre-aggregation of mapper output (`1`s) into per-word partial counts.

**Reducer** (`wordcount/reducer.py`): sums partial counts per word, emits `{"key": word, "value": "<count>"}`.

### Local validation

Runs the full pipeline locally (no cluster needed) and checks correctness:

```bash
python benchmarks/wordcount/validate.py
```

---

## Distributed Grep

Filters corpus lines matching a regex pattern.

**Mapper** (`grep/mapper.py`): emits a record unchanged if its `value` field matches the pattern. Pattern set via env var `GREP_PATTERN` (default: `love`).

**Reducer** (`grep/reducer.py`): identity — passes all matching records through.

### Local validation

```bash
python benchmarks/grep/validate.py
```

With a custom pattern:

```bash
python benchmarks/grep/validate.py --pattern "captain"
```

Expected output:
```
Running Grep pipeline locally (pattern='love') ...
  Corpus: 77517 lines
  Matches: 842 lines (1.1%)
  Sample: 'She had found out that much love had been sacrificed...'
PASS
```

### Compare against cluster output

```bash
python benchmarks/grep/validate.py --compare /path/to/cluster-output.jsonl --pattern "love"
```

---

## Running Benchmarks on a Live Cluster (GKE)

### Prerequisites
Ensure you have logged in via the CLI and set the `API_URL` environment variable.
```bash
export API_URL="http://<YOUR_API_IP>"
```

### Distributed GKE Benchmark Suite
To run the automated performance and scalability benchmarks:
```bash
python benchmarks/distributed_benchmark.py
```

### Manual Job Submission

1. **Build corpus** (Step 1 above)

2. **Submit WordCount job (with combiner)**:
   ```bash
   go run ./cli-service/cmd/cli jobs submit \
     --mapper benchmarks/wordcount/mapper.py \
     --combiner benchmarks/wordcount/combiner.py \
     --reducer benchmarks/wordcount/reducer.py \
     --input benchmarks/data/corpus.jsonl \
     --reducers 4
   ```

3. **Download output and validate**:
   ```bash
   go run ./cli-service/cmd/cli jobs download --id <job-id> --output ./results/
   cat ./results/*.json > cluster-output.jsonl
   python benchmarks/wordcount/validate.py --compare cluster-output.jsonl
   ```

---

## Results & Visualizations
Final results and visualizations are located in `benchmarks/results/`.
Detailed analysis can be found in `docs/PRESENTATION_TESTS.md` and `docs/EXTENDED_BENCHMARKS.md`.

---

## Mapper/Combiner/Reducer Protocol

Mappers, combiners, and reducers communicate via JSONL on stdin/stdout:

- **Input to mapper**: `{"key": "<source_key>", "value": "<text>"}`
- **Mapper output**: `{"key": "<emit_key>", "value": "<emit_value>"}`
- **Combiner input/output** (optional): same schema as mapper/reducer records, keyed by `key`
- **Input to reducer**: same JSONL format, **sorted lexicographically by key** (guaranteed by the framework)
- **Reducer output**: `{"key": "<result_key>", "value": "<result_value>"}`

The framework handles partitioning, shuffling, and sorting between map and reduce phases.
