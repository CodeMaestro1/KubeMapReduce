# Benchmarks

Two benchmark suites validating the MapReduce pipeline: **WordCount** and **Distributed Grep**.

## Prerequisites

- Python 3 (no third-party packages required)
- 6 Project Gutenberg HTML files in `corpus/` (already present in this repo)

## Step 1 — Build the corpus

Strips HTML from the Gutenberg books and produces a JSONL dataset:

```bash
python benchmarks/build_corpus.py
```

Output: `benchmarks/data/corpus.jsonl` (~77,000 lines, one per text line from each book).

Each record: `{"key": "pg1342:000042", "value": "It is a truth universally acknowledged"}`

The `benchmarks/data/` directory is gitignored. Re-run this command after cloning.

---

## WordCount

Counts word frequencies across the entire corpus.

**Mapper** (`wordcount/mapper.py`): tokenises each line into lowercase words, emits `{"key": word, "value": "1"}` per word.

**Reducer** (`wordcount/reducer.py`): sums counts per word, emits `{"key": word, "value": "<count>"}`.

### Local validation

Runs the full pipeline locally (no cluster needed) and checks correctness:

```bash
python benchmarks/wordcount/validate.py
```

Expected output:
```
Running WordCount pipeline locally against .../corpus.jsonl ...
  12345 unique words found
  Top 10 words: the=18432, and=14201, of=12800, ...
PASS
```

### Compare against cluster output

After running the job on a live cluster and downloading the output:

```bash
python benchmarks/wordcount/validate.py --compare /path/to/cluster-output.jsonl
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

## Running on a live cluster

1. **Build corpus** (Step 1 above)

2. **Upload corpus to MinIO**:
   ```bash
   # Via the CLI (replace values as needed)
   kubemr files upload benchmarks/data/corpus.jsonl
   ```

3. **Upload mapper and reducer**:
   ```bash
   kubemr files upload benchmarks/wordcount/mapper.py
   kubemr files upload benchmarks/wordcount/reducer.py
   ```

4. **Submit the job**:
   ```bash
   kubemr jobs submit \
     --input corpus.jsonl \
     --mapper s3://mapreduce-inputs/mapper.py \
     --reducer s3://mapreduce-inputs/reducer.py \
     --reducers 4
   ```

5. **Download output and validate**:
   ```bash
   kubemr files download <output-uri> -o cluster-output.jsonl
   python benchmarks/wordcount/validate.py --compare cluster-output.jsonl
   ```

---

## Mapper/Reducer Protocol

Mappers and reducers communicate via JSONL on stdin/stdout:

- **Input to mapper**: `{"key": "<source_key>", "value": "<text>"}`
- **Mapper output**: `{"key": "<emit_key>", "value": "<emit_value>"}`
- **Input to reducer**: same JSONL format, **sorted lexicographically by key** (guaranteed by the framework)
- **Reducer output**: `{"key": "<result_key>", "value": "<result_value>"}`

The framework handles partitioning, shuffling, and sorting between map and reduce phases.
