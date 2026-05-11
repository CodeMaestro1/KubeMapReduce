import subprocess
import time
import os
import json
import matplotlib.pyplot as plt

# Configuration
API_URL = os.environ.get("API_URL", "http://localhost:8081")
CLI_PATH = "./bin/cli"
INPUT_FILE = "benchmarks/data/corpus.jsonl"
MAPPER_FILE = "benchmarks/wordcount/mapper.py"
REDUCER_FILE = "benchmarks/wordcount/reducer.py"

def run_cli(args):
    env = os.environ.copy()
    env["API_URL"] = API_URL
    cmd = [CLI_PATH] + args
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"CLI Error: {result.stderr}")
        return None
    return result.stdout.strip()

def get_job_id(output):
    try:
        data = json.loads(output)
        return data.get("jobId")
    except:
        return None

def get_job_status(job_id):
    output = run_cli(["jobs", "status", "--id", job_id])
    if not output: return "Unknown"
    try:
        data = json.loads(output)
        return data.get("status", "Unknown")
    except:
        return "Unknown"

def benchmark():
    reducer_counts = [1, 2, 4, 8]
    results = []

    print(f"Starting benchmarks against {API_URL}")

    for r in reducer_counts:
        print(f"\n--- Testing with {r} reducers ---")
        start_time = time.time()
        
        output = run_cli([
            "jobs", "submit",
            "--input", INPUT_FILE,
            "--mapper", MAPPER_FILE,
            "--reducer", REDUCER_FILE,
            "--reducers", str(r)
        ])
        
        job_id = get_job_id(output)
        if not job_id:
            print("Failed to submit job")
            continue
            
        print(f"Job ID: {job_id}. Waiting for completion...")
        
        while True:
            status = get_job_status(job_id)
            print(f"Status: {status}")
            if status in ["Completed", "Failed", "Cancelled"]:
                break
            time.sleep(5)
            
        end_time = time.time()
        duration = end_time - start_time
        
        if status == "Completed":
            print(f"Job finished in {duration:.2f} seconds")
            results.append({"reducers": r, "duration": duration})
        else:
            print(f"Job failed with status: {status}")

    # Save results
    with open("benchmarks/results/distributed_benchmark.json", "w") as f:
        json.dump(results, f, indent=2)
        
    # Plot results
    if results:
        rs = [res["reducers"] for res in results]
        ds = [res["duration"] for res in results]
        
        plt.figure(figsize=(10, 6))
        plt.plot(rs, ds, marker='o', linestyle='-', color='b')
        plt.title("KubeMapReduce Scaling Performance on GKE")
        plt.xlabel("Number of Reducers")
        plt.ylabel("Execution Time (seconds)")
        plt.grid(True)
        plt.savefig("benchmarks/results/scaling_chart.png")
        print("\nBenchmark complete! Results saved to benchmarks/results/")

if __name__ == "__main__":
    benchmark()
