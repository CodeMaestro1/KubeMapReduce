# KubeMapReduce on Google Cloud Platform (GKE)

This tutorial provides a comprehensive, step-by-step guide to deploying and benchmarking the KubeMapReduce system on a production-grade Google Kubernetes Engine (GKE) cluster.

## Prerequisites

Before starting, ensure you have the following installed on your local machine:
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Docker](https://docs.docker.com/get-docker/)
- [Go 1.26+](https://go.dev/doc/install)
- [Python 3.12+](https://www.python.org/downloads/) (for running benchmarks)

---

## Phase 1: GCP Infrastructure Setup

### 1. Authenticate and Configure GCP
First, log in to your Google Cloud account and set your project and preferred region.

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID # Example: gen-lang-client-0105646163
gcloud config set compute/region europe-north1
```

### 2. Create the GKE Cluster
We will create a GKE cluster using `t2d-standard-2` Spot VMs to balance performance and cost. These instances offer dedicated vCPUs which are ideal for data-processing workloads.

```bash
gcloud container clusters create kubemapreduce-gcp \
    --zone=europe-north1-a \
    --machine-type=t2d-standard-2 \
    --num-nodes=8 \
    --spot \
    --gateway-api=standard \
    --enable-ip-alias \
    --disk-size=15 \
    --disk-type=pd-balanced
```

*Note: The minimum disk size for Google COS images is 12GB. We use 15GB balanced persistent disks to satisfy this constraint while remaining cost-effective.*

### 3. Connect `kubectl` to your Cluster
Install the GKE auth plugin and configure your local `kubeconfig`:

```bash
# Install the auth plugin using gcloud
gcloud components install gke-gcloud-auth-plugin

# Get cluster credentials
gcloud container clusters get-credentials kubemapreduce-gcp --region europe-north1
```

---

## Phase 2: Build and Push Images

KubeMapReduce requires three Docker images: `api`, `manager`, and `worker`. We will host them on Google Artifact Registry.

### 1. Create an Artifact Registry Repository
```bash
gcloud artifacts repositories create mapreduce \
    --repository-format=docker \
    --location=europe-north1 \
    --description="KubeMapReduce images"
```

Configure Docker to authenticate with Artifact Registry:
```bash
gcloud auth configure-docker europe-north1-docker.pkg.dev
```

### 2. Build and Push
Replace `YOUR_PROJECT_ID` with your actual GCP Project ID.

```bash
export IMAGE_BASE="europe-north1-docker.pkg.dev/YOUR_PROJECT_ID/mapreduce"

# Build images
docker build -f infra/docker/Dockerfile.api -t $IMAGE_BASE/api:latest .
docker build -f infra/docker/Dockerfile.manager -t $IMAGE_BASE/manager:latest .
docker build -f infra/docker/Dockerfile.worker -t $IMAGE_BASE/worker:latest .

# Push images
docker push $IMAGE_BASE/api:latest
docker push $IMAGE_BASE/manager:latest
docker push $IMAGE_BASE/worker:latest
```

### 3. Update Kubernetes Manifests
In `k8s/30-manager.yaml`, update the `manager` image and inject the `WORKER_IMAGE` environment variable:
```yaml
        - name: manager
          image: europe-north1-docker.pkg.dev/YOUR_PROJECT_ID/mapreduce/manager:latest
          env:
            - name: WORKER_IMAGE
              value: "europe-north1-docker.pkg.dev/YOUR_PROJECT_ID/mapreduce/worker:latest"
```

In `k8s/35-api.yaml`, update the `api` image:
```yaml
        - name: api
          image: europe-north1-docker.pkg.dev/YOUR_PROJECT_ID/mapreduce/api:latest
```

---

## Phase 3: Deploy KubeMapReduce

### 1. Apply Namespace and Base Services
We use Kustomize to apply our manifests.

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -k k8s/
```

### 2. Expose Services via LoadBalancer
For external access (CLI interactions, uploading data, authentication), we deploy LoadBalancer services for API, Keycloak, and MinIO.

```bash
# Create external services (make sure you created api-lb.yaml, keycloak-lb.yaml, minio-lb.yaml)
kubectl apply -f k8s/api-lb.yaml
kubectl apply -f k8s/keycloak-lb.yaml
kubectl apply -f k8s/minio-lb.yaml
```

Wait a few minutes for GCP to provision external IPs:
```bash
kubectl get svc -n mapreduce | grep LoadBalancer
```
Example Output:
```
api-external        LoadBalancer   34.118.226.14    35.228.168.214   80:30565/TCP                    107m
keycloak-external   LoadBalancer   34.118.237.168   34.88.236.143    80:32432/TCP                    107m
minio-external      LoadBalancer   34.118.232.113   34.88.131.63     9000:31981/TCP,9001:32183/TCP   107m
```

### 3. Configure External Endpoints
Once you have the external IPs, update your configurations.
In the example above:
- `<API_IP>` = `35.228.168.214`
- `<KEYCLOAK_IP>` = `34.88.236.143`
- `<MINIO_IP>` = `34.88.131.63`

#### Update Manager ConfigMap and Manifests
Open `k8s/30-manager.yaml` and update the following:
1. `KEYCLOAK_ISSUER`: `http://<KEYCLOAK_IP>/realms/mapreduce`
2. `MINIO_ENDPOINT`: `<MINIO_IP>:9000`

#### Update API Manifest
Open `k8s/35-api.yaml` and update the following:
1. `MINIO_ENDPOINT`: `<MINIO_IP>:9000`
2. `KEYCLOAK_ISSUER`: `http://<KEYCLOAK_IP>/realms/mapreduce`

Re-apply the manifests and restart the deployments:
```bash
kubectl apply -f k8s/30-manager.yaml
kubectl apply -f k8s/35-api.yaml
kubectl rollout restart statefulset/manager -n mapreduce
kubectl rollout restart deployment/api -n mapreduce
```

### 4. Setup Keycloak Realm
Run the automated auth setup script against your external Keycloak IP:

```bash
export KEYCLOAK_BASE_URL="http://<KEYCLOAK_IP>"
go run auth-service/cmd/setup/main.go --url $KEYCLOAK_BASE_URL --admin-password admin
```

---

## Phase 4: Run Benchmarks and Verify Fault Tolerance

### 1. Compile the CLI and Login
Compile the Go CLI binary locally:
```bash
go build -o bin/cli cli-service/cmd/cli/main.go
```

Authenticate your CLI session against Keycloak.
```bash
export KEYCLOAK_BASE_URL="http://<KEYCLOAK_IP>"
export API_URL="http://<API_IP>"

./bin/cli login --username platform-admin --password admin
```
*You will see: `Login successful!`*

### 2. Run the Benchmark Script
The `distributed_benchmark.py` script automatically runs the MapReduce WordCount algorithm on the Project Gutenberg corpus, scaling across multiple reducer counts (R=1, 2, 4, 8) to test horizontal scalability.

First, ensure your Python environment has the `requests` and `matplotlib` libraries installed:
```bash
pip install requests matplotlib
```

Then, run the benchmark:
```bash
export API_URL="http://<API_IP>"
python benchmarks/distributed_benchmark.py
```

### 3. Verify Fault Tolerance
To prove that KubeMapReduce recovers from node failures or pod evictions:
1. While a job is running (e.g., during the R=4 benchmark), open a second terminal.
2. Find an active worker pod:
   ```bash
   kubectl get pods -n mapreduce | grep worker
   ```
3. forcefully delete it:
   ```bash
   kubectl delete pod <worker-pod-name> -n mapreduce
   ```
4. Observe the manager logs. The Manager's Active Reaper will detect the missed heartbeats, transition the task to `Failed`, and re-assign it to a new worker. The job will still finish successfully, albeit with a slight delay.

### 4. Analyze Results
Once the benchmark script finishes, it prints out the completion time for each replica configuration. You can use these results for your performance analysis charts to demonstrate the system's distributed throughput capabilities.
