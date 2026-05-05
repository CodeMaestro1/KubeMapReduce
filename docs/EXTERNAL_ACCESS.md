# External Access Guide for KubeMapReduce

How to expose the KubeMapReduce API (`api.mapreduce.local`) to external clients so they can remotely submit jobs.

**Your architecture:**
- **Internal services:** Manager, PostgreSQL, MinIO (cluster-only)
- **External service:** UI/CLI API on port 8080 (needs external routing)
- **External clients:** Users submitting jobs from outside the Kubernetes cluster

---

## Table of Contents

1. [Quick Start: minikube + Ingress](#quick-start-minikube--ingress)
2. [Production: Kubernetes Gateway API](#production-kubernetes-gateway-api)
3. [Alternative: NodePort (Development Only)](#alternative-nodeport-development-only)
4. [Alternative: Port Forwarding (Local Dev)](#alternative-port-forwarding-local-dev)
5. [DNS Setup](#dns-setup)
6. [Verification](#verification)

---

## Quick Start: minikube + Ingress

**Best option for minikube:** Use NGINX Ingress (lightweight, widely supported).

### Step 1: Enable NGINX Ingress in minikube

```bash
minikube addons enable ingress
kubectl get pods -n ingress-nginx
# Wait for: ingress-nginx-controller-*  RUNNING
```

### Step 2: Create Ingress Resource

Save this as `k8s/06-ingress-minikube.yaml`:

```yaml
---
# For minikube: Route http://api.mapreduce.local:80 → ui:8080
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: mapreduce-ingress
  namespace: mapreduce
  annotations:
    nginx.ingress.kubernetes.io/proxy-connect-timeout: "30"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "45"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "30"
spec:
  rules:
    - host: api.mapreduce.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: ui
                port:
                  number: 8080
    - host: storage.mapreduce.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: minio
                port:
                  number: 9000
    - host: auth.mapreduce.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: keycloak
                port:
                  number: 8080
```

### Step 3: Apply Ingress

```bash
kubectl apply -f k8s/06-ingress-minikube.yaml
kubectl get ingress -n mapreduce
# Should show: mapreduce-ingress with ADDRESS: <minikube IP>
```

### Step 4: Get minikube IP

```bash
MINIKUBE_IP=$(minikube ip)
echo $MINIKUBE_IP
# Example: 192.168.99.100
```

### Step 5: Add /etc/hosts Entry (Linux/macOS)

On your **host machine** (the VM running minikube), add:

```bash
sudo nano /etc/hosts
# Add these lines:
$MINIKUBE_IP  api.mapreduce.local
$MINIKUBE_IP  storage.mapreduce.local
$MINIKUBE_IP  auth.mapreduce.local
```

**Windows:** Edit `C:\Windows\System32\drivers\etc\hosts` with admin privileges and add the same lines.

### Step 6: Test from VM Host

```bash
# Should return 200 with API response
curl -v http://api.mapreduce.local:80/api/v1/jobs

# Should return JSON list of jobs
curl -H "Authorization: Bearer <JWT_TOKEN>" \
  http://api.mapreduce.local:80/api/v1/jobs
```

### Step 7: Expose VM to External Network (Optional)

If you want **clients outside the VM** to access the API:

#### Option A: Configure VM Network Bridge (Recommended)

In VirtualBox settings:
1. VM → Settings → Network → Adapter 1
2. Change from "NAT" to **"Bridged Adapter"**
3. Select your host network interface
4. Restart minikube: `minikube stop && minikube start`

Then external clients can access:
```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://api.mapreduce.local:80/api/v1/jobs
```

#### Option B: Port Forward from VM Host to External Port

```bash
# On the VM, forward port 80 to a higher port:
sudo iptables -t nat -A PREROUTING -p tcp --dport 8080 -j REDIRECT --to-port 80

# External clients now connect to:
curl -H "Authorization: Bearer $TOKEN" \
  http://<VM_IP>:8080/api/v1/jobs
```

---

## Production: Kubernetes Gateway API

**Recommended for production clusters (GKE, EKS, k3s, kind).**

Supports advanced features: TLS termination, traffic splitting, advanced routing.

### Step 1: Install Gateway API CRDs

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.0.0/standard-install.yaml
```

### Step 2: Install Gateway Controller

Choose one:

#### Istio Gateway
```bash
curl -L https://istio.io/downloadIstio | sh -
istioctl install --set profile=production -y
```

#### Envoy Gateway (Simpler)
```bash
helm repo add envoy-gateway https://envoyproxy.io/charts
helm install envoy-gateway envoy-gateway/envoy-gateway \
  --namespace envoy-gateway-system --create-namespace
```

#### Contour
```bash
kubectl apply -f https://projectcontour.io/quickstart/contour.yaml
```

### Step 3: Apply Gateway + HTTPRoutes

Use the existing `k8s/60-gateway.yaml`:

```bash
# First, create TLS Secret (use your cert or self-signed for testing)
kubectl create secret tls mapreduce-tls \
  --cert=path/to/cert.crt \
  --key=path/to/key.key \
  -n mapreduce

# Apply Gateway (uses gatewayClassName: mapreduce — must match controller)
kubectl apply -f k8s/60-gateway.yaml
```

### Step 4: Configure External Load Balancer

Most cloud providers automatically create a LoadBalancer Service. Find the external IP:

```bash
kubectl get gateway -n mapreduce
# Look for EXTERNAL-IP

# Or check the underlying Service:
kubectl get svc -n envoy-gateway-system
# Example: envoy-gateway → 35.192.123.45 (GCP)
```

### Step 5: Update DNS Records

Point your domain `api.mapreduce.local` to the external IP:

```bash
# In your DNS provider (AWS Route53, Cloudflare, etc.):
api.mapreduce.local  A  35.192.123.45
storage.mapreduce.local  A  35.192.123.45
auth.mapreduce.local  A  35.192.123.45
```

---

## Alternative: NodePort (Development Only)

**Simplest option, but exposes high port (e.g., 30080).**

### Step 1: Create NodePort Service

Save as `k8s/06-nodeport-dev.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ui-nodeport
  namespace: mapreduce
spec:
  type: NodePort
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: 30080  # Fixed port (must be 30000-32767)
  selector:
    app.kubernetes.io/name: ui
---
apiVersion: v1
kind: Service
metadata:
  name: minio-nodeport
  namespace: mapreduce
spec:
  type: NodePort
  ports:
    - port: 9000
      targetPort: 9000
      nodePort: 30090
  selector:
    app.kubernetes.io/name: minio
---
apiVersion: v1
kind: Service
metadata:
  name: keycloak-nodeport
  namespace: mapreduce
spec:
  type: NodePort
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: 30810
  selector:
    app.kubernetes.io/name: keycloak
```

### Step 2: Apply

```bash
kubectl apply -f k8s/06-nodeport-dev.yaml
```

### Step 3: Access via VM IP + Port

```bash
MINIKUBE_IP=$(minikube ip)

# Users connect directly to:
curl -H "Authorization: Bearer $TOKEN" \
  http://$MINIKUBE_IP:30080/api/v1/jobs
```

---

## Alternative: Port Forwarding (Local Dev)

**Easiest for local testing; access via localhost only.**

### Step 1: Port Forward UI Service

```bash
kubectl port-forward -n mapreduce svc/ui 8080:8080
```

### Step 2: Access via localhost

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/v1/jobs
```

---

## DNS Setup

### Option 1: Local /etc/hosts (For testing)

```bash
# On your host machine
echo "$MINIKUBE_IP api.mapreduce.local" >> /etc/hosts
echo "$MINIKUBE_IP storage.mapreduce.local" >> /etc/hosts
echo "$MINIKUBE_IP auth.mapreduce.local" >> /etc/hosts
```

### Option 2: DNS Service (CoreDNS)

If you control a DNS server:

```bash
api.mapreduce.local     A     <EXTERNAL_IP>
storage.mapreduce.local A     <EXTERNAL_IP>
auth.mapreduce.local    A     <EXTERNAL_IP>
```

### Option 3: Local DNS Resolver (dnsmasq on Linux)

```bash
# Install
sudo apt-get install dnsmasq -y

# Configure
echo "address=/mapreduce.local/192.168.99.100" | sudo tee -a /etc/dnsmasq.conf

# Restart
sudo systemctl restart dnsmasq
```

---

## Verification

### Step 1: Verify Services are Running

```bash
kubectl get svc -n mapreduce
# Should show: ui, minio, keycloak all with healthy endpoints
```

### Step 2: Verify Ingress/Gateway

```bash
# For minikube (Ingress):
kubectl get ingress -n mapreduce
kubectl describe ingress mapreduce-ingress -n mapreduce

# For production (Gateway):
kubectl get gateway -n mapreduce
kubectl describe gateway mapreduce -n mapreduce
```

### Step 3: Verify DNS Resolution

```bash
nslookup api.mapreduce.local
# Should resolve to Ingress/Gateway IP
```

### Step 4: Test API Health

```bash
curl -v http://api.mapreduce.local/healthz
# Expected: 200 OK

curl -v http://api.mapreduce.local/readyz
# Expected: 200 OK
```

### Step 5: Test Job Submission (Requires Auth)

```bash
# Get a JWT token first (via Keycloak)
TOKEN=$(curl -s -X POST http://auth.mapreduce.local:8080/realms/mapreduce/protocol/openid-connect/token \
  -d "client_id=mapreduce-api&client_secret=<secret>&grant_type=password&username=user&password=pass" \
  | jq -r '.access_token')

# Submit a job
curl -X POST http://api.mapreduce.local/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "sample.jsonl",
    "mapper": {"language": "python3", "interface": "process(k,v)"},
    "reducer": {"language": "python3", "interface": "process(k,v)"},
    "num_reducers": 3
  }'

# Expected: 202 Accepted + job_id
```

---

## Summary: Which Option to Use?

| Scenario | Option | Difficulty | External Access |
|----------|--------|-----------|-----------------|
| **minikube, local testing** | Ingress | ⭐ Easy | VM only |
| **minikube, external clients** | Ingress + Bridged Network | ⭐⭐ Medium | Yes (needs VM bridge) |
| **Development, quick test** | Port Forward | ⭐ Easy | localhost only |
| **Development, wider access** | NodePort | ⭐⭐ Easy | Via VM IP + port |
| **Production (GKE/EKS/AKS)** | Gateway API + Cloud LB | ⭐⭐⭐ Advanced | Yes (managed by cloud) |
| **Production (k3s/kind)** | Ingress or Gateway API | ⭐⭐ Medium | Yes (configure LoadBalancer) |

---

## Next Steps

1. **Choose your option** based on deployment environment
2. **Apply the appropriate YAML** (Ingress, NodePort, or Gateway)
3. **Configure DNS** (/etc/hosts or DNS server)
4. **Test connectivity** with curl/Postman
5. **Share the external endpoint** with users for job submission

For production deployments, see [docs/DEPLOYMENT.md](DEPLOYMENT.md) for complete setup procedures.
