# External Access Architecture for KubeMapReduce

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         EXTERNAL CLIENTS                         │
│                 (Users submitting jobs remotely)                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           │ HTTP/HTTPS
                           │ api.mapreduce.local:80/443
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🔥 INGRESS CONTROLLER                         │
│           (NGINX Ingress on minikube, or Gateway API)            │
│               Terminates HTTP, routes to Services                │
└──────────────────┬─────────────────────────────────────────────┘
                   │
        ┌──────────┼──────────┬──────────┐
        │          │          │          │
        ▼          ▼          ▼          ▼
    ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
    │ api.   │ │storage.│ │auth.   │ │(other) │
    │mapre.. │ │mapre.. │ │mapre.. │ │routes  │
    │.local  │ │.local  │ │.local  │ └────────┘
    └───┬────┘ └───┬────┘ └───┬────┘
        │          │          │
        ▼          ▼          ▼
    ┌─────────────────────────────────────┐
    │   KUBERNETES CLUSTER (minikube)     │
    │                                     │
    │  ┌─────────┐  ┌─────────┐         │
    │  │ ui:8080 │  │ minio:  │         │
    │  │ Service │  │ 9000    │         │
    │  │ (Pod)   │  │ Service │  ...    │
    │  └────┬────┘  └────┬────┘         │
    │       │            │               │
    │       │  Internal  │ minio-creds  │
    │       │  Services  │ postgres-    │
    │       │            │ creds        │
    │       ▼            ▼               │
    │    Manager / DB / Other Services   │
    │                                     │
    └─────────────────────────────────────┘
        ▲
        │
   ┌────┴─────────────────────────┐
   │   VM Host (minikube)          │
   │   • /etc/hosts entry          │
   │   • NGINX Ingress enabled     │
   └───────────────────────────────┘
```

---

## Three Deployment Scenarios

### 🟢 Scenario 1: Local Testing (minikube on Your Machine)

```
Your Computer
    │
    ├─ /etc/hosts: 192.168.99.100 api.mapreduce.local
    │
    └─► VirtualBox/KVM
        └─► minikube cluster
            └─► NGINX Ingress
                └─► ui:8080
                    └─► API responds
```

**Access:** `curl http://api.mapreduce.local/api/v1/jobs`

---

### 🟡 Scenario 2: minikube on VM, Internal Network Only

```
Your Network (192.168.x.x)
    │
    ├─ Workstation 1
    │   ├─ /etc/hosts: 192.168.99.100 api.mapreduce.local
    │   └─► curl http://api.mapreduce.local/api/v1/jobs
    │
    ├─ Workstation 2
    │   ├─ /etc/hosts: 192.168.99.100 api.mapreduce.local
    │   └─► curl http://api.mapreduce.local/api/v1/jobs
    │
    └─ VM (minikube, bridged network)
        └─► NGINX Ingress @ 192.168.99.100:80
            └─► ui:8080
```

**Setup:** VirtualBox Bridged Adapter + /etc/hosts on each workstation

---

### 🔴 Scenario 3: Production (GKE / EKS / k3s)

```
External Internet
    │
    └─► Cloud Provider (GKE / EKS / AKS)
        │
        ├─► DNS: api.mapreduce.local → 35.192.123.45
        │   (Managed by Route53 / Cloud DNS)
        │
        └─► Load Balancer (managed by cloud)
            └─► Gateway API Ingress
                └─► HTTPRoutes
                    └─► ui:8080
                        └─► API responds
```

**Access:** `curl https://api.mapreduce.local/api/v1/jobs` (TLS via cloud LB)

---

## Port/Service Mapping

| Hostname | Internal Port | Purpose | Example |
|----------|---------------|---------|---------|
| `api.mapreduce.local` | ui:8080 | REST API for job submission | `/api/v1/jobs` |
| `storage.mapreduce.local` | minio:9000 | S3-compatible storage (presigned URLs) | `/bucket/object` |
| `auth.mapreduce.local` | keycloak:8080 | OIDC issuer (token endpoint) | `/realms/mapreduce/protocol/...` |

---

## What We DID Have vs. What Was Missing

### ✅ What Already Existed
- **k8s/60-gateway.yaml** — Gateway API definition (Kubernetes native routing)
- **Gateway HTTPRoutes** — Defined for api/storage/auth hostnames
- **k8s/40-ui.yaml** — UI Service definition
- **Documentation** — DEPLOYMENT.md covers Kubernetes setup

### ❌ What Was Missing for minikube
- **Explicit minikube Ingress setup** — No instructions for enabling NGINX Ingress
- **/etc/hosts configuration** — Users didn't know to add hostname entries
- **External access options** — No guide for bridged networking or NodePort
- **Quick reference** — No step-by-step for minikube specifically
- **Testing examples** — No curl examples showing how to access the API

### ✅ What We Just Added
- **docs/EXTERNAL_ACCESS.md** — 11K comprehensive guide covering all 5 options
- **docs/MINIKUBE_EXTERNAL_ACCESS.md** — 7K quick setup for minikube specifically
- **README.md update** — New link to external access guide
- **k8s/06-ingress-minikube.yaml** — Ready-to-apply NGINX Ingress manifest
- **Bridged network guide** — Instructions for exposing to external clients

---

## Why Gateway API Alone Wasn't Enough

The **Gateway API** (k8s/60-gateway.yaml) requires a **Gateway Controller** to function:

| Cluster Type | Controller | Status |
|--------------|-----------|--------|
| minikube (default) | ❌ None | Must manually enable NGINX or install Envoy Gateway |
| kind | ❌ None | Must install Contour, Istio, or Envoy Gateway |
| k3s | ⚠️ Traefik (basic) | Works but not full Kubernetes Gateway API v1 |
| GKE | ✅ GKE Ingress (managed) | Works automatically |
| EKS | ⚠️ Optional AWS ALB/NLB | Must be configured |

**Solution:** For immediate access on minikube, use **NGINX Ingress** (lightweight, built-in addon).

---

## Quick Decision Tree

```
Do you want to deploy?
│
├─► On minikube (local VM)
│   ├─► Just me testing locally?
│   │   └─► Use Port Forwarding (easiest, no setup)
│   │
│   ├─► Multiple users on same network?
│   │   └─► Use NGINX Ingress + /etc/hosts (Recommended)
│   │
│   └─► External clients (outside VM)?
│       └─► Use NGINX Ingress + Bridged Networking
│
├─► On production K8s (GKE/EKS)
│   └─► Use Gateway API + Cloud LoadBalancer (managed by cloud)
│
└─► Quick demo / CI/CD pipeline?
    └─► Use NodePort (simple, no DNS needed)
```

---

## Files You Now Have

```
docs/
├── EXTERNAL_ACCESS.md           ← Full guide: all options + details
├── MINIKUBE_EXTERNAL_ACCESS.md  ← Quick start: 5-minute setup for minikube
├── DEPLOYMENT.md                ← Full deployment guide (already existed)
├── LINKERD_SETUP.md             ← Service mesh (already existed)
└── TIMEOUT_CONFIGURATION.md     ← Timeout tuning (already existed)

k8s/
├── 60-gateway.yaml              ← Gateway API (production, already existed)
├── 40-ui.yaml                   ← UI Service (already existed)
└── 06-ingress-minikube.yaml     ← NGINX Ingress for minikube (ready to apply)

README.md                         ← Updated with EXTERNAL_ACCESS.md link
```

---

## Next Steps

1. **Choose your deployment option:**
   - Local only? → Port forwarding
   - minikube + local network? → NGINX Ingress (recommended)
   - minikube + external clients? → NGINX Ingress + bridged networking
   - Production? → Gateway API on managed K8s

2. **Follow the appropriate guide:**
   - Quick setup → docs/MINIKUBE_EXTERNAL_ACCESS.md
   - All options → docs/EXTERNAL_ACCESS.md
   - Production → docs/DEPLOYMENT.md

3. **Test connectivity:**
   ```bash
   curl http://api.mapreduce.local/healthz
   curl -H "Authorization: Bearer $TOKEN" http://api.mapreduce.local/api/v1/jobs
   ```

4. **Share the endpoint with users:**
   - Give them `http://api.mapreduce.local` (or your domain)
   - Provide Keycloak URL for authentication
   - They can now submit jobs remotely!
