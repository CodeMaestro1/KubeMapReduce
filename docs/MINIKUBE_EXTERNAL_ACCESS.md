# KubeMapReduce on minikube: Quick Setup for External Access

This is a step-by-step checklist for exposing your KubeMapReduce API to external clients on minikube.

## 🎯 Your Goal

Users outside the VM (or on the VM) can access:
```
http://api.mapreduce.local/api/v1/jobs
```
to submit MapReduce jobs remotely.

---

## ⚡ Quick Setup (5 minutes)

### 1. Enable NGINX Ingress in minikube

```bash
minikube addons enable ingress
sleep 10
kubectl get pods -n ingress-nginx
# Wait for: ingress-nginx-controller-*  →  RUNNING
```

### 2. Create Ingress Resource

```bash
cat > k8s/06-ingress-minikube.yaml << 'EOF'
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: mapreduce-ingress
  namespace: mapreduce
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
EOF

kubectl apply -f k8s/06-ingress-minikube.yaml
```

### 3. Get minikube IP

```bash
MINIKUBE_IP=$(minikube ip)
echo "minikube IP: $MINIKUBE_IP"
```

### 4. Update /etc/hosts

**Linux / macOS:**
```bash
echo "$MINIKUBE_IP api.mapreduce.local" | sudo tee -a /etc/hosts
echo "$MINIKUBE_IP storage.mapreduce.local" | sudo tee -a /etc/hosts
echo "$MINIKUBE_IP auth.mapreduce.local" | sudo tee -a /etc/hosts
```

**Windows (PowerShell as Admin):**
```powershell
$MINIKUBE_IP = minikube ip
Add-Content -Path "C:\Windows\System32\drivers\etc\hosts" -Value "`n$MINIKUBE_IP api.mapreduce.local"
Add-Content -Path "C:\Windows\System32\drivers\etc\hosts" -Value "`n$MINIKUBE_IP storage.mapreduce.local"
Add-Content -Path "C:\Windows\System32\drivers\etc\hosts" -Value "`n$MINIKUBE_IP auth.mapreduce.local"
```

### 5. Verify Ingress is Ready

```bash
kubectl get ingress -n mapreduce
# Should show: mapreduce-ingress   api.mapreduce.local   <INGRESS_IP>

kubectl describe ingress mapreduce-ingress -n mapreduce
# Look for: "Endpoints" with "ui:8080"
```

### 6. Test Health

```bash
curl -v http://api.mapreduce.local/healthz
# Expected: 200 OK

curl -v http://api.mapreduce.local/readyz
# Expected: 200 OK
```

### 7. Get a JWT Token

```bash
# Log in to Keycloak first
TOKEN=$(curl -s -X POST http://auth.mapreduce.local:8080/realms/mapreduce/protocol/openid-connect/token \
  -d "client_id=mapreduce-api" \
  -d "client_secret=<client_secret>" \
  -d "grant_type=password" \
  -d "username=<user>" \
  -d "password=<password>" \
  | jq -r '.access_token')

echo "Token: $TOKEN"
```

### 8. Submit a Job

```bash
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

## 🌍 Expose to External Clients

If you need clients **outside the VM** to access the API:

### Option A: VirtualBox Network Bridge (Recommended)

1. Stop minikube:
   ```bash
   minikube stop
   ```

2. In VirtualBox UI:
   - VM Settings → Network → Adapter 1
   - Change from "NAT" to **"Bridged Adapter"**
   - Select your host network interface (e.g., `eth0`, `wlan0`, or `Ethernet`)

3. Start minikube:
   ```bash
   minikube start
   ```

4. Get the new bridge IP:
   ```bash
   MINIKUBE_IP=$(minikube ip)
   echo "New bridge IP: $MINIKUBE_IP"
   ```

5. Update /etc/hosts on **external machines** to point to this IP

6. External clients can now access:
   ```bash
   curl -H "Authorization: Bearer $TOKEN" \
     http://api.mapreduce.local/api/v1/jobs
   ```

### Option B: SSH Port Forwarding (Temporary)

From an external machine:

```bash
ssh -L 8080:127.0.0.1:80 user@vm-host

# Then access locally:
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/v1/jobs
```

---

## ✅ Verification Checklist

- [ ] NGINX Ingress is running: `kubectl get pods -n ingress-nginx`
- [ ] Ingress resource exists: `kubectl get ingress -n mapreduce`
- [ ] Hostnames resolve: `nslookup api.mapreduce.local`
- [ ] Endpoints are active: `kubectl describe ingress mapreduce-ingress -n mapreduce`
- [ ] Health check passes: `curl http://api.mapreduce.local/healthz`
- [ ] API responds: `curl http://api.mapreduce.local/api/v1/jobs` (may need auth)
- [ ] External clients can connect (if using bridge networking)

---

## 🐛 Troubleshooting

### Ingress not getting an IP

```bash
# Check if NGINX controller is running
kubectl get pods -n ingress-nginx

# If not running, restart:
minikube addons disable ingress
minikube addons enable ingress
sleep 20
```

### Hostname doesn't resolve

```bash
# Test DNS resolution
nslookup api.mapreduce.local

# If not found, check /etc/hosts contains the correct IP
cat /etc/hosts | grep mapreduce.local
```

### Connection refused / timeout

```bash
# Check if UI service is running
kubectl get svc -n mapreduce
kubectl get pods -n mapreduce

# Port-forward to test directly
kubectl port-forward -n mapreduce svc/ui 8080:8080

# Then test: curl localhost:8080/healthz
```

### External clients can't connect

- Ensure VirtualBox is in **Bridged mode** (not NAT)
- Update /etc/hosts on **external machine** with minikube's bridge IP
- Check firewall rules on VM and external network

---

## 📚 Full Documentation

For more detailed options and troubleshooting, see:
- **[External Access Guide](EXTERNAL_ACCESS.md)** — All deployment options with pros/cons
- **[Deployment Guide](DEPLOYMENT.md)** — Production deployment procedures

---

## 🚀 Next Steps

1. Run the quick setup (steps 1-6 above)
2. Test health endpoints (step 6)
3. Get a token from Keycloak (step 7)
4. Submit a job (step 8)
5. If external clients needed, configure bridge networking (Option A above)
6. Share `http://api.mapreduce.local` endpoint with users

**Done!** Your API is now externally accessible.
