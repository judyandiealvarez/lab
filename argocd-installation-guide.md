# ArgoCD Installation Guide for K3s Cluster

This guide documents the exact steps to install ArgoCD in our K3s cluster, including troubleshooting the etcd consistency issue we encountered.

## Prerequisites

- K3s cluster running (tested on v1.33.6+k3s1)
- `kubectl` configured to access the cluster
- `helm` installed on the control plane node
- Access to control plane node (10.11.4.11)

## Installation Steps

### 1. Add ArgoCD Helm Repository

```bash
ssh root@10.11.4.11 "helm repo add argo https://argoproj.github.io/argo-helm"
ssh root@10.11.4.11 "helm repo update"
```

### 2. Create ArgoCD Namespace

```bash
ssh root@10.11.4.11 "kubectl create namespace argocd"
```

### 3. Install ArgoCD via Helm

```bash
ssh root@10.11.4.11 "helm install argocd argo/argo-cd --namespace argocd"
```

**Note:** This installs ArgoCD with default settings. The installation creates:
- Application Controller (StatefulSet)
- ApplicationSet Controller (Deployment)
- Dex Server (Deployment)
- Notifications Controller (Deployment)
- Redis (Deployment)
- Repo Server (Deployment)
- Server (Deployment)

### 4. Wait for Pods to Start

```bash
ssh root@10.11.4.11 "kubectl get pods -n argocd -w"
```

**Expected:** All pods should eventually reach `Running` status. However, you may encounter issues (see Troubleshooting below).

## Critical Issue: etcd Consistency Problem

### Symptoms

Pods fail with `CreateContainerConfigError: secret "argocd-redis" not found` even though:
- The secret exists: `kubectl get secret argocd-redis -n argocd`
- Pod configuration is correct
- Worker nodes are Ready

### Root Cause

K3s control plane etcd experiences revision mismatches:
```
error while range on /registry/.../: rpc error: code = OutOfRange desc = etcdserver: mvcc: required revision is a future revision
```

This prevents the API server from reliably serving secrets to worker nodes.

### Solution: Restart K3s Control Plane

```bash
ssh root@10.11.4.11 "systemctl restart k3s"
```

Wait for K3s to fully restart (30-60 seconds), then verify:

```bash
ssh root@10.11.4.11 "kubectl get nodes"
ssh root@10.11.4.11 "kubectl get pods -n argocd"
```

**After restart:**
- etcd errors should disappear from logs
- Pods should be able to access secrets
- `argocd-repo-server` and `argocd-server` should transition from `CreateContainerConfigError` to `Running`

### Verify etcd Health

```bash
ssh root@10.11.4.11 "journalctl -u k3s -n 50 --no-pager | grep -i 'etcd\|error'"
```

Should show no etcd errors after restart.

## Exposing ArgoCD Service

### Option 1: NodePort (Recommended for Quick Access)

```bash
ssh root@10.11.4.11 "kubectl patch svc argocd-server -n argocd -p '{\"spec\":{\"type\":\"NodePort\"}}'"
```

Get the NodePort:
```bash
ssh root@10.11.4.11 "kubectl get svc -n argocd argocd-server"
```

Access via: `https://<node-ip>:<nodeport>` (e.g., `https://10.11.4.11:31628`)

### Option 2: Port Forward (For Testing)

```bash
kubectl port-forward service/argocd-server -n argocd 8080:443
```

Access via: `https://localhost:8080`

### Option 3: Ingress (For Production)

Configure ingress with SSL passthrough or termination (see ArgoCD documentation).

## Getting Initial Admin Credentials

```bash
ssh root@10.11.4.11 "kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d && echo ''"
```

**Default credentials:**
- Username: `admin`
- Password: (from command above)

**Security Note:** After first login, delete the initial admin secret and change the password:
```bash
ssh root@10.11.4.11 "kubectl delete secret argocd-initial-admin-secret -n argocd"
```

## Verification Checklist

- [ ] All pods in `Running` state: `kubectl get pods -n argocd`
- [ ] No etcd errors in logs: `journalctl -u k3s | grep etcd`
- [ ] Service exposed: `kubectl get svc -n argocd argocd-server`
- [ ] Can access UI via NodePort or port-forward
- [ ] Can login with admin credentials
- [ ] Initial admin secret deleted after password change

## Troubleshooting

### Pods Stuck in CreateContainerConfigError

**Symptom:** Pods can't find secrets even though they exist.

**Check:**
```bash
kubectl get secret argocd-redis -n argocd
kubectl describe pod <pod-name> -n argocd | grep -A 5 Events
```

**Solution:** Restart K3s control plane (see above).

### Pods Stuck in Pending

**Symptom:** Pods show `Pending` status.

**Check:**
```bash
kubectl describe pod <pod-name> -n argocd | grep -A 10 Events
```

**Common causes:**
- NodeSelector/tolerations misconfiguration (if you tried to force pods to control plane)
- Resource constraints
- Node not ready

**Solution:** Check node status and remove any custom nodeSelectors if added.

### Dex Server CrashLoopBackOff

**Symptom:** `argocd-dex-server` keeps restarting.

**Check logs:**
```bash
kubectl logs -n argocd -l app.kubernetes.io/name=argocd-dex-server --tail=50
```

**Common cause:** Missing `argocd-secret` or configuration issues.

**Solution:** Usually resolves after etcd restart and secret access is restored.

### Cannot Access UI

**Check:**
1. Service type: `kubectl get svc -n argocd argocd-server`
2. Pod status: `kubectl get pods -n argocd | grep server`
3. Firewall rules (if accessing externally)

**Solution:** Ensure service is NodePort or use port-forward. Accept self-signed certificate warning.

## Post-Installation

### 1. Change Admin Password

Login to UI → User Settings → Update Password

### 2. Configure RBAC

Set up proper RBAC policies for your team (see ArgoCD documentation).

### 3. Connect Repositories

Add Git repositories via UI or CLI:
```bash
argocd repo add <repo-url> --type git
```

### 4. Create Applications

Create ArgoCD Applications to manage your deployments.

## Architecture Notes

- **Control Plane:** 10.11.4.11 (k3s-control)
- **Worker Nodes:** 10.11.4.12, 10.11.4.13, 10.11.4.14
- **Namespace:** `argocd`
- **Service Type:** NodePort (after patching)
- **HTTPS Port:** 31628 (example, may vary)

## Known Issues

1. **etcd Consistency:** K3s etcd can experience revision mismatches. Restart K3s control plane resolves it.
2. **Secret Propagation:** Worker nodes access secrets via API server, not direct etcd access. etcd issues affect secret access.
3. **Self-Signed Certificates:** ArgoCD uses self-signed certs by default. Accept browser warning or configure proper certificates.

## References

- [ArgoCD Documentation](https://argo-cd.readthedocs.io/)
- [ArgoCD Helm Chart](https://github.com/argoproj/argo-helm)
- [K3s Troubleshooting](https://docs.k3s.io/troubleshooting)

