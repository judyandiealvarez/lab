"""Kubernetes (k3s) orchestration utilities."""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional, Sequence
from libs.config import LabConfig
from libs.logger import get_logger
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)

@dataclass
class KubernetesDeployContext:
    """Shared data computed once for Kubernetes deployment."""
    cfg: LabConfig
    control: Sequence[object]
    workers: Sequence[object]
    _token: Optional[str] = None
    
    @property
    def proxmox_host(self):
        """Return cached proxmox host string."""
        return self.cfg.proxmox_host
    
    @property
    def all_nodes(self):
        """Return list combining control and worker nodes."""
        return list(self.control) + list(self.workers)

def deploy_kubernetes(cfg: LabConfig):
    """Deploy Kubernetes (k3s) cluster - containers should already exist from deploy process"""
    context = _build_kubernetes_context(cfg)
    if not context:
        return False
    # Containers should already exist from the deploy process
    # We only need to perform k3s-specific orchestration (get token, join workers, install Rancher)
    # k3s should already be installed via actions
    control_config = context.control[0]
    if not _get_k3s_token(context, control_config):
        return False
    if not _join_workers_to_cluster(context, control_config):
        return False
    if not _taint_control_plane(context, control_config):
        return False
    if not _install_rancher(context, control_config):
        return False
    logger.info("Kubernetes (k3s) cluster deployed")
    return True

def _build_kubernetes_context(cfg: LabConfig) -> Optional[KubernetesDeployContext]:
    """Collect and validate configuration needed for Kubernetes deployment."""
    if not cfg.kubernetes or not cfg.kubernetes.control or not cfg.kubernetes.workers:
        logger.error("Kubernetes configuration not found or incomplete")
        return None
    # Find containers by ID from kubernetes config
    control_ids = set(cfg.kubernetes.control)
    worker_ids = set(cfg.kubernetes.workers)
    control = [c for c in cfg.containers if c.id in control_ids]
    workers = [c for c in cfg.containers if c.id in worker_ids]
    if not control:
        logger.error("Kubernetes control node not found in configuration")
        return None
    if not workers:
        logger.warning("No Kubernetes worker nodes found in configuration")
    return KubernetesDeployContext(cfg, control, workers)

def _deploy_k3s_nodes(context: KubernetesDeployContext) -> bool:
    """Create and configure all k3s containers using PCTService - NOT USED (containers already exist)"""
    # This function is no longer called - containers are created during deploy process
    # and kubernetes setup only orchestrates existing containers
    return True

def _get_k3s_token(context: KubernetesDeployContext, control_config) -> bool:
    """Get k3s server token from control node."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    control_id = control_config.id
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        logger.info("Getting k3s server token...")
        # Wait for k3s to be ready
        max_wait = 60
        wait_time = 0
        while wait_time < max_wait:
            check_cmd = "systemctl is-active k3s || echo inactive"
            check_output, _ = pct_service.execute(str(control_id), check_cmd)
            if check_output and "active" in check_output:
                break
            time.sleep(2)
            wait_time += 2
        if wait_time >= max_wait:
            logger.error("k3s service not ready on control node")
            return False
        # Get the token
        token_cmd = "cat /var/lib/rancher/k3s/server/node-token"
        token_output, _ = pct_service.execute(str(control_id), token_cmd)
        if not token_output or not token_output.strip():
            logger.error("Failed to get k3s token")
            return False
        context._token = token_output.strip()
        logger.info("k3s token retrieved successfully")
        return True
    finally:
        lxc_service.disconnect()

def _join_workers_to_cluster(context: KubernetesDeployContext, control_config) -> bool:
    """Join worker nodes to k3s cluster."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    control_id = control_config.id
    control_ip = control_config.ip_address
    token = context._token
    if not token:
        logger.error("k3s token not available")
        return False
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        for worker_config in context.workers:
            worker_id = worker_config.id
            logger.info("Joining worker %s to k3s cluster...", worker_id)
            # Uninstall existing k3s agent if present
            uninstall_cmd = "/usr/local/bin/k3s-agent-uninstall.sh 2>&1 || true"
            pct_service.execute(str(worker_id), uninstall_cmd)
            # Install k3s agent with proper token and server URL
            # Run installation directly (not in background) so we can see output and errors
            logger.info("Installing k3s agent on worker %s...", worker_id)
            join_cmd = f"curl -sfL https://get.k3s.io | K3S_URL=https://{control_ip}:6443 K3S_TOKEN={token} sh -"
            install_output, install_exit = pct_service.execute(str(worker_id), join_cmd, timeout=600)
            if install_exit is None:
                logger.error("k3s agent installation timed out on worker %s", worker_id)
                return False
            if install_exit != 0:
                logger.error("k3s agent installation failed on worker %s", worker_id)
                if install_output:
                    logger.error("Installation output: %s", install_output[-1000:])
                return False
            logger.info("k3s agent installation completed on worker %s", worker_id)
            if install_output:
                logger.info("Installation output: %s", install_output[-500:])
            # Wait a moment for service to be created
            time.sleep(2)
            # Check if k3s-agent service exists
            service_exists_cmd = "systemctl list-unit-files | grep -q k3s-agent.service && echo exists || echo not_exists"
            service_check, _ = pct_service.execute(str(worker_id), service_exists_cmd)
            if service_check and "not_exists" in service_check:
                logger.error("k3s-agent service was not created after installation")
                return False
            # Wait for agent service to be ready AND verify node appears in cluster
            max_wait_service = 120
            wait_time_service = 0
            worker_name = worker_config.hostname or f"k3s-worker-{worker_id}"
            while wait_time_service < max_wait_service:
                # Check if service is active (must be exactly "active", not "activating" or "inactive")
                check_cmd = "systemctl is-active k3s-agent 2>&1"
                check_output, check_exit = pct_service.execute(str(worker_id), check_cmd)
                if check_exit == 0 and check_output and check_output.strip() == "active":
                    # Service is active, now verify node appears in cluster
                    # Check from control node that this worker appears in kubectl get nodes
                    verify_node_cmd = f"kubectl get nodes | grep -E '{worker_name}|{worker_config.ip_address}' || echo not_found"
                    verify_output, verify_exit = pct_service.execute(str(control_id), verify_node_cmd)
                    if verify_exit == 0 and verify_output and "not_found" not in verify_output and "Ready" in verify_output:
                        logger.info("Worker %s (%s) joined cluster successfully and is Ready", worker_id, worker_name)
                        break  # Success, continue to next worker
                    else:
                        # Service is active but node not ready yet - check if service is still active
                        # Re-check service status to ensure it didn't fail
                        recheck_cmd = "systemctl is-active k3s-agent 2>&1"
                        recheck_output, recheck_exit = pct_service.execute(str(worker_id), recheck_cmd)
                        if recheck_exit != 0 or recheck_output.strip() != "active":
                            logger.warning("Worker %s service became inactive, waiting for it to become active again...", worker_id)
                        else:
                            logger.info("Worker %s service is active but not yet Ready in cluster, waiting...", worker_id)
                else:
                    # Service is not active yet
                    if wait_time_service % 10 == 0:  # Log every 10 seconds
                        logger.info("Worker %s service is not active yet (status: %s), waiting...", worker_id, check_output.strip() if check_output else "unknown")
                time.sleep(2)
                wait_time_service += 2
            else:
                # Loop exhausted - service not ready or node not in cluster
                logger.error("k3s-agent service not ready or node not in cluster on worker %s after %d seconds", worker_id, max_wait_service)
                # Check final status
                final_check, _ = pct_service.execute(str(worker_id), check_cmd)
                if final_check and "active" in final_check:
                    logger.error("Service is active but node did not appear in cluster")
                else:
                    logger.error("Service is not active")
                return False
        return True
    finally:
        lxc_service.disconnect()

def _taint_control_plane(context: KubernetesDeployContext, control_config) -> bool:
    """Taint the control plane node to prevent regular pods from scheduling on it."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    control_id = control_config.id
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        logger.info("Tainting control plane node to prevent regular pods from scheduling...")
        # Wait for kubectl to be available
        max_wait = 60
        wait_time = 0
        while wait_time < max_wait:
            check_cmd = "kubectl get nodes 2>&1"
            check_output, check_exit = pct_service.execute(str(control_id), check_cmd, timeout=30)
            if check_exit == 0 and check_output and "Ready" in check_output:
                break
            time.sleep(2)
            wait_time += 2
        if wait_time >= max_wait:
            logger.warning("kubectl not ready, skipping control plane taint")
            return True  # Don't fail deployment if we can't taint
        
        # Apply taint to control plane node
        # This prevents regular pods from being scheduled on the control plane
        # System pods (with tolerations) will still run
        taint_cmd = "kubectl taint nodes k3s-control node-role.kubernetes.io/control-plane:NoSchedule --overwrite 2>&1"
        taint_output, taint_exit = pct_service.execute(str(control_id), taint_cmd, timeout=30)
        if taint_exit == 0:
            logger.info("Control plane node tainted successfully - regular pods will not schedule on it")
            return True
        else:
            # Check if taint already exists
            if taint_output and ("already has" in taint_output or "modified" in taint_output):
                logger.info("Control plane node already tainted")
                return True
            logger.warning("Failed to taint control plane node: %s", taint_output[-200:] if taint_output else "No output")
            return True  # Don't fail deployment if taint fails
    finally:
        lxc_service.disconnect()

def _install_rancher(context: KubernetesDeployContext, control_config) -> bool:
    """Install Rancher on control node."""
    if not context.cfg.services.rancher:
        logger.info("Rancher not configured, skipping installation")
        return True
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    control_id = control_config.id
    rancher_image = cfg.services.rancher.image or "rancher/rancher:latest"
    rancher_port = cfg.services.rancher.port or 8443
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        logger.info("Installing Rancher...")
        # Install kubectl if not present
        kubectl_check_cmd = "command -v kubectl >/dev/null 2>&1 && echo installed || echo not_installed"
        kubectl_check, _ = pct_service.execute(str(control_id), kubectl_check_cmd)
        if kubectl_check and "not_installed" in kubectl_check:
            logger.info("Installing kubectl...")
            install_kubectl_cmd = "curl -LO https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && export PATH=/usr/local/bin:$PATH"
            pct_service.execute(str(control_id), install_kubectl_cmd, timeout=120)
            # Verify kubectl is installed
            verify_cmd = "test -f /usr/local/bin/kubectl && echo installed || echo not_installed"
            verify_output, _ = pct_service.execute(str(control_id), verify_cmd)
            if verify_output and "not_installed" in verify_output:
                logger.error("kubectl installation failed")
                return False
        # Verify k3s service is running
        logger.info("Verifying k3s service is running...")
        max_wait_k3s = 120
        wait_time_k3s = 0
        while wait_time_k3s < max_wait_k3s:
            k3s_check_cmd = "systemctl is-active k3s 2>&1 || echo inactive"
            k3s_check, _ = pct_service.execute(str(control_id), k3s_check_cmd)
            if k3s_check and "active" in k3s_check:
                logger.info("k3s service is running")
                break
            logger.info("Waiting for k3s service to be active (waited %d/%d seconds)...", wait_time_k3s, max_wait_k3s)
            time.sleep(5)
            wait_time_k3s += 5
        else:
            logger.error("k3s service not active after %d seconds", max_wait_k3s)
            return False
        # Update k3s kubeconfig to use actual IP instead of 127.0.0.1 or 0.0.0.0 (standard k3s config location)
        logger.info("Updating k3s kubeconfig with control node IP...")
        control_ip = control_config.ip_address
        # Fix kubeconfig server IP
        kubeconfig_cmd = f"sudo sed -i 's|server: https://127.0.0.1:6443|server: https://{control_ip}:6443|g; s|server: https://0.0.0.0:6443|server: https://{control_ip}:6443|g' /etc/rancher/k3s/k3s.yaml"
        pct_service.execute(str(control_id), kubeconfig_cmd)
        # Copy kubeconfig to standard location for root user
        # Use /root/.kube/config explicitly (not ~/.kube/config) to avoid shell expansion issues
        # Note: No sudo needed in command since execute() is called with sudo
        setup_kubeconfig_cmd = "mkdir -p /root/.kube && cp /etc/rancher/k3s/k3s.yaml /root/.kube/config && chown root:root /root/.kube/config && chmod 600 /root/.kube/config"
        pct_service.execute(str(control_id), setup_kubeconfig_cmd)
        # Verify kubectl works without specifying KUBECONFIG (should use /root/.kube/config automatically)
        logger.info("Verifying kubectl works without KUBECONFIG specified...")
        verify_kubectl_cmd = "kubectl get nodes"
        verify_kubectl_output, verify_kubectl_exit = pct_service.execute(str(control_id), verify_kubectl_cmd, timeout=30)
        if verify_kubectl_exit != 0 or not verify_kubectl_output or "Ready" not in verify_kubectl_output:
            logger.error("kubectl does not work without KUBECONFIG specified")
            if verify_kubectl_output:
                logger.error("kubectl output: %s", verify_kubectl_output[-500:])
            return False
        logger.info("kubectl works correctly without KUBECONFIG specified")
        # Verify Kubernetes API is reachable before proceeding
        logger.info("Verifying Kubernetes API is reachable...")
        verify_api_cmd = "kubectl cluster-info"
        max_verify_attempts = 20
        for attempt in range(max_verify_attempts):
            verify_output, verify_exit = pct_service.execute(str(control_id), verify_api_cmd, timeout=30)
            if verify_exit == 0 and verify_output and "is running at" in verify_output:
                logger.info("Kubernetes API is reachable")
                break
            if attempt < max_verify_attempts - 1:
                logger.info("Waiting for Kubernetes API to be ready (attempt %d/%d)...", attempt + 1, max_verify_attempts)
                time.sleep(5)
            else:
                logger.error("Kubernetes API not reachable after %d attempts", max_verify_attempts)
                if verify_output:
                    logger.error("API check output: %s", verify_output)
                return False
        # Wait for CNI to be ready before installing cert-manager
        # k3s embeds Flannel (doesn't run as pods) - check that CNI is actually working
        logger.info("Waiting for CNI plugin (Flannel) to be ready...")
        max_cni_wait = 120
        cni_wait_time = 0
        cni_ready = False
        while cni_wait_time < max_cni_wait:
            # Check 1: Nodes must be Ready (not NotReady)
            nodes_cmd = "kubectl get nodes -o jsonpath='{.items[*].status.conditions[?(@.type==\"Ready\")].status}' 2>&1"
            nodes_output, nodes_exit = pct_service.execute(str(control_id), nodes_cmd, timeout=30)
            nodes_ready = nodes_exit == 0 and nodes_output and "True" in nodes_output and "False" not in nodes_output
            
            # Check 2: CNI config file exists (k3s embeds Flannel, doesn't use pods)
            cni_config_cmd = "test -f /var/lib/rancher/k3s/agent/etc/cni/net.d/10-flannel.conflist && echo exists || echo missing"
            cni_config_output, cni_config_exit = pct_service.execute(str(control_id), cni_config_cmd, timeout=10)
            cni_config_exists = cni_config_exit == 0 and cni_config_output and "exists" in cni_config_output
            
            # Check 3: Flannel subnet.env exists (indicates Flannel backend is running)
            flannel_subnet_cmd = "test -f /run/flannel/subnet.env && echo exists || echo missing"
            flannel_subnet_output, flannel_subnet_exit = pct_service.execute(str(control_id), flannel_subnet_cmd, timeout=10)
            flannel_subnet_exists = flannel_subnet_exit == 0 and flannel_subnet_output and "exists" in flannel_subnet_output
            
            # Check 4: System pods are actually running (not stuck in Pending due to CNI errors)
            pending_cni_cmd = "kubectl get pods -n kube-system --field-selector=status.phase=Pending -o jsonpath='{.items[*].status.conditions[?(@.type==\"PodScheduled\")].message}' 2>&1 | grep -q 'network is not ready' && echo cni_error || echo no_cni_error"
            pending_cni_output, pending_cni_exit = pct_service.execute(str(control_id), pending_cni_cmd, timeout=30)
            no_cni_errors = pending_cni_exit == 0 and pending_cni_output and "no_cni_error" in pending_cni_output
            
            # Check 5: At least some system pods are Running (CNI is working)
            running_pods_cmd = "kubectl get pods -n kube-system --field-selector=status.phase=Running --no-headers 2>&1 | wc -l"
            running_pods_output, running_pods_exit = pct_service.execute(str(control_id), running_pods_cmd, timeout=30)
            pods_running = False
            if running_pods_exit == 0 and running_pods_output:
                try:
                    running_count = int(running_pods_output.strip())
                    pods_running = running_count >= 3  # At least 3 system pods should be running
                except ValueError:
                    pass
            
            if nodes_ready and cni_config_exists and flannel_subnet_exists and no_cni_errors and pods_running:
                logger.info("CNI plugin (Flannel) is ready - nodes Ready, CNI config exists, Flannel subnet exists, system pods running")
                cni_ready = True
                break
            
            if cni_wait_time % 20 == 0:
                logger.info("Waiting for CNI plugin to be ready (waited %d/%d seconds)...", cni_wait_time, max_cni_wait)
                if nodes_output:
                    logger.info("Nodes Ready: %s, CNI config: %s, Flannel subnet: %s, Running pods: %s", 
                              "True" if nodes_ready else "False",
                              "exists" if cni_config_exists else "missing",
                              "exists" if flannel_subnet_exists else "missing",
                              running_pods_output.strip() if running_pods_output else "unknown")
            
            time.sleep(5)
            cni_wait_time += 5
        
        if not cni_ready:
            logger.error("CNI plugin (Flannel) not ready after %d seconds - cannot proceed with cert-manager installation", max_cni_wait)
            logger.error("Nodes Ready: %s, CNI config: %s, Flannel subnet: %s, Running pods: %s",
                        "True" if nodes_ready else "False",
                        "exists" if cni_config_exists else "missing", 
                        "exists" if flannel_subnet_exists else "missing",
                        running_pods_output.strip() if running_pods_output else "unknown")
            return False
        
        # Create namespace for Rancher (kubectl should use /root/.kube/config automatically)
        namespace_cmd = "kubectl create namespace cattle-system --dry-run=client -o yaml | kubectl apply -f -"
        pct_service.execute(str(control_id), namespace_cmd)
        # Install cert-manager (required for Rancher) - kubectl should use /root/.kube/config automatically
        logger.info("Installing cert-manager...")
        cert_manager_cmd = "kubectl apply --validate=false --server-side --force-conflicts -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml"
        max_retries = 3
        for retry in range(max_retries):
            cert_manager_output, cert_manager_exit = pct_service.execute(str(control_id), cert_manager_cmd, timeout=300)
            # Check if resources were applied (even if exit code is non-zero due to connection errors)
            if cert_manager_output and "serverside-applied" in cert_manager_output:
                logger.info("cert-manager resources applied successfully")
                # Try to verify cert-manager namespace exists, but don't fail if API is temporarily unavailable
                verify_cmd = "kubectl get namespace cert-manager"
                verify_output, verify_exit = pct_service.execute(str(control_id), verify_cmd, timeout=30)
                if verify_exit == 0 and verify_output and "cert-manager" in verify_output:
                    logger.info("cert-manager installed and verified successfully")
                    break
                elif "serverside-applied" in cert_manager_output and cert_manager_output.count("serverside-applied") >= 10:
                    # If we applied many resources successfully, consider it installed even if verification fails
                    logger.info("cert-manager resources applied successfully (verification skipped due to API unavailability)")
                    break
            if retry < max_retries - 1:
                logger.error("cert-manager installation failed (attempt %d/%d), retrying in 10 seconds...", retry + 1, max_retries)
                if cert_manager_output:
                    logger.error("Error output: %s", cert_manager_output[-500:])
                time.sleep(10)
            else:
                logger.error("Failed to install cert-manager after %d attempts: %s", max_retries, cert_manager_output)
                return False
        # Wait for cert-manager webhook to be ready - REQUIRED for Rancher installation
        logger.info("Waiting for cert-manager webhook to be ready...")
        max_webhook_wait = 300
        webhook_wait_time = 0
        webhook_ready = False
        while webhook_wait_time < max_webhook_wait:
            # Check if cert-manager webhook pods are ready
            webhook_check_cmd = "kubectl get pods -n cert-manager -l app.kubernetes.io/component=webhook -o jsonpath='{.items[*].status.conditions[?(@.type==\"Ready\")].status}' 2>&1"
            webhook_output, webhook_exit = pct_service.execute(str(control_id), webhook_check_cmd, timeout=30)
            if webhook_exit == 0 and webhook_output:
                # Check if all webhook pods are Ready
                ready_count = webhook_output.count("True")
                if ready_count > 0:
                    # Also verify the webhook service has endpoints (critical - without endpoints, webhook calls fail)
                    endpoints_cmd = "kubectl get endpoints cert-manager-webhook -n cert-manager -o jsonpath='{.subsets[*].addresses[*].ip}' 2>&1"
                    endpoints_output, endpoints_exit = pct_service.execute(str(control_id), endpoints_cmd, timeout=30)
                    if endpoints_exit == 0 and endpoints_output and endpoints_output.strip():
                        logger.info("cert-manager webhook is ready with %d pod(s) and endpoints available", ready_count)
                        webhook_ready = True
                        break
            if webhook_wait_time % 30 == 0:
                logger.info("Waiting for cert-manager webhook to be ready (waited %d/%d seconds)...", webhook_wait_time, max_webhook_wait)
                if webhook_output:
                    logger.info("Webhook pods status: %s", webhook_output[:200])
            time.sleep(10)
            webhook_wait_time += 10
        
        if not webhook_ready:
            logger.error("cert-manager webhook not ready after %d seconds - cannot proceed with Rancher installation", max_webhook_wait)
            logger.error("Rancher installation will fail with 'no endpoints available for service cert-manager-webhook'")
            return False
        # Verify Kubernetes API is reachable
        logger.info("Verifying Kubernetes API is reachable...")
        verify_api_cmd = "kubectl cluster-info"
        max_verify_attempts = 10
        for attempt in range(max_verify_attempts):
            verify_output, verify_exit = pct_service.execute(str(control_id), verify_api_cmd, timeout=30)
            if verify_exit == 0 and verify_output and "is running at" in verify_output:
                logger.info("Kubernetes API is reachable")
                break
            if attempt < max_verify_attempts - 1:
                logger.info("Waiting for Kubernetes API to be ready (attempt %d/%d)...", attempt + 1, max_verify_attempts)
                time.sleep(10)
            else:
                logger.error("Kubernetes API not reachable after %d attempts", max_verify_attempts)
                if verify_output:
                    logger.error("API check output: %s", verify_output)
                return False
        # Verify API server is stable before proceeding with Helm operations
        logger.info("Verifying Kubernetes API server stability...")
        stable_checks = 3
        for i in range(stable_checks):
            verify_cmd = "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml && /usr/local/bin/kubectl cluster-info 2>&1"
            verify_output, verify_exit = pct_service.execute(str(control_id), verify_cmd, timeout=30)
            if verify_exit != 0 or not verify_output or "is running at" not in verify_output:
                logger.error("API server check %d/%d failed, waiting 5 seconds...", i + 1, stable_checks)
                if verify_output:
                    logger.error("API check output: %s", verify_output)
                time.sleep(5)
            else:
                logger.info("API server check %d/%d passed", i + 1, stable_checks)
                time.sleep(2)  # Small delay between checks
        # Install Rancher using Helm
        logger.info("Installing Rancher using Helm...")
        # Check if Helm is installed
        helm_check_cmd = "command -v helm >/dev/null 2>&1 && echo installed || echo not_installed"
        helm_check, _ = pct_service.execute(str(control_id), helm_check_cmd)
        if helm_check and "not_installed" in helm_check:
            logger.info("Installing Helm...")
            helm_install_cmd = "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash && export PATH=/usr/local/bin:$PATH"
            pct_service.execute(str(control_id), helm_install_cmd, timeout=120)
        # Add Rancher Helm repo (use standard k3s kubeconfig) with retry
        repo_add_cmd = "export PATH=/usr/local/bin:$PATH && helm repo add rancher-stable https://releases.rancher.com/server-charts/stable && helm repo update"
        max_repo_retries = 3
        for repo_retry in range(max_repo_retries):
            repo_output, repo_exit = pct_service.execute(str(control_id), repo_add_cmd, timeout=120)
            if repo_exit == 0:
                break
            if repo_retry < max_repo_retries - 1:
                logger.error("Helm repo add failed (attempt %d/%d), retrying in 5 seconds...", repo_retry + 1, max_repo_retries)
                if repo_output:
                    logger.error("Error output: %s", repo_output[-500:])
                time.sleep(5)
            else:
                logger.error("Failed to add Helm repo after %d attempts: %s", max_repo_retries, repo_output)
                return False
        # Install Rancher (use upgrade --install to handle both new installs and upgrades)
        control_hostname = control_config.hostname
        # Use NodePort with fixed port from config, or default to 30443
        rancher_node_port = cfg.services.rancher.port or 30443
        # Verify API is reachable before installation
        verify_cmd = "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml && /usr/local/bin/kubectl cluster-info 2>&1"
        verify_output, verify_exit = pct_service.execute(str(control_id), verify_cmd, timeout=30)
        if verify_exit != 0 or not verify_output or "is running at" not in verify_output:
            logger.error("API server not reachable before Rancher installation")
            if verify_output:
                logger.error("API check output: %s", verify_output[-500:])
            return False
        # Use helm upgrade --install which handles both install and upgrade cases
        install_rancher_cmd = (
            f"export PATH=/usr/local/bin:$PATH && helm upgrade --install rancher rancher-stable/rancher "
            f"--namespace cattle-system "
            f"--set hostname={control_hostname} "
            f"--set replicas=1 "
            f"--set bootstrapPassword=admin "
            f"--set service.type=NodePort "
            f"--set service.ports.http=8080 "
            f"--set service.ports.https=443 "
            f"--set service.nodePorts.https={rancher_node_port}"
        )
        install_output, install_exit = pct_service.execute(str(control_id), install_rancher_cmd, timeout=600)
        if install_exit == 0:
            logger.info("Rancher installed successfully")
            # Patch the service to set the correct NodePort (Helm chart doesn't always respect nodePorts setting)
            logger.info("Setting Rancher service NodePort to %s...", rancher_node_port)
            # Get current http nodePort to preserve it
            get_http_port_cmd = "kubectl get svc rancher -n cattle-system -o jsonpath='{.spec.ports[?(@.name==\"http\")].nodePort}'"
            http_port_output, _ = pct_service.execute(str(control_id), get_http_port_cmd, timeout=10)
            http_node_port = http_port_output.strip() if http_port_output else "30625"
            patch_cmd = (
                f"kubectl patch svc rancher -n cattle-system -p "
                f"'{{\"spec\":{{\"ports\":[{{\"name\":\"http\",\"port\":80,\"protocol\":\"TCP\",\"targetPort\":80,\"nodePort\":{http_node_port}}},"
                f"{{\"name\":\"https\",\"port\":443,\"protocol\":\"TCP\",\"targetPort\":443,\"nodePort\":{rancher_node_port}}}]}}}}'"
            )
            patch_output, patch_exit = pct_service.execute(str(control_id), patch_cmd, timeout=30)
            if patch_exit == 0:
                logger.info("Rancher service NodePort set to %s", rancher_node_port)
            else:
                logger.warning("Failed to patch Rancher service NodePort: %s", patch_output)
            return True
        else:
            logger.error("Rancher installation failed: %s", install_output[-1000:] if install_output else "No output")
            # Check k3s service status on failure
            k3s_status_cmd = "systemctl status k3s --no-pager -l 2>&1 | head -50"
            k3s_status, _ = pct_service.execute(str(control_id), k3s_status_cmd, timeout=10)
            if k3s_status:
                logger.error("k3s service status: %s", k3s_status)
            # Check k3s logs
            k3s_logs_cmd = "journalctl -u k3s --no-pager -n 50 2>&1"
            k3s_logs, _ = pct_service.execute(str(control_id), k3s_logs_cmd, timeout=10)
            if k3s_logs:
                logger.error("k3s service logs: %s", k3s_logs)
            return False
    finally:
        lxc_service.disconnect()

