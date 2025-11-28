"""
Install k3s action
"""
import logging
from cli import Curl, Shell, Command
from .base import Action
logger = logging.getLogger(__name__)

class InstallK3sAction(Action):
    """Action to install k3s"""
    description = "k3s installation"

    def execute(self) -> bool:
        """Install k3s"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False

        logger.info("Installing k3s...")

        # Check if k3s is already installed
        k3s_check_cmd = Command().command("k3s").exists()
        check_output, check_exit = self.ssh_service.execute(k3s_check_cmd, sudo=True)
        
        if check_exit == 0 and check_output:
            # Verify it's actually k3s by checking version
            version_cmd = "k3s --version 2>&1"
            version_output, version_exit = self.ssh_service.execute(version_cmd, sudo=True)
            if version_exit == 0 and version_output and "k3s" in version_output.lower():
                logger.info("k3s is already installed: %s", version_output.strip())
                # Even if k3s is installed, ensure /dev/kmsg exists and restart k3s if needed
                is_control = False
                if self.container_cfg and self.cfg and self.cfg.kubernetes:
                    is_control = self.container_cfg.id in self.cfg.kubernetes.control
                if is_control:
                    # Always ensure /dev/kmsg exists - use symlink to /dev/console (official k3s LXC requirement)
                    logger.info("Ensuring /dev/kmsg exists for k3s (LXC requirement)...")
                # Remove existing file/device if it exists
                remove_cmd = "rm -f /dev/kmsg 2>/dev/null || true"
                self.ssh_service.execute(remove_cmd, sudo=True)
                # Create /dev/kmsg as a symlink to /dev/console (official k3s LXC requirement)
                create_kmsg_cmd = "ln -sf /dev/console /dev/kmsg 2>&1"
                create_output, create_exit = self.ssh_service.execute(create_kmsg_cmd, sudo=True)
                if create_exit is not None and create_exit != 0:
                    logger.error("Failed to create /dev/kmsg symlink: %s", create_output[-200:] if create_output else "No output")
                    return False
                else:
                    logger.info("/dev/kmsg symlink created successfully")
                    # Verify it exists
                    verify_cmd = "test -e /dev/kmsg && ls -l /dev/kmsg && echo exists || echo missing"
                    verify_output, verify_exit = self.ssh_service.execute(verify_cmd, sudo=True)
                    if verify_output:
                        logger.info("/dev/kmsg status: %s", verify_output.strip())
                    # Restart k3s to pick up the device
                    logger.info("Restarting k3s service...")
                    restart_cmd = "systemctl restart k3s 2>&1"
                    restart_output, restart_exit = self.ssh_service.execute(restart_cmd, sudo=True)
                    if restart_exit is not None and restart_exit != 0:
                        logger.warning("k3s restart had issues: %s", restart_output[-200:] if restart_output else "No output")
                    else:
                        logger.info("k3s service restarted")
                        # Wait a bit for k3s to start
                        import time
                        time.sleep(5)
                return True

        # Check if curl is available, install if needed
        from cli import Apt
        curl_check_output, curl_check_exit = self.ssh_service.execute(
            Command().command("curl").exists(), 
            sudo=False
        )
        has_curl = curl_check_exit == 0 and curl_check_output and "curl" in curl_check_output
        
        if not has_curl:
            logger.info("curl not found, installing...")
            install_curl_cmd = Apt().install(["curl"])
            curl_install_output = self.apt_service.execute(install_curl_cmd, timeout=120)
            if curl_install_output and ("E: Package" in curl_install_output or "Unable to locate package" in curl_install_output):
                logger.error("Failed to install curl: %s", curl_install_output)
                return False
            logger.info("curl installed successfully")
        
        # Install k3s using official install script
        logger.info("Downloading k3s install script...")
        install_cmd = Curl().fail_silently(False).silent(False).show_errors(True).url("https://get.k3s.io").download()
        download_output, download_exit = self.ssh_service.execute(install_cmd, sudo=False, timeout=60)

        if download_exit is not None and download_exit != 0:
            logger.error("Failed to download k3s install script: %s", download_output)
            return False

        # Determine if this is a control node or worker node
        # Control nodes have type "k3s-control", workers have type "k3s-worker"
        # Check if this is a control node by checking if container ID is in kubernetes.control
        is_control = False
        if self.container_cfg and self.cfg and self.cfg.kubernetes:
            is_control = self.container_cfg.id in self.cfg.kubernetes.control
        
        # Create /dev/kmsg device inside container (required for k3s in LXC)
        # /dev/kmsg is a character device (major 1, minor 11)
        if is_control:
            logger.info("Creating /dev/kmsg device for k3s...")
            kmsg_check_cmd = "test -c /dev/kmsg && echo exists || echo missing"
            kmsg_check_output, kmsg_check_exit = self.ssh_service.execute(kmsg_check_cmd, sudo=True)
            # Always ensure /dev/kmsg is a symlink to /dev/console (official k3s LXC requirement)
            logger.info("Ensuring /dev/kmsg is a symlink to /dev/console (k3s LXC requirement)...")
            remove_cmd = "rm -f /dev/kmsg 2>/dev/null || true"
            self.ssh_service.execute(remove_cmd, sudo=True)
            create_kmsg_cmd = "ln -sf /dev/console /dev/kmsg 2>&1"
            create_output, create_exit = self.ssh_service.execute(create_kmsg_cmd, sudo=True)
            if create_exit is not None and create_exit != 0:
                logger.error("Failed to create /dev/kmsg symlink: %s", create_output[-200:] if create_output else "No output")
                return False
            # Verify it was created
            verify_cmd = "test -L /dev/kmsg && ls -l /dev/kmsg && echo symlink_ok || echo symlink_failed"
            verify_output, verify_exit = self.ssh_service.execute(verify_cmd, sudo=True)
            if verify_exit == 0 and verify_output and "symlink_ok" in verify_output:
                logger.info("/dev/kmsg symlink verified: %s", verify_output.strip())
            else:
                logger.error("/dev/kmsg symlink creation failed: %s", verify_output)
                return False
        
        if is_control:
            logger.info("Installing k3s server (control node)...")
            # Create standard k3s config file before installation
            # k3s reads from /etc/rancher/k3s/config.yaml on startup
            config_dir = "/etc/rancher/k3s"
            config_file = f"{config_dir}/config.yaml"
            
            # Get control node IP address
            control_ip = self.container_cfg.ip_address if self.container_cfg and self.container_cfg.ip_address else "127.0.0.1"
            
            logger.info("Creating standard k3s config file at %s...", config_file)
            config_content = f"""# k3s configuration file
# This file is automatically generated
tls-san:
  - {control_ip}
  - {self.container_cfg.hostname if self.container_cfg else 'k3s-control'}
bind-address: 0.0.0.0
advertise-address: {control_ip}
"""
            create_config_cmd = f"mkdir -p {config_dir} && cat > {config_file} << 'EOFCONFIG'\n{config_content}EOFCONFIG"
            config_output, config_exit = self.ssh_service.execute(create_config_cmd, sudo=True)
            if config_exit is not None and config_exit != 0:
                logger.error("Failed to create k3s config file: %s", config_output[-200:] if config_output else "No output")
                return False
            logger.info("k3s config file created successfully")
            
            # Install k3s server (it will read from config.yaml)
            install_cmd = "curl -sfL https://get.k3s.io | sh -"
            install_output, install_exit = self.ssh_service.execute(
                install_cmd,
                sudo=True,
                timeout=300
            )
        else:
            logger.info("Skipping k3s agent installation for worker node (will be installed during orchestration)...")
            # Worker nodes will be installed during orchestration with proper token
            # Just verify k3s is not already installed
            return True

        if install_exit is not None and install_exit != 0:
            logger.error("k3s installation failed with exit code %s", install_exit)
            if install_output:
                logger.error("k3s installation output: %s", install_output[-1000:])
            return False

        # Verify k3s is installed
        k3s_check_cmd = Command().command("k3s").exists()
        check_output, check_exit_code = self.ssh_service.execute(k3s_check_cmd, sudo=True)

        if check_exit_code != 0 or not check_output:
            logger.error("k3s installation failed - k3s command not found")
            return False

        # Verify version
        version_cmd = "k3s --version 2>&1"
        version_output, version_exit = self.ssh_service.execute(version_cmd, sudo=True)
        if version_exit != 0 or not version_output or "k3s" not in version_output.lower():
            logger.error("k3s installation failed - verification shows k3s is not installed")
            return False

        logger.info("k3s installed successfully: %s", version_output.strip())
        
        # Setup kubectl PATH and kubeconfig for root user
        if is_control:
            logger.info("Setting up kubectl PATH and kubeconfig...")
            # Create symlink for kubectl in /usr/bin (which is in PATH)
            # Note: No sudo needed in command since execute() is called with sudo=True
            symlink_cmd = "ln -sf /usr/local/bin/kubectl /usr/bin/kubectl 2>/dev/null || true"
            self.ssh_service.execute(symlink_cmd, sudo=True)
            # Wait for k3s to generate kubeconfig (with timeout)
            import time
            logger.info("Waiting for k3s to generate kubeconfig...")
            max_wait = 60
            wait_time = 0
            kubeconfig_ready = False
            while wait_time < max_wait:
                check_cmd = "test -f /etc/rancher/k3s/k3s.yaml && echo exists || echo missing"
                check_output, check_exit = self.ssh_service.execute(check_cmd, sudo=True)
                if check_exit == 0 and check_output and "exists" in check_output:
                    kubeconfig_ready = True
                    break
                time.sleep(2)
                wait_time += 2
            if not kubeconfig_ready:
                logger.error("k3s kubeconfig not generated after %d seconds", max_wait)
                return False
            logger.info("k3s kubeconfig generated")
            # Fix kubeconfig server IP and copy to standard location
            control_ip = self.container_cfg.ip_address if self.container_cfg and self.container_cfg.ip_address else "127.0.0.1"
            # Use /root/.kube/config explicitly (not ~/.kube/config) to avoid shell expansion issues
            # Note: No sudo needed in command since execute() is called with sudo=True
            fix_kubeconfig_cmd = f"sed -i 's|server: https://127.0.0.1:6443|server: https://{control_ip}:6443|g; s|server: https://0.0.0.0:6443|server: https://{control_ip}:6443|g' /etc/rancher/k3s/k3s.yaml && mkdir -p /root/.kube && cp /etc/rancher/k3s/k3s.yaml /root/.kube/config && chown root:root /root/.kube/config && chmod 600 /root/.kube/config"
            fix_output, fix_exit = self.ssh_service.execute(fix_kubeconfig_cmd, sudo=True)
            if fix_exit is not None and fix_exit != 0:
                logger.error("Failed to setup kubeconfig: %s", fix_output[-200:] if fix_output else "No output")
                return False
            # Verify kubeconfig was copied
            verify_cmd = "test -f /root/.kube/config && echo exists || echo missing"
            verify_output, verify_exit = self.ssh_service.execute(verify_cmd, sudo=True)
            if verify_exit != 0 or not verify_output or "exists" not in verify_output:
                logger.error("kubeconfig was not copied to /root/.kube/config")
                return False
            logger.info("kubeconfig setup completed")
        
        # Fix k3s service to skip modprobe checks (modules must be loaded on Proxmox host, not in container)
        if is_control:
            logger.info("Configuring k3s service to skip kernel module checks (LXC workaround)...")
            # Comment out the modprobe ExecStartPre lines in the k3s service file
            # Handle both ExecStartPre=-/sbin/modprobe and ExecStartPre=/sbin/modprobe formats
            service_file = "/etc/systemd/system/k3s.service"
            # Use sed to comment out lines containing modprobe br_netfilter or modprobe overlay
            # Match both ExecStartPre=-/sbin/modprobe and ExecStartPre=/sbin/modprobe
            # Modules must be loaded on Proxmox host, not in container
            fix_cmd = (
                f"sed -i 's|^ExecStartPre=-/sbin/modprobe br_netfilter|#ExecStartPre=-/sbin/modprobe br_netfilter|g' {service_file} && "
                f"sed -i 's|^ExecStartPre=-/sbin/modprobe overlay|#ExecStartPre=-/sbin/modprobe overlay|g' {service_file} && "
                f"sed -i 's|^ExecStartPre=/sbin/modprobe br_netfilter|#ExecStartPre=/sbin/modprobe br_netfilter|g' {service_file} && "
                f"sed -i 's|^ExecStartPre=/sbin/modprobe overlay|#ExecStartPre=/sbin/modprobe overlay|g' {service_file}"
            )
            fix_output, fix_exit = self.ssh_service.execute(fix_cmd, sudo=True)
            if fix_exit is not None and fix_exit != 0:
                logger.warning("Failed to modify k3s service file: %s", fix_output[-200:] if fix_output else "No output")
            else:
                logger.info("k3s service file modified successfully")
                # Verify the fix
                verify_cmd = f"grep -E '^ExecStartPre.*modprobe' {service_file} || echo 'no_modprobe_lines'"
                verify_output, verify_exit = self.ssh_service.execute(verify_cmd, sudo=True)
                if verify_output and "no_modprobe_lines" not in verify_output:
                    logger.warning("Some modprobe lines may still be active: %s", verify_output)
                # Reload systemd and restart k3s
                reload_cmd = "systemctl daemon-reload"
                reload_output, reload_exit = self.ssh_service.execute(reload_cmd, sudo=True)
                if reload_exit is not None and reload_exit != 0:
                    logger.warning("Failed to reload systemd: %s", reload_output[-200:] if reload_output else "No output")
                else:
                    logger.info("systemd reloaded, restarting k3s...")
                    restart_cmd = "systemctl restart k3s"
                    restart_output, restart_exit = self.ssh_service.execute(restart_cmd, sudo=True, timeout=60)
                    if restart_exit is not None and restart_exit != 0:
                        logger.warning("k3s restart had issues: %s", restart_output[-200:] if restart_output else "No output")
                    else:
                        logger.info("k3s service restarted with fixed configuration")
                        # Create systemd service to ensure /dev/kmsg symlink persists across reboots
                        logger.info("Creating systemd service to persist /dev/kmsg symlink...")
                        service_content = """[Unit]
Description=Create /dev/kmsg symlink for k3s
Before=k3s.service
DefaultDependencies=no

[Service]
Type=oneshot
ExecStart=/bin/ln -sf /dev/console /dev/kmsg
RemainAfterExit=yes

[Install]
WantedBy=sysinit.target
"""
                        create_service_cmd = f"cat > /etc/systemd/system/kmsg-symlink.service << 'EOFSERVICE'\n{service_content}EOFSERVICE"
                        service_output, service_exit = self.ssh_service.execute(create_service_cmd, sudo=True)
                        if service_exit is not None and service_exit != 0:
                            logger.warning("Failed to create kmsg-symlink service: %s", service_output[-200:] if service_output else "No output")
                        else:
                            enable_cmd = "systemctl daemon-reload && systemctl enable kmsg-symlink.service && systemctl start kmsg-symlink.service"
                            enable_output, enable_exit = self.ssh_service.execute(enable_cmd, sudo=True)
                            if enable_exit is not None and enable_exit != 0:
                                logger.warning("Failed to enable kmsg-symlink service: %s", enable_output[-200:] if enable_output else "No output")
                            else:
                                logger.info("kmsg-symlink service created and enabled")
                        # Wait for k3s to start
                        import time
                        time.sleep(5)
        
        return True

