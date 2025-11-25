"""
Install Docker action
"""
import logging
from cli import Apt, Docker, Command, Curl, Shell
from .base import Action
logger = logging.getLogger(__name__)

class InstallDockerAction(Action):
    """Action to install Docker"""
    description = "docker installation"

    def execute(self) -> bool:
        """Install Docker"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False

        logger.info("Installing Docker...")

        # Check if curl is available
        curl_check_output, curl_check_exit = self.ssh_service.execute(
            Command().command("curl").exists(), 
            sudo=False
        )

        has_curl = curl_check_exit == 0 and curl_check_output and "curl" in curl_check_output
        if has_curl:
            # Try official Docker install script
            logger.info("Downloading Docker install script...")

            download_cmd = Curl().fail_silently(True).silent(True).show_errors(True).location(True).output("/tmp/get-docker.sh").url("https://get.docker.com").download()
            download_output, download_exit = self.ssh_service.execute(download_cmd, sudo=False)

            if download_exit is not None and download_exit != 0:
                logger.warning("Failed to download Docker install script: %s", download_output)
                has_curl = False
            else:
                logger.info("Running Docker install script...")
                
                script_cmd = Shell().shell("sh").script("/tmp/get-docker.sh").execute()
                script_output, script_exit = self.ssh_service.execute(script_cmd, sudo=False, timeout=300)

                # Check for installation errors in output
                if script_output and ("E: Package" in script_output or "Unable to locate package" in script_output or "has no installation candidate" in script_output):

                    logger.warning("Docker install script failed, falling back to docker.io")

                    has_curl = False
                elif script_exit is not None and script_exit != 0:

                    logger.warning("Docker install script failed with exit code %s, falling back to docker.io", script_exit)

                    has_curl = False
                else:
                    # Verify Docker is installed after script
                    docker_check_cmd = Docker().is_installed_check()
                    check_output, check_exit_code = self.ssh_service.execute(docker_check_cmd, sudo=True)

                    if Docker.parse_is_installed(check_output):

                        logger.info("Docker installed successfully via install script")

                        return True

                    logger.warning("Docker install script completed but Docker not found, falling back to docker.io")

                    has_curl = False

        # Fallback to docker.io via apt
        if not has_curl:

            logger.info("Installing docker.io via apt...")

            install_cmd = Apt().install(["docker.io"])

            if self.apt_service:
                install_output = self.apt_service.execute(install_cmd, timeout=300)

                # APTService.execute returns output string, check for errors
                if install_output and ("E: Package" in install_output or "Unable to locate package" in install_output or "has no installation candidate" in install_output):

                    logger.error("Docker installation failed - packages not found")
                    logger.error("Docker installation output: %s", install_output[-1000:])

                    return False
            else:
                install_output, install_exit = self.ssh_service.execute(f"sudo -n {install_cmd}", sudo=False, timeout=300)

                if install_exit is not None and install_exit != 0:
                    logger.error("Docker installation failed with exit code %s", install_exit)

                    if install_output:
                        logger.error("Docker installation output: %s", install_output[-1000:])
                    return False

                if install_output and ("E: Package" in install_output or "Unable to locate package" in install_output or "has no installation candidate" in install_output):

                    logger.error("Docker installation failed - packages not found")
                    logger.error("Docker installation output: %s", install_output[-1000:])

                    return False

        # Verify Docker is installed
        docker_check_cmd = Docker().is_installed_check()
        check_output, check_exit_code = self.ssh_service.execute(docker_check_cmd, sudo=True)

        if not Docker.parse_is_installed(check_output):

            logger.error("Docker installation failed - verification shows Docker is not installed")

            return False

        logger.info("Docker installed successfully")

        return True

