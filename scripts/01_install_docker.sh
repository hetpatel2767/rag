#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " Installing Docker, Docker Compose & NVIDIA Container Toolkit"
echo " (Optimized for Ubuntu on WSL2 / Linux with NVIDIA GPU)"
echo "=========================================================="

# Check if run with sudo permissions
if [ "$EUID" -ne 0 ]; then
  echo "Please run this script with sudo: sudo ./scripts/01_install_docker.sh"
  exit 1
fi

ACTUAL_USER="${SUDO_USER:-$USER}"

echo "Step 1: Installing system prerequisites..."
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release

echo "Step 2: Adding Docker official GPG key & repository..."
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "Step 3: Installing Docker Engine, CLI, and Docker Compose Plugin..."
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "Step 4: Adding user '$ACTUAL_USER' to docker group..."
groupadd -f docker
usermod -aG docker "$ACTUAL_USER"

echo "Step 5: Installing NVIDIA Container Toolkit for GPU passthrough..."
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor --yes -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update
apt-get install -y nvidia-container-toolkit

echo "Step 6: Configuring Docker daemon to use NVIDIA runtime by default..."
nvidia-ctk runtime configure --runtime=docker

# Start docker daemon service
if command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1; then
  systemctl restart docker
else
  service docker restart
fi

echo "=========================================================="
echo " Docker & NVIDIA Container Toolkit installation complete!"
echo " Log out and log back in (or run 'newgrp docker') to activate group permissions."
echo " Test with: docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi"
echo "=========================================================="
