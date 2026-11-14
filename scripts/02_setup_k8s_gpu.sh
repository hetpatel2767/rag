#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " Phase 2: Local Kubernetes Setup (Minikube / k3d with NVIDIA GPU)"
echo "=========================================================="

# Check for Docker
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: Docker is not installed or not in PATH. Please run scripts/01_install_docker.sh first."
  exit 1
fi

echo "Step 1: Installing kubectl..."
if ! command -v kubectl >/dev/null 2>&1; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  rm kubectl
  echo "kubectl installed successfully."
fi

echo "Step 2: Installing Helm..."
if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  echo "Helm installed successfully."
fi

echo "Step 3: Choose your preferred local cluster tool (Minikube or k3d):"
echo "1) Minikube (Recommended for GPU pass-through)"
echo "2) k3d (Runs k3s inside Docker)"
read -p "Enter 1 or 2 [Default: 1]: " CHOICE
CHOICE=${CHOICE:-1}

if [ "$CHOICE" -eq 1 ]; then
  echo "Installing Minikube..."
  if ! command -v minikube >/dev/null 2>&1; then
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
    rm minikube-linux-amd64
  fi

  echo "Cleaning up any previous failed minikube state..."
  minikube delete 2>/dev/null || true

  EXTRA_FLAGS=""
  if [ "$EUID" -eq 0 ]; then
    echo "Running as root, adding --force flag for minikube..."
    EXTRA_FLAGS="--force"
  fi

  echo "Starting Minikube cluster with NVIDIA GPU and 16GB RAM..."
  minikube start --driver=docker --container-runtime=docker --gpus=all --cpus=6 --memory=16384 $EXTRA_FLAGS

  echo "Enabling metrics-server & nvidia addon..."
  minikube addons enable metrics-server
  minikube addons enable nvidia-device-plugin 2>/dev/null || minikube addons enable nvidia-gpu-device-plugin 2>/dev/null || true

elif [ "$CHOICE" -eq 2 ]; then
  echo "Installing k3d..."
  if ! command -v k3d >/dev/null 2>&1; then
    curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
  fi

  echo "Creating k3d cluster with GPU passthrough..."
  k3d cluster create rag-cluster \
    --gpus=1 \
    -p "8000:8000@loadbalancer" \
    -p "8002:8002@loadbalancer"
fi

echo "Step 4: Installing KEDA (Kubernetes Event-driven Autoscaler)..."
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm upgrade --install keda kedacore/keda --namespace keda --create-namespace

echo "Step 5: Installing NVIDIA GPU Device Plugin for Kubernetes..."
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm repo update
helm upgrade --install nvdp nvdp/nvidia-device-plugin \
  --namespace gpu-operator \
  --create-namespace \
  --set compatWithCPUManager=true

echo "Step 6: Building and loading local application images into the cluster..."
docker build -t local-rag/ingestion:latest services/ingestion
docker build -t local-rag/worker:latest services/worker
docker build -t local-rag/retrieval:latest services/retrieval

if [ "$CHOICE" -eq 1 ]; then
  echo "Loading images into Minikube..."
  minikube image load local-rag/ingestion:latest
  minikube image load local-rag/worker:latest
  minikube image load local-rag/retrieval:latest
elif [ "$CHOICE" -eq 2 ]; then
  echo "Loading images into k3d..."
  k3d image import -c rag-cluster \
    local-rag/ingestion:latest \
    local-rag/worker:latest \
    local-rag/retrieval:latest
fi

echo "=========================================================="
echo " Kubernetes cluster with GPU passthrough & KEDA is READY!"
echo " Deploy the RAG stack with:"
echo "   kubectl apply -f k8s/"
echo "   kubectl exec -it -n local-rag deploy/ollama -- ollama pull qwen2.5:3b"
echo "=========================================================="
