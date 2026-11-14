#!/bin/bash
set -e

echo "=========================================================="
echo " Local Kubernetes (k3s / minikube) Setup with NVIDIA GPU"
echo " Optimized for Ryzen CPU + 6GB VRAM + 32GB RAM"
echo "=========================================================="

echo "Step 1: Check NVIDIA Driver & CUDA support..."
nvidia-smi

echo ""
echo "Select your preferred local Kubernetes engine:"
echo "Option A: Minikube with Docker/KVM and NVIDIA GPU"
echo "Option B: K3s (Lightweight Kubernetes) with containerd & NVIDIA Container Toolkit"
echo ""

echo "--- For Minikube (Recommended for easy GPU passthrough on PC) ---"
cat << 'EOF'
1. Install Minikube & Kubectl:
   curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
   sudo install minikube-linux-amd64 /usr/local/bin/minikube
   curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
   sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

2. Start Minikube with NVIDIA GPU and 16GB RAM / 4 CPUs allocated:
   minikube start --driver=docker --gpus=all --cpus=6 --memory=16384

3. Install KEDA (Kubernetes Event-driven Autoscaling):
   helm repo add kedacore https://kedacore.github.io/charts
   helm repo update
   helm install keda kedacore/keda --namespace keda --create-namespace

4. Apply the RAG manifests:
   kubectl apply -f k8s/00-namespace.yaml
   kubectl apply -f k8s/01-minio.yaml
   kubectl apply -f k8s/02-redpanda.yaml
   kubectl apply -f k8s/03-mongodb.yaml
   kubectl apply -f k8s/04-qdrant.yaml
   kubectl apply -f k8s/05-ollama.yaml
   kubectl apply -f k8s/06-ingestion.yaml
   kubectl apply -f k8s/07-worker.yaml
   kubectl apply -f k8s/08-keda-scaler.yaml
   kubectl apply -f k8s/09-retrieval.yaml

5. Pull the Small Weight Model inside Ollama (Qwen2.5:3b - ~1.9GB VRAM):
   kubectl exec -it -n local-rag deploy/ollama -- ollama pull qwen2.5:3b
EOF

echo ""
echo "--- Alternatively, for Docker Compose (Zero K8s overhead for instant testing) ---"
cat << 'EOF'
If you want to run everything right now with docker compose:
   docker compose up -d
   docker exec -it rag-ollama ollama pull qwen2.5:3b
   python3 scripts/test_pipeline.py
EOF
