#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

echo "=========================================================="
echo " Rebuilding & Updating RAG UI in Kubernetes (Minikube)"
echo "=========================================================="

DOCKER_CMD="docker"
if ! docker info >/dev/null 2>&1; then
    DOCKER_CMD="sudo docker"
fi

KUBECTL_CMD="kubectl"
MINIKUBE_CMD="minikube"
if ! kubectl get nodes >/dev/null 2>&1; then
    KUBECTL_CMD="sudo kubectl"
    MINIKUBE_CMD="sudo minikube"
fi

echo "[1/3] Building local-rag/retrieval:latest with HTML UI..."
$DOCKER_CMD build -t local-rag/retrieval:latest services/retrieval

echo "[2/3] Loading updated image into Minikube cluster..."
$MINIKUBE_CMD image load local-rag/retrieval:latest --overwrite 2>/dev/null || $MINIKUBE_CMD image load local-rag/retrieval:latest

echo "[3/3] Restarting retrieval-api deployment in local-rag namespace..."
$KUBECTL_CMD apply -f k8s/09-retrieval.yaml
$KUBECTL_CMD rollout restart deployment/retrieval-api -n local-rag
$KUBECTL_CMD rollout status deployment/retrieval-api -n local-rag --timeout=90s

echo ""
echo "=========================================================="
echo " [✓] Update Complete! Refresh your browser tab:"
echo "     👉 http://localhost:8002"
echo "=========================================================="
