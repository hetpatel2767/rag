#!/usr/bin/env bash

KUBECTL="kubectl"
if ! kubectl version --client >/dev/null 2>&1 || ! kubectl get namespace local-rag >/dev/null 2>&1; then
    if sudo kubectl get namespace local-rag >/dev/null 2>&1; then
        KUBECTL="sudo kubectl"
    fi
fi

echo "Starting Kubernetes port-forwards for local access (using $KUBECTL)..."
$KUBECTL port-forward -n local-rag svc/ingestion-api 8000:8000 >/dev/null 2>&1 &
PID1=$!
$KUBECTL port-forward -n local-rag svc/retrieval-api 8002:8000 >/dev/null 2>&1 &
PID2=$!
$KUBECTL port-forward -n local-rag svc/ollama 11434:11434 >/dev/null 2>&1 &
PID3=$!
$KUBECTL port-forward -n local-rag svc/qdrant 6333:6333 >/dev/null 2>&1 &
PID4=$!
$KUBECTL port-forward -n local-rag svc/minio 9001:9001 >/dev/null 2>&1 &
PID5=$!

echo "Port-forwards active:"
echo " - RAG Web Dashboard (Chrome UI): http://localhost:8002"
echo " - Ingestion API:                 http://localhost:8000"
echo " - Retrieval API / Swagger Docs:  http://localhost:8002/docs"
echo " - Qdrant Vector DB Console:      http://localhost:6333/dashboard"
echo " - MinIO S3 Web Console:          http://localhost:9001"
echo " - Ollama Local LLM:              http://localhost:11434"
echo "Press Ctrl+C to terminate port-forwards."

trap "kill $PID1 $PID2 $PID3 $PID4 $PID5 2>/dev/null; exit" INT TERM
wait
