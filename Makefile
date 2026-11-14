.PHONY: help compose-up compose-down compose-logs compose-ps \
        k8s-setup k8s-apply k8s-delete k8s-port-forward k8s-ui-update \
        test test-batch eval clean

SHELL := /bin/bash

help: ## Show this help menu
	@echo "=================================================================="
	@echo " Local Enterprise RAG - Developer Makefile"
	@echo "=================================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# -------------------------------------------------------------
# Docker Compose Workflow
# -------------------------------------------------------------
compose-up: ## Start the entire RAG pipeline using Docker Compose
	docker compose up -d --build
	@echo "[✓] Docker Compose started. Web UI available at: http://localhost:8002"

compose-down: ## Stop and remove all Docker Compose containers
	docker compose down

compose-logs: ## Tail logs of all Docker Compose services
	docker compose logs -f

compose-ps: ## View running containers and ports
	docker compose ps

ollama-pull: ## Pull the small-weight model (Qwen 2.5 3B) into Ollama
	docker exec -it rag-ollama ollama pull qwen2.5:3b

# -------------------------------------------------------------
# Kubernetes Workflow (Minikube / k3d with GPU)
# -------------------------------------------------------------
k8s-setup: ## Run the automated Kubernetes & GPU driver setup script
	./scripts/02_setup_k8s_gpu.sh

k8s-apply: ## Apply all Kubernetes manifests (Storage, DB, Redpanda, Ollama, KEDA, Services)
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

k8s-delete: ## Delete all RAG resources from local-rag namespace
	kubectl delete -f k8s/ --ignore-not-found

k8s-pods: ## View status of all pods in local-rag namespace
	kubectl get pods -n local-rag -o wide

k8s-port-forward: ## Forward ports for Ingestion, Retrieval, Ollama & Qdrant
	./scripts/port_forward_k8s.sh

k8s-ui-update: ## Rebuild & reload updated retrieval UI container into Minikube
	./scripts/rebuild_and_restart_ui.sh

# -------------------------------------------------------------
# Testing & Quality Gates
# -------------------------------------------------------------
test: ## Run end-to-end integration test (claim check, deduplication, PII, ACL query)
	python3 scripts/test_pipeline.py

test-batch: ## Run 20-file batch ingestion and observe KEDA autoscaling 0->4 pods
	python3 scripts/batch_upload_test.py

eval: ## Run automated RAG evaluation quality gate (Recall@5 & faithfulness)
	python3 services/eval/eval.py

clean: ## Clean up temporary bytecode and caches
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
