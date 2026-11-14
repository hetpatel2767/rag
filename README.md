# Local Enterprise RAG System (Open-Source Architecture)

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python: 3.11+](https://img.shields.io/badge/Python-3.11+-brightgreen.svg?logo=python)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](docker-compose.yml)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-KEDA%20Autoscaling-326CE5?logo=kubernetes&logoColor=white)](k8s/)
[![Vector DB](https://img.shields.io/badge/Qdrant-Hybrid%20Dense%20%2B%20BM25-dc2626)](https://qdrant.tech)
[![Local LLM](https://img.shields.io/badge/Ollama-Qwen%202.5%203B%20(6GB%20GPU)-000000)](https://ollama.ai)

A production-grade, local implementation of an enterprise Retrieval-Augmented Generation (RAG) system modeled after high-throughput streaming architectures, built entirely with **100% open-source local technologies**. Includes an interactive Web UI, KEDA event-driven autoscaling, and real-time PII redaction.

Designed specifically to run efficiently on **local PCs** with:
- **GPU:** 6 GB VRAM budget (e.g., NVIDIA RTX 3060 / 2060 / 3060 Ti)
- **RAM:** 32 GB System Memory
- **CPU:** AMD Ryzen Multi-Core Processor
- **Orchestration:** Local Kubernetes (`k3s` or `minikube`) with KEDA autoscaling and NVIDIA GPU passthrough.

---

## 1. Architecture Mapping & Implementation

| Pipeline Stage | Architectural Function | Local Open-Source Implementation | Rationale & Specs |
|---|---|---|---|
| **1. Connectors & Storage** | S3 Claim-Check Storage & Dedup | **MinIO (Local S3-Compatible)** | Files stored as raw objects; JSON claim-check pointer emitted to Kafka with SHA-256 deduplication. |
| **2. Message Bus** | Priority Ingestion Event Bus | **Redpanda** (Kafka API Compatible) | Single C++ binary, zero Zookeeper/JVM overhead, ultra-fast, consumes <400 MB RAM. |
| **3. Scaled Worker** | Dynamic Autoscaling Pods (0 → N) | **KEDA + Stateless Python Pods** | KEDA scales pods from 0 to N based on Kafka topic consumer lag. |
| **3. OCR & Parser** | Layout/Table Document Extraction | **PyMuPDF / pdfplumber / Docling** | Stateless CPU extraction leveraging Ryzen multi-core SIMD vector instructions. |
| **3. PII Redaction** | Real-Time PII Redaction | **Microsoft Presidio + Regex Engine** | Detects & anonymizes emails, SSNs, phone numbers, credit cards, and API keys before embedding. |
| **3. Chunking** | Semantic Chunking (512–1024 tok, 15% overlap) | **Sliding Window Semantic Chunker** | Paragraph-aware splitting with exact metadata preservation (page, doc_id, ACL bitmask). |
| **3. DLQ** | Dead-Letter Queue (DLQ) | **Kafka DLQ (`doc-ingest-dlq`) + MongoDB DLQ Collection** | Poison-pill / corrupt PDFs retried 3x with exponential backoff before routing to DLQ; never blocks partition. |
| **4. Embedding Engine** | High-Throughput Embedder | **FastEmbed (BAAI/bge-small-en-v1.5 + Qdrant BM25)** | High-speed ONNX runtime executed on Ryzen CPU (~130MB RAM). Generates **both Dense (384-dim) and Sparse (BM25)** vectors concurrently with zero GPU footprint. |
| **5. Vector DB** | Hybrid Search Store (Dense + BM25) | **Qdrant Vector DB** | Native Dense HNSW + Sparse BM25 index in a single engine. Payload filtering and deterministic UUID upserts. |
| **5. Metadata Store** | Document State & ACL Repository | **MongoDB** | Stores raw doc metadata, deduplication hashes, chunk manifests, ACL groups, and audit traces. |
| **6. ACL Pre-Filter** | Mandatory Pre-Filter (tenant_id + ACLs) | **Qdrant Payload Pre-Filter** | Fast indexed boolean filtering: `tenant_id == X AND acl_groups IN user_groups`. |
| **7. Hybrid Fusion & Rerank**| Hybrid RRF + Cross-Encoder Reranker | **Reciprocal Rank Fusion (RRF) + FlashRank Cross-Encoder** | Fuses Dense Top-50 + BM25 Top-50. Reranks with FlashRank (`ms-marco-MiniLM-L-12-v2` in ONNX, ~30MB, <15ms latency on CPU) to Top-5. Cuts LLM prompt tokens by 95%. |
| **8. Generation LLM** | Small-Weight Generative LLM | **Ollama: `qwen2.5:3b` or `llama3.2:3b`** | Small-weight instruct model loaded entirely into 6GB GPU VRAM (~2.0 GB VRAM), leaving ample headroom for 8k–16k context window. |
| **9. RAG Evaluation** | Continuous Quality & Drift Gate | **Python Eval Suite (`eval.py`)** | Automated Recall@5, Citation Faithfulness, and P95 latency regression test. |

---

## 2. Hardware Resource Budgeting (6GB GPU / 32GB RAM / Ryzen CPU)

Running enterprise RAG locally with a 6GB VRAM constraint requires a deliberate separation between CPU and GPU workloads:

```
+-------------------------------------------------------------------------------+
|                       SYSTEM MEMORY (32 GB RAM)                               |
|                                                                               |
|   [Redpanda Broker]  ~512 MB       [MinIO Storage]    ~256 MB                 |
|   [MongoDB Metadata] ~1.0 GB       [Qdrant Vector DB] ~1.5 GB                 |
|   [KEDA Parser Pods] ~2.0 GB       [Retrieval Gateway]~1.0 GB                 |
|   [FastEmbed ONNX]   ~300 MB (CPU) [FlashRank Reranker] ~150 MB (CPU)         |
|                                                                               |
|   TOTAL RAM CONSUMED: ~6.5 - 8.0 GB  (Available Headroom: >24 GB)             |
+-------------------------------------------------------------------------------+
|                       GPU MEMORY (6.0 GB VRAM)                                |
|                                                                               |
|   [Ollama Engine]                ~400 MB                                      |
|   [Qwen2.5-3B-Instruct (Q4_K_M)] ~1.9 GB                                      |
|   [KV Cache (8,192 Context)]     ~1.2 GB                                      |
|                                                                               |
|   TOTAL VRAM CONSUMED: ~3.5 GB / 6.0 GB  (Safe Headroom: ~2.5 GB)             |
+-------------------------------------------------------------------------------+
```

> **Why this design avoids OOM crashes:**
> By offloading **Embedding (FastEmbed)** and **Reranking (FlashRank)** to the Ryzen CPU using SIMD/AVX ONNX acceleration, the GPU is **100% dedicated to LLM generation**. You will never hit CUDA Out-Of-Memory errors caused by concurrent embedding and generation calls.

---

## 3. Directory Layout

```
rag/
├── docker-compose.yml              # Instant local standalone runner
├── README.md                       # Complete architecture documentation
├── k8s/                            # Production Kubernetes manifests
│   ├── 00-namespace.yaml           # 'local-rag' namespace
│   ├── 01-minio.yaml               # S3 Claim-Check storage
│   ├── 02-redpanda.yaml            # Kafka-compatible event broker
│   ├── 03-mongodb.yaml             # Metadata & Document state store
│   ├── 04-qdrant.yaml              # Dense + Sparse BM25 vector DB
│   ├── 05-ollama.yaml              # Local LLM service with GPU passthrough
│   ├── 06-ingestion.yaml           # Ingestion API
│   ├── 07-worker.yaml              # Stateless parser & PII worker
│   ├── 08-keda-scaler.yaml         # KEDA ScaledObject for consumer lag
│   └── 09-retrieval.yaml           # Hybrid Retrieval & Reranker API
├── services/
│   ├── ingestion/                  # FastAPI Ingestion & Claim-Check Service
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── worker/                     # KEDA Parser, PII Redactor & Upserter
│   │   ├── worker.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── retrieval/                  # Hybrid RRF, Reranker & Ollama Generator
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── eval/                       # RAG Evaluation Quality Gate
│       └── eval.py
└── scripts/
    ├── setup_k8s.sh                # Automated K8s / Minikube setup script
    └── test_pipeline.py            # End-to-end integration & ACL verification
```

---

## 4. Kubernetes Deployment Guide

### Prerequisites
1. **NVIDIA Driver & Container Toolkit** installed on your host / WSL2:
   ```bash
   nvidia-smi
   ```
2. **Minikube** or **K3s** installed.

### Option A: Setup using Minikube (Recommended for Local PC with GPU)

1. **Start Minikube with NVIDIA GPU passthrough:**
   ```bash
   minikube start --driver=docker --gpus=all --cpus=6 --memory=16384
   ```

2. **Install KEDA (Event-driven Autoscaler):**
   ```bash
   helm repo add kedacore https://kedacore.github.io/charts
   helm repo update
   helm install keda kedacore/keda --namespace keda --create-namespace
   ```

3. **Deploy all RAG infrastructure components:**
   ```bash
   kubectl apply -f k8s/00-namespace.yaml
   kubectl apply -f k8s/01-minio.yaml
   kubectl apply -f k8s/02-redpanda.yaml
   kubectl apply -f k8s/03-mongodb.yaml
   kubectl apply -f k8s/04-qdrant.yaml
   kubectl apply -f k8s/05-ollama.yaml
   ```

4. **Pull the small-weight model inside Ollama pod:**
   ```bash
   kubectl exec -it -n local-rag deploy/ollama -- ollama pull qwen2.5:3b
   ```

5. **Build local images (or load into Minikube):**
   ```bash
   eval $(minikube docker-env)
   docker build -t local-rag/ingestion:latest services/ingestion
   docker build -t local-rag/worker:latest services/worker
   docker build -t local-rag/retrieval:latest services/retrieval
   ```

6. **Deploy the application workloads & KEDA scaler:**
   ```bash
   kubectl apply -f k8s/06-ingestion.yaml
   kubectl apply -f k8s/07-worker.yaml
   kubectl apply -f k8s/08-keda-scaler.yaml
   kubectl apply -f k8s/09-retrieval.yaml
   ```

7. **Port-forward for local testing:**
   ```bash
   kubectl port-forward -n local-rag svc/ingestion-api 8000:8000 &
   kubectl port-forward -n local-rag svc/retrieval-api 8002:8000 &
   ```

---

## 5. Quickstart with Docker Compose & Makefile

You can run the entire stack with a single command via the included `Makefile`:

```bash
cd /home/het/rag

# 1. Start all infrastructure & microservices
make compose-up

# 2. Pull the 3B parameter model in Ollama (uses ~1.9GB VRAM)
make ollama-pull

# 3. Open the interactive RAG Web UI in your browser:
# 👉 http://localhost:8002

# 4. Run the end-to-end integration test
make test

# 5. Run the batch generation & KEDA scaling benchmark (20 enterprise docs)
make test-batch
```

---

## 6. End-to-End Pipeline Verification

Run the test script to test ingestion, deduplication, PII redaction, and ACL-filtered retrieval:

```bash
python3 scripts/test_pipeline.py
```

### What `test_pipeline.py` verifies:
1. **Claim-Check Upload:** Uploads a confidential document containing raw PII (SSN, Email, Credit Card) to MinIO and MongoDB.
2. **Deduplication / ACL-Only Fast Lane:** Re-uploads the same document with new ACL tags. The ingestion service recognizes the identical SHA-256 hash, updates the ACLs in MongoDB, and bypasses redundant Kafka/embedding processing.
3. **KEDA Worker Ingestion:** The worker picks up the Kafka event, downloads from MinIO, redacts PII using Presidio/regex, produces Dense + Sparse embeddings, and idempotently upserts to Qdrant.
4. **Authorized Query (ACL Match):** Executes a hybrid query (Dense + BM25) with `user_acl_groups: ["finance"]`. Returns Top-5 reranked citations and a grounded response from the local LLM.
5. **Unauthorized Query (ACL Mismatch):** Executes the same query with `user_acl_groups: ["marketing"]`. Mandatory Qdrant pre-filter blocks unauthorized access, returning zero citations.

---

## 7. RAG Evaluation CI/CD Quality Gate

Run the automated evaluation suite:

```bash
python3 services/eval/eval.py
```

Checks:
- **Recall@5** (target > 80%)
- **Citation Faithfulness**
- **P95 Latency Regression**
- Fails the build/deploy step if performance drops by > 1%.
