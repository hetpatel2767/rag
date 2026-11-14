# Contributing to Local Enterprise RAG

Thank you for contributing! This guide provides instructions on how to set up, develop, and test changes to the Local Enterprise RAG system.

---

## 1. Architecture Guidelines

- **CPU-First Embedding & Reranking:**  
  Embedding (`fastembed`) and reranking (`flashrank`) must remain accelerated via ONNX Runtime on the CPU to protect the strict **6GB GPU VRAM** budget for the local LLM.
- **Mandatory Pre-Filtering:**  
  All retrieval queries must enforce tenant and ACL boundaries at the vector database level before vector similarity scoring.
- **PII Compliance:**  
  Raw personal information (emails, SSNs, credit cards, phone numbers) must be redacted before text chunks are embedded and indexed.

---

## 2. Local Development Workflow

1. **Clone and Setup:**
   ```bash
   git clone <repo-url>
   cd rag
   cp .env.example .env
   ```

2. **Run Containers with Docker Compose:**
   ```bash
   make compose-up
   make ollama-pull
   ```

3. **Run Verification Tests:**
   ```bash
   make test
   make test-batch
   make eval
   ```

4. **Code Quality:**
   Ensure clean formatting and no syntax errors:
   ```bash
   make clean
   flake8 . --exclude=.venv,batch_test_docs
   ```

---

## 3. Submitting Changes

1. Create a feature branch: `git checkout -b feature/my-cool-feature`
2. Commit changes with clear, descriptive commit messages.
3. Push to your branch and open a Pull Request.
