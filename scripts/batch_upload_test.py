#!/usr/bin/env python3
"""
Batch Ingestion Script for Local RAG Pipeline:
- Ingests all 20 test files (PDF & DOCX) in batch to build queue lag on Kafka.
- Triggers KEDA autoscaler from 0 up to 4 parser-worker pods.
- Monitors document processing status until all 20 files are indexed.
"""

import os
import sys
import glob
import time
import requests
import subprocess
import json

DOCS_DIR = "/home/het/rag/batch_test_docs"
INGESTION_URL = os.getenv("INGESTION_URL", "http://localhost:8000/api/v1/documents/upload")
STATUS_URL = os.getenv("STATUS_URL", "http://localhost:8000/api/v1/documents")

def check_keda_pods():
    """Check running parser-worker pod count via kubectl if available."""
    try:
        res = subprocess.run(
            ["kubectl", "get", "pods", "-n", "local-rag", "-l", "app=parser-worker", "--no-headers"],
            capture_output=True, text=True, timeout=5
        )
        if res.returncode == 0:
            lines = [l for l in res.stdout.strip().split("\n") if l.strip()]
            return len(lines), res.stdout.strip()
    except Exception:
        pass
    return None, None

def run_batch_upload():
    files = sorted(glob.glob(os.path.join(DOCS_DIR, "*.*")))
    if not files:
        print(f"Error: No documents found in {DOCS_DIR}!")
        return

    print("================================================================")
    print(f"  Starting Batch Ingestion of {len(files)} Enterprise Documents")
    print(f"  Target Ingestion API: {INGESTION_URL}")
    print("================================================================\n")

    initial_pods, pod_out = check_keda_pods()
    if initial_pods is not None:
        print(f"[*] Initial KEDA parser-worker pods running: {initial_pods}")
    else:
        print("[*] (Kubectl not directly connected or running standalone)")

    uploaded_doc_ids = []

    # Rapid dispatch to create Kafka consumer lag and trigger KEDA
    print("\n[Stage 1] Dispatching documents in rapid succession...")
    t_start = time.time()
    for idx, filepath in enumerate(files, 1):
        filename = os.path.basename(filepath)
        content_type = "application/pdf" if filename.endswith(".pdf") else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        
        with open(filepath, "rb") as f:
            file_bytes = f.read()

        payload = {
            "tenant_id": "aegis_corp",
            "acl_groups": "engineering,exec,public",
            "priority": "realtime"  # routes to doc-ingest-realtime (lag threshold = 5)
        }
        multipart_files = {
            "file": (filename, file_bytes, content_type)
        }

        try:
            r = requests.post(INGESTION_URL, files=multipart_files, data=payload, timeout=10)
            if r.status_code == 200:
                doc_id = r.json().get("doc_id")
                status = r.json().get("status")
                uploaded_doc_ids.append((filename, doc_id))
                print(f"  [{idx:02d}/{len(files)}] Dispatched: {filename[:45]:<45} -> Status: {status} (doc_id: {doc_id[:8]}...)")
            else:
                print(f"  [{idx:02d}/{len(files)}] Failed: {filename} -> HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"  [{idx:02d}/{len(files)}] Connection error for {filename}: {e}")

    dispatch_duration = time.time() - t_start
    print(f"\n[+] Dispatched {len(uploaded_doc_ids)} documents in {dispatch_duration:.2f}s!")
    print("[*] Redpanda queue has high consumer lag. KEDA should now scale parser-worker up to 4 pods.\n")

    # Monitor KEDA pod scaling & Ingestion status
    print("[Stage 2] Monitoring KEDA Pod Scaling & Ingestion Progress...")
    max_wait_seconds = 180
    start_poll = time.time()

    while time.time() - start_poll < max_wait_seconds:
        pod_count, _ = check_keda_pods()
        pod_info = f"Pods: {pod_count}" if pod_count is not None else "Standalone"

        indexed_count = 0
        processing_count = 0
        queued_count = 0
        failed_count = 0

        for filename, doc_id in uploaded_doc_ids:
            try:
                sr = requests.get(f"{STATUS_URL}/{doc_id}", timeout=5)
                if sr.status_code == 200:
                    st = sr.json().get("status")
                    if st == "INDEXED":
                        indexed_count += 1
                    elif st == "PROCESSING":
                        processing_count += 1
                    elif st == "QUEUED":
                        queued_count += 1
                    elif st == "FAILED":
                        failed_count += 1
            except Exception:
                pass

        print(f"  -> [{int(time.time() - start_poll):02d}s elapsed | {pod_info}] Indexed: {indexed_count}/{len(uploaded_doc_ids)} | Processing: {processing_count} | Queued: {queued_count} | Failed: {failed_count}")

        if indexed_count == len(uploaded_doc_ids):
            print(f"\n[SUCCESS] All {indexed_count} documents indexed successfully in {time.time() - start_poll:.1f}s!")
            break

        time.sleep(3)

    print("\n================================================================")
    print("  Batch Ingestion Completed!")
    print("  You can now run your test queries via Chrome UI or script.")
    print("================================================================")

if __name__ == "__main__":
    run_batch_upload()
