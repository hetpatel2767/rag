import io
import time
import requests
import json

INGESTION_URL = "http://localhost:8000/api/v1/documents/upload"
STATUS_URL = "http://localhost:8000/api/v1/documents"
RETRIEVAL_URL = "http://localhost:8002/api/v1/rag/query"

SAMPLE_DOC_CONTENT = """
ACME ENTERPRISE EXECUTIVE POLICY & BENEFITS 2026

1. Confidential Executive Compensation & Retention
Executive retention bonuses will be disbursed quarterly on the last business day of each quarter.
All disbursements are managed by Johnathan Doe (Email: john.doe@acmewidgets.com, Phone: 415-555-0199, SSN: 987-65-4321).
Payment card on file for travel incidentals: 4111-2222-3333-4444.

2. Annual Leave and Severance Terms
Executives with over 2 years of tenure are entitled to 30 days of paid executive leave.
Severance terms dictate a minimum 6-month base salary package upon mutual separation, subject to board approval.
Invoices and corporate billing must strictly follow Net 30 payment terms.
"""

def run_test():
    print("=== Step 1: Upload Document with PII via Claim-Check Ingestion Pipeline ===")
    file_bytes = SAMPLE_DOC_CONTENT.encode("utf-8")
    files = {"file": ("acme_executive_policy_2026.txt", io.BytesIO(file_bytes), "text/plain")}
    data = {
        "tenant_id": "tenant_alpha",
        "acl_groups": "finance,exec",
        "priority": "realtime"
    }

    try:
        r = requests.post(INGESTION_URL, files=files, data=data, timeout=10)
        print("Ingestion Response:", json.dumps(r.json(), indent=2))
        doc_id = r.json().get("doc_id")
    except Exception as e:
        print(f"Could not connect to Ingestion API at {INGESTION_URL}: {e}")
        print("Make sure the pipeline services are running!")
        return

    print("\n=== Step 2: Test Deduplication & ACL-Only Fast Lane ===")
    files2 = {"file": ("acme_executive_policy_2026.txt", io.BytesIO(file_bytes), "text/plain")}
    data2 = {
        "tenant_id": "tenant_alpha",
        "acl_groups": "legal,board",
        "priority": "realtime"
    }
    r_dedup = requests.post(INGESTION_URL, files=files2, data=data2, timeout=10)
    print("Dedup Response:", json.dumps(r_dedup.json(), indent=2))

    print("\n=== Step 3: Wait for KEDA Parser Worker to Extract, Redact PII, & Index Chunks ===")
    for _ in range(15):
        time.sleep(2)
        try:
            status_resp = requests.get(f"{STATUS_URL}/{doc_id}", timeout=5)
            status_data = status_resp.json()
            print(f"Doc Status: {status_data.get('status')} | Total Chunks: {status_data.get('total_chunks', 0)}")
            if status_data.get("status") in ["INDEXED", "FAILED"]:
                break
        except Exception as e:
            print("Checking status...", e)

    print("\n=== Step 4: Query with Authorized ACLs (finance) ===")
    query_payload = {
        "query": "What are the payment terms and severance conditions for executives?",
        "tenant_id": "tenant_alpha",
        "user_acl_groups": ["finance"],
        "top_k_candidates": 20,
        "rerank_top_n": 3
    }
    try:
        q_resp = requests.post(RETRIEVAL_URL, json=query_payload, timeout=60)
        print("Authorized Query Response:")
        print(json.dumps(q_resp.json(), indent=2))
    except Exception as e:
        print(f"Retrieval error: {e}")

    print("\n=== Step 5: Query with Unauthorized ACLs (guest/marketing) - Expect 0 Citations ===")
    unauth_payload = {
        "query": "What are the payment terms and severance conditions for executives?",
        "tenant_id": "tenant_alpha",
        "user_acl_groups": ["marketing"],
        "top_k_candidates": 20,
        "rerank_top_n": 3
    }
    try:
        unauth_resp = requests.post(RETRIEVAL_URL, json=unauth_payload, timeout=10)
        print("Unauthorized Query Response:")
        print(json.dumps(unauth_resp.json(), indent=2))
    except Exception as e:
        print(f"Retrieval error: {e}")

if __name__ == "__main__":
    run_test()
