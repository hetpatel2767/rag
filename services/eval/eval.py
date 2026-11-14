import time
import requests
import statistics
import json

EVAL_SUITE = [
    {
        "query": "What are the payment terms specified in the agreement?",
        "tenant_id": "default_tenant",
        "user_acl_groups": ["public", "finance"],
        "expected_doc": "contract_2026.pdf",
        "gold_keywords": ["net 30", "invoice", "payment"]
    },
    {
        "query": "What is the employee health insurance deductible?",
        "tenant_id": "default_tenant",
        "user_acl_groups": ["hr", "employee"],
        "expected_doc": "benefits_guide.pdf",
        "gold_keywords": ["deductible", "$1,000", "in-network"]
    }
]

def run_evaluation(api_url: str = "http://localhost:8002/api/v1/rag/query"):
    latencies = []
    recalls = []
    faithfulness_scores = []

    print(f"Starting RAG Evaluation Suite against {api_url}...")

    for test in EVAL_SUITE:
        payload = {
            "query": test["query"],
            "tenant_id": test["tenant_id"],
            "user_acl_groups": test["user_acl_groups"],
            "top_k_candidates": 50,
            "rerank_top_n": 5
        }
        t0 = time.perf_counter()
        resp = requests.post(api_url, json=payload, timeout=60)
        dur = (time.perf_counter() - t0) * 1000
        latencies.append(dur)

        if resp.status_code == 200:
            data = resp.json()
            citations = data.get("citations", [])
            answer = data.get("answer", "")

            # Recall@5 check: did expected doc appear in top-5 citations?
            hit = any(test["expected_doc"].lower() in c["filename"].lower() for c in citations)
            recalls.append(1.0 if hit else 0.0)

            # Faithfulness check: does answer contain expected gold keywords?
            keyword_matches = sum(1 for kw in test["gold_keywords"] if kw.lower() in answer.lower())
            faithfulness = keyword_matches / max(len(test["gold_keywords"]), 1)
            faithfulness_scores.append(faithfulness)
        else:
            recalls.append(0.0)
            faithfulness_scores.append(0.0)

    p95_latency = statistics.quantiles(latencies, n=20)[-1] if len(latencies) >= 20 else max(latencies)
    avg_recall = statistics.mean(recalls)
    avg_faithfulness = statistics.mean(faithfulness_scores)

    results = {
        "tests_run": len(EVAL_SUITE),
        "recall_at_5": round(avg_recall, 4),
        "citation_faithfulness": round(avg_faithfulness, 4),
        "p95_latency_ms": round(p95_latency, 2)
    }

    print("\n=== Evaluation Results ===")
    print(json.dumps(results, indent=2))

    # Auto-blocking regression check
    if avg_recall < 0.8:
        print("[FAIL] Recall@5 dropped below 80% threshold!")
        return False
    print("[PASS] RAG Quality Gate passed successfully.")
    return True

if __name__ == "__main__":
    run_evaluation()
