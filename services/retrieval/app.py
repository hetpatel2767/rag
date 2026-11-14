import os
import time
import logging
import re
import urllib.parse
from typing import List, Dict, Any, Optional
import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from fastembed import TextEmbedding, SparseTextEmbedding
from flashrank import Ranker, RerankRequest
from qdrant_client import QdrantClient
from qdrant_client.http import models

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rag-retrieval")

app = FastAPI(title="Local RAG Hybrid Retrieval & Generation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "rag_documents")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "rag_metadata")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:3b")

INGESTION_SERVICE_URL = os.getenv("INGESTION_SERVICE_URL", "http://ingestion-api:8000")

# Initialize Vector DB & Mongo
qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB_NAME]
metrics_col = db["query_metrics"]

# Initialize Open-Source Embeddings (Dense & Sparse)
logger.info("Initializing query embedders...")
dense_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

# Initialize High-Speed CPU Cross-Encoder Reranker (FlashRank - ms-marco-MiniLM-L-12-v2)
logger.info("Initializing FlashRank local cross-encoder reranker...")
reranker = Ranker(model_name="ms-marco-MiniLM-L-12-v2")

# Initialize MongoDB Hackathon Governance Engines (Statements 1, 2, 3)
from governance.identity_policy import IdentityPolicyEngine
from governance.governed_memory import GovernedMemoryEngine
from governance.durable_tasks import DurableTaskEngine

governance_engine = IdentityPolicyEngine(db)
memory_engine = GovernedMemoryEngine(db, embed_fn=dense_model.embed)
task_engine = DurableTaskEngine(db, embed_fn=dense_model.embed)

class QueryRequest(BaseModel):
    query: str
    tenant_id: str = "default_tenant"
    actor_identity: str = "user_analyst_01"  # Statement One
    user_acl_groups: List[str] = ["public", "engineering", "exec"]
    top_k_candidates: int = 50
    rerank_top_n: int = 5
    enable_web_fallback: bool = True
    stream: bool = False
    trace_id: Optional[str] = None

class Citation(BaseModel):
    doc_id: str
    filename: str
    page_number: int
    chunk_index: int
    score: float
    text: str

class QueryResponse(BaseModel):
    answer: str
    source_type: str  # 'local_knowledge_base' or 'public_internet' or 'policy_block' or 'none'
    web_fallback_used: bool = False
    citations: List[Citation]
    retrieval_latency_ms: float
    rerank_latency_ms: float
    total_latency_ms: float
    trace_id: Optional[str] = None
    policy_decision: str = "ALLOWED"
    governed_memories_used: List[Dict[str, Any]] = []

async def search_public_web(query: str) -> List[Dict[str, str]]:
    """
    Public web search fallback using Wikipedia OpenSearch API.
    Zero external API key required.
    """
    results = []
    try:
        encoded_query = urllib.parse.quote(query)
        async with httpx.AsyncClient(timeout=8.0) as client:
            wiki_url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch={encoded_query}&utf8=1&format=json"
            wiki_resp = await client.get(wiki_url, headers={"User-Agent": "LocalRAG-WebFallback/1.0"})
            if wiki_resp.status_code == 200:
                data = wiki_resp.json()
                for item in data.get("query", {}).get("search", [])[:3]:
                    clean_text = re.sub(r'<[^>]+>', '', item.get("snippet", ""))
                    results.append({
                        "title": item.get("title"),
                        "text": clean_text,
                        "url": f"https://en.wikipedia.org/wiki/{urllib.parse.quote(item.get('title', ''))}"
                    })
    except Exception as e:
        logger.warning(f"Public web search fallback error: {e}")
    return results

def compute_rrf(dense_hits: List[Any], sparse_hits: List[Any], k: int = 60) -> List[Dict[str, Any]]:
    """
    Reciprocal Rank Fusion (RRF) to merge dense and sparse rank results.
    RRF Score = sum(1 / (k + rank))
    """
    scores = {}
    docs = {}

    for rank, hit in enumerate(dense_hits):
        doc_id = hit.id
        scores[doc_id] = scores.get(doc_id, 0.0) + (1.0 / (k + rank + 1))
        docs[doc_id] = hit.payload

    for rank, hit in enumerate(sparse_hits):
        doc_id = hit.id
        scores[doc_id] = scores.get(doc_id, 0.0) + (1.0 / (k + rank + 1))
        docs[doc_id] = hit.payload

    sorted_doc_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)
    return [{"id": did, "rrf_score": scores[did], "payload": docs[did]} for did in sorted_doc_ids]

def build_acl_filter(tenant_id: str, acl_groups: List[str]) -> models.Filter:
    """
    Step 6: Mandatory Pre-Filter:
    WHERE tenant_id == X AND acl_groups INTERSECT user_acl_groups
    """
    return models.Filter(
        must=[
            models.FieldCondition(
                key="tenant_id",
                match=models.MatchValue(value=tenant_id)
            ),
            models.FieldCondition(
                key="acl_groups",
                match=models.MatchAny(any=acl_groups)
            )
        ]
    )

@app.post("/api/v1/rag/query", response_model=QueryResponse)
async def query_rag(req: QueryRequest):
    t0 = time.perf_counter()
    import uuid

    trace_id = req.trace_id or str(uuid.uuid4())
    primary_acl = req.user_acl_groups[0] if req.user_acl_groups else "public"

    # Statement One: Pre-Execution Policy Enforcement via MongoDB
    is_allowed, pol_id, pol_reason = governance_engine.evaluate_policy(
        actor_id=req.actor_identity,
        action="rag:query",
        resource_context={"target_acl_group": primary_acl}
    )
    if not is_allowed:
        governance_engine.record_audit_step(
            trace_id=trace_id,
            step_index=1,
            actor_id=req.actor_identity,
            action="rag:query",
            target_resource="rag_documents",
            policy_eval=(False, pol_id, pol_reason),
            inputs={"query": req.query, "acls": req.user_acl_groups},
            citations_used=[],
            llm_reasoning="Action preemptively blocked by MongoDB identity policy layer before execution.",
            output_summary="Access Denied"
        )
        return QueryResponse(
            answer=f"[POLICY BLOCKED]: {pol_reason}",
            source_type="policy_block",
            web_fallback_used=False,
            citations=[],
            retrieval_latency_ms=0.0,
            rerank_latency_ms=0.0,
            total_latency_ms=round((time.perf_counter() - t0) * 1000, 2),
            trace_id=trace_id,
            policy_decision="DENIED",
            governed_memories_used=[]
        )

    # Statement Two: Retrieve Governed Memories from MongoDB
    retrieved_memories = memory_engine.retrieve_governed_memories(
        query=req.query,
        user_roles=req.user_acl_groups,
        top_k=2
    )

    # 1. Embed query (Dense + Sparse)
    query_dense = list(dense_model.embed([req.query]))[0].tolist()
    query_sparse = list(sparse_model.embed([req.query]))[0]

    acl_filter = build_acl_filter(req.tenant_id, req.user_acl_groups)

    # 2. Parallel hybrid fetch from Qdrant with ACL Pre-filtering
    dense_results = []
    sparse_results = []
    try:
        dense_results = qdrant_client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=("dense", query_dense),
            query_filter=acl_filter,
            limit=req.top_k_candidates
        )

        sparse_results = qdrant_client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=models.NamedSparseVector(
                name="sparse",
                vector=models.SparseVector(
                    indices=query_sparse.indices.tolist(),
                    values=query_sparse.values.tolist()
                )
            ),
            query_filter=acl_filter,
            limit=req.top_k_candidates
        )
    except Exception as e:
        logger.warning(f"Qdrant search warning (collection may not exist yet or empty): {e}")

    t_retrieval = time.perf_counter()
    retrieval_ms = (t_retrieval - t0) * 1000

    # 3. Hybrid Reciprocal Rank Fusion
    fused_candidates = compute_rrf(dense_results, sparse_results, k=60)
    top_candidates = fused_candidates[:req.top_k_candidates]

    top_reranked = []
    rerank_ms = 0.0

    if top_candidates:
        t_rerank_start = time.perf_counter()
        passages = [
            {"id": c["id"], "text": c["payload"]["text"], "meta": c["payload"]}
            for c in top_candidates
        ]
        rerank_req = RerankRequest(query=req.query, passages=passages)
        rerank_results = reranker.rerank(rerank_req)
        top_reranked = rerank_results[:req.rerank_top_n]
        rerank_ms = (time.perf_counter() - t_rerank_start) * 1000

    # Determine whether local documents are sufficient or if we fall back to public internet
    # Score threshold: if highest score is < 0.05, local context is irrelevant unless governed memories exist
    highest_score = max([float(x.get("score", 0.0)) for x in top_reranked], default=0.0)
    local_match_found = (len(top_reranked) > 0 and highest_score >= 0.05) or (len(retrieved_memories) > 0)

    # Case A: Fallback to Public Internet
    if not local_match_found and req.enable_web_fallback:
        web_snippets = await search_public_web(req.query)
        citations = []
        if web_snippets:
            web_context_blocks = []
            for idx, w in enumerate(web_snippets, 1):
                cit = Citation(
                    doc_id=f"web-{idx}",
                    filename=f"Public Internet / Wikipedia: {w['title']}",
                    page_number=1,
                    chunk_index=idx,
                    score=0.90,
                    text=f"{w['text']} [Source URL: {w['url']}]"
                )
                citations.append(cit)
                web_context_blocks.append(f"[Web Source {idx} | {w['title']}]:\n{w['text']}\nURL: {w['url']}")

            context_str = "\n\n".join(web_context_blocks)
            system_prompt = (
                "You are an intelligent enterprise assistant.\n"
                "NOTICE: No relevant documents were found in the local company repository for this query.\n"
                "The system has fallen back to the PUBLIC INTERNET to find relevant information.\n"
                "State clearly in your response that no local internal documents matched and that this answer is derived from public internet sources.\n"
                "Cite the sources inline as [Web Source X]."
            )
            user_prompt = f"Public Web Context:\n{context_str}\n\nQuestion: {req.query}\nAnswer:"
        else:
            system_prompt = (
                "You are an enterprise assistant. Inform the user that neither local internal company files "
                "nor public internet searches contained specific matching records for this query."
            )
            user_prompt = f"Question: {req.query}\nAnswer:"

        llm_payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False,
            "options": {"temperature": 0.2, "num_ctx": 4096}
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=llm_payload)
                resp.raise_for_status()
                answer_text = resp.json()["message"]["content"]
        except Exception as e:
            answer_text = f"[PUBLIC INTERNET FALLBACK]: No local documents matched. Query routed to internet fallback. (LLM note: {e})"

        total_ms = (time.perf_counter() - t0) * 1000

        governance_engine.record_audit_step(
            trace_id=trace_id,
            step_index=1,
            actor_id=req.actor_identity,
            action="rag:query",
            target_resource="public_web",
            policy_eval=(True, "POL_ALLOW_DEFAULT", "Query permitted; routed to public internet fallback"),
            inputs={"query": req.query, "acls": req.user_acl_groups},
            citations_used=[{"file": c.filename, "page": c.page_number, "score": c.score} for c in citations],
            llm_reasoning="Synthesized response via public internet fallback.",
            output_summary=answer_text[:250]
        )

        return QueryResponse(
            answer=answer_text,
            source_type="public_internet",
            web_fallback_used=True,
            citations=citations,
            retrieval_latency_ms=round(retrieval_ms, 2),
            rerank_latency_ms=round(rerank_ms, 2),
            total_latency_ms=round(total_ms, 2),
            trace_id=trace_id,
            policy_decision="ALLOWED",
            governed_memories_used=retrieved_memories
        )

    # Case B: No local documents and web fallback is disabled
    if not local_match_found:
        total_ms = (time.perf_counter() - t0) * 1000

        governance_engine.record_audit_step(
            trace_id=trace_id,
            step_index=1,
            actor_id=req.actor_identity,
            action="rag:query",
            target_resource="rag_documents",
            policy_eval=(True, "POL_ALLOW_DEFAULT", "Query permitted by policy; no matching documents in repository"),
            inputs={"query": req.query, "acls": req.user_acl_groups},
            citations_used=[],
            llm_reasoning="No local document matches found.",
            output_summary="No matching documents found"
        )

        return QueryResponse(
            answer="No relevant documents found for your query matching your access permissions in the local repository.",
            source_type="none",
            web_fallback_used=False,
            citations=[],
            retrieval_latency_ms=round(retrieval_ms, 2),
            rerank_latency_ms=round(rerank_ms, 2),
            total_latency_ms=round(total_ms, 2),
            trace_id=trace_id,
            policy_decision="ALLOWED",
            governed_memories_used=retrieved_memories
        )

    # Case C: Local Documents or Governed Memories Found -> 100% Grounded, Bypasses Public Internet!
    citations = []
    context_blocks = []
    for rank_idx, item in enumerate(top_reranked, 1):
        meta = item.get("meta", {})
        cit = Citation(
            doc_id=meta.get("doc_id", "unknown"),
            filename=meta.get("filename", "unknown"),
            page_number=meta.get("page_number", 1),
            chunk_index=meta.get("chunk_index", 0),
            score=round(float(item.get("score", 0.0)), 4),
            text=item.get("text", "")
        )
        citations.append(cit)
        context_blocks.append(
            f"[Source {rank_idx} | File: {cit.filename} | Page: {cit.page_number}]:\n{cit.text}"
        )

    memory_context_str = ""
    if retrieved_memories:
        mem_lines = [f"[Governed Memory v{m['version']} | ACL: {m['read_acl']}]: {m['content']}" for m in retrieved_memories]
        memory_context_str = "Governed Past Memories & User Corrections (Retrieved from MongoDB):\n" + "\n".join(mem_lines) + "\n\n"
        if not citations:
            for idx, m in enumerate(retrieved_memories, 1):
                citations.append(Citation(
                    doc_id=m["memory_id"],
                    filename=f"MongoDB Governed Memory (v{m['version']})",
                    page_number=1,
                    chunk_index=idx,
                    score=round(float(m.get("similarity", 1.0)), 4),
                    text=m["content"]
                ))

    context_str = "\n\n".join(context_blocks) if context_blocks else "No file passages; grounded strictly on governed memory policies."

    system_prompt = (
        "You are an enterprise assistant. Answer the user question strictly using the provided internal context passages and governed memories below.\n"
        "Cite the sources with [Source X | File: <filename> | Page: <page>] or [Governed Memory vX] inline after statements.\n"
        "If governed past memories or corrections apply, incorporate them respectfully into your reasoning.\n"
        "Do not invent facts not present in the documents. Public internet was NOT used for this response."
    )
    user_prompt = f"{memory_context_str}Internal Context Passages:\n{context_str}\n\nQuestion: {req.query}\nAnswer:"

    llm_payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_ctx": 4096
        }
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=llm_payload)
            resp.raise_for_status()
            answer_text = resp.json()["message"]["content"]
    except Exception as e:
        logger.error(f"Error calling local LLM at {OLLAMA_BASE_URL}: {e}")
        answer_text = f"[LLM Offline]: Retrieved {len(citations)} local citations, but failed to connect to local LLM: {str(e)}"

    total_ms = (time.perf_counter() - t0) * 1000

    # Statement One: Record Replay Audit Step in MongoDB
    governance_engine.record_audit_step(
        trace_id=trace_id,
        step_index=1,
        actor_id=req.actor_identity,
        action="rag:query",
        target_resource="rag_documents",
        policy_eval=(True, "POL_ALLOW_DEFAULT", "Query verified by MongoDB policy engine"),
        inputs={"query": req.query, "acls": req.user_acl_groups},
        citations_used=[{"file": c.filename, "page": c.page_number, "score": c.score} for c in citations],
        llm_reasoning=f"Answered with {len(citations)} citations and {len(retrieved_memories)} active governed memories.",
        output_summary=answer_text[:250]
    )

    metrics_col.insert_one({
        "query": req.query,
        "tenant_id": req.tenant_id,
        "source_type": "local_knowledge_base",
        "retrieval_ms": retrieval_ms,
        "rerank_ms": rerank_ms,
        "total_ms": total_ms,
        "top_rerank_scores": [c.score for c in citations],
        "citations_count": len(citations),
        "timestamp": time.time()
    })

    return QueryResponse(
        answer=answer_text,
        source_type="local_knowledge_base",
        web_fallback_used=False,
        citations=citations,
        retrieval_latency_ms=round(retrieval_ms, 2),
        rerank_latency_ms=round(rerank_ms, 2),
        total_latency_ms=round(total_ms, 2),
        trace_id=trace_id,
        policy_decision="ALLOWED",
        governed_memories_used=retrieved_memories
    )

# -------------------------------------------------------------
# Interactive Chrome Web UI
# -------------------------------------------------------------
HTML_UI = """<!DOCTYPE html>
<html lang="en" class="h-full bg-slate-950 text-slate-100">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Enterprise Local RAG Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: #0f172a; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #475569; }
  </style>
</head>
<body class="h-full flex flex-col font-sans">
  <!-- Top Navigation -->
  <header class="bg-slate-900 border-b border-slate-800 px-6 py-4 flex items-center justify-between">
    <div class="flex items-center space-x-3">
      <div class="w-10 h-10 rounded-lg bg-indigo-600 flex items-center justify-center text-white font-bold text-xl shadow-lg shadow-indigo-500/20">
        <i class="fa-solid fa-brain"></i>
      </div>
      <div>
        <h1 class="text-lg font-bold tracking-tight text-white flex items-center gap-2">
          Local RAG System
          <span class="text-xs bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 px-2 py-0.5 rounded-full font-mono">v1.0.0</span>
        </h1>
        <p class="text-xs text-slate-400">Hybrid Dense + Sparse BM25 + FlashRank Reranker + KEDA Autoscaling</p>
      </div>
    </div>
    
    <div class="flex items-center gap-4 text-xs font-mono">
      <div class="flex items-center gap-2 bg-slate-800/80 border border-slate-700/60 px-3 py-1.5 rounded-lg">
        <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse"></span>
        <span class="text-slate-300">FastEmbed ONNX (CPU)</span>
      </div>
      <div class="flex items-center gap-2 bg-slate-800/80 border border-slate-700/60 px-3 py-1.5 rounded-lg">
        <span class="w-2.5 h-2.5 rounded-full bg-indigo-400"></span>
        <span class="text-slate-300">Ollama LLM (GPU)</span>
      </div>
      <div class="flex items-center gap-2 bg-slate-800/80 border border-slate-700/60 px-3 py-1.5 rounded-lg">
        <span class="w-2.5 h-2.5 rounded-full bg-amber-400"></span>
        <span class="text-slate-300">KEDA Scaler (0 &rarr; 4 Pods)</span>
      </div>
    </div>
  </header>

  <!-- Main Grid Content -->
  <main class="flex-1 grid grid-cols-12 gap-6 p-6 overflow-hidden">
    <!-- Left Column: Ingestion & Batch Testing -->
    <section class="col-span-12 lg:col-span-4 flex flex-col gap-5 overflow-y-auto pr-1">
      <!-- Batch Processing Card -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
        <div class="flex items-center justify-between mb-3">
          <h2 class="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <i class="fa-solid fa-bolt text-amber-400"></i>
            KEDA Batch Ingestion
          </h2>
          <span class="text-xs text-slate-400 font-mono">20 Test Docs</span>
        </div>
        <p class="text-xs text-slate-400 mb-4 leading-relaxed">
          Dispatches 10 PDFs & 10 Word DOCX files to Kafka queue in rapid succession. Creates lag and triggers KEDA autoscaling from 0 to 4 parser pods.
        </p>

        <button id="batchBtn" onclick="runBatchIngestion()" class="w-full bg-gradient-to-r from-amber-500 to-orange-600 hover:from-amber-600 hover:to-orange-700 text-white font-medium py-2.5 px-4 rounded-lg text-xs flex items-center justify-center gap-2 transition duration-150 shadow-lg shadow-orange-500/20">
          <i class="fa-solid fa-play"></i>
          Trigger 20-Doc Batch Ingestion
        </button>

        <div id="batchLogBox" class="mt-4 bg-slate-950 border border-slate-800 rounded-lg p-3 text-[11px] font-mono text-slate-300 max-h-40 overflow-y-auto hidden">
          <div id="batchLogs" class="space-y-1"></div>
        </div>
      </div>

      <!-- Quick Upload Single Document Card -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
        <h2 class="text-sm font-semibold text-slate-200 flex items-center gap-2 mb-3">
          <i class="fa-solid fa-file-arrow-up text-indigo-400"></i>
          Upload Document (PDF / DOCX)
        </h2>
        <div class="border-2 border-dashed border-slate-700 hover:border-indigo-500 rounded-lg p-4 text-center cursor-pointer transition" onclick="document.getElementById('fileInput').click()">
          <input type="file" id="fileInput" class="hidden" accept=".pdf,.docx,.doc,.txt" onchange="uploadSingleFile(this.files[0])">
          <i class="fa-solid fa-cloud-arrow-up text-2xl text-slate-500 mb-2"></i>
          <p class="text-xs text-slate-300 font-medium">Click to select file or drop here</p>
          <p class="text-[10px] text-slate-500 mt-1">Supports PDF, Word (.docx), TXT</p>
        </div>
        <div id="uploadStatus" class="mt-2 text-xs font-mono text-slate-400"></div>
      </div>

      <!-- Test Presets Card -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl flex-1">
        <h2 class="text-sm font-semibold text-slate-200 flex items-center gap-2 mb-3">
          <i class="fa-solid fa-vial text-cyan-400"></i>
          Test Scenario Presets
        </h2>
        <div class="space-y-2">
          <button onclick="setQuery('What is the proprietary propellant mixture and operating chamber pressure for Project Hyperion\\'s cold-gas plasma thruster?')" class="w-full text-left bg-slate-800/80 hover:bg-slate-800 border border-slate-700/60 p-3 rounded-lg text-xs text-slate-300 transition">
            <span class="block font-semibold text-amber-400 mb-1">🧪 Step 1 & 2 Test Prompt (Aegis Hyperion Engine)</span>
            <span class="text-slate-400 text-[11px] block">"What is the proprietary propellant mixture and operating chamber pressure for Project Hyperion's cold-gas plasma thruster?"</span>
          </button>

          <button onclick="setQuery('Under what conditions is Code Amber-7 triggered for the Hyperion engine, and who must authorize it?')" class="w-full text-left bg-slate-800/80 hover:bg-slate-800 border border-slate-700/60 p-3 rounded-lg text-xs text-slate-300 transition">
            <span class="block font-semibold text-emerald-400 mb-1">🧪 Fail-Safe Protocol (Word DOCX Test)</span>
            <span class="text-slate-400 text-[11px] block">"Under what conditions is Code Amber-7 triggered for the Hyperion engine, and who must authorize it?"</span>
          </button>

          <button onclick="setQuery('What are the latest developments in quantum computing and qubits?')" class="w-full text-left bg-slate-800/80 hover:bg-slate-800 border border-slate-700/60 p-3 rounded-lg text-xs text-slate-300 transition">
            <span class="block font-semibold text-sky-400 mb-1">🌐 Public Internet Fallback Test</span>
            <span class="text-slate-400 text-[11px] block">"What are the latest developments in quantum computing and qubits?"</span>
          </button>
        </div>
      </div>
    </section>

    <!-- Right Column: Query & Response Interface -->
    <section class="col-span-12 lg:col-span-8 flex flex-col gap-4 overflow-hidden">
      <!-- Query Input Box -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
        <label for="queryInput" class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-2">
          Enter Your Question
        </label>
        <div class="relative">
          <textarea id="queryInput" rows="3" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 font-sans" placeholder="Ask a question about internal documents, policies, or technical specifications..."></textarea>
        </div>

        <div class="flex items-center justify-between mt-3 pt-3 border-t border-slate-800">
          <div class="flex items-center gap-3">
            <label class="flex items-center gap-2 cursor-pointer text-xs text-slate-300 select-none">
              <input type="checkbox" id="webFallbackToggle" checked class="rounded border-slate-700 bg-slate-950 text-indigo-600 focus:ring-indigo-500 w-4 h-4">
              <span>Enable Public Internet Fallback</span>
            </label>
            <span class="text-slate-600">|</span>
            <span class="text-xs text-slate-400 font-mono">Tenant: aegis_corp</span>
          </div>

          <button id="sendBtn" onclick="submitQuery()" class="bg-indigo-600 hover:bg-indigo-700 text-white font-medium py-2 px-5 rounded-lg text-xs flex items-center gap-2 transition duration-150 shadow-lg shadow-indigo-500/20">
            <i class="fa-solid fa-paper-plane"></i>
            Run RAG Query
          </button>
        </div>
      </div>

      <!-- Results Area -->
      <div class="flex-1 bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl flex flex-col overflow-hidden">
        <div class="flex items-center justify-between pb-3 border-b border-slate-800">
          <h2 class="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <i class="fa-solid fa-message text-indigo-400"></i>
            Generated Answer & Citations
          </h2>
          <div id="sourceBadge" class="hidden text-xs font-mono px-2.5 py-1 rounded-full font-bold"></div>
        </div>

        <!-- Telemetry bar -->
        <div id="telemetryBar" class="hidden my-3 py-2 px-3 bg-slate-950/60 border border-slate-800/80 rounded-lg flex items-center justify-between text-[11px] font-mono text-slate-400">
          <div><i class="fa-solid fa-stopwatch mr-1"></i> Retrieval: <span id="retrievalTime" class="text-slate-200">0ms</span></div>
          <div><i class="fa-solid fa-layer-group mr-1"></i> Reranker: <span id="rerankTime" class="text-slate-200">0ms</span></div>
          <div><i class="fa-solid fa-clock mr-1"></i> Total: <span id="totalTime" class="text-indigo-400 font-bold">0ms</span></div>
          <div><i class="fa-solid fa-quote-left mr-1"></i> Citations: <span id="citCount" class="text-slate-200">0</span></div>
        </div>

        <!-- Response Body -->
        <div class="flex-1 overflow-y-auto mt-2 pr-1 space-y-4">
          <div id="welcomePlaceholder" class="text-center py-16 text-slate-500">
            <i class="fa-solid fa-compass text-4xl mb-3 text-slate-600"></i>
            <p class="text-sm">Ready to test. Select a preset query or enter your own prompt above.</p>
            <p class="text-xs text-slate-600 mt-1">Try querying before batch ingestion to see the Public Internet Fallback, then ingest to see Local Citations!</p>
          </div>

          <div id="loadingIndicator" class="hidden text-center py-12">
            <div class="inline-block animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-indigo-500 mb-3"></div>
            <p class="text-xs text-slate-400 font-mono">Hybrid search & Ollama inference in progress...</p>
          </div>

          <div id="answerContent" class="hidden">
            <div class="bg-slate-950 border border-slate-800 rounded-lg p-4 text-sm text-slate-200 leading-relaxed whitespace-pre-wrap font-sans" id="answerText"></div>
            
            <div class="mt-4">
              <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-2">
                <i class="fa-solid fa-bookmark text-amber-400"></i>
                Retrieved Passages & Grounding Citations
              </h3>
              <div id="citationsList" class="space-y-2"></div>
            </div>
          </div>
        </div>
      </div>
    </section>
  </main>

  <script>
    function setQuery(text) {
      document.getElementById('queryInput').value = text;
    }

    async function submitQuery() {
      const query = document.getElementById('queryInput').value.trim();
      if (!query) return;

      const webFallback = document.getElementById('webFallbackToggle').checked;
      
      document.getElementById('welcomePlaceholder').classList.add('hidden');
      document.getElementById('answerContent').classList.add('hidden');
      document.getElementById('loadingIndicator').classList.remove('hidden');
      document.getElementById('telemetryBar').classList.add('hidden');
      document.getElementById('sourceBadge').classList.add('hidden');

      const payload = {
        query: query,
        tenant_id: "aegis_corp",
        user_acl_groups: ["engineering", "exec", "public"],
        top_k_candidates: 50,
        rerank_top_n: 5,
        enable_web_fallback: webFallback
      };

      try {
        const res = await fetch('/api/v1/rag/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!res.ok) {
          throw new Error('Server returned HTTP ' + res.status);
        }

        const data = await res.json();
        renderResponse(data);
      } catch (err) {
        document.getElementById('loadingIndicator').classList.add('hidden');
        document.getElementById('answerContent').classList.remove('hidden');
        document.getElementById('answerText').innerHTML = '<span class="text-rose-400 font-mono">Error querying RAG system: ' + err.message + '</span>';
      }
    }

    function renderResponse(data) {
      document.getElementById('loadingIndicator').classList.add('hidden');
      document.getElementById('answerContent').classList.remove('hidden');
      document.getElementById('telemetryBar').classList.remove('hidden');

      // Update telemetry
      document.getElementById('retrievalTime').innerText = data.retrieval_latency_ms + 'ms';
      document.getElementById('rerankTime').innerText = data.rerank_latency_ms + 'ms';
      document.getElementById('totalTime').innerText = data.total_latency_ms + 'ms';
      document.getElementById('citCount').innerText = data.citations.length;

      // Update Source Badge
      const badge = document.getElementById('sourceBadge');
      badge.classList.remove('hidden');
      if (data.source_type === 'local_knowledge_base') {
        badge.className = 'text-xs font-mono px-3 py-1 rounded-full font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30';
        badge.innerHTML = '<i class="fa-solid fa-shield-halved mr-1"></i> SOURCE: LOCAL KNOWLEDGE BASE (INTERNET BYPASSED)';
      } else if (data.source_type === 'public_internet') {
        badge.className = 'text-xs font-mono px-3 py-1 rounded-full font-bold bg-amber-500/10 text-amber-400 border border-amber-500/30';
        badge.innerHTML = '<i class="fa-solid fa-globe mr-1"></i> SOURCE: PUBLIC INTERNET FALLBACK (0 LOCAL DOCS)';
      } else {
        badge.className = 'text-xs font-mono px-3 py-1 rounded-full font-bold bg-slate-800 text-slate-400 border border-slate-700';
        badge.innerText = 'NO RELEVANT MATCH';
      }

      // Format answer text
      document.getElementById('answerText').innerText = data.answer;

      // Render Citations
      const citList = document.getElementById('citationsList');
      citList.innerHTML = '';

      if (data.citations.length === 0) {
        citList.innerHTML = '<p class="text-xs text-slate-500 italic">No citations available for this response.</p>';
        return;
      }

      data.citations.forEach((cit, idx) => {
        const card = document.createElement('div');
        card.className = 'bg-slate-950 border border-slate-800 hover:border-slate-700 rounded-lg p-3 text-xs transition';
        card.innerHTML = `
          <div class="flex items-center justify-between text-slate-300 font-medium mb-1.5">
            <span class="flex items-center gap-2">
              <span class="w-5 h-5 rounded bg-indigo-900/60 text-indigo-400 flex items-center justify-center text-[10px] font-bold font-mono">${idx + 1}</span>
              <span class="text-indigo-300">${escapeHtml(cit.filename)}</span>
              <span class="text-slate-500 font-mono">Page ${cit.page_number}</span>
            </span>
            <span class="font-mono text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400">Score: ${cit.score}</span>
          </div>
          <div class="text-slate-400 text-[11px] leading-relaxed bg-slate-900/60 p-2 rounded border border-slate-800/60 font-mono">
            ${escapeHtml(cit.text)}
          </div>
        `;
        citList.appendChild(card);
      });
    }

    function escapeHtml(str) {
      if (!str) return '';
      return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    async function uploadSingleFile(file) {
      if (!file) return;
      const statusDiv = document.getElementById('uploadStatus');
      statusDiv.innerHTML = '<span class="text-indigo-400"><i class="fa-solid fa-spinner animate-spin"></i> Uploading ' + file.name + '...</span>';

      const formData = new FormData();
      formData.append('file', file);
      formData.append('tenant_id', 'aegis_corp');
      formData.append('acl_groups', 'engineering,exec,public');
      formData.append('priority', 'realtime');

      try {
        const res = await fetch('/api/v1/documents/upload', {
          method: 'POST',
          body: formData
        });
        const data = await res.json();
        statusDiv.innerHTML = '<span class="text-emerald-400"><i class="fa-solid fa-check"></i> ' + file.name + ' queued (Doc ID: ' + data.doc_id.slice(0, 8) + '...)</span>';
      } catch (err) {
        statusDiv.innerHTML = '<span class="text-rose-400"><i class="fa-solid fa-xmark"></i> Upload failed: ' + err.message + '</span>';
      }
    }

    async function runBatchIngestion() {
      const btn = document.getElementById('batchBtn');
      const logBox = document.getElementById('batchLogBox');
      const logs = document.getElementById('batchLogs');

      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Dispatching 20 batch documents...';
      logBox.classList.remove('hidden');
      logs.innerHTML = '<div class="text-amber-400">[*] Contacting backend to dispatch batch documents...</div>';

      try {
        const res = await fetch('/api/v1/batch/ingest-test-docs', { method: 'POST' });
        const data = await res.json();

        logs.innerHTML += `<div class="text-emerald-400">[+] Dispatched ${data.dispatched_count} documents to Redpanda queue!</div>`;
        logs.innerHTML += `<div class="text-slate-400">[*] Consumer lag increased. KEDA will trigger replica scaling.</div>`;
        logs.innerHTML += `<div class="text-sky-400">[*] Check terminal: <code class="text-slate-200">kubectl get pods -n local-rag -l app=parser-worker -w</code></div>`;
        
        btn.innerHTML = '<i class="fa-solid fa-check text-emerald-400"></i> Ingestion Batch Dispatched!';
      } catch (err) {
        logs.innerHTML += `<div class="text-rose-400">[-] Error triggering batch: ${err.message}. You can run scripts/batch_upload_test.py from terminal instead!</div>`;
        btn.innerHTML = '<i class="fa-solid fa-play"></i> Retry Batch Ingestion';
        btn.disabled = false;
      }
    }
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
@app.get("/ui", response_class=HTMLResponse)
async def serve_ui():
    """Serves the interactive web UI directly in Google Chrome."""
    return HTMLResponse(content=HTML_UI)

@app.post("/api/v1/documents/upload")
async def proxy_document_upload(request: Request):
    """Proxies document upload from the web UI to the ingestion service."""
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    target_urls = [
        f"{INGESTION_SERVICE_URL}/api/v1/documents/upload",
        "http://localhost:8000/api/v1/documents/upload"
    ]
    for url in target_urls:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, content=body, headers=headers)
                return Response(
                    content=resp.content,
                    status_code=resp.status_code,
                    media_type=resp.headers.get("content-type", "application/json")
                )
        except Exception as e:
            logger.warning(f"Failed to proxy upload to {url}: {e}")
            continue
    raise HTTPException(status_code=502, detail="Ingestion service unreachable.")

@app.post("/api/v1/batch/ingest-test-docs")
async def trigger_batch_docs():
    """Endpoint for the Chrome UI to trigger ingestion of the 20 test documents."""
    import glob
    test_dir = os.getenv("BATCH_TEST_DOCS_DIR", "/app/batch_test_docs")
    if not os.path.exists(test_dir) or not os.listdir(test_dir):
        test_dir = "/home/het/rag/batch_test_docs"
    files = sorted(glob.glob(os.path.join(test_dir, "*.*")))
    if not files:
        raise HTTPException(status_code=404, detail="No test documents found in batch_test_docs folder.")

    dispatched = 0
    # Try internal cluster service first, then localhost
    target_urls = [
        f"{INGESTION_SERVICE_URL}/api/v1/documents/upload",
        "http://localhost:8000/api/v1/documents/upload"
    ]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for fpath in files:
            fname = os.path.basename(fpath)
            ctype = "application/pdf" if fname.endswith(".pdf") else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            with open(fpath, "rb") as f:
                content = f.read()

            for url in target_urls:
                try:
                    resp = await client.post(
                        url,
                        data={"tenant_id": "aegis_corp", "acl_groups": "engineering,exec,public", "priority": "realtime"},
                        files={"file": (fname, content, ctype)}
                    )
                    if resp.status_code == 200:
                        dispatched += 1
                        break
                except Exception:
                    continue

    return {"status": "ok", "dispatched_count": dispatched, "total_files": len(files)}

# =============================================================
# MongoDB Hackathon: Agents in Context - Problem Statement APIs
# =============================================================

# --- Statement One: Agents You Can Audit ---
@app.get("/api/v1/governance/identities")
def list_identities():
    """List all registered user and agent identities in MongoDB."""
    return list(db["identities"].find({}, {"_id": 0}))

@app.post("/api/v1/governance/identities")
def create_identity(identity: Dict[str, Any]):
    """Register or update an identity in MongoDB."""
    ident_id = governance_engine.register_identity(identity)
    return {"status": "ok", "identity_id": ident_id}

@app.get("/api/v1/governance/policies")
def list_policies():
    """List all active governance policies stored in MongoDB."""
    return list(db["policies"].find({}, {"_id": 0}))

@app.post("/api/v1/governance/policies")
def create_policy(policy: Dict[str, Any]):
    """Register a new policy rule in MongoDB."""
    db["policies"].update_one({"policy_id": policy["policy_id"]}, {"$set": policy}, upsert=True)
    return {"status": "ok", "policy_id": policy["policy_id"]}

@app.get("/api/v1/governance/audit/traces")
def list_audit_traces(limit: int = 20):
    """List recent audit logs from MongoDB."""
    logs = list(db["audit_logs"].find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
    return logs

@app.get("/api/v1/governance/audit/traces/{trace_id}/reconstruct")
def reconstruct_trace(trace_id: str):
    """Reviewer Reconstruction API: Rebuilds complete execution trace from MongoDB."""
    return governance_engine.reconstruct_trace(trace_id)


# --- Statement Two: Memory You Can Govern ---
class StoreMemoryRequest(BaseModel):
    agent_id: str = "agent_rag_copilot"
    content: str
    read_acl: List[str] = ["employee", "finance"]
    ttl_seconds: Optional[int] = 86400  # 24 hour TTL by default
    memory_type: str = "factual_correction"

class CorrectMemoryRequest(BaseModel):
    corrected_content: str
    actor_id: str = "user_analyst_01"

@app.post("/api/v1/governance/memory/store")
def store_governed_memory(req: StoreMemoryRequest):
    """Persist a memory in MongoDB with explicit read access rules and TTL expiration."""
    doc = memory_engine.store_memory(
        agent_id=req.agent_id,
        content=req.content,
        read_acl=req.read_acl,
        ttl_seconds=req.ttl_seconds,
        memory_type=req.memory_type
    )
    return doc

@app.get("/api/v1/governance/memory/list")
def list_governed_memories():
    """List all governed memories from MongoDB (including active, superseded, forgotten)."""
    return list(db["agent_memories"].find({}, {"_id": 0, "vector": 0}).sort("created_at", -1))

@app.put("/api/v1/governance/memory/{memory_id}/correct")
def correct_governed_memory(memory_id: str, req: CorrectMemoryRequest):
    """Correct an existing memory, creating a new version and superseding the previous."""
    try:
        new_mem = memory_engine.correct_memory(memory_id, req.corrected_content, req.actor_id)
        return new_mem
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.delete("/api/v1/governance/memory/{memory_id}/forget")
def forget_governed_memory(memory_id: str, actor_id: str = "user_analyst_01"):
    """Tombstone / forget a memory so the agent never retrieves it again."""
    success = memory_engine.forget_memory(memory_id, actor_id)
    if not success:
        raise HTTPException(status_code=404, detail="Memory not found")
    return {"status": "forgotten", "memory_id": memory_id}


# --- Statement Three: Finish the Job ---
class CreateTaskRequest(BaseModel):
    task_name: str
    steps: List[Dict[str, Any]]
    context: Dict[str, Any] = {}

class StepAdvanceRequest(BaseModel):
    step_output: Dict[str, Any]

@app.post("/api/v1/governance/tasks/create")
def create_durable_task(req: CreateTaskRequest):
    """Initialize a durable multi-step task with verified state in MongoDB."""
    task_id = task_engine.create_task(req.task_name, req.steps, req.context)
    return {"status": "QUEUED", "task_id": task_id}

@app.post("/api/v1/governance/tasks/{task_id}/step")
def advance_task_step(task_id: str, req: StepAdvanceRequest):
    """Execute and checkpoint a step in MongoDB so state survives crashes."""
    try:
        return task_engine.advance_step(task_id, req.step_output)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/api/v1/governance/tasks/{task_id}/resume")
def resume_task_after_crash(task_id: str):
    """Crash recovery: Resume task strictly from last verified MongoDB checkpoint."""
    try:
        return task_engine.recover_and_resume(task_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/api/v1/governance/tasks/{task_id}")
def get_task_details(task_id: str):
    """Get full state and step checkpoints for a durable task from MongoDB."""
    task = db["durable_tasks"].find_one({"task_id": task_id}, {"_id": 0})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.post("/api/v1/governance/tasks/resolve-error")
def resolve_error_with_past_experience(error_query: Dict[str, str]):
    """Query MongoDB Vector Search over past experiences to adapt to system changes or errors."""
    q = error_query.get("error_description", "")
    return task_engine.query_past_experience(q)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
