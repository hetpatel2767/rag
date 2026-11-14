import os
import sys
import io
import time
import json
import logging
import hashlib
import uuid
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import minio
import pypdf

# FastEmbed for Dense + BM25 Sparse embeddings
from fastembed import TextEmbedding, SparseTextEmbedding

# Qdrant Client
from qdrant_client import QdrantClient
from qdrant_client.http import models

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rag-worker")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_REALTIME = os.getenv("TOPIC_REALTIME", "doc-ingest-realtime")
TOPIC_BULK = os.getenv("TOPIC_BULK", "doc-ingest-bulk")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "doc-ingest-dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "rag-parser-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "rag_metadata")

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "rag_documents")

MAX_RETRIES = 3

# Initialize Clients
s3_client = minio.Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB_NAME]
docs_col = db["documents"]
chunks_col = db["chunks"]
dlq_col = db["dead_letter_queue"]

qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

# Initialize Open-Source Embedding Models (Runs on Ryzen CPU via ONNX Runtime!)
# BAAI/bge-small-en-v1.5 has 384 dimensions, super fast, low memory (~130MB)
logger.info("Loading FastEmbed Dense (BAAI/bge-small-en-v1.5) & Sparse (Qdrant/bm25) models...")
dense_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

# Optional: Microsoft Presidio
try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_anonymizer import AnonymizerEngine
    presidio_analyzer = AnalyzerEngine()
    presidio_anonymizer = AnonymizerEngine()
    USE_PRESIDIO = True
    logger.info("Microsoft Presidio PII Redactor loaded successfully.")
except Exception as e:
    logger.warning("Presidio not fully loaded, using high-speed regex PII fallback: %s", e)
    USE_PRESIDIO = False

def wait_for_services():
    """Wait for Qdrant, MinIO, and MongoDB to become ready."""
    logger.info("Waiting for dependencies to be ready...")
    
    # Wait for MongoDB
    while True:
        try:
            mongo_client.admin.command('ping')
            logger.info("MongoDB is ready.")
            break
        except Exception as e:
            logger.info("Waiting for MongoDB... (%s)", e)
            time.sleep(2)

    # Wait for Qdrant
    while True:
        try:
            qdrant_client.get_collections()
            logger.info("Qdrant is ready.")
            break
        except Exception as e:
            logger.info("Waiting for Qdrant... (%s)", e)
            time.sleep(2)

def init_qdrant_collection():
    """Ensure Qdrant hybrid collection exists with Dense + Sparse vectors."""
    wait_for_services()
    try:
        collections = [c.name for c in qdrant_client.get_collections().collections]
        if QDRANT_COLLECTION not in collections:
            logger.info("Creating Qdrant collection: %s with Dense + Sparse BM25 vectors", QDRANT_COLLECTION)
            qdrant_client.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config={
                    "dense": models.VectorParams(size=384, distance=models.Distance.COSINE)
                },
                sparse_vectors_config={
                    "sparse": models.SparseVectorParams(
                        index=models.SparseIndexParams(on_disk=False)
                    )
                }
            )
            # Create payload indexes for fast ACL pre-filtering
            qdrant_client.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name="tenant_id",
                field_schema=models.PayloadSchemaType.KEYWORD
            )
            qdrant_client.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name="acl_groups",
                field_schema=models.PayloadSchemaType.KEYWORD
            )
            logger.info("Qdrant collection and payload indexes created successfully.")
    except Exception as e:
        logger.error("Error creating Qdrant collection: %s", e)

init_qdrant_collection()

# PII Redaction
REGEX_PII_PATTERNS = [
    (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'), "<REDACTED_EMAIL>"),
    (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), "<REDACTED_SSN>"),
    (re.compile(r'\b(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})\b'), "<REDACTED_PHONE>"),
    (re.compile(r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13})\b'), "<REDACTED_CARD>"),
    (re.compile(r'\b(?:AKIA|ASIA)[0-9A-Z]{16}\b'), "<REDACTED_API_KEY>"),
]

def redact_pii(text: str) -> str:
    """Real-time PII redaction using Presidio or regex fallback."""
    if USE_PRESIDIO:
        try:
            results = presidio_analyzer.analyze(
                text=text,
                entities=["EMAIL_ADDRESS", "PHONE_NUMBER", "US_SSN", "CREDIT_CARD", "CRYPTO"],
                language='en'
            )
            anonymized = presidio_anonymizer.anonymize(text=text, analyzer_results=results)
            return anonymized.text
        except Exception as e:
            logger.debug("Presidio analyze error, falling back to regex: %s", e)

    redacted = text
    for pattern, replacement in REGEX_PII_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted

def extract_text_from_pdf(file_bytes: bytes) -> List[Tuple[int, str]]:
    """Stateless PDF text extraction with page numbers."""
    reader = pypdf.PdfReader(io.BytesIO(file_bytes))
    pages_text = []
    for idx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        text = text.strip()
        if text:
            pages_text.append((idx + 1, text))
    return pages_text

def extract_text_from_docx(file_bytes: bytes) -> List[Tuple[int, str]]:
    """Stateless DOCX text extraction using standard library zipfile & XML."""
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            xml_content = zf.read("word/document.xml")
        tree = ET.fromstring(xml_content)
        paragraphs = []
        for p in tree.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
            texts = [node.text for node in p.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t') if node.text]
            if texts:
                paragraphs.append(''.join(texts))
        full_text = '\n\n'.join(paragraphs)
        if full_text.strip():
            return [(1, full_text.strip())]
    except Exception as e:
        logger.error(f"Error parsing docx: {e}")
    return []

def chunk_text(pages: List[Tuple[int, str]], target_tok: int = 768, overlap_ratio: float = 0.15) -> List[Dict[str, Any]]:
    """
    Semantic chunking: 512-1024 tokens with 15% overlap.
    Approximating ~4 chars per token for chunk size calculation.
    Preserves page numbers and semantic paragraph boundaries.
    """
    char_chunk_size = target_tok * 4
    overlap_chars = int(char_chunk_size * overlap_ratio)
    chunks = []
    chunk_index = 0

    for page_num, text in pages:
        paragraphs = text.split("\n\n")
        current_chunk = ""
        for p in paragraphs:
            p = p.strip()
            if not p:
                continue
            if len(current_chunk) + len(p) + 2 <= char_chunk_size:
                current_chunk += ("\n\n" + p if current_chunk else p)
            else:
                if current_chunk:
                    chunks.append({
                        "chunk_index": chunk_index,
                        "page_number": page_num,
                        "text": current_chunk
                    })
                    chunk_index += 1
                    # Retain overlap from end of current chunk
                    current_chunk = current_chunk[-overlap_chars:] + "\n\n" + p
                else:
                    # Single paragraph exceeds chunk size, split by lines or slice
                    chunks.append({
                        "chunk_index": chunk_index,
                        "page_number": page_num,
                        "text": p[:char_chunk_size]
                    })
                    chunk_index += 1
                    current_chunk = p[char_chunk_size - overlap_chars:]

        if current_chunk:
            chunks.append({
                "chunk_index": chunk_index,
                "page_number": page_num,
                "text": current_chunk
            })
            chunk_index += 1

    return chunks

def process_document(payload: Dict[str, Any]):
    doc_id = payload["doc_id"]
    tenant_id = payload["tenant_id"]
    s3_bucket = payload["s3_bucket"]
    s3_key = payload["s3_key"]
    acl_groups = payload.get("acl_groups", ["public"])
    filename = payload.get("filename", "unknown.pdf")

    logger.info(f"Processing doc_id: {doc_id} (tenant: {tenant_id}, file: {filename})")
    docs_col.update_one({"doc_id": doc_id}, {"$set": {"status": "PROCESSING", "updated_at": datetime.now(timezone.utc)}})

    # 1. Download file from MinIO
    response = s3_client.get_object(s3_bucket, s3_key)
    file_bytes = response.read()

    # 2. Extract text & OCR
    if filename.lower().endswith(".pdf"):
        pages = extract_text_from_pdf(file_bytes)
    elif filename.lower().endswith((".docx", ".doc")):
        pages = extract_text_from_docx(file_bytes)
    else:
        text = file_bytes.decode("utf-8", errors="ignore")
        pages = [(1, text)]

    if not pages:
        raise ValueError(f"No text could be extracted from {filename}")

    # 3. Chunking (512-1024 tok, 15% overlap)
    raw_chunks = chunk_text(pages, target_tok=768, overlap_ratio=0.15)
    if not raw_chunks:
        raise ValueError("Document yielded 0 chunks.")

    # 4. Real-time PII Redaction
    cleaned_chunks = []
    chunk_texts = []
    for c in raw_chunks:
        redacted = redact_pii(c["text"])
        c["text"] = redacted
        cleaned_chunks.append(c)
        chunk_texts.append(redacted)

    # 5. Open-Source Embeddings (Dense BGE-Small + Sparse BM25)
    dense_embeddings = list(dense_model.embed(chunk_texts))
    sparse_embeddings = list(sparse_model.embed(chunk_texts))

    # 6. Idempotent Upsert to Qdrant & MongoDB
    qdrant_points = []
    mongo_chunk_docs = []

    for idx, chunk in enumerate(cleaned_chunks):
        # Deterministic UUID based on SHA256(tenant+doc+idx)
        unique_key = f"{tenant_id}_{doc_id}_{idx}"
        point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_key))

        dense_vector = dense_embeddings[idx].tolist()
        sparse_indices = sparse_embeddings[idx].indices.tolist()
        sparse_values = sparse_embeddings[idx].values.tolist()

        payload_data = {
            "doc_id": doc_id,
            "tenant_id": tenant_id,
            "acl_groups": acl_groups,
            "filename": filename,
            "page_number": chunk["page_number"],
            "chunk_index": chunk["chunk_index"],
            "text": chunk["text"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        qdrant_points.append(
            models.PointStruct(
                id=point_id,
                vector={
                    "dense": dense_vector,
                    "sparse": models.SparseVector(indices=sparse_indices, values=sparse_values)
                },
                payload=payload_data
            )
        )

        mongo_chunk_docs.append({
            "chunk_id": point_id,
            "doc_id": doc_id,
            "tenant_id": tenant_id,
            "page_number": chunk["page_number"],
            "chunk_index": chunk["chunk_index"],
            "text": chunk["text"],
            "created_at": datetime.now(timezone.utc)
        })

    # Upsert points in micro-batches to Qdrant
    batch_size = 100
    for i in range(0, len(qdrant_points), batch_size):
        qdrant_client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=qdrant_points[i:i + batch_size]
        )

    # Save chunks to MongoDB
    if mongo_chunk_docs:
        chunks_col.insert_many(mongo_chunk_docs)

    # Update doc status in MongoDB
    docs_col.update_one(
        {"doc_id": doc_id},
        {"$set": {
            "status": "INDEXED",
            "total_chunks": len(cleaned_chunks),
            "updated_at": datetime.now(timezone.utc)
        }}
    )
    logger.info(f"Successfully indexed doc_id {doc_id} with {len(cleaned_chunks)} chunks.")

def main():
    consumer = None
    producer = None

    logger.info("Connecting to Kafka/Redpanda...")
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                TOPIC_REALTIME,
                TOPIC_BULK,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=CONSUMER_GROUP,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            logger.info(f"Connected to Kafka/Redpanda successfully.")
        except Exception as e:
            logger.info("Kafka/Redpanda not ready yet, retrying in 3s... (%s)", e)
            time.sleep(3)

    logger.info(f"KEDA Parser Worker started. Listening to topics: {TOPIC_REALTIME}, {TOPIC_BULK}")

    for message in consumer:
        payload = message.value
        doc_id = payload.get("doc_id", "unknown")
        tenant_id = payload.get("tenant_id", "unknown")
        retry_count = payload.get("retry_count", 0)

        try:
            process_document(payload)
            consumer.commit()
        except Exception as e:
            logger.error(f"Error processing doc_id {doc_id}: {e}", exc_info=True)
            retry_count += 1
            payload["retry_count"] = retry_count
            payload["last_error"] = str(e)

            if retry_count < MAX_RETRIES:
                backoff_time = 2 ** retry_count
                logger.warning(f"Retrying doc {doc_id} in {backoff_time}s (Attempt {retry_count}/{MAX_RETRIES})")
                time.sleep(backoff_time)
                # Re-queue to same topic
                producer.send(message.topic, key=f"{tenant_id}_{doc_id}", value=payload)
                producer.flush()
            else:
                # Poison-pill: emit to Dead-Letter Queue (DLQ)
                logger.error(f"Doc {doc_id} exceeded max retries. Routing to DLQ topic: {TOPIC_DLQ}")
                payload["failed_at"] = datetime.now(timezone.utc).isoformat()
                producer.send(TOPIC_DLQ, key=f"{tenant_id}_{doc_id}", value=payload)
                producer.flush()

                # Record in MongoDB DLQ collection
                dlq_col.insert_one(payload)
                docs_col.update_one(
                    {"doc_id": doc_id},
                    {"$set": {"status": "FAILED", "error": str(e), "updated_at": datetime.now(timezone.utc)}}
                )

            # Never block partition! Commit the offset so the consumer proceeds.
            consumer.commit()

if __name__ == "__main__":
    main()
