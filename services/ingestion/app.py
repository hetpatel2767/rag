import os
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, status
from pydantic import BaseModel
from pymongo import MongoClient
import minio
from kafka import KafkaProducer
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Local RAG Ingestion API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration from Environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-documents")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "rag_metadata")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_REALTIME = os.getenv("TOPIC_REALTIME", "doc-ingest-realtime")
TOPIC_BULK = os.getenv("TOPIC_BULK", "doc-ingest-bulk")

# Clients
s3_client = minio.Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB_NAME]
docs_col = db["documents"]

# Create indexes
docs_col.create_index([("tenant_id", 1), ("sha256", 1)])
docs_col.create_index("doc_id", unique=True)

kafka_producer = None

def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            retries=5
        )
    return kafka_producer

@app.on_event("startup")
def startup_event():
    import time
    for attempt in range(15):
        try:
            if not s3_client.bucket_exists(MINIO_BUCKET):
                s3_client.make_bucket(MINIO_BUCKET)
            break
        except Exception as e:
            time.sleep(2)

class IngestionResponse(BaseModel):
    doc_id: str
    status: str
    message: str
    sha256: str
    is_duplicate: bool = False

@app.post("/api/v1/documents/upload", response_model=IngestionResponse)
async def upload_document(
    file: UploadFile = File(...),
    tenant_id: str = Form("default_tenant"),
    acl_groups: str = Form("public"),  # Comma-separated ACL strings, e.g. "hr,finance"
    priority: str = Form("realtime"),  # 'realtime' or 'bulk'
):
    """
    Step 1 & 2 of the Ingestion Pipeline:
    - Claim-Check pattern: Stores raw PDF/doc in local MinIO.
    - Computes SHA256 hash for deduplication.
    - Tracks metadata in MongoDB.
    - Dispatches claim check pointer into Kafka with partition key tenant_id + doc_id.
    """
    content = await file.read()
    file_size = len(content)
    if file_size == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    sha256_hash = hashlib.sha256(content).hexdigest()
    parsed_acls = [g.strip() for g in acl_groups.split(",") if g.strip()]

    # Deduplication Check in MongoDB
    existing_doc = docs_col.find_one({"tenant_id": tenant_id, "sha256": sha256_hash})
    if existing_doc:
        # ACL-Only Fast Lane: If identical file already processed, only update ACLs and skip re-embedding
        updated_acls = list(set(existing_doc.get("acl_groups", []) + parsed_acls))
        docs_col.update_one(
            {"_id": existing_doc["_id"]},
            {"$set": {"acl_groups": updated_acls, "updated_at": datetime.now(timezone.utc)}}
        )
        return IngestionResponse(
            doc_id=existing_doc["doc_id"],
            status="DEDUPLICATED",
            message="Document identical hash already exists. Updated ACLs via ACL-only fast lane.",
            sha256=sha256_hash,
            is_duplicate=True
        )

    doc_id = str(uuid.uuid4())
    s3_key = f"{tenant_id}/{doc_id}/{file.filename}"

    # Store raw bytes in MinIO (Claim-Check pattern)
    from io import BytesIO
    s3_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=s3_key,
        data=BytesIO(content),
        length=file_size,
        content_type=file.content_type or "application/octet-stream"
    )

    # Save document record in MongoDB
    doc_record = {
        "doc_id": doc_id,
        "filename": file.filename,
        "tenant_id": tenant_id,
        "acl_groups": parsed_acls,
        "sha256": sha256_hash,
        "file_size": file_size,
        "s3_bucket": MINIO_BUCKET,
        "s3_key": s3_key,
        "status": "QUEUED",
        "retry_count": 0,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc)
    }
    docs_col.insert_one(doc_record)

    # Send message to Kafka / Redpanda Bus
    target_topic = TOPIC_REALTIME if priority == "realtime" else TOPIC_BULK
    partition_key = f"{tenant_id}_{doc_id}"

    payload = {
        "doc_id": doc_id,
        "tenant_id": tenant_id,
        "s3_bucket": MINIO_BUCKET,
        "s3_key": s3_key,
        "sha256": sha256_hash,
        "acl_groups": parsed_acls,
        "filename": file.filename
    }

    producer = get_kafka_producer()
    producer.send(target_topic, key=partition_key, value=payload)
    producer.flush()

    return IngestionResponse(
        doc_id=doc_id,
        status="QUEUED",
        message=f"Uploaded to MinIO and dispatched to Kafka topic {target_topic}",
        sha256=sha256_hash,
        is_duplicate=False
    )

@app.get("/api/v1/documents/{doc_id}")
def get_document_status(doc_id: str):
    doc = docs_col.find_one({"doc_id": doc_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
