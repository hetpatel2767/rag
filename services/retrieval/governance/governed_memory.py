import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import numpy as np
from pymongo import MongoClient, ASCENDING

class GovernedMemoryEngine:
    """
    Statement Two: Memory You Can Govern
    Uses MongoDB and Vector Search to persist and retrieve memory that:
      1. Changes what the agent does on the next run.
      2. Enforces rules for WHO can read each memory (read_acl).
      3. Enforces HOW LONG it lives (MongoDB native TTL Index).
      4. Defines HOW it gets corrected (versioned superseding) or forgotten (GDPR tombstoning).
    """

    def __init__(self, db, embed_fn):
        self.db = db
        self.memories = self.db["agent_memories"]
        self.embed_fn = embed_fn  # Function that takes [text] and returns list of embeddings

        self._ensure_indexes()

    def _ensure_indexes(self):
        self.memories.create_index("memory_id", unique=True)
        self.memories.create_index("agent_id")
        self.memories.create_index("read_acl")
        self.memories.create_index("status")
        # Native MongoDB TTL Index: automatically deletes expired memories when expires_at timestamp is reached
        self.memories.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)

    def store_memory(
        self,
        agent_id: str,
        content: str,
        read_acl: List[str],
        ttl_seconds: Optional[int] = None,
        memory_type: str = "episodic",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Stores governed memory in MongoDB with explicit read access control and TTL.
        """
        memory_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds) if ttl_seconds else None

        # Compute vector embedding for memory retrieval
        vector = list(self.embed_fn([content]))[0].tolist()

        doc = {
            "memory_id": memory_id,
            "agent_id": agent_id,
            "content": content,
            "vector": vector,
            "memory_type": memory_type,
            "read_acl": read_acl,
            "status": "active",
            "version": 1,
            "superseded_by": None,
            "created_at": now,
            "updated_at": now,
            "expires_at": expires_at,
            "metadata": metadata or {}
        }
        self.memories.insert_one(doc)
        doc.pop("_id", None)
        return doc

    def correct_memory(
        self,
        memory_id: str,
        corrected_content: str,
        actor_id: str
    ) -> Dict[str, Any]:
        """
        Corrects an existing memory:
        Marks the existing memory as 'superseded' and inserts a new active version linked to the prior.
        """
        existing = self.memories.find_one({"memory_id": memory_id, "status": "active"})
        if not existing:
            raise ValueError(f"Active memory with id '{memory_id}' not found.")

        new_memory_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        new_vector = list(self.embed_fn([corrected_content]))[0].tolist()

        # Update old memory to superseded
        self.memories.update_one(
            {"memory_id": memory_id},
            {"$set": {
                "status": "superseded",
                "superseded_by": new_memory_id,
                "updated_at": now
            }}
        )

        # Insert updated version
        new_doc = {
            "memory_id": new_memory_id,
            "agent_id": existing["agent_id"],
            "content": corrected_content,
            "vector": new_vector,
            "memory_type": existing["memory_type"],
            "read_acl": existing["read_acl"],
            "status": "active",
            "version": existing["version"] + 1,
            "superseded_by": None,
            "prior_memory_id": memory_id,
            "corrected_by": actor_id,
            "created_at": now,
            "updated_at": now,
            "expires_at": existing.get("expires_at"),
            "metadata": existing.get("metadata", {})
        }
        self.memories.insert_one(new_doc)
        new_doc.pop("_id", None)
        return new_doc

    def forget_memory(self, memory_id: str, actor_id: str) -> bool:
        """
        Right-to-be-forgotten / tombstone mechanism:
        Immediately deactivates the memory so the agent will never recall it.
        """
        result = self.memories.update_one(
            {"memory_id": memory_id},
            {"$set": {
                "status": "forgotten",
                "forgotten_at": datetime.now(timezone.utc),
                "forgotten_by": actor_id
            }}
        )
        return result.modified_count > 0

    def retrieve_governed_memories(
        self,
        query: str,
        user_roles: List[str],
        top_k: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Vector search across memories governed by read ACLs and active status.
        Uses cosine vector similarity scoring over active MongoDB memory vectors.
        """
        query_vector = np.array(list(self.embed_fn([query]))[0])

        # Filter strictly by active status and read_acl intersection
        query_filter = {
            "status": "active",
            "read_acl": {"$in": user_roles}
        }
        candidates = list(self.memories.find(query_filter, {"_id": 0}))
        if not candidates:
            return []

        scored = []
        for cand in candidates:
            cand_vec = np.array(cand["vector"])
            norm = (np.linalg.norm(query_vector) * np.linalg.norm(cand_vec))
            sim = float(np.dot(query_vector, cand_vec) / norm) if norm > 0 else 0.0
            if sim > 0.4:  # Relevance threshold
                scored.append({
                    "memory_id": cand["memory_id"],
                    "content": cand["content"],
                    "version": cand["version"],
                    "read_acl": cand["read_acl"],
                    "similarity": round(sim, 4),
                    "created_at": cand["created_at"].isoformat() if isinstance(cand["created_at"], datetime) else str(cand["created_at"]),
                    "expires_at": cand["expires_at"].isoformat() if isinstance(cand.get("expires_at"), datetime) else str(cand.get("expires_at"))
                })

        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return scored[:top_k]
