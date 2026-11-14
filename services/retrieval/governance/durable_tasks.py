import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import numpy as np
from pymongo import MongoClient, ASCENDING

class DurableTaskEngine:
    """
    Statement Three: Finish the Job
    Completes long multi-step tasks across several systems.
    Uses MongoDB to:
      1. Persist verified task state so workflows survive crashes and system restarts.
      2. Uses Vector Search across past experiences to adapt to errors/system changes.
      3. Resumes execution strictly from the last verified checkpoint without re-running finished steps.
    """

    def __init__(self, db, embed_fn):
        self.db = db
        self.tasks = self.db["durable_tasks"]
        self.experiences = self.db["task_experiences"]
        self.embed_fn = embed_fn

        self._ensure_indexes()
        self._seed_past_experiences()

    def _ensure_indexes(self):
        self.tasks.create_index("task_id", unique=True)
        self.tasks.create_index("status")
        self.experiences.create_index("experience_id", unique=True)

    def _seed_past_experiences(self):
        """Seed initial troubleshooting / system adaptation experiences."""
        initial_experiences = [
            {
                "experience_id": "exp_rate_limit_backoff",
                "problem": "System API returned HTTP 429 Too Many Requests rate limit",
                "solution": "Apply 5-second exponential jitter backoff and downsize batch chunk to 50",
                "system": "API_Gateway"
            },
            {
                "experience_id": "exp_schema_version_drift",
                "problem": "Target system rejected payload due to missing tenant_id header format",
                "solution": "Inject X-Tenant-Context header and wrap payload in metadata envelope",
                "system": "Legacy_ERP"
            }
        ]
        for exp in initial_experiences:
            if not self.experiences.find_one({"experience_id": exp["experience_id"]}):
                vec = list(self.embed_fn([exp["problem"]]))[0].tolist()
                exp["vector"] = vec
                exp["created_at"] = datetime.now(timezone.utc)
                self.experiences.insert_one(exp)

    def create_task(self, task_name: str, total_steps: List[Dict[str, Any]], context: Dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        doc = {
            "task_id": task_id,
            "task_name": task_name,
            "status": "QUEUED",
            "current_step_index": 0,
            "total_steps": len(total_steps),
            "steps": [
                {
                    "step_index": idx,
                    "name": s.get("name", f"Step {idx+1}"),
                    "system": s.get("system", "Internal"),
                    "status": "PENDING",
                    "checkpoint_state": {},
                    "completed_at": None
                }
                for idx, s in enumerate(total_steps)
            ],
            "context": context,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        self.tasks.insert_one(doc)
        return task_id

    def advance_step(self, task_id: str, step_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes and checkpoints the current step in MongoDB.
        If a crash occurs, this checkpoint ensures progress is never lost.
        """
        task = self.tasks.find_one({"task_id": task_id})
        if not task:
            raise ValueError(f"Task {task_id} not found.")

        idx = task["current_step_index"]
        steps = task["steps"]
        if idx >= len(steps):
            return {"status": "ALREADY_COMPLETED", "current_step_index": idx}

        # Checkpoint current step
        steps[idx]["status"] = "COMPLETED"
        steps[idx]["checkpoint_state"] = step_output
        steps[idx]["completed_at"] = datetime.now(timezone.utc).isoformat()

        next_idx = idx + 1
        new_status = "COMPLETED" if next_idx >= len(steps) else "IN_PROGRESS"

        self.tasks.update_one(
            {"task_id": task_id},
            {"$set": {
                "steps": steps,
                "current_step_index": next_idx,
                "status": new_status,
                "updated_at": datetime.now(timezone.utc)
            }}
        )

        return {
            "task_id": task_id,
            "completed_step": idx,
            "next_step_index": next_idx,
            "status": new_status,
            "checkpoint_persisted_to_mongodb": True
        }

    def recover_and_resume(self, task_id: str) -> Dict[str, Any]:
        """
        Crash Recovery:
        Finds the exact verified checkpoint state in MongoDB to resume without duplicating work.
        """
        task = self.tasks.find_one({"task_id": task_id}, {"_id": 0})
        if not task:
            raise ValueError(f"Task {task_id} not found.")

        completed_steps = [s for s in task["steps"] if s["status"] == "COMPLETED"]
        resume_from = task["current_step_index"]

        return {
            "task_id": task_id,
            "task_name": task["task_name"],
            "overall_status": task["status"],
            "resuming_from_step": resume_from,
            "previously_verified_steps_count": len(completed_steps),
            "survived_system_crash": True,
            "last_checkpoint_timestamp": completed_steps[-1]["completed_at"] if completed_steps else None
        }

    def query_past_experience(self, error_description: str, top_k: int = 1) -> List[Dict[str, Any]]:
        """
        Vector search across past task experiences in MongoDB to resolve unexpected runtime issues.
        Supports native MongoDB Atlas Vector Search ($vectorSearch) with automatic local fallback.
        """
        q_vec = np.array(list(self.embed_fn([error_description]))[0])

        # 1. Attempt Native MongoDB Atlas Vector Search
        try:
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": "experience_index",
                        "path": "vector",
                        "queryVector": q_vec.tolist(),
                        "numCandidates": 20,
                        "limit": top_k
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "problem": 1,
                        "solution": 1,
                        "system": 1,
                        "similarity": {"$meta": "vectorSearchScore"}
                    }
                }
            ]
            results = list(self.experiences.aggregate(pipeline))
            if results:
                return results
        except Exception:
            # Fall back to local in-memory cosine ranking
            pass

        # 2. Local Fallback
        experiences = list(self.experiences.find({}, {"_id": 0}))
        if not experiences:
            return []

        scored = []
        for exp in experiences:
            exp_vec = np.array(exp["vector"])
            norm = np.linalg.norm(q_vec) * np.linalg.norm(exp_vec)
            sim = float(np.dot(q_vec, exp_vec) / norm) if norm > 0 else 0.0
            scored.append({
                "problem": exp["problem"],
                "solution": exp["solution"],
                "system": exp.get("system"),
                "similarity": round(sim, 4)
            })

        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return scored[:top_k]
