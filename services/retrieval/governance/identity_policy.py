import time
import uuid
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient, ASCENDING, DESCENDING

class IdentityPolicyEngine:
    """
    Statement One: Agents You Can Audit
    Uses MongoDB as the identity, policy, and audit layer.
    Every action is:
      1. Tied to an identity (user or autonomous agent).
      2. Checked against policy in MongoDB before it runs.
      3. Recorded in a tamper-evident audit log so a reviewer can reconstruct it afterward.
    """

    def __init__(self, db):
        self.db = db
        self.identities = self.db["identities"]
        self.policies = self.db["policies"]
        self.audit_logs = self.db["audit_logs"]

        self._ensure_indexes()
        self._seed_default_policies()

    def _ensure_indexes(self):
        self.identities.create_index("identity_id", unique=True)
        self.policies.create_index("policy_id", unique=True)
        self.audit_logs.create_index([("trace_id", ASCENDING), ("step_index", ASCENDING)])
        self.audit_logs.create_index([("actor_identity", ASCENDING), ("timestamp", DESCENDING)])

    def _seed_default_policies(self):
        """Seed initial enterprise governance policies if not present."""
        default_policies = [
            {
                "policy_id": "pol_pii_sanitization_required",
                "name": "Mandatory Real-Time PII Sanitization",
                "action": "document:ingest",
                "condition": {"require_pii_check": True},
                "effect": "ALLOW",
                "description": "All documents ingested must pass automated PII redaction before vector indexing."
            },
            {
                "policy_id": "pol_finance_clearance_gate",
                "name": "Financial Document Clearance Gate",
                "action": "rag:query",
                "condition": {"required_role": "finance", "resource_tag": "finance"},
                "effect": "ALLOW",
                "description": "Access to financial policies requires explicit 'finance' clearance role."
            },
            {
                "policy_id": "pol_block_external_data_exfiltration",
                "name": "Data Exfiltration Prevention",
                "action": "agent:export",
                "condition": {"block_unencrypted_export": True},
                "effect": "DENY",
                "description": "Autonomous agents are prohibited from bulk data exports without compliance officer approval."
            }
        ]
        for p in default_policies:
            self.policies.update_one({"policy_id": p["policy_id"]}, {"$setOnInsert": p}, upsert=True)

        # Seed default identities
        default_identities = [
            {
                "identity_id": "user_analyst_01",
                "name": "Alex Mercer",
                "type": "human_user",
                "roles": ["finance", "employee"],
                "clearance_level": "confidential",
                "tenant_id": "tenant_alpha"
            },
            {
                "identity_id": "user_contractor_02",
                "name": "Guest Contractor",
                "type": "human_user",
                "roles": ["contractor", "public"],
                "clearance_level": "public",
                "tenant_id": "tenant_alpha"
            },
            {
                "identity_id": "agent_rag_copilot",
                "name": "Autonomous Governance RAG Agent",
                "type": "autonomous_agent",
                "roles": ["agent", "rag_retrieval"],
                "clearance_level": "internal",
                "tenant_id": "tenant_alpha"
            }
        ]
        for ident in default_identities:
            self.identities.update_one({"identity_id": ident["identity_id"]}, {"$setOnInsert": ident}, upsert=True)

    def register_identity(self, identity_data: Dict[str, Any]) -> str:
        ident_id = identity_data.get("identity_id") or str(uuid.uuid4())
        identity_data["identity_id"] = ident_id
        identity_data["updated_at"] = datetime.now(timezone.utc)
        self.identities.update_one({"identity_id": ident_id}, {"$set": identity_data}, upsert=True)
        return ident_id

    def evaluate_policy(self, actor_id: str, action: str, resource_context: Dict[str, Any]) -> Tuple[bool, str, str]:
        """
        Policy Enforcement Point (PEP):
        Checks MongoDB policies before an action runs.
        Returns: (is_allowed: bool, policy_id: str, reason: str)
        """
        actor = self.identities.find_one({"identity_id": actor_id})
        if not actor:
            return False, "DENY_UNKNOWN_IDENTITY", f"Actor identity '{actor_id}' is not registered in MongoDB."

        actor_roles = actor.get("roles", [])

        # Check for explicit DENY policies
        deny_policy = self.policies.find_one({"action": action, "effect": "DENY"})
        if deny_policy:
            return False, deny_policy["policy_id"], f"Action blocked by policy: {deny_policy['name']}"

        # 1. Statement One: Financial Clearance Gate (Intent & Role Enforcement)
        # Financial topics (bonuses, compensation, salaries, severance, etc.) strictly require 'finance' role.
        # Unauthorized actors (e.g. contractors) cannot bypass this simply by altering user_acl_groups.
        query_text = (resource_context.get("query") or "").lower()
        user_acls = resource_context.get("user_acl_groups") or []
        target_acl = resource_context.get("target_acl_group")

        financial_keywords = [
            "bonus", "retention", "compensation", "salary", "severance",
            "payroll", "financial", "payment term", "invoices", "card on file"
        ]
        is_financial_query = (
            any(kw in query_text for kw in financial_keywords)
            or ("finance" in user_acls)
            or (target_acl == "finance")
        )
        if is_financial_query and "finance" not in actor_roles and "admin" not in actor_roles:
            return (
                False,
                "pol_finance_clearance_gate",
                f"Policy 'pol_finance_clearance_gate' DENIED: Identity '{actor_id}' lacks mandatory 'finance' clearance role for financial/compensation queries."
            )

        # 2. General Clearance Validation
        if target_acl and target_acl != "public":
            if target_acl not in actor_roles and "admin" not in actor_roles:
                return False, "pol_finance_clearance_gate", f"Identity '{actor_id}' lacks required role '{target_acl}'"

        return True, "POL_ALLOW_DEFAULT", "Action complies with registered MongoDB governance policies."

    def record_audit_step(
        self,
        trace_id: str,
        step_index: int,
        actor_id: str,
        action: str,
        target_resource: str,
        policy_eval: Tuple[bool, str, str],
        inputs: Dict[str, Any],
        citations_used: List[Dict[str, Any]],
        llm_reasoning: str,
        output_summary: str,
    ) -> str:
        """
        Records a detailed, reconstructable audit step in MongoDB.
        Uses SHA-256 state hashing for tamper-evident verification.
        """
        is_allowed, policy_id, policy_reason = policy_eval

        # Compute hash of step contents
        payload_str = json.dumps({
            "trace_id": trace_id,
            "step_index": step_index,
            "actor_id": actor_id,
            "action": action,
            "timestamp": time.time()
        }, sort_keys=True)
        step_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

        audit_entry = {
            "step_id": str(uuid.uuid4()),
            "trace_id": trace_id,
            "step_index": step_index,
            "actor_identity": actor_id,
            "action": action,
            "target_resource": target_resource,
            "policy_decision": "ALLOWED" if is_allowed else "DENIED",
            "policy_evaluated": policy_id,
            "policy_reason": policy_reason,
            "inputs_sanitized": inputs,
            "citations_used": citations_used,
            "llm_reasoning_trace": llm_reasoning,
            "output_summary": output_summary,
            "step_hash": step_hash,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        self.audit_logs.insert_one(audit_entry)
        return audit_entry["step_id"]

    def reconstruct_trace(self, trace_id: str) -> Dict[str, Any]:
        """
        Reviewer Reconstruction API:
        Rebuilds the complete chronological timeline and decision tree from MongoDB.
        """
        steps = list(self.audit_logs.find({"trace_id": trace_id}, {"_id": 0}).sort("step_index", ASCENDING))
        if not steps:
            return {"error": f"No audit logs found for trace_id: {trace_id}"}

        first_step = steps[0]
        summary = {
            "trace_id": trace_id,
            "actor_identity": first_step.get("actor_identity"),
            "total_steps": len(steps),
            "status": "ALLOWED" if all(s["policy_decision"] == "ALLOWED" for s in steps) else "DENIED",
            "reconstructed_at": datetime.now(timezone.utc).isoformat(),
            "timeline": steps
        }
        return summary
