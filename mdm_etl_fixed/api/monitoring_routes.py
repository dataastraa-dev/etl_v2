"""
api/monitoring_routes.py
Dedicated blueprint for L0, L1, and L2 support dashboards.
"""
from flask import Blueprint, jsonify, request
from core.database import get_conn
import json

monitoring_bp = Blueprint("monitoring", __name__)

@monitoring_bp.route("/batches/summary", methods=["GET"])
def get_batch_summary():
    """L0: Returns high-level status of the most recent 50 batches."""
    try:
        conn = get_conn()
        sql = """
            SELECT batch_id, use_case_id, status, created_datetime, run_by
            FROM batch_master
            ORDER BY created_datetime DESC
            LIMIT 50
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            batches = [dict(zip(cols, row)) for row in cur.fetchall()]
        
        return jsonify({"batches": batches}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@monitoring_bp.route("/review-queue/pending", methods=["GET"])
def get_pending_reviews():
    """L1: Fetch rows that require manual stewardship."""
    try:
        conn = get_conn()
        sql = """
            SELECT review_id, staging_id, rule_id, flagged_row, severity
            FROM review_queue
            WHERE status = 'PENDING_REVIEW'
            ORDER BY severity ASC
            LIMIT 100
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            queue = [dict(zip(cols, row)) for row in cur.fetchall()]

        # flagged_row is a JSONB column — psycopg2 may return it as a raw
        # string depending on the driver version. Parse it if needed so the
        # frontend receives a proper object instead of an empty-keyed string.
        for item in queue:
            if isinstance(item.get("flagged_row"), str):
                try:
                    item["flagged_row"] = json.loads(item["flagged_row"])
                except (ValueError, TypeError):
                    item["flagged_row"] = {}

        return jsonify({"queue": queue}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@monitoring_bp.route("/review-queue/resolve", methods=["POST"])
def resolve_review():
    """L1 Action: Approve or Reject a flagged row."""
    payload = request.get_json()
    review_id = payload.get("review_id")
    action = payload.get("action")  # 'APPROVED' or 'REJECTED'
    
    try:
        conn = get_conn()
        sql = "UPDATE review_queue SET status = %s WHERE review_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, (action, review_id))
        conn.commit()
        return jsonify({"message": f"Row {action} successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@monitoring_bp.route("/batches/<batch_id>/records/<staging_id>/diff", methods=["GET"])
def get_record_diff(batch_id, staging_id):
    """L2: Join Bronze and Silver tables to return raw vs transformed payloads."""
    try:
        conn = get_conn()
        sql = """
            SELECT 
                r.raw_payload, 
                t.payload AS transformed_payload
            FROM unified_raw_staging r
            LEFT JOIN unified_transformed_staging t 
                ON r.staging_id = t.staging_id
            WHERE r.batch_id = %s AND r.staging_id = %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id, staging_id))
            row = cur.fetchone()
            
        if not row:
            return jsonify({"error": "Record not found"}), 404
            
        return jsonify({
            "raw": row[0],
            "transformed": row[1] if row[1] else {"_error": "Data did not reach the Silver layer (Transformation failed)"}
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500