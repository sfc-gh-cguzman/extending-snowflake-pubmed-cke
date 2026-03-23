-- =============================================================================
-- PubMed Incremental Load & SPCS Service Scheduling
-- =============================================================================
-- This script sets up three automated workflows:
--
-- 1. LOAD_PUBMED_INCREMENTAL (stored procedure + task)
--    - Runs daily at 5AM CT
--    - Resumes the MedCPT Article Encoder SPCS service, waits until ready
--    - Performs a Type 1 incremental load (delete+insert at PMID grain) from
--      the PubMed Marketplace listing into PUBMED_OA_MEDCPT_FULL_EMBEDDINGS
--    - Generates MedCPT article embeddings inline via the SPCS service
--    - Suspends the Article Encoder service when done (or on failure)
--
-- 2. RESUME_QUERY_ENCODER_SVC (task)
--    - Runs daily at 5AM CT
--    - Resumes the MedCPT Query Encoder SPCS service for business hours
--    - This is the service that powers real-time search queries
--
-- 3. SUSPEND_QUERY_ENCODER_SVC (task)
--    - Runs daily at 11PM CT
--    - Suspends the Query Encoder service to avoid GPU costs overnight
--
-- Source: PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_VW (Marketplace listing)
-- Target: SFSE_PUBMED_DB.PUBMED.PUBMED_OA_MEDCPT_FULL_EMBEDDINGS (~72M rows)
--
-- Why delete+insert instead of MERGE?
--   When the source re-chunks an updated article, CHUNK_IDs change entirely.
--   A MERGE on CHUNK_ID would leave orphaned old chunks. Delete+insert at the
--   PMID (article) level handles re-chunking cleanly.
-- =============================================================================

use role sysadmin;
use schema SFSE_PUBMED_DB.pubmed;
use warehouse pubmed_adhoc_wh;

-- =============================================================================
-- 1. Stored Procedure: Incremental load with SPCS service lifecycle management
-- =============================================================================
-- Flow:
--   a) Check for new/updated articles in the last N days (default 3)
--   b) Resume the MedCPT Article Encoder SPCS service, poll until READY
--   c) Delete existing chunks for affected PMIDs
--   d) Insert new chunks with embeddings generated inline via SPCS
--   e) Suspend the Article Encoder service
--   f) On failure: rollback the transaction AND suspend the service
-- =============================================================================

CREATE OR REPLACE PROCEDURE SFSE_PUBMED_DB.PUBMED.LOAD_PUBMED_INCREMENTAL(LOOKBACK_DAYS INT DEFAULT 3)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import time
import json

def wait_for_service(session, service_name, timeout=300, poll_interval=10):
    session.sql(f"ALTER SERVICE {service_name} RESUME").collect()
    elapsed = 0
    while elapsed < timeout:
        rows = session.sql(f"DESCRIBE SERVICE {service_name}").collect()
        statuses = [row["status"] for row in rows]
        if all(s == "READY" for s in statuses):
            return True
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError(f"{service_name} not ready after {timeout}s. Last statuses: {statuses}")

def run(session, lookback_days):
    service_name = "SFSE_PUBMED_DB.PUBMED.MEDCPT_EMBEDDER_INCREMENTAL_SVC"
    source_view = "PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_VW"
    target_table = "SFSE_PUBMED_DB.PUBMED.PUBMED_OA_MEDCPT_FULL_EMBEDDINGS"

    row = session.sql(f"""
        SELECT COUNT(DISTINCT PMID) AS cnt
        FROM {source_view}
        WHERE LAST_UPDATED_UTC > CURRENT_DATE() - {lookback_days}
          AND CHUNK_ID IS NOT NULL
    """).collect()[0]
    source_pmids = row["CNT"]

    if source_pmids == 0:
        return {"status": "no_new_data", "lookback_days": lookback_days}

    try:
        wait_for_service(session, service_name)

        session.sql("BEGIN").collect()

        session.sql(f"""
            DELETE FROM {target_table}
            WHERE PMID IN (
                SELECT DISTINCT PMID
                FROM {source_view}
                WHERE LAST_UPDATED_UTC > CURRENT_DATE() - {lookback_days}
                  AND CHUNK_ID IS NOT NULL
            )
        """).collect()
        deleted = session.sql("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").collect()
        deleted_count = deleted[0]["number of rows deleted"] if deleted else 0

        session.sql(f"""
            INSERT INTO {target_table}
                (KEY, ETAG, ARTICLE_CITATION, ACCESSIONID, LAST_UPDATED_UTC,
                 PMID, RETRACTED, LICENSE, CHUNK_ID, CHUNK, ARTICLE_URL, EMBEDDING)
            SELECT
                KEY, ETAG, ARTICLE_CITATION, ACCESSIONID, LAST_UPDATED_UTC,
                PMID, RETRACTED, LICENSE, CHUNK_ID, CHUNK, ARTICLE_URL,
                medcpt_embedder_incremental_svc!encode(CHUNK):EMBEDDING::VECTOR(FLOAT, 768)
            FROM {source_view}
            WHERE LAST_UPDATED_UTC > CURRENT_DATE() - {lookback_days}
              AND CHUNK_ID IS NOT NULL
        """).collect()
        inserted = session.sql("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").collect()
        inserted_count = inserted[0]["number of rows inserted"] if inserted else 0

        session.sql("COMMIT").collect()

        session.sql(f"ALTER SERVICE {service_name} SUSPEND").collect()

        return {
            "status": "success",
            "lookback_days": lookback_days,
            "source_pmids": source_pmids,
            "rows_deleted": deleted_count,
            "rows_inserted": inserted_count,
        }

    except Exception as e:
        session.sql("ROLLBACK").collect()
        session.sql(f"ALTER SERVICE {service_name} SUSPEND").collect()
        return {"status": "error", "message": str(e)}
$$;

-- =============================================================================
-- Task: Run the incremental load procedure daily at 5AM CT
-- =============================================================================
-- The procedure handles the Article Encoder service lifecycle internally
-- (resume before load, suspend after load or on failure)
-- =============================================================================

CREATE OR REPLACE TASK SFSE_PUBMED_DB.PUBMED.DAILY_PUBMED_INCREMENTAL_LOAD
    WAREHOUSE = PUBMED_ADHOC_WH
    SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'
AS
    CALL SFSE_PUBMED_DB.PUBMED.LOAD_PUBMED_INCREMENTAL();

ALTER TASK SFSE_PUBMED_DB.PUBMED.DAILY_PUBMED_INCREMENTAL_LOAD RESUME;

-- =============================================================================
-- 2. Task: Resume the Query Encoder SPCS service at 5AM CT (business hours)
-- =============================================================================
-- The Query Encoder is the service that powers real-time Cortex Search queries
-- using the MedCPT asymmetric model. It needs to be running for users to search.
-- Fire-and-forget: the service starts up in the background on GPU_ML_M_POOL.
-- =============================================================================

CREATE OR REPLACE TASK SFSE_PUBMED_DB.PUBMED.RESUME_QUERY_ENCODER_SVC
    WAREHOUSE = PUBMED_ADHOC_WH
    SCHEDULE  = 'USING CRON 0 5 * * * America/Chicago'
AS
    ALTER SERVICE SFSE_PUBMED_DB.PUBMED.MEDCPT_QUERY_ENCODER_SVC RESUME;

ALTER TASK SFSE_PUBMED_DB.PUBMED.RESUME_QUERY_ENCODER_SVC RESUME;

-- =============================================================================
-- 3. Task: Suspend the Query Encoder SPCS service at 11PM CT (off-hours)
-- =============================================================================
-- Shuts down the GPU compute to avoid costs when no one is searching.
-- The service will be resumed again at 5AM by the task above.
-- =============================================================================

CREATE OR REPLACE TASK SFSE_PUBMED_DB.PUBMED.SUSPEND_QUERY_ENCODER_SVC
    WAREHOUSE = PUBMED_ADHOC_WH
    SCHEDULE  = 'USING CRON 0 23 * * * America/Chicago'
AS
    ALTER SERVICE SFSE_PUBMED_DB.PUBMED.MEDCPT_QUERY_ENCODER_SVC SUSPEND;

ALTER TASK SFSE_PUBMED_DB.PUBMED.SUSPEND_QUERY_ENCODER_SVC RESUME;

-- =============================================================================
-- Manual testing
-- =============================================================================
-- CALL SFSE_PUBMED_DB.PUBMED.LOAD_PUBMED_INCREMENTAL();
-- CALL SFSE_PUBMED_DB.PUBMED.LOAD_PUBMED_INCREMENTAL(7);
--
-- Check task history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;
