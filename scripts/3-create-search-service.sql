-- =============================================================================
-- Cortex Search Service: PubMed MedCPT Multi-Index (Text + Vector)
-- =============================================================================
-- Creates a Cortex Search service over the full PubMed embeddings table.
-- This service supports multi-index queries combining:
--   - Text index on CHUNK (keyword/semantic text search, auto-built by Cortex)
--   - Vector index on EMBEDDING (768-dim MedCPT article embeddings)
--
-- The MedCPT Query Encoder SPCS service generates query vectors at search time,
-- which are passed via the multi_index_query parameter in the Python API.
--
-- Source table: PUBMED_OA_MEDCPT_FULL_EMBEDDINGS (~72M rows)
-- Primary key:  CHUNK_ID (unique per chunk, used for dedup during refresh)
-- Refresh lag:  1 day (picks up new rows from the daily incremental load)
-- Warehouse:    PUBMED_L_WH (Large - initial build takes ~2 hours)
--
-- Note: Initial creation is expensive (~2 hours). Subsequent refreshes are
-- incremental and much faster since the table uses change tracking.
-- =============================================================================

use role sysadmin;
use schema SFSE_PUBMED_DB.pubmed;
use warehouse pubmed_l_wh;

CREATE OR REPLACE CORTEX SEARCH SERVICE pubmed_oa_medcpt_search_svc
  VECTOR INDEXES (EMBEDDING)
  PRIMARY KEY (CHUNK_ID)
  WAREHOUSE = PUBMED_L_WH
  TARGET_LAG = '1 day'
AS (
  SELECT *
  FROM pubmed_oa_medcpt_full_embeddings
);
