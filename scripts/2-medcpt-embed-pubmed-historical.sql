-- =============================================================================
-- Historical Embedding: Bulk embed all PubMed articles with MedCPT Article Encoder
-- =============================================================================
-- This script performs a one-time bulk embedding of the entire PubMed Open Access
-- corpus using the MedCPT Article Encoder SPCS service (MEDCPT_EMBEDDER_SVC).
--
-- The Article Encoder generates 768-dim vectors for each text chunk. These
-- embeddings are stored alongside the source data in PUBMED_OA_FULL_EMBEDDINGS,
-- which feeds the Cortex Search service for vector + text multi-index queries.
--
-- Source table: PUBMED_OA (chunked PubMed articles from the Marketplace listing)
-- Target table: PUBMED_OA_FULL_EMBEDDINGS (same schema + EMBEDDING VECTOR(FLOAT,768))
-- SPCS service: MEDCPT_EMBEDDER_SVC (MedCPT Article Encoder on GPU)
--
-- Steps:
--   1. Test on 10 records to validate embeddings look correct
--   2. Truncate test data
--   3. Run full historical embed (~20 hours for ~72M rows)
--   4. Suspend the SPCS service to stop GPU costs
-- =============================================================================

use role sysadmin;
use schema SFSE_PUBMED_DB.pubmed;
use warehouse pubmed_l_wh;

-- =============================================================================
-- Step 1: Test embed on 10 records to validate output before committing to
-- the full ~20 hour run. Verify EMBEDDING column is populated with 768-dim vectors.
-- =============================================================================

insert into PUBMED_OA_FULL_EMBEDDINGS
select *, MEDCPT_EMBEDDER_SVC!encode(CHUNK):EMBEDDING::VECTOR(FLOAT, 768) AS EMBEDDING
from PUBMED_OA
limit 10;

select embedding, *
from PUBMED_OA_FULL_EMBEDDINGS
;

-- =============================================================================
-- Step 2: Clean up test data before the full load
-- =============================================================================

truncate table PUBMED_OA_FULL_EMBEDDINGS;

-- =============================================================================
-- Step 3: Full historical embed - one-time bulk load of all PubMed chunks
-- This calls the SPCS Article Encoder for every row. ~20 hours on PUBMED_L_WH.
--
-- For a quicker demo, add LIMIT 1000000 to the query below. 1M records is
-- enough to demonstrate end-to-end search quality and runs in ~15-20 minutes
-- instead of ~20 hours.
-- =============================================================================

insert into PUBMED_OA_FULL_EMBEDDINGS
select *, MEDCPT_EMBEDDER_SVC!encode(CHUNK):EMBEDDING::VECTOR(FLOAT, 768) AS EMBEDDING
from PUBMED_OA;

-- =============================================================================
-- Step 4: Suspend the SPCS service to stop GPU compute costs
-- The service is only needed again for incremental loads (handled by the
-- daily task which manages its own service lifecycle).
-- =============================================================================

alter service MEDCPT_EMBEDDER_SVC suspend;
