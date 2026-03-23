-- =============================================================================
-- Environment Setup: PubMed MedCPT Embedding & Search Pipeline
-- =============================================================================
-- This script provisions all infrastructure needed for the PubMed MedCPT
-- embedding and Cortex Search pipeline:
--
--   1. Get the PubMed Marketplace listing (free, ~72M chunked articles)
--   2. Create the working database and schemas
--   3. Grant SPCS and integration privileges to SYSADMIN
--   4. Create warehouses (Large for bulk, XSmall for incremental/adhoc)
--   5. Create GPU compute pools (Medium for bulk, Small for incremental)
--   6. Materialize the PubMed data locally for embedding
--   7. Create the target table with a VECTOR column for MedCPT embeddings
--
-- Run this script once before proceeding to the model registration notebooks.
-- =============================================================================

-- =============================================================================
-- Step 1: Get the PubMed Marketplace listing
-- =============================================================================
-- The PubMed Biomedical Research Corpus is a free Snowflake Marketplace listing
-- containing ~72M pre-chunked Open Access articles. This creates a shared
-- database that stays in sync with the provider's updates.
-- Listing: https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQW
-- =============================================================================

use role accountadmin;

DESCRIBE AVAILABLE LISTING GZSTZ67BY9OQW;

CREATE DATABASE IF NOT EXISTS PUBMED_BIOMEDICAL_RESEARCH_CORPUS FROM LISTING 'GZSTZ67BY9OQW';

-- =============================================================================
-- Step 2: Create working database and schemas
-- =============================================================================
-- SFSE_PUBMED_DB.PUBMED  - main schema for tables, models, services, and agents
-- SFSE_PUBMED_DB.UTILS   - utility objects
-- =============================================================================

use role sysadmin;

create or alter DATABASE SFSE_PUBMED_DB;
create or alter SCHEMA PUBMED;
create or alter SCHEMA UTILS;

-- Verify the listing data is accessible (~72M rows)
SELECT COUNT (*)
FROM PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_VW;

SELECT TOP 100
FROM PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_VW;

-- =============================================================================
-- Step 3: Grant SPCS and integration privileges to SYSADMIN
-- =============================================================================
-- SYSADMIN needs these to create compute pools (for SPCS GPU services) and
-- integrations (for external access if needed). One-time ACCOUNTADMIN grants.
-- =============================================================================

use role accountadmin;
grant create integration on account to role sysadmin;
grant create compute pool on account to role sysadmin;

-- =============================================================================
-- Step 4: Create warehouses and GPU compute pools
-- =============================================================================
-- Two warehouse tiers:
--   PUBMED_L_WH     - Large, for the one-time historical embed (~20hr) and
--                      initial Cortex Search service build (~2hr)
--   PUBMED_ADHOC_WH - XSmall, for daily incremental loads and ad-hoc queries
--
-- Two GPU compute pools:
--   GPU_ML_M_POOL      - Medium GPUs (GPU_NV_M), up to 10 nodes, for the bulk
--                         historical embedding run with high parallelism
--   PUBMED_GPU_S_POOL  - Small GPUs (GPU_NV_S), up to 4 nodes, for the
--                         incremental embedding service and query encoder
-- =============================================================================

use role sysadmin;

create or replace warehouse pubmed_l_wh
with warehouse_size='LARGE'
auto_suspend=60;

CREATE COMPUTE POOL IF NOT EXISTS GPU_ML_M_POOL 
  min_nodes = 1
  max_nodes = 10 
  instance_family = 'GPU_NV_M'
;

create or replace warehouse pubmed_adhoc_wh
with warehouse_size='XSMALL'
auto_suspend=60;

CREATE COMPUTE POOL IF NOT EXISTS PUBMED_GPU_S_POOL 
  min_nodes = 1
  max_nodes = 4 
  instance_family = 'GPU_NV_S'
;

-- =============================================================================
-- Step 5: Materialize PubMed data locally and create target tables
-- =============================================================================
-- PUBMED_OA                  - Full local copy of the listing view (~72M rows).
--                              Materializing avoids repeated cross-account reads
--                              during the bulk embedding run.
-- PUBMED_OA_TEST_SET         - 0.15% sample (~108K rows) for quick iteration
--                              and testing during model development.
-- PUBMED_OA_FULL_EMBEDDINGS  - Same schema as PUBMED_OA plus a 768-dim VECTOR
--                              column for MedCPT article embeddings. This is
--                              the target for both historical and incremental loads,
--                              and the source for the Cortex Search service.
-- =============================================================================

use warehouse pubmed_l_wh;

-- ~1min40s on Large warehouse
CREATE OR REPLACE TABLE PUBMED_OA SELECT
SELECT *
FROM PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_VW;

create or replace table pubmed_oa_test_set as 
select *
from pubmed_oa 
sample (0.15)
;

create or replace table PUBMED_OA_FULL_EMBEDDINGS 
like PUBMED_OA;

ALTER TABLE PUBMED_OA_FULL_EMBEDDINGS ADD COLUMN EMBEDDING VECTOR(FLOAT, 768);
