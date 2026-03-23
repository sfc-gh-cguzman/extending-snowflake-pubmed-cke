# Extending Snowflake's PubMed CKE with Custom MedCPT Embeddings

## What this is

A reference implementation that extends Snowflake's **PubMed Biomedical Research Corpus** Curated Knowledge Extension (CKE) with domain-specific [MedCPT](https://github.com/ncbi/MedCPT) embeddings, Cortex Search multi-index queries, and a Cortex Agent - all running natively on Snowflake.

The PubMed CKE provides ~72 million pre-chunked Open Access biomedical articles as a free Marketplace listing. This project layers custom medical embeddings on top to enable high-quality semantic search over the full corpus, exposed through a conversational agent.

## Why extend a CKE?

Snowflake's Curated Knowledge Extensions do the heavy lifting:

- **Daily ingestion** of new PubMed articles from NLM's Open Access subset
- **Deduplication** across article versions and retractions
- **Chunking** into ~4,000 character segments with metadata (PMID, citation, license, URLs)
- **Pre-built views** (`PUBMED_OA_VW`) ready for downstream consumption

You get a production-grade, continuously updated biomedical knowledge base without building any data pipelines. The CKE is the foundation - this project extends it by adding domain-specific embeddings and search capabilities that the generic listing doesn't provide.

## Why MedCPT over a generalist embedding model?

Snowflake's built-in `snowflake-arctic-embed-m-v1.5` is a strong general-purpose embedding model that works well for most text. But for biomedical literature search, a domain-specific model like MedCPT delivers measurably better results.

**MedCPT** (Medical Contrastive Pre-Training) was developed by NCBI (the same organization that runs PubMed) and trained specifically on biomedical text:

| | MedCPT (Article + Query Encoders) | snowflake-arctic-embed-m-v1.5 |
|---|---|---|
| **Training data** | 26M PubMed article-query pairs from real user click logs | General web text (MS MARCO, NLI, etc.) |
| **Architecture** | Asymmetric dual-encoder (separate article and query models) | Symmetric single encoder |
| **Domain vocabulary** | Natively understands medical terminology, drug names, gene symbols, ICD codes | General English vocabulary |
| **Retrieval task** | Optimized for biomedical literature retrieval | Optimized for general web search |
| **Embedding dim** | 768 | 768 |

### What does "asymmetric" mean in practice?

MedCPT uses two separate encoders because articles and queries are fundamentally different:
- **Article Encoder**: Processes long-form biomedical text (study descriptions, methods, results). Used at index time.
- **Query Encoder**: Processes short clinical questions or search terms. Used at search time.

This asymmetry means a query like "aortic stenosis treatment outcomes" gets mapped to the same vector space as article passages describing TAVR procedures, valve replacement studies, and hemodynamic measurements - even though the surface-level text looks very different.

### When does this matter?

- **Medical terminology**: MedCPT understands that "MI" means "myocardial infarction," not "Michigan"
- **Clinical reasoning**: A query about "first-line treatment for HFrEF" correctly retrieves articles about SGLT2 inhibitors and beta-blockers
- **Abbreviations and synonyms**: "CKD stage 3" matches articles discussing "chronic kidney disease with GFR 30-59"
- **Drug-disease associations**: Queries about side effects retrieve pharmacovigilance studies, not just articles containing the drug name

For non-medical use cases, `snowflake-arctic-embed` is the better choice - it's simpler (one model, no SPCS needed), cheaper, and performs well on general text. MedCPT is for when retrieval quality on biomedical content is the priority.

## Architecture

![Architecture Diagram](architecture.drawio.png)

> Open [`architecture.drawio`](architecture.drawio) in [draw.io](https://app.diagrams.net) for the interactive version. Also available as [`architecture.excalidraw`](assets/architecture.excalidraw).
>
> To regenerate the PNG: open `architecture.drawio` in draw.io, then **File > Export as > PNG** (scale 2x) and save as `architecture.drawio.png`.

### SPCS services

| Service | Model | Purpose | Compute Pool | Lifecycle |
|---|---|---|---|---|
| `MEDCPT_EMBEDDER_SVC` | ncbi/MedCPT-Article-Encoder | Bulk historical embedding (one-time) | GPU_ML_M_POOL (GPU_NV_M) | Manual |
| `medcpt_embedder_incremental_svc` | ncbi/MedCPT-Article-Encoder | Daily incremental embedding | PUBMED_GPU_S_POOL (GPU_NV_S) | Auto (proc manages resume/suspend) |
| `medcpt_query_encoder_svc` | ncbi/MedCPT-Query-Encoder | Real-time query encoding at search time | PUBMED_GPU_S_POOL (GPU_NV_S) | Auto (tasks: 5AM resume, 11PM suspend) |

## Scripts

Run these in order:

| Script | Description |
|---|---|
| `0-setup.sql` | Get the Marketplace listing, create database/schemas, provision warehouses and GPU compute pools, materialize source data |
| `1-medcpt-ml-serving-inference-article-encoder.ipynb` | Register MedCPT Article Encoder in ML Registry, deploy as SPCS service, validate embeddings |
| `2-medcpt-embed-pubmed-historical.sql` | One-time bulk embed of all ~72M chunks (~20 hours). For a demo, add `LIMIT 1000000` |
| `3-create-search-service.sql` | Create Cortex Search service with text + vector indexes over the embeddings table |
| `4-medcpt-ml-serving-inference-query-encoder.ipynb` | Register MedCPT Query Encoder in ML Registry, deploy as SPCS service, test end-to-end search |
| `5-create-custom-medcpt-pubmed-agents.sql` | Create the search stored procedure and Cortex Agent |
| `6-automate-incremental-scripts.sql` | Daily incremental load procedure (with SPCS lifecycle management) and service scheduling tasks |

## Cost breakdown

All estimates assume **AWS US West (Oregon)**, **Enterprise Edition** ($3.00/credit on-demand). Capacity pricing will be lower. Rates sourced from the [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) (March 2026).

### One-time historical embedding (~72M rows)

This is the most expensive step. You run it once to bootstrap the full corpus.

| Resource | Config | Duration | Credits/hr | Total credits | Est. cost |
|---|---|---|---|---|---|
| MEDCPT_EMBEDDER_SVC (Article Encoder) | GPU_NV_M, 4 instances | ~20 hrs | 2.68 x 4 = 10.72 | ~214 | ~$642 |
| PUBMED_L_WH (Large warehouse) | L warehouse for INSERT | ~20 hrs | 8 | ~160 | ~$480 |
| **Total** | | | | **~374** | **~$1,122** |

> For a demo or POC, use `LIMIT 1000000` on the source query. 1M rows runs in ~15-20 minutes and costs under $5.

### Cortex Search service creation (one-time)

| Resource | Config | Duration | Credits/hr | Total credits | Est. cost |
|---|---|---|---|---|---|
| PUBMED_L_WH (Large warehouse) | L warehouse for initial build | ~2 hrs | 8 | ~16 | ~$48 |

Subsequent daily refreshes are incremental and much cheaper (minutes, not hours).

### Daily incremental load

The daily task processes new/updated articles from the last 3 days. Typical volume: ~20-30 PMIDs, ~500-600 chunks.

| Resource | Config | Duration | Credits/hr | Total credits | Est. cost |
|---|---|---|---|---|---|
| medcpt_embedder_incremental_svc | GPU_NV_S, 1 instance | ~5-10 min | 0.57 | ~0.10 | ~$0.30 |
| PUBMED_ADHOC_WH (XSmall) | XS warehouse for DELETE+INSERT | ~2-3 min | 1 | ~0.05 | ~$0.15 |
| **Total per day** | | | | **~0.15** | **~$0.45** |
| **Total per month** | | | | **~4.5** | **~$13.50** |

The stored procedure manages the SPCS service lifecycle automatically - resumes before load, suspends after - so you only pay for GPU time during the actual embedding work.

### Query Encoder service (business hours)

The Query Encoder runs during business hours (5AM - 11PM CT) to serve real-time search requests.

| Resource | Config | Duration | Credits/hr | Total credits | Est. cost |
|---|---|---|---|---|---|
| medcpt_query_encoder_svc | GPU_NV_S, 1 instance | 18 hrs/day | 0.57 | 10.26/day | ~$30.78/day |
| **Total per month (weekdays)** | | ~22 business days | | **~226** | **~$677** |

> This is the biggest ongoing cost. To reduce it: shorten the window (e.g., 8AM-6PM), suspend on weekends, or consider moving query encoding to a CPU-based approach if latency tolerance allows.

### Monthly steady-state summary

| Component | Monthly credits | Monthly cost (on-demand) |
|---|---|---|
| Query Encoder SPCS (business hours, weekdays) | ~226 | ~$677 |
| Daily incremental load | ~4.5 | ~$13.50 |
| Cortex Search refresh (serverless) | ~5-10 | ~$15-30 |
| Storage (~300GB compressed) | - | ~$7 |
| **Total** | **~240-250** | **~$713-728** |

### Demo / POC cost (1M rows, no always-on query encoder)

| Component | Credits | Cost |
|---|---|---|
| Historical embed (1M rows) | ~2 | ~$6 |
| Cortex Search build | ~2 | ~$6 |
| Query Encoder (on-demand, suspend when not using) | ~0.57/hr | ~$1.71/hr |
| **Setup total** | **~4** | **~$12** |

## Prerequisites

- Snowflake Enterprise Edition (or higher)
- ACCOUNTADMIN role (for Marketplace listing + privilege grants)
- SYSADMIN role (for all other objects)
- GPU compute pool access (GPU_NV_M and GPU_NV_S instance families)
- Snowflake ML Registry access

## References

- [PubMed Marketplace Listing](https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQW/snowflake-pubmed-biomedical-research-corpus)
- [MedCPT Paper (NCBI)](https://arxiv.org/abs/2307.00589)
- [MedCPT-Article-Encoder (HuggingFace)](https://huggingface.co/ncbi/MedCPT-Article-Encoder)
- [MedCPT-Query-Encoder (HuggingFace)](https://huggingface.co/ncbi/MedCPT-Query-Encoder)
- [Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Snowflake ML Registry Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/overview)
- [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
