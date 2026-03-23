-- =============================================================================
-- PubMed MedCPT Cortex Agent: Semantic Search over Biomedical Literature
-- =============================================================================
-- This script creates two objects:
--
-- 1. SEARCH_PUBMED_MEDCPT_SP (stored procedure)
--    - Takes a natural language query and returns the top 10 relevant PubMed
--      article chunks using MedCPT asymmetric embeddings + Cortex Search
--    - Flow:
--      a) Encode the query via medcpt_query_encoder_svc (SPCS Query Encoder)
--      b) Pass the resulting 768-dim vector to Cortex Search multi_index_query
--      c) Return matching chunks with article URLs
--    - MedCPT is asymmetric: queries use Query-Encoder, articles use
--      Article-Encoder. This proc handles the query side.
--
-- 2. MEDCPT_PUBMED_SP_AGENT (Cortex Agent)
--    - Wraps the search procedure as a tool in a Cortex Agent
--    - The agent orchestrates user questions, calls the search tool, and
--      synthesizes evidence-based answers with citations
--    - Uses the "generic" tool type to invoke the stored procedure
--
-- Dependencies:
--   - SPCS service: medcpt_query_encoder_svc (must be running for search)
--   - Cortex Search: PUBMED_OA_MEDCPT_SEARCH_SVC (multi-index: text + vector)
--   - Target table: PUBMED_OA_MEDCPT_FULL_EMBEDDINGS (~72M rows)
-- =============================================================================

use role sysadmin;
use schema SFSE_PUBMED_DB.pubmed;
use warehouse pubmed_adhoc_wh;

-- =============================================================================
-- Stored Procedure: Semantic search using MedCPT Query Encoder + Cortex Search
-- =============================================================================
-- 1. Encodes the user's query into a 768-dim vector via the MedCPT Query
--    Encoder SPCS service (asymmetric model - separate from article encoder)
-- 2. Passes the vector to Cortex Search multi_index_query which searches
--    both the text index (CHUNK) and vector index (EMBEDDING) simultaneously
-- 3. Returns top 10 matching chunks with article URLs and keys
-- =============================================================================

CREATE OR REPLACE PROCEDURE SFSE_PUBMED_DB.PUBMED.SEARCH_PUBMED_MEDCPT_SP(query_text VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'snowflake')
HANDLER = 'run'
AS
$$
import json
from snowflake.core import Root

def run(session, query_text):
    safe_query = query_text.replace("'", "''")
    result = session.sql(
        f"SELECT medcpt_query_encoder_svc!encode('{safe_query}'):EMBEDDING AS vec"
    ).collect()
    query_vector = json.loads(result[0]["VEC"])

    root = Root(session)
    svc = (root
        .databases["SFSE_PUBMED_DB"]
        .schemas["PUBMED"]
        .cortex_search_services["PUBMED_OA_MEDCPT_SEARCH_SVC"]
    )

    resp = svc.search(
        multi_index_query={
            "EMBEDDING": [{"vector": query_vector}]
        },
        columns=["CHUNK", "ARTICLE_URL", "KEY"],
        limit=10
    )
    return resp.results
$$;

-- quick test
-- CALL SFSE_PUBMED_DB.PUBMED.SEARCH_PUBMED_MEDCPT_SP('Aortic stenosis treatment outcomes');

-- =============================================================================
-- Cortex Agent: Wraps the search procedure as an agent tool
-- =============================================================================
-- - The agent receives user questions about biomedical topics
-- - Orchestration routes questions to the pubmed_custom_search_svc tool
-- - The tool invokes SEARCH_PUBMED_MEDCPT_SP to get relevant article chunks
-- - The agent synthesizes the retrieved evidence into a cited answer
-- - Instructions enforce that ARTICLE_URL is always included in responses
-- =============================================================================

CREATE OR REPLACE AGENT SFSE_PUBMED_DB.PUBMED.MEDCPT_PUBMED_SP_AGENT
  COMMENT = 
$$
# PubMed Custom Search Service

## Overview
A custom function that performs intelligent semantic search over **PubMed Open Access medical literature** using vector embeddings and Cortex Search technology.

## How It Works
1. Accepts a natural language query about medical topics
2. Converts the query into vector embeddings using a specialized medical concept encoder service (`medcpt_query_encoder_svc`)
3. Searches the PubMed Open Access database via Cortex Search (`PUBMED_OA_MEDCPT_SEARCH_SVC`)
4. Returns the **top 5 most semantically similar document chunks** along with article URLs and keys

## Key Features
- **Semantic Understanding**: Finds relevant articles even without exact keyword matches
- **Medical Domain Optimized**: Uses specialized medical concept encoders
- **Context-Aware**: Understands the meaning and intent behind queries
- **Results Limitation**: Returns top 5 chunks per query for performance optimization

## Usage Scenarios

### Medical Research Discovery
Researchers can query "treatment options for type 2 diabetes" to find relevant articles discussing diabetes management, enabling comprehensive literature reviews.

### Clinical Decision Support
Healthcare applications can integrate this to provide clinicians with evidence-based research in real-time during patient consultations.

### Biomedical Knowledge Extraction
Data scientists can build RAG (Retrieval-Augmented Generation) pipelines where retrieved chunks serve as context for AI models answering medical questions.

### Pharmaceutical Research
Drug development teams can search for information about drug interactions, side effects, or efficacy studies using natural language descriptions.

### Medical Education
Educational platforms can create interactive learning tools that retrieve relevant research articles based on student queries about medical concepts, diseases, or treatment protocols.

## Important Notes
- Requires underlying services (encoder and Cortex Search) to be available and properly configured
- Limited to PubMed Open Access publications
- SQL injection protection via quote escaping
- Executes with function owner's permissions
$$
  PROFILE = '{"display_name": "Pubmed Research Agent (MedCPT SP)"}'
  FROM SPECIFICATION
  $$
{
    "models": {
        "orchestration": "auto"
    },
    "orchestration": {},
    "instructions": {
        "response": "Provide concise, evidence-based answers. ALWAYS include the ARTICLE_URL for every article referenced in your response. Format each citation as a clickable link. If multiple articles are relevant, summarize the key findings across them and list all article URLs.",
        "orchestration": "Always use pubmed_search_custom_search_svc for any biomedical, clinical, or research question. Pass the user query directly as the search query. ALWAYS include the ARTICLE_URL for every article referenced in your response."
    },
    "tools": [
        {
            "tool_spec": {
                "type": "generic",
                "name": "pubmed_custom_search_svc",
                "description": "PROCEDURE/FUNCTION DETAILS:\n- Type: Custom Function (UDF)\n- Language: Python 3.11\n- Signature: (QUERY_TEXT VARCHAR)\n- Returns: VARIANT\n- Execution: OWNER with CALLED ON NULL INPUT\n- Volatility: VOLATILE\n- Primary Function: Semantic search using vector embeddings and Cortex Search\n- Target: PubMed Open Access medical literature database\n- Error Handling: Relies on Snowflake session error handling; SQL injection protection via quote escaping\n\nDESCRIPTION:\nThis function performs intelligent semantic search over PubMed Open Access medical literature by converting natural language queries into vector embeddings and retrieving the most relevant research articles. When called with a text query, it first encodes the query using a specialized medical concept encoder service (medcpt_query_encoder_svc), then searches the PUBMED_OA_MEDCPT_SEARCH_SVC Cortex Search service to find the top 5 most semantically similar document chunks along with their article URLs and keys. The function executes as OWNER, meaning it runs with the permissions of the function owner rather than the caller, which is important for accessing the underlying encoder service and Cortex Search service in the SFSE_PUBMED_DB database. This is particularly useful for building medical research applications, clinical decision support tools, literature review automation, or any application requiring context-aware retrieval of biomedical publications without requiring exact keyword matches. Users should be aware that this function depends on external services (the encoder and Cortex Search service) being available and properly configured, and results are limited to 5 chunks per query for performance optimization.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query_text": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "query_text"
                    ]
                }
            }
        }
    ],
    "skills": [],
    "tool_resources": {
        "pubmed_custom_search_svc": {
            "execution_environment": {
                "type": "warehouse",
                "warehouse": ""
            },
            "identifier": "SFSE_PUBMED_DB.PUBMED.SEARCH_PUBMED_MEDCPT_SP",
            "name": "SEARCH_PUBMED_MEDCPT_SP(VARCHAR)",
            "type": "procedure"
        }
    }
}
  $$;

-- =============================================================================
-- Example queries to test the agent
-- =============================================================================
-- what are aortic stenosis clinical outcomes
-- What are the risk factors for progression of aortic stenosis?
-- which companies are the leader in aortic stenosis treatment?
