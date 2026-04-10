# Build Journal — Podcast Knowledge Pipeline

A running log of architectural decisions, tradeoffs, and lessons learned. Written for interview prep — each phase includes talking points you can use when discussing this project.

---

## Phase 1: Project Setup & Infrastructure

**Date:** 2026-04-10

### What was built
- Git repo with full project structure
- Docker Compose orchestrating 5 services: PostgreSQL (with pgvector), Airflow webserver, Airflow scheduler, FastAPI, and Streamlit
- Local Airflow using LocalExecutor (single-machine, no Celery/Redis overhead)
- GCP integration: BigQuery for warehouse, GCS for raw object storage

### Why this approach
- **Local Airflow + remote BigQuery:** Cloud Composer costs ~$300/month minimum. Running Airflow locally in Docker is free, and BigQuery's free tier (1TB queries/month) covers this project easily. The Airflow DAGs are identical whether they run locally or in Cloud Composer — the orchestration logic is what matters on a resume, not where it's hosted.
- **pgvector in the same Postgres instance:** Rather than standing up a separate vector DB (Pinecone, Weaviate, etc.), we use pgvector as an extension on the same PostgreSQL that Airflow uses for metadata. One fewer service to manage, and pgvector is production-grade for our scale.
- **LocalExecutor over CeleryExecutor:** With 4 podcast channels and daily incremental loads, we don't need distributed task execution. LocalExecutor keeps the Docker setup simple (no Redis, no separate worker containers).

### What I learned
- `dbt-bigquery` and Airflow have conflicting dependencies (`google-cloud-aiplatform` requires SQLAlchemy 2.x, Airflow 2.10 requires < 2.0). Solution: isolate DBT in its own Python venv within the same Docker image, call it via BashOperator. This is the standard production pattern.
- The Airflow Docker image has a custom entrypoint — non-Airflow services (FastAPI, Streamlit) need `entrypoint: []` to bypass it.
- `PYTHONPATH` must be set explicitly for Airflow to import the `src` package from DAG files.

### Interview talking points
- "I chose local Airflow over Cloud Composer to keep costs near zero while keeping the DAG code portable — the same DAGs would run in Cloud Composer with just a connection config change."
- "I used pgvector instead of a managed vector DB because at this scale, a PostgreSQL extension gives me the same retrieval quality without adding another service to maintain."
- "The Docker Compose setup mirrors a production topology — separate webserver, scheduler, and application services — even though it runs on a single machine."

---

## Phase 2: YouTube Ingestion Pipeline

**Date:** 2026-04-10

### What was built
- YouTube API client fetching video metadata and transcripts from 3 channels (Joe Rogan, Huberman Lab, Lex Fridman)
- GCS writer storing raw JSON (metadata + full transcript) per video
- BigQuery loader with MERGE-based upserts for idempotent staging
- Incremental loading using high watermark (most recent publish date per channel)
- Two Airflow DAGs: `ingest_and_extract` (daily scheduled) and `backfill_channel` (manual trigger)

### Why this approach
- **`youtube-transcript-api` over the official Captions API:** The official API requires OAuth and video owner permission. The transcript library scrapes auto-generated captions directly — zero API quota cost for the most expensive operation.
- **MERGE for upserts:** BigQuery doesn't support INSERT ON CONFLICT. MERGE gives us idempotent loads — re-running the pipeline on the same videos updates rather than duplicates.
- **High watermark over Airflow's execution_date:** Tracking the watermark in BigQuery itself makes the pipeline self-contained. You can blow away the Airflow metadata DB and the pipeline still knows where it left off.
- **One Airflow task per channel:** Channels are independent — if Joe Rogan fails, Huberman still ingests. Simple parallelism without complex DAG dependencies.

### What I learned
- `youtube-transcript-api` v1.x has a completely different API from v0.x. The class is now instantiated (`YouTubeTranscriptApi()`), uses `.fetch()` instead of `.get_transcript()`, and returns `FetchedTranscript` objects with `.snippets`.
- YouTube channel IDs can go stale or be ambiguous — always verify with a live API call rather than hardcoding from documentation.
- A single 3-hour Joe Rogan episode can produce 250K+ characters of transcript. Chunking strategy for LLM extraction (Phase 3) will need to handle this scale.

### Interview talking points
- "The pipeline uses incremental loading with a BigQuery-stored watermark — each run only fetches videos published after the last successful ingest. This keeps API costs near zero for daily runs."
- "I chose MERGE statements over INSERT for idempotent loads. You can safely re-run any DAG without creating duplicate records — important for production reliability."
- "Transcript fetching bypasses the YouTube API entirely using a direct caption scraper, which means the most data-intensive operation costs zero API quota."

---

## Phase 3: LLM Extraction

**Date:** 2026-04-10

### What was built
- Token-aware transcript chunker (6K tokens/chunk with 200-token overlap)
- Structured extraction prompt producing JSON with guests, topics, claims, tools/products, and recommendations
- Claude Haiku extractor with retry logic for rate limits and parse failures
- Cross-chunk merger that deduplicates entities per episode (guests by name, topics by name, etc.)
- BigQuery loader for 5 extraction tables with idempotent delete-then-insert pattern
- Wired into Airflow DAG: extraction runs automatically after ingestion, only processes unextracted episodes

### Why this approach
- **6K token chunks over 4K:** Fewer LLM calls (6 vs 9 for a typical episode) saves ~35% on API costs. Haiku handles 6K input chunks without quality degradation.
- **Max 5 claims per chunk:** Claims are the biggest output token driver. Capping at 5 "most significant" per chunk cuts output costs ~50% while keeping the most valuable insights.
- **Compressed prompt template:** Reduced prompt overhead from 474 to 262 tokens/chunk by eliminating verbose JSON examples. Total cost optimization: ~$0.05/episode vs ~$0.10 before.
- **Delete-then-insert for idempotency:** BigQuery doesn't support upsert on non-primary-key columns well. Deleting existing extraction rows for a video_id before reinserting is simple, safe, and makes re-extraction trivial.

### What I learned
- The Anthropic SDK's `anthropic.BadRequestError` for credit issues is confusingly named — it looks like a request format error but is actually a billing issue.
- Haiku produces clean JSON reliably with a concise prompt. The markdown code block stripping logic (`\`\`\`json...\`\`\``) fires occasionally but isn't needed most of the time.
- For a 3-hour podcast (250K chars, ~60K tokens), extraction across 11 chunks takes about 30-45 seconds. Workable for batch processing.

### Interview talking points
- "I optimized LLM costs by 50% through three changes: larger chunks (fewer API calls), capping claims per chunk (less output), and compressing the prompt template. For 500 episodes, that's $27 vs $50."
- "The extraction pipeline is idempotent — you can re-run it on any episode and get fresh results without duplicates. Important when you're iterating on prompt quality."
- "I used a structured JSON extraction approach over free-form text because it feeds directly into dimensional models downstream. The LLM output is the data pipeline's input — schema consistency matters."

---

## Phase 4: DBT Modeling

**Date:** 2026-04-10

### What was built
- DBT project with BigQuery adapter running in an isolated venv
- 5 staging views (guests, claims, topics, tools/products, recommendations)
- 1 intermediate view (episodes enriched with transcript metadata)
- 5 mart tables: dim_channels, dim_episodes, dim_guests, dim_topics, fact_claims, mart_recommendations
- 13 data tests (unique, not_null, relationships) — all passing
- DBT wired into Airflow DAG via BashOperator (dbt run → dbt test)

### Why this approach
- **Star schema:** fact_claims at the center, linked to dimension tables for guests, topics, episodes, and channels. This enables cross-cutting queries like "Which guests appear on multiple shows?" and "What topics trend over time?" — exactly the kind of questions the RAG chat and API will serve.
- **DBT in isolated venv, called via BashOperator:** DBT and Airflow have conflicting dependencies. The production pattern is running DBT in its own environment, triggered by Airflow. Same approach used at companies running Cloud Composer + DBT.
- **Views for staging/intermediate, tables for marts:** Staging views are cheap (no storage cost, always fresh). Mart tables materialize the aggregations so downstream queries are fast.

### What I learned
- DBT staging models can't share names with existing BigQuery tables — if the ingestion loader creates `stg_videos` as a table, DBT can't create a view with the same name. Solution: reference source tables directly in intermediate models.
- DBT 1.11 uses `dbtRunner().invoke()` for programmatic execution — the CLI entry point had silent exit code 2 errors due to permission issues on the logs directory.
- BigQuery's `ARRAY_AGG(DISTINCT ... IGNORE NULLS)` is perfect for building the "channels appeared on" arrays in dimension tables.

### Interview talking points
- "The dimensional model turns unstructured LLM output into queryable analytics. dim_guests tracks cross-channel appearances, fact_claims links every insight to its speaker, topic, and episode — all with referential integrity tested by DBT."
- "DBT runs inside the Airflow DAG as a BashOperator calling an isolated venv. This is the same pattern used in production Cloud Composer deployments — it avoids dependency conflicts while keeping orchestration centralized."
- "All 13 data quality tests pass on every run: uniqueness on dimension keys, not-null on required fields, and referential integrity between fact and dimension tables."

---

## Phase 5: Embedding Generation & Vector Store

**Date:** 2026-04-10

### What was built
- Local embedding model (`all-MiniLM-L6-v2` via sentence-transformers) — runs entirely in Docker, zero API cost
- Character-based chunker splitting transcripts into ~500-char segments for granular retrieval
- pgvector storage with HNSW index for fast cosine similarity search
- Idempotent loader (deletes + reinserts per video_id)
- Wired into Airflow DAG as the final task in the pipeline

### Why this approach
- **Local embeddings over OpenAI:** Eliminates an API dependency and cost. `all-MiniLM-L6-v2` produces 384-dim vectors that are perfectly adequate for semantic search over podcast transcripts. At our scale (~1000 chunks per episode), the quality difference vs OpenAI's ada-002 is negligible.
- **500-char embedding chunks (smaller than 6K extraction chunks):** Embedding chunks are for retrieval — you want granular matches. Extraction chunks are for comprehension — you want the LLM to see enough context. Different purposes, different sizes.
- **HNSW index:** Approximate nearest neighbor search that's fast at query time. With ~1000 embeddings per episode, even exact search would be fine, but HNSW scales to millions of rows.

### What I learned
- psycopg2 requires plain `postgresql://` URIs, not SQLAlchemy's `postgresql+psycopg2://` — a common gotcha when sharing connection strings across ORMs and raw drivers.
- The model loads take ~1 second each. In a production pipeline, you'd load once and reuse across episodes — but for a daily batch pipeline processing a handful of new episodes, per-episode loading is fine.

### Interview talking points
- "I chose local embeddings over OpenAI to eliminate a cost dependency — the sentence-transformers model runs inside Docker for free and produces vectors that work well for semantic search at our scale."
- "The vector store uses pgvector in the same PostgreSQL instance as Airflow metadata — one fewer service to manage. The HNSW index gives sub-millisecond similarity search even as the embedding count grows."
- "There's a deliberate size difference between extraction chunks (6K tokens) and embedding chunks (500 chars). Extraction needs broad context for the LLM to reason about. Retrieval needs granular segments so search results are precise."

---

## Phase 6: FastAPI + RAG Chat

**Date:** 2026-04-10

### What was built
- 6 REST endpoints: /episodes, /episodes/{id}, /guests, /topics/trending, /claims/search, /recommendations
- RAG chat endpoint (/chat) combining vector search + Claude Haiku for sourced answers
- Streamlit chat UI with conversation history, source citations, and sidebar stats
- All endpoints query BigQuery mart tables via parameterized queries

### Why this approach
- **Hybrid retrieval (vector search + structured SQL):** The REST endpoints serve structured queries (guest counts, topic trends) directly from BigQuery marts. The RAG chat uses pgvector for semantic search over raw transcripts. Different access patterns, different storage — each optimized for its use case.
- **Claude Haiku for RAG generation:** Same model as extraction, consistent cost. The system prompt constrains it to cite sources by number, preventing hallucination about content not in the retrieved context.
- **Streamlit over React:** For a DE portfolio project, the pipeline is what matters. Streamlit gets a functional chat UI running in 60 lines. React would add days of frontend work with no additional resume signal for DE roles.

### What I learned
- FastAPI with BigQuery works well — parameterized queries prevent injection, and the BigQuery client handles connection pooling.
- RAG quality depends heavily on chunk size and embedding model matching. The 500-char embedding chunks produce more precise retrieval than the 6K extraction chunks would.
- The singleton pattern for the sentence-transformers model prevents reloading on every request — important for latency.

### Interview talking points
- "The serving layer has two access patterns: structured REST endpoints for analytics queries, backed by BigQuery marts, and a RAG chat for open-ended questions, backed by pgvector. Different access patterns, different storage engines."
- "The RAG pipeline is: embed question → pgvector cosine similarity → top-K chunks → Claude Haiku with source-constrained prompt → answer with citations. The whole path runs in under 3 seconds."
- "I deliberately kept the chat UI minimal with Streamlit — the value of this project is the pipeline, not the frontend. But the chat demonstrates the end-to-end value: ask a natural language question, get a sourced answer from structured podcast data."

---

## Phase 7: Polish & Production Readiness

**Date:** 2026-04-10

### What was built
- Comprehensive README with architecture diagram, quick start, API docs, and cost breakdown
- Full backfill: 16 episodes across 3 channels (10 Rogan, 3 Lex Fridman, 3 Huberman Lab)
- Final dataset: 513 claims, 503 topics, 27 guests, 180 recommendations, 4,658 embeddings
- Resume bullet points prepared

### What I learned
- Some YouTube channels have transcripts disabled — the pipeline correctly handles this by recording the metadata and marking `has_transcript = false`, so it doesn't retry endlessly.
- BigQuery's streaming buffer prevents DELETE on recently inserted rows. In production, you'd either wait 90 minutes or use table-level operations (TRUNCATE + INSERT) instead of row-level DELETE for idempotent loads.
- Joe Rogan episodes are consistently 3-4 hours long (5,000+ transcript segments each). The pipeline handles these without issue thanks to chunking.

### Final interview talking points
- "The complete pipeline — YouTube ingestion, LLM extraction, dimensional modeling, vector embedding, and API/chat serving — runs in Docker Compose with five services. One command to start."
- "I processed 16 episodes across 3 channels, extracting 513 structured claims, 503 topics, and 180 recommendations. The star schema enables queries like 'which guests appear on multiple shows' and 'what topics trend across channels.'"
- "Total infrastructure cost: $0/month (local Docker + BigQuery free tier). Total LLM cost for 16 episodes: approximately $1. Scales to 500+ episodes for under $30."
