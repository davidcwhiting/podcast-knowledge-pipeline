# Podcast Knowledge Pipeline

An Airflow-orchestrated data pipeline that ingests YouTube podcast transcripts, extracts structured knowledge using Claude AI, models it in BigQuery with DBT, and serves it via a REST API and RAG-powered chat interface.

## Architecture

```
YouTube API ──► Airflow DAG ──► GCS (raw JSON)
                    │
                    ▼
              BigQuery Staging
                    │
                    ▼
            Claude Haiku (LLM extraction)
                    │
                    ▼
           BigQuery Raw Extraction Tables
                    │
                    ▼
              DBT (star schema)
                    │
                    ├──► BigQuery Marts ──► FastAPI REST API
                    │
                    └──► pgvector (embeddings) ──► RAG Chat
```

## What It Does

**Ingests** podcast episodes from YouTube channels (transcripts + metadata), **extracts** structured knowledge (guests, topics, claims, recommendations) using LLM-powered extraction, **models** it into a dimensional star schema, and **serves** it through both structured REST endpoints and a natural language chat interface.

### Seed Channels
- Joe Rogan Experience
- Huberman Lab (Andrew Huberman)
- Lex Fridman Podcast

### Extracted Entities
| Entity | Description |
|--------|-------------|
| **Guests** | Name, title, organization, cross-channel appearances |
| **Topics** | Categorized topics with episode frequency |
| **Claims** | Attributed statements classified as factual, opinion, prediction, or anecdote |
| **Tools/Products** | Mentioned products with endorsement/criticism/neutral context |
| **Recommendations** | Books, practices, people recommended by guests |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow (LocalExecutor, Docker) |
| Warehouse | Google BigQuery |
| Object Storage | Google Cloud Storage |
| Transformation | DBT (BigQuery adapter) |
| LLM Extraction | Claude Haiku (Anthropic API) |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2, local) |
| Vector Store | PostgreSQL + pgvector |
| API | FastAPI |
| Chat UI | Streamlit |
| Infrastructure | Docker Compose |

## Quick Start

### Prerequisites
- Docker and Docker Compose
- GCP project with BigQuery + Cloud Storage APIs enabled
- GCP service account key with BigQuery Admin + Storage Admin roles
- YouTube Data API v3 key
- Anthropic API key

### Setup

```bash
# Clone the repo
git clone https://github.com/davidcwhiting/podcast-knowledge-pipeline.git
cd podcast-knowledge-pipeline

# Configure credentials
cp .env.example .env
# Edit .env with your API keys and GCP project ID
# Place your GCP service account key at config/gcp-key.json

# Build and start
docker compose up -d

# Wait for Airflow to initialize (~30 seconds)
# Then access:
#   Airflow UI:  http://localhost:8080 (admin/admin)
#   FastAPI:     http://localhost:8000/docs
#   Chat UI:     http://localhost:8501
```

### Run the Pipeline

**Backfill a channel** (first run — loads historical episodes):
1. Open Airflow UI → DAGs → `backfill_channel`
2. Trigger with config: `{"channel_name": "lex_fridman", "max_videos": 50}`

**Daily ingestion** (incremental — new episodes only):
1. Unpause the `ingest_and_extract` DAG
2. It runs daily, fetching only videos published since the last run

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/episodes` | GET | List episodes, filter by channel |
| `/episodes/{id}` | GET | Full episode detail with all extracted entities |
| `/guests` | GET | Guests ranked by appearance count |
| `/topics/trending` | GET | Topics ranked by episode frequency |
| `/claims/search?q=` | GET | Full-text search across claims |
| `/recommendations` | GET | Recommendations ranked by mention count |
| `/chat` | POST | RAG-powered Q&A over transcript content |

### Example Chat Queries
- "What does Jensen Huang think about AI scaling?"
- "Which guests have appeared on multiple shows?"
- "What books have been recommended across all channels?"
- "Compare opinions on leadership"

## Data Model (DBT)

Star schema in BigQuery:

- **fact_claims** — Central fact table: every extracted claim linked to speaker, topic, episode
- **dim_episodes** — Episode metadata with entity counts
- **dim_guests** — Guest profiles with cross-channel appearance tracking
- **dim_topics** — Topic taxonomy with episode frequency
- **dim_channels** — Channel-level aggregations
- **mart_recommendations** — Deduplicated recommendations with mention counts

All models tested with DBT: uniqueness, not-null, referential integrity.

## Project Structure

```
podcast-knowledge-pipeline/
├── dags/                    # Airflow DAG definitions
│   ├── ingest_and_extract.py   # Daily pipeline: ingest → extract → model → embed
│   └── backfill_channel.py     # Manual: full channel history
├── src/
│   ├── ingestion/           # YouTube API client, GCS writer, BQ loader
│   ├── extraction/          # LLM chunking, prompts, extraction, merging
│   ├── embedding/           # sentence-transformers embedder, pgvector loader
│   └── serving/             # FastAPI endpoints, RAG logic, Streamlit chat
├── models/                  # DBT project (staging → intermediate → marts)
├── docs/
│   └── build_journal.md     # Architectural decisions and interview prep
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Cost

| Component | Cost |
|-----------|------|
| YouTube API | Free (10K quota/day, transcripts bypass API) |
| BigQuery | Free tier (1TB queries/month) |
| Cloud Storage | ~$0.02/month for transcript JSON |
| Claude Haiku (extraction) | ~$0.05/episode (~$27 for 500 episodes) |
| Embeddings | Free (runs locally) |
| **Total for 500 episodes** | **~$27** |
