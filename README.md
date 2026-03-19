# taldbt - AI Powered Talend to dbt Migration

Convert legacy Talend ETL to modern dbt SQL using semantic AI transpilation.

**Product Page:** https://taldbt.netlify.app
**Live Demo:** https://taldbt.streamlit.app
**Docker Image:** `docker pull souravetl/taldbt:latest`

## Installation

```bash
# Core AI migration agent + CLI
pip install taldbt==0.2.1

# With Streamlit web UI
pip install taldbt[ui]==0.2.1

# With Temporal orchestration
pip install taldbt[temporal]==0.2.1

# Everything
pip install taldbt[all]==0.2.1
```

## CLI Usage

```bash
# Launch the web UI
taldbt ui

# Discover and analyze a Talend project
taldbt discover ./my_talend_project

# Full migration to dbt
taldbt migrate ./my_talend_project ./dbt_output

# Check version
taldbt version
```

## Quick Start

### Docker (recommended)
```bash
docker pull souravetl/taldbt:latest
docker pull ollama/ollama:latest
docker compose up -d
docker exec taldbt-ollama ollama pull qwen3-coder:30b
# Open http://localhost:8501
```

### Cloud (no install)
Upload your Talend ZIP at https://taldbt.streamlit.app

### Local Development
```bash
pip install taldbt[all]==0.2.1
streamlit run taldbt/ui/app.py
```

## Tech Stack

| Component | Purpose |
|-----------|---------|
| DuckDB + Flock | In-process analytics + LLM-in-SQL validation |
| dbt-core | SQL transformation framework |
| Temporal.io | DAG-aware workflow orchestration |
| Ollama / Cerebras / Groq | AI translation (local or cloud) |
| sqlglot | Multi-dialect SQL transpilation |
| Faker | Synthetic test data with FK integrity |
| networkx | Dependency graph + topological sort |
| lxml + Pydantic | XML parsing + type-safe AST |

## Project Structure

```
taldbt/
├── Dockerfile              # Tier 1: Docker image
├── docker-compose.yml      # Tier 1: full stack
├── docker-compose.cpu.yml  # Tier 1: no-GPU override
├── docker/entrypoint.sh    # Docker startup script
├── requirements.txt        # Python dependencies
├── packages.txt            # Tier 3: apt deps (Streamlit Cloud)
├── .streamlit/             # Streamlit config + secrets
├── docs/                   # Architecture docs
├── main.py                 # CLI entry point
└── taldbt/                 # Core application
    ├── ui/                 # Streamlit web app
    ├── parsers/            # XML parsing + component parsers
    ├── codegen/            # SQL generation + dbt scaffolding
    ├── engine/             # DuckDB + validation + test data
    ├── expert/             # Component knowledge base (549 components)
    ├── graphing/           # DAG builder + data lineage
    ├── llm/                # LLM provider chain
    ├── models/             # Pydantic AST models
    ├── orchestration/      # Temporal + AutoPilot
    └── tests/              # Test suite
```

## Deployment Tiers

- **Tier 1 (Docker):** `docker compose up -d` — Ollama + Temporal + UI
- **Tier 2 (pip):** `pip install taldbt[all]` — CLI + web UI + Temporal
- **Tier 3 (Cloud):** Streamlit Cloud + Cerebras/Groq AI — no local install

## Requirements

- Python 3.10+
- [Ollama](https://ollama.com) for local AI (optional — falls back to free cloud AI via Cerebras/Groq)


Proprietary. Contact souravroy7864@gmail.com for licensing.
