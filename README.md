# ETL-AI-Schema: Reproducible Thesis Codebase

This repository contains the code, data, and LaTeX sources for a Master's thesis on using retrieval-augmented large language models (LLMs) to support schema evolution and ETL maintenance on the NYC yellow taxi dataset.

The core contributions implemented here are:

- A **schema change detection module** for flat tabular schemas.
- An **LLM-based mapping regenerator** that uses schema and change artefacts plus reference code as non-parametric context.
- A set of **Python scripts and Jupyter notebooks** that reproduce the quantitative experiments reported in the thesis (schema detection, canonical ingestion, ablation, and end-to-end evaluation).

## 1. Environment Setup

### 1.1. Prerequisites

- Python 3.10+ recommended.
- macOS or Linux (Windows should work via WSL).
- Optional but recommended:
  - [Ollama](https://ollama.com) for running local `llama3`.
  - Access to:
    - Mistral Cloud API (for `mistral` model), and/or
    - Hugging Face Inference Router (for `hf:openai/gpt-oss-120b:groq`).

### 1.2. Create a virtual environment and install dependencies

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install --upgrade pip
pip install -r requirements.txt  # if present
```

If there is no `requirements.txt`, install the main libraries manually:

```bash
pip install pandas numpy sqlite3 jupyter matplotlib seaborn requests openai
```

> Note: The exact dependencies may vary slightly depending on your local environment; if a script reports a missing package, install it with `pip install <package>`.

### 1.3. Configure model backends

The code supports three main families of models:

- **Local `llama3` via Ollama**  
  - Install Ollama and pull the model:
    ```bash
    ollama pull llama3
    ollama serve
    ```
  - Ensure the OpenAI-compatible endpoint is available at `http://localhost:11434/v1` (the default used in `ai/regenerate_mapping.py`).

- **Mistral Cloud (`mistral` / `mistral-small-latest`)**  
  - Set:
    ```bash
    export MISTRAL_API_KEY="your_mistral_api_key"
    ```

- **Hugging Face Router (`hf:openai/gpt-oss-120b:groq`)**  
  - Set:
    ```bash
    export HF_API_TOKEN="your_hf_token"
    export HF_CHAT_URL="https://router.huggingface.co/v1/chat/completions"
    ```

Avoid setting `OPENAI_API_BASE` to the Hugging Face router; HF calls should be routed through the dedicated helper in `ai/regenerate_mapping.py`.

## 2. Data and Schema Preparation

### 2.1. Raw taxi data

The repository assumes NYC TLC yellow taxi Parquet files have been downloaded and converted into down-sampled CSVs under `data/raw/` (e.g. `yellow_base_v1.csv`, `yellow_base_v2.csv`). If needed, use:

```bash
python scripts/convert_parquet_to_csv.py
```

to create the V1 base CSV from a Parquet source, then:

```bash
python scripts/create_taxi_schema_versions.py
```

to construct V2 and other schema variants.

### 2.2. Schema detection artefacts

Run the schema detection script for the taxi V1 → V2 scenario:

```bash
python scripts/run_taxi_schema_detection.py
```

This will populate:

- `schemas/yellow_v1_schema.json`
- `schemas/yellow_v2_schema.json`
- `schemas/yellow_v1_to_v2_changes.json`

and is used by both the detection evaluation and the mapping regenerator.

For the canonical ↔ V2 scenario, the canonical schema is defined in `etl/canonical_schema.py` and is materialised automatically by the canonical ingestion scripts (see below).

## 3. Core Experiments

All quantitative thesis results are reproduced via a combination of Python scripts and Jupyter notebooks.

### 3.1. Schema detection (RQ1)

1. **Heuristic detector evaluation**  
   Run:
   ```bash
   python scripts/evaluate_schema_detection.py --output results/schema_detection_eval.json
   ```
   This compares the JSON change sets under `schemas/` against ground truth in `docs/schema_detection_ground_truth.json` and reports precision/recall/F1.

2. **RAG-LLM detector vs heuristic for canonical ↔ V2**  
   - Generate the canonical change set and canonical ingestion artefacts (see Section 3.2).
   - Run:
     ```bash
     python scripts/run_canonical_schema_detection_llm.py --model hf:openai/gpt-oss-120b:groq
     ```
     This writes `results/canonical_changes_v2_llm_HF_openai_gpt-oss-120b_groq.json`.
   - Open and execute the notebook:
     ```bash
     jupyter notebook notebooks/schema_detection_llm_vs_heuristic.ipynb
     ```
     The notebook loads `results/canonical_changes_v2.json` and the LLM output, computes precision/recall/F1 for added/removed/renamed, and builds the comparison table reported in the thesis.

### 3.2. Canonical ingestion experiments (RQ2)

The canonical ingestion experiments compare deterministic and LLM-generated mappings for the V2 → canonical scenario.

1. **Baseline vs LLM canonical ingestion (single-run metrics and canonical change set)**  
   ```bash
   python scripts/evaluate_baseline_vs_llm_canonical.py \
       --version v2 \
       --model llama3 \
       --output results/canonical_eval_llama3.json
   ```
   This also writes `results/canonical_changes_v2.json`, which is treated as the canonical change set for schema detection metrics.

   Repeat for `mistral` and `hf:openai/gpt-oss-120b:groq` if desired.

2. **10-run reliability and multi-model comparison**  
   Open the main evaluation notebook:
   ```bash
   jupyter notebook notebooks/thesis_evaluation_metrics.ipynb
   ```

   - The notebook runs the canonical ingestion script for multiple models (`llama3`, `mistral`, `hf:openai/gpt-oss-120b:groq`), saving JSON outputs under `results/`.
   - It aggregates:
     - cell agreement (%),
     - row-count match,
     - per-column agreement,
     - and success rates across several runs per model.
   - It produces tables and figures used in the Results chapter.

3. **Ablation: zero-shot vs RAG-grounded canonical mappings**  
   ```bash
   jupyter notebook notebooks/canonical_ablation_analysis.ipynb
   ```

   This notebook drives `scripts/evaluate_canonical_ablation.py`, runs both zero-shot and RAG-grounded conditions for `llama3` and HF 120B, and summarises:

   - mapping accuracy,
   - code executability,
   - schema hallucinations,
   - row-count match rate.

### 3.3. End-to-end AI-assisted workflow (RQ3)

The end-to-end evaluation integrates schema detection, mapping regeneration, and the ETL pipeline.

1. **Generate a taxi transform via AI** (for the V1 → V2 scenario):

   ```bash
   python scripts/run_taxi_regenerate_mapping.py --scenario v1_to_v2 --model llama3
   ```

   This writes `etl/transform_generated.py` using the mapping regenerator.

2. **End-to-end evaluation across models**  
   ```bash
   python scripts/run_taxi_ai_evaluation.py \
       --models "llama3,mistral,hf:openai/gpt-oss-120b:groq" \
       --output results/e2e_ai_eval.jsonl
   ```

   This script:

   - regenerates `transform_generated.py` per model,
   - runs the taxi ETL pipeline in generated mode,
   - checks basic properties of the resulting SQLite tables (row count, derived columns, simple sanity checks),
   - records success flags and durations in `results/e2e_ai_eval.jsonl`.

3. **Aggregate E2E results**  
   The same `notebooks/thesis_evaluation_metrics.ipynb` notebook loads `results/e2e_ai_eval.jsonl`, aggregates success and duration per model, and produces the bar chart reported in the Results chapter.

## 4. Reproducing the LaTeX Thesis

The complete thesis is under `Documentation/`.

To compile locally (after installing a TeX distribution such as MacTeX or TeX Live):

```bash
cd Documentation

# One-shot build with latexmk (recommended)
latexmk -pdf -interaction=nonstopmode main.tex

# Or manually with pdflatex + biber:
pdflatex -interaction=nonstopmode main.tex
biber main
pdflatex -interaction=nonstopmode main.tex
pdflatex -interaction=nonstopmode main.tex
```

On Overleaf, ensure the compiler is set to use `pdflatex` with `biber` (the template uses BibLaTeX via `\addbibresource{...}` in `preamble/bib-setup.tex`).

## 5. Directory Overview

- `ai/` – schema detection (`detect_schema_change.py`), mapping regenerator (`regenerate_mapping.py`), and LLM-based schema detector (`llm_schema_detection.py`).
- `etl/` – extraction, baseline taxi transform, canonical schema, canonical mapping, and pipeline code.
- `scripts/` – CLI entry points for:
  - schema detection and evaluation,
  - canonical ingestion,
  - ablation study,
  - taxi mapping regeneration,
  - end-to-end AI evaluation,
  - helper utilities for canonical schema support.
- `notebooks/` – analysis notebooks used to generate tables and figures:
  - `thesis_evaluation_metrics.ipynb`,
  - `canonical_ablation_analysis.ipynb`,
  - `schema_detection_llm_vs_heuristic.ipynb`,
  - and supporting exploratory notebooks.
- `results/` – JSON and JSONL files with all recorded experimental outputs.
- `Documentation/` – LaTeX sources for the thesis, including:
  - `main.tex`, chapter files, template class, preamble, and figures.

## 6. Notes on Reproducibility

- All scripts are designed to be deterministic given:
  - fixed source CSVs under `data/raw/`,
  - fixed model backends and decoding settings (the default settings used in the thesis),
  - and fixed random seeds where applicable.
- Due to the stochastic nature of LLM sampling, some metrics (especially in the ablation study and multi-run reliability experiments) may exhibit small variations across runs. The notebooks are structured to:
  - reuse existing result files when present, and
  - run missing experiments only when needed.

This README aims to provide enough detail for another researcher to recreate the main experiments and regenerate the figures and tables used in the thesis, given access to comparable LLM backends and the NYC taxi data sources.

