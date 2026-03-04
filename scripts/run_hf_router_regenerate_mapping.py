#!/usr/bin/env python3
"""
Generate an ETL transform using the Hugging Face router (OpenAI-compatible API)
for a given model, reusing the RAG prompt from MappingRegenerator.

This script is useful when you want to evaluate hosted Hugging Face models
such as `openai/gpt-oss-20b:groq` with their own API tokens, independently of
the local Ollama / Mistral / OpenAI setup used elsewhere in the project.

It:
  1. Extracts the current taxi schema for a given version (e.g. v2).
  2. Computes changes relative to the canonical schema.
  3. Builds the same RAG prompt as the standard MappingRegenerator.
  4. Calls the HF router at https://router.huggingface.co/v1/chat/completions
     with the given model ID and HF token.
  5. Extracts Python code from the response and saves it to an output module
     (e.g. etl/transform_hf_gpt_oss_20b.py).

You can then plug this transform module into your existing evaluation or
ablation notebooks to compare against local LLMs like llama3 and mistral.

Example usage:
  export HF_TOKEN="hf_xxx..."
  python scripts/run_hf_router_regenerate_mapping.py \\
    --version v2 \\
    --model openai/gpt-oss-20b:groq \\
    --output etl/transform_hf_gpt_oss_20b.py
"""

import argparse
import json
import os
import sys
from typing import Optional

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
ETL_DIR = os.path.join(PROJECT_ROOT, "etl")

sys.path.insert(0, PROJECT_ROOT)

from ai.detect_schema_change import SchemaChangeDetector  # type: ignore
from ai.regenerate_mapping import MappingRegenerator  # type: ignore
from etl.canonical_schema import get_canonical_schema_dict  # type: ignore

try:
    from scripts.canonical_schema_support import get_current_schema_for_version  # type: ignore
except ImportError:
    from canonical_schema_support import get_current_schema_for_version  # type: ignore


def get_taxi_transform_snippet() -> str:
    """
    Read etl/transform_taxi.py and return the main transform function as a snippet.
    This snippet is injected into the RAG prompt so that the HF model preserves
    the existing business logic under the new schema.
    """
    path = os.path.join(ETL_DIR, "transform_taxi.py")
    try:
        with open(path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        return ""

    start = content.find("def transform(")
    if start == -1:
        return content.strip()
    end = content.find("\n\ndef ", start + 1)
    if end == -1:
        end = len(content)
    return content[start:end].strip()


def extract_code_from_text(text: str) -> str:
    """
    Extract Python code from a text response, handling optional markdown fences.

    Args:
        text: Raw text from the model.

    Returns:
        Extracted code string.
    """
    code = text.strip()
    if "```python" in code:
        try:
            code = code.split("```python", 1)[1].split("```", 1)[0].strip()
        except Exception:
            code = code
    elif "```" in code:
        try:
            code = code.split("```", 1)[1].split("```", 1)[0].strip()
        except Exception:
            code = code
    return code


def call_hf_router_chat(
    prompt: str,
    model_spec: str,
    hf_token: str,
    api_url: str = "https://router.huggingface.co/v1/chat/completions",
) -> str:
    """
    Call the Hugging Face router /v1/chat/completions endpoint with the given
    prompt and model spec (e.g. 'openai/gpt-oss-20b:groq').

    Args:
        prompt: The user prompt to send (RAG prompt with schemas, changes, snippet).
        model_spec: Model identifier understood by the router.
        hf_token: HF access token.
        api_url: Router endpoint; default is the chat completions path.

    Returns:
        Assistant message content as a string.
    """
    headers = {
        "Authorization": f"Bearer {hf_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_spec,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": "You are an expert Python and ETL engineer. Generate clean, production-ready code.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
    }

    resp = requests.post(api_url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Hugging Face router call failed (status {resp.status_code}): {resp.text}"
        )

    data = resp.json()
    try:
        message = data["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Unexpected HF router response format: {data}") from exc
    return message or ""


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an ETL transform using the Hugging Face router with the "
            "RAG prompt from MappingRegenerator."
        )
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Taxi version whose schema should be mapped to canonical (e.g. v2).",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Hugging Face router model spec, e.g. 'openai/gpt-oss-20b:groq'.",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(ETL_DIR, "transform_hf_generated.py"),
        help="Output path for the generated transform module.",
    )
    parser.add_argument(
        "--token-env",
        default="HF_TOKEN",
        help="Environment variable name that holds the HF access token (default: HF_TOKEN).",
    )
    args = parser.parse_args(argv)

    hf_token = os.getenv(args.token_env)
    if not hf_token:
        raise SystemExit(
            f"Environment variable {args.token_env} is not set. "
            "Set it to your Hugging Face access token."
        )

    version = args.version
    csv_path = os.path.join(RAW_DIR, f"yellow_base_{version}.csv")
    if not os.path.isfile(csv_path):
        raise SystemExit(f"Source CSV not found: {csv_path}")

    # Extract current schema and canonical schema, then compute changes.
    current_schema, _ = get_current_schema_for_version(version, csv_path)
    canonical_schema = get_canonical_schema_dict()
    detector = SchemaChangeDetector()
    changes = detector.compare_schemas(current_schema, canonical_schema)

    # Build RAG prompt using the same logic as MappingRegenerator.
    reg = MappingRegenerator(api_key=None)
    snippet = get_taxi_transform_snippet()
    prompt = reg.generate_mapping_prompt(
        old_schema=current_schema,
        new_schema=canonical_schema,
        changes=changes,
        current_transform_snippet=snippet,
    )

    print(f"Calling Hugging Face router model={args.model} for version={version}...")
    raw_text = call_hf_router_chat(prompt, args.model, hf_token)
    code = extract_code_from_text(raw_text)

    # Make the generated module self-contained if it uses pandas.
    needs_pd = ("pd." in code) or ("pd[" in code) or ("pd(" in code) or ("pd.DataFrame" in code)
    has_import_pd = "import pandas as pd" in code

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        f.write("# Auto-generated transform function via HF router\n")
        f.write(f"# Model: {args.model}\n\n")
        if needs_pd and not has_import_pd:
            f.write("import pandas as pd\n\n")
        f.write(code)

    print(f"✓ Generated HF-based mapping saved to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

