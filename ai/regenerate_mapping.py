"""
AI-Powered ETL Mapping Regeneration Module

This module uses an OpenAI-compatible chat API (by default a local Ollama
endpoint) to automatically generate ETL transformation mappings when schema
changes are detected. It also supports calling Hugging Face models either via
the hosted Inference API or, optionally, via the local transformers library.

Supported backends:
- Ollama (default): OPENAI_API_BASE=http://localhost:11434/v1, models e.g. llama3, mistral
- Mistral Cloud: set MISTRAL_API_KEY; use --model mistral or mistral-small-latest
- OpenAI: set OPENAI_API_BASE and OPENAI_API_KEY for OpenAI models
- Hugging Face Inference API: use model names prefixed with \"hf:\" or \"hf/\", e.g.
  hf:meta-llama/Meta-Llama-3-8B-Instruct. Requires HF_API_TOKEN or
  HUGGINGFACEHUB_API_TOKEN in the environment.
- Hugging Face transformers (local): use model names prefixed with \"hf-local:\"
  or \"hf-local/\", e.g. hf-local:gpt2 (requires `transformers` and local weights).
"""
import json
import os
from typing import Dict, Any, Optional
import openai
from pathlib import Path
import requests

try:
    # Optional: used when model names start with hf-local: or hf-local/
    from transformers import pipeline as hf_pipeline  # type: ignore
except ImportError:
    hf_pipeline = None

# Default: local Ollama. Override with OPENAI_API_BASE or use Mistral via MISTRAL_API_KEY.
DEFAULT_OLLAMA_BASE = "http://localhost:11434/v1"
MISTRAL_API_BASE = "https://api.mistral.ai/v1"
# Hugging Face router chat completions endpoint (OpenAI-compatible).
HF_CHAT_URL = os.getenv("HF_CHAT_URL", "https://router.huggingface.co/v1/chat/completions")
openai.api_base = os.getenv("OPENAI_API_BASE", DEFAULT_OLLAMA_BASE)


def _resolve_mistral_model(model: str) -> str:
    """Map short name 'mistral' to Mistral Cloud model ID."""
    if model.lower() == "mistral":
        return "mistral-small-latest"
    return model


class MappingRegenerator:
    """Uses AI to regenerate ETL mappings based on schema changes."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the mapping regenerator.
        
        Args:
            api_key: OpenAI/Mistral API key (or set OPENAI_API_KEY / MISTRAL_API_KEY in env)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.mistral_api_key = os.getenv("MISTRAL_API_KEY")
        self.using_ollama_default = os.getenv("OPENAI_API_BASE") in (None, "", DEFAULT_OLLAMA_BASE)

        if self.api_key:
            openai.api_key = self.api_key
        elif self.mistral_api_key:
            # Mistral Cloud will be used when model starts with \"mistral\"
            pass
        elif self.using_ollama_default:
            # Local Ollama: no key needed, but some clients expect a non-empty value.
            openai.api_key = os.getenv("OPENAI_API_KEY", "ollama")
        else:
            print(
                "Warning: OPENAI_API_KEY and MISTRAL_API_KEY not set, and OPENAI_API_BASE "
                "is not the default Ollama endpoint. AI calls may fail; falling back to "
                "rule-based mapping when necessary."
            )
    
    def generate_mapping_prompt(self, old_schema: Dict, new_schema: Dict, 
                                 changes: Dict,
                                 current_transform_snippet: Optional[str] = None) -> str:
        """
        Generate a prompt for the AI to create new ETL mappings.

        Args:
            old_schema: Original schema
            new_schema: New schema
            changes: Detected changes
            current_transform_snippet: Optional custom transform code (def transform(df): ...)
                                       to preserve; if None, uses default user/full_name example.
        Returns:
            Formatted prompt string
        """
        default_snippet = """def transform(df):
    df["full_name"] = df["first_name"] + " " + df["last_name"]
    return df"""
        snippet = (current_transform_snippet or default_snippet).strip()

        prompt = f"""You are an expert ETL (Extract, Transform, Load) engineer. 
Your task is to generate Python code for a transform function that handles schema changes.

OLD SCHEMA:
{json.dumps(old_schema, indent=2)}

NEW SCHEMA:
{json.dumps(new_schema, indent=2)}

DETECTED CHANGES:
- Added columns: {[c['column'] for c in changes.get('added_columns', [])]}
- Removed columns: {[c['column'] for c in changes.get('removed_columns', [])]}
- Renamed columns: {[f"{r['old_column']} -> {r['new_column']}" for r in changes.get('renamed_columns', [])]}
- Type changes: {[f"{tc['column']}: {tc['old_type']} -> {tc['new_type']}" for tc in changes.get('type_changes', [])]}

The NEW SCHEMA is the canonical table and uses V1-style column names (e.g. VendorID, trip_distance).
Your output DataFrame must include all columns listed in NEW SCHEMA (with exactly those names; do not use vendor_id or trip_distance_km).
In addition, you must preserve any derived business-logic columns from the current transform such as trip_revenue and trip_duration_minutes.
Do not drop these derived columns when aligning to the NEW SCHEMA; they should still be present in the returned DataFrame.

CURRENT TRANSFORM FUNCTION (for reference; preserve this business logic under the new schema):
```python
{snippet}
```

TASK:
Using ONLY the information in OLD SCHEMA, NEW SCHEMA, and DETECTED CHANGES above:

1. Generate a single top-level function with the exact signature:

   ```python
   def transform(df: pandas.DataFrame) -> pandas.DataFrame:
       ...
   ```

   - Do NOT define alternative entry points such as transform_v1, transform_v2,
     or any function that takes old_schema, new_schema, or other extra parameters.
   - You may define helper functions inside the module, but the entry point
     used by the caller is always transform(df: pandas.DataFrame).

2. Handle ONLY the structural changes described in DETECTED CHANGES:
   - For renamed columns: implement explicit renaming from the old name to the
     new name using the pairs listed above. Rename only the pairs listed; do
     not rename any other column (especially do not rename a removed column to
     another column name).
   - For removed columns: drop them or omit them from the output. Do NOT rename
     a removed column to another column (e.g. do not rename payment_type to
     VendorID). Treat them as optional in the input; if you drop them, use
     errors=\"ignore\" or check membership first so you never raise KeyError.
   - For added columns: create or populate only the columns listed as added,
     and only when they do not already exist.
   - For type changes: convert types appropriately for the columns listed as
     having type changes.
   - For all other columns (those not mentioned in DETECTED CHANGES), pass them
     through unchanged; do not invent or drop additional columns.

3. Maintain the business logic from the CURRENT TRANSFORM FUNCTION snippet above
   under the NEW SCHEMA. This includes preserving how derived metrics and
   filters are computed, adapted to the new column names/types.

Return ONLY the Python function code (no explanations). The code should:
- Include any and all imports required
- Implement exactly one public entry point: transform(df: pandas.DataFrame) -> pandas.DataFrame
- Handle missing columns gracefully (never crash if a column from DETECTED CHANGES is absent)
- Use pandas operations
- When specifying dtypes, always use string names like \"int64\" or \"float64\" (or np.int64 if you explicitly import numpy). Never use a bare name like int64 without quotes.
- When converting to integers, first coerce to numeric with errors=\"coerce\" and fill missing values (for example, .fillna(0).astype(\"int64\")) so you never raise \"Cannot convert non-finite values (NA or inf) to integer\".
- Never write code that tests the truthiness of a pandas Series or NA directly (for example, avoid 'if df[\"col\"]:' or 'if some_series:'). Build boolean masks with comparisons and combine them with &, and if a mask can contain NA, call .fillna(False) before using it to filter rows.

Generate the transform(df) function now:"""

        return prompt
    
    def call_ai(self, prompt: str, model: str = "llama3") -> str:
        """
        Call a language model to generate mapping code.

        Supports:
        - Ollama / OpenAI-compatible chat API (default),
        - Mistral Cloud (MISTRAL_API_KEY) when model starts with \"mistral\",
        - Hugging Face Inference API when model starts with \"hf:\" or \"hf/\",
        - Hugging Face transformers locally when model starts with \"hf-local:\"
          or \"hf-local/\".
        
        Args:
            prompt: The prompt to send to the AI
            model: Model to use (e.g. llama3, mistral, mistral-small-latest)
            
        Returns:
            Generated code as string
        """
        # Hugging Face Inference API backend: model names prefixed with hf: or hf/
        if model.startswith("hf:") or model.startswith("hf/"):
            return self._call_hf_api(prompt, model)

        # Hugging Face transformers backend (local): model names prefixed with hf-local: or hf-local/
        if model.startswith("hf-local:") or model.startswith("hf-local/"):
            if hf_pipeline is None:
                raise RuntimeError(
                    "Requested Hugging Face transformers backend (model starts with "
                    "'hf-local:' or 'hf-local/') but transformers is not installed. "
                    "Install it via 'pip install transformers'."
                )
            # Allow both hf-local:repo_id and hf-local/repo_id forms; strip the prefix
            if model.startswith("hf-local:"):
                hf_model_name = model.split(":", 1)[1]
            else:  # hf-local/...
                hf_model_name = model.split("/", 1)[1]
            return self._call_hf(prompt, hf_model_name)

        use_mistral = (
            self.mistral_api_key
            and (model.lower() == "mistral" or model.lower().startswith("mistral-"))
        )
        if use_mistral:
            api_key = self.mistral_api_key
            api_base = MISTRAL_API_BASE
            model_id = _resolve_mistral_model(model)
            if not api_key:
                return self._generate_fallback_mapping()
        else:
            api_base = openai.api_base
            api_key = self.api_key
            model_id = model
            # For local Ollama, allow running without a real key.
            if (not api_key) and api_base == DEFAULT_OLLAMA_BASE:
                api_key = os.getenv("OPENAI_API_KEY", "ollama")
            elif not api_key:
                return self._generate_fallback_mapping()

        saved_base, saved_key = openai.api_base, openai.api_key
        try:
            openai.api_base = api_base
            openai.api_key = api_key
            response = openai.ChatCompletion.create(
                model=model_id,
                messages=[
                    {"role": "system", "content": "You are an expert Python and ETL engineer. Generate clean, production-ready code."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )
            
            raw = response.choices[0].message.content.strip()
            return self._extract_code_from_text(raw)
            
        except Exception as e:
            print(f"Error calling AI API: {e}")
            print("Falling back to rule-based mapping generation...")
            return self._generate_fallback_mapping()
        finally:
            openai.api_base = saved_base
            openai.api_key = saved_key
    
    def _call_hf(self, prompt: str, model_name: str) -> str:
        """
        Call a Hugging Face transformers model locally via the text-generation pipeline.

        Args:
            prompt: The prompt string to send.
            model_name: The Hugging Face model identifier (e.g. 'gpt2',
                        'mistralai/Mixtral-8x7B-Instruct-v0.1').

        Returns:
            Generated code as string.
        """
        if hf_pipeline is None:
            raise RuntimeError(
                "Hugging Face transformers is not available. Install via 'pip install transformers'."
            )

        # Use a simple text-generation pipeline; for larger models users may
        # configure device placement externally.
        gen = hf_pipeline("text-generation", model=model_name)

        # Generate continuation; keep sampling deterministic for reproducibility.
        outputs = gen(
            prompt,
            max_new_tokens=512,
            do_sample=False,
            temperature=0.0,
        )
        generated = outputs[0]["generated_text"]

        # Heuristic: often the model echoes the prompt and appends code.
        # Prefer the suffix after the original prompt if present.
        if generated.startswith(prompt):
            tail = generated[len(prompt) :].strip()
        else:
            tail = generated.strip()

        return self._extract_code_from_text(tail or generated)

    def _call_hf_api(self, prompt: str, model_spec: str) -> str:
        """
        Call a Hugging Face model via the hosted router chat completions API.

        Args:
            prompt: The prompt string to send.
            model_spec: The model spec with hf: or hf/ prefix (e.g. 'hf:openai/gpt-oss-20b:groq').

        Returns:
            Generated code as string.
        """
        # Strip hf: or hf/ prefix to get the actual router model id.
        if model_spec.startswith("hf:"):
            model_id = model_spec.split(":", 1)[1]
        elif model_spec.startswith("hf/"):
            model_id = model_spec.split("/", 1)[1]
        else:
            model_id = model_spec

        token = (
            os.getenv("HF_TOKEN")
            or os.getenv("HF_API_TOKEN")
            or os.getenv("HUGGINGFACEHUB_API_TOKEN")
        )
        if not token:
            raise RuntimeError(
                "HF_TOKEN, HF_API_TOKEN or HUGGINGFACEHUB_API_TOKEN environment "
                "variable not set. Set it to your Hugging Face router access token."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model_id,
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

        resp = requests.post(HF_CHAT_URL, headers=headers, json=payload, timeout=120)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Hugging Face router call failed (status {resp.status_code}): {resp.text}"
            )

        data = resp.json()
        try:
            raw = data["choices"][0]["message"]["content"]
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Unexpected Hugging Face router response format: {data}"
            ) from exc

        return self._extract_code_from_text(raw or "")

    def _extract_code_from_text(self, text: str) -> str:
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
    
    def _generate_fallback_mapping(self) -> str:
        """
        Generate a basic mapping using rule-based approach when AI is unavailable.
        
        Returns:
            Python code for transform function
        """
        # Fallback is a taxi-aware transform that mirrors the baseline logic in
        # etl/transform_taxi.py so that generated mappings remain close to the
        # business semantics used in V1.
        
    
    def regenerate_transform(self, old_schema: Dict, new_schema: Dict, 
                            changes: Dict,
                            current_transform_snippet: Optional[str] = None,
                            model: str = "llama3") -> str:
        """
        Main function to regenerate transform mapping.

        Args:
            old_schema: Original schema
            new_schema: New schema
            changes: Detected changes
            current_transform_snippet: Optional custom transform code to preserve

        Returns:
            Generated Python code for transform function
        """
        print("Generating AI prompt...")
        prompt = self.generate_mapping_prompt(
            old_schema, new_schema, changes,
            current_transform_snippet=current_transform_snippet,
        )
        
        print(f"Calling AI to generate mapping (model={model})...")
        code = self.call_ai(prompt, model=model)
        
        return code
    
    def validate_generated_code(self, code: str) -> bool:
        """
        Validate that the generated code is syntactically correct.
        
        Args:
            code: Python code to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            compile(code, '<string>', 'exec')
            return True
        except SyntaxError as e:
            print(f"Generated code has syntax errors: {e}")
            return False
    
    def save_generated_mapping(self, code: str, output_path: str):
        """
        Save generated mapping code to a file.
        
        Args:
            code: Generated Python code
            output_path: Path to save the code
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # If the generated code uses `pd` but does not import pandas, make the
        # module self-contained by adding `import pandas as pd` after the header.
        needs_pd = ("pd." in code) or ("pd[" in code) or ("pd(" in code) or ("pd.DataFrame" in code)
        has_import_pd = "import pandas as pd" in code

        with open(output_path, "w") as f:
            f.write("# Auto-generated transform function used in the taxi mapping experiments\n\n")
            if needs_pd and not has_import_pd:
                f.write("import pandas as pd\n\n")
            f.write(code)
        
        print(f"✓ Generated mapping saved to {output_path}")


def regenerate_mapping(old_schema_path: str, new_schema_path: str, 
                       changes_path: str, output_path: str,
                       api_key: Optional[str] = None,
                       current_transform_snippet: Optional[str] = None,
                       model: str = "llama3") -> str:
    """
    Main function to regenerate ETL mapping from schema changes.

    Args:
        old_schema_path: Path to old schema JSON
        new_schema_path: Path to new schema JSON
        changes_path: Path to changes JSON
        output_path: Path to save generated transform code
        api_key: Optional OpenAI API key
        current_transform_snippet: Optional custom transform code to preserve (e.g. taxi logic)

    Returns:
        Generated transform code
    """
    regenerator = MappingRegenerator(api_key=api_key)

    # Load schemas and changes
    with open(old_schema_path, 'r') as f:
        old_schema = json.load(f)

    with open(new_schema_path, 'r') as f:
        new_schema = json.load(f)

    with open(changes_path, 'r') as f:
        changes = json.load(f)

    # Generate new mapping
    code = regenerator.regenerate_transform(
        old_schema, new_schema, changes,
        current_transform_snippet=current_transform_snippet,
        model=model,
    )
    
    # Validate
    if regenerator.validate_generated_code(code):
        print("✓ Generated code is valid")
    else:
        print("⚠ Generated code has issues, but saving anyway")
    
    # Save
    regenerator.save_generated_mapping(code, output_path)
    
    return code


if __name__ == "__main__":
    # Example usage
    import os
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    old_schema_path = os.path.join(project_root, "schemas", "old_schema.json")
    new_schema_path = os.path.join(project_root, "schemas", "new_schema.json")
    
    # For this example, we'll create a simple changes dict
    # In practice, this would come from detect_schema_change.py
    changes = {
        "added_columns": [{"column": "phone_number", "type": "object"}],
        "removed_columns": [],
        "renamed_columns": [{"old_column": "age", "new_column": "user_age", "similarity": 0.8}],
        "type_changes": []
    }
    
    # Save changes temporarily
    changes_path = os.path.join(project_root, "schemas", "changes.json")
    with open(changes_path, 'w') as f:
        json.dump(changes, f, indent=2)
    
    output_path = os.path.join(project_root, "etl", "transform_generated.py")
    
    print("Regenerating ETL mapping...")
    code = regenerate_mapping(
        old_schema_path, 
        new_schema_path, 
        changes_path,
        output_path
    )
    
    print("\nGenerated code:")
    print("=" * 50)
    print(code)

