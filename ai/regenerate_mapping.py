"""
AI-Powered ETL Mapping Regeneration Module

This module uses AI (LLM) to automatically generate ETL transformation mappings
when schema changes are detected.
"""
import json
import os
from typing import Dict, Any, Optional
import openai
from pathlib import Path


class MappingRegenerator:
    """Uses AI to regenerate ETL mappings based on schema changes."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the mapping regenerator.
        
        Args:
            api_key: OpenAI API key (or set OPENAI_API_KEY environment variable)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if self.api_key:
            openai.api_key = self.api_key
        else:
            print("Warning: OPENAI_API_KEY not set. AI features will be limited.")
    
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

CURRENT TRANSFORM FUNCTION (for reference; preserve this business logic under the new schema):
```python
{snippet}
```

TASK:
Generate a new transform function that:
1. Handles renamed columns (map old names to new names)
2. Handles removed columns (either drop them or provide defaults)
3. Handles added columns (include them in output if they exist)
4. Handles type changes (convert types appropriately)
5. Maintains existing business logic from the current transform above
6. Returns a DataFrame with the correct schema

Return ONLY the Python function code, no explanations. The function should:
- Take a pandas DataFrame as input
- Return a pandas DataFrame as output
- Handle missing columns gracefully
- Use pandas operations

Generate the transform function:"""

        return prompt
    
    def call_ai(self, prompt: str, model: str = "gpt-4") -> str:
        """
        Call OpenAI API to generate mapping code.
        
        Args:
            prompt: The prompt to send to the AI
            model: Model to use (default: gpt-4)
            
        Returns:
            Generated code as string
        """
        if not self.api_key:
            return self._generate_fallback_mapping()
        
        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert Python and ETL engineer. Generate clean, production-ready code."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )
            
            code = response.choices[0].message.content.strip()
            
            # Extract code from markdown code blocks if present
            if "```python" in code:
                code = code.split("```python")[1].split("```")[0].strip()
            elif "```" in code:
                code = code.split("```")[1].split("```")[0].strip()
            
            return code
            
        except Exception as e:
            print(f"Error calling AI API: {e}")
            print("Falling back to rule-based mapping generation...")
            return self._generate_fallback_mapping()
    
    def _generate_fallback_mapping(self) -> str:
        """
        Generate a basic mapping using rule-based approach when AI is unavailable.
        
        Returns:
            Python code for transform function
        """
        return """def transform(df):
    import pandas as pd
    
    # Handle renamed columns (add mapping logic here)
    # Handle removed columns (drop or default)
    # Handle added columns (include if present)
    
    # Existing business logic
    if 'first_name' in df.columns and 'last_name' in df.columns:
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    return df"""
    
    def regenerate_transform(self, old_schema: Dict, new_schema: Dict, 
                            changes: Dict,
                            current_transform_snippet: Optional[str] = None) -> str:
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
        
        print("Calling AI to generate mapping...")
        code = self.call_ai(prompt)
        
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
        
        with open(output_path, 'w') as f:
            f.write(f"# Auto-generated transform function\n")
            f.write(f"# Generated by AI mapping regenerator\n\n")
            f.write(code)
        
        print(f"✓ Generated mapping saved to {output_path}")


def regenerate_mapping(old_schema_path: str, new_schema_path: str, 
                       changes_path: str, output_path: str,
                       api_key: Optional[str] = None,
                       current_transform_snippet: Optional[str] = None) -> str:
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

