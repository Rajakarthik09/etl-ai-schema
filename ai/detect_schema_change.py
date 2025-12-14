"""
Schema Change Detection Module

This module detects changes between two data schemas and classifies them.
"""
import json
import pandas as pd
from typing import Dict, List, Tuple, Any
from pathlib import Path


class SchemaChangeDetector:
    """Detects and classifies schema changes between two data sources."""
    
    def __init__(self):
        self.changes = {
            "added_columns": [],
            "removed_columns": [],
            "renamed_columns": [],
            "type_changes": [],
            "constraint_changes": []
        }
    
    def extract_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Extract schema information from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary containing schema information
        """
        df = pd.read_csv(file_path, nrows=0)  # Read only headers
        
        schema = {
            "columns": {},
            "column_count": len(df.columns),
            "file_path": file_path
        }
        
        # Read a sample to infer types
        df_sample = pd.read_csv(file_path, nrows=100)
        
        for col in df.columns:
            # Convert numpy types to native Python types for JSON serialization
            sample_series = df_sample[col].dropna()
            sample_values = []
            if not sample_series.empty:
                for val in sample_series.head(3):
                    # Convert numpy types to Python native types
                    if pd.isna(val):
                        continue
                    if isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
                        sample_values.append(str(val))
                    elif hasattr(val, 'item'):  # numpy scalar
                        sample_values.append(val.item())
                    else:
                        sample_values.append(val)
            
            schema["columns"][col] = {
                "dtype": str(df_sample[col].dtype),
                "nullable": bool(df_sample[col].isna().any()),
                "sample_values": sample_values
            }
        
        return schema
    
    def compare_schemas(self, old_schema: Dict, new_schema: Dict) -> Dict[str, Any]:
        """
        Compare two schemas and detect changes.
        
        Args:
            old_schema: Schema of the old/current data
            new_schema: Schema of the new/changed data
            
        Returns:
            Dictionary containing detected changes
        """
        old_cols = set(old_schema["columns"].keys())
        new_cols = set(new_schema["columns"].keys())
        
        # Detect added columns
        added = new_cols - old_cols
        self.changes["added_columns"] = [
            {
                "column": col,
                "type": new_schema["columns"][col]["dtype"],
                "sample_values": new_schema["columns"][col]["sample_values"]
            }
            for col in added
        ]
        
        # Detect removed columns
        removed = old_cols - new_cols
        self.changes["removed_columns"] = [
            {
                "column": col,
                "type": old_schema["columns"][col]["dtype"]
            }
            for col in removed
        ]
        
        # Detect renamed columns (using similarity)
        common_cols = old_cols & new_cols
        potential_renames = []
        
        for old_col in old_cols - common_cols:
            for new_col in new_cols - common_cols:
                similarity = self._calculate_similarity(old_col, new_col)
                if similarity > 0.6:  # Threshold for potential rename
                    potential_renames.append({
                        "old_column": old_col,
                        "new_column": new_col,
                        "similarity": similarity,
                        "old_type": old_schema["columns"][old_col]["dtype"],
                        "new_type": new_schema["columns"][new_col]["dtype"]
                    })
        
        self.changes["renamed_columns"] = potential_renames
        
        # Detect type changes
        for col in common_cols:
            old_type = old_schema["columns"][col]["dtype"]
            new_type = new_schema["columns"][col]["dtype"]
            if old_type != new_type:
                self.changes["type_changes"].append({
                    "column": col,
                    "old_type": old_type,
                    "new_type": new_type
                })
        
        return self.changes
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity between two strings (simple implementation).
        Uses Levenshtein distance normalized by max length.
        """
        from difflib import SequenceMatcher
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def classify_changes(self) -> Dict[str, Any]:
        """
        Classify changes by severity and impact.
        
        Returns:
            Classification of changes
        """
        classification = {
            "breaking_changes": [],
            "non_breaking_changes": [],
            "migration_required": False,
            "severity": "low"
        }
        
        # Breaking changes
        if self.changes["removed_columns"]:
            classification["breaking_changes"].extend([
                f"Column removed: {c['column']}" for c in self.changes["removed_columns"]
            ])
            classification["migration_required"] = True
            classification["severity"] = "high"
        
        # Type changes can be breaking
        for tc in self.changes["type_changes"]:
            if self._is_breaking_type_change(tc["old_type"], tc["new_type"]):
                classification["breaking_changes"].append(
                    f"Breaking type change: {tc['column']} ({tc['old_type']} -> {tc['new_type']})"
                )
                classification["migration_required"] = True
                if classification["severity"] != "high":
                    classification["severity"] = "medium"
        
        # Non-breaking changes
        if self.changes["added_columns"]:
            classification["non_breaking_changes"].extend([
                f"Column added: {c['column']}" for c in self.changes["added_columns"]
            ])
        
        if self.changes["renamed_columns"]:
            classification["non_breaking_changes"].extend([
                f"Column renamed: {r['old_column']} -> {r['new_column']}" 
                for r in self.changes["renamed_columns"]
            ])
            classification["migration_required"] = True
        
        return classification
    
    def _is_breaking_type_change(self, old_type: str, new_type: str) -> bool:
        """Determine if a type change is breaking."""
        # Simple heuristic: numeric to string or vice versa is breaking
        numeric_types = ["int64", "float64", "int32", "float32"]
        string_types = ["object", "string"]
        
        old_is_numeric = any(nt in old_type for nt in numeric_types)
        new_is_numeric = any(nt in new_type for nt in numeric_types)
        old_is_string = any(st in old_type for st in string_types)
        new_is_string = any(st in new_type for st in string_types)
        
        return (old_is_numeric and new_is_string) or (old_is_string and new_is_numeric)
    
    def save_schema(self, schema: Dict, file_path: str):
        """Save schema to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(schema, f, indent=2)
        print(f"✓ Schema saved to {file_path}")
    
    def load_schema(self, file_path: str) -> Dict:
        """Load schema from JSON file."""
        with open(file_path, 'r') as f:
            return json.load(f)


def detect_changes(old_file: str, new_file: str, 
                   old_schema_path: str = None, 
                   new_schema_path: str = None) -> Dict[str, Any]:
    """
    Main function to detect schema changes between two files.
    
    Args:
        old_file: Path to old CSV file
        new_file: Path to new CSV file
        old_schema_path: Optional path to save old schema JSON
        new_schema_path: Optional path to save new schema JSON
        
    Returns:
        Dictionary containing all detected changes and classification
    """
    detector = SchemaChangeDetector()
    
    # Extract schemas
    print(f"Extracting schema from {old_file}...")
    old_schema = detector.extract_schema(old_file)
    
    print(f"Extracting schema from {new_file}...")
    new_schema = detector.extract_schema(new_file)
    
    # Save schemas if paths provided
    if old_schema_path:
        detector.save_schema(old_schema, old_schema_path)
    if new_schema_path:
        detector.save_schema(new_schema, new_schema_path)
    
    # Compare schemas
    print("Comparing schemas...")
    changes = detector.compare_schemas(old_schema, new_schema)
    
    # Classify changes
    classification = detector.classify_changes()
    
    result = {
        "old_schema": old_schema,
        "new_schema": new_schema,
        "changes": changes,
        "classification": classification
    }
    
    return result


if __name__ == "__main__":
    # Example usage
    import os
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    old_file = os.path.join(project_root, "data", "raw", "users_v1.csv")
    new_file = os.path.join(project_root, "data", "raw", "users_v2.csv")
    
    old_schema_path = os.path.join(project_root, "schemas", "old_schema.json")
    new_schema_path = os.path.join(project_root, "schemas", "new_schema.json")
    
    # Ensure schema directory exists
    os.makedirs(os.path.dirname(old_schema_path), exist_ok=True)
    
    result = detect_changes(old_file, new_file, old_schema_path, new_schema_path)
    
    print("\n" + "=" * 50)
    print("SCHEMA CHANGE DETECTION RESULTS")
    print("=" * 50)
    print(f"\nAdded columns: {len(result['changes']['added_columns'])}")
    for col in result['changes']['added_columns']:
        print(f"  + {col['column']} ({col['type']})")
    
    print(f"\nRemoved columns: {len(result['changes']['removed_columns'])}")
    for col in result['changes']['removed_columns']:
        print(f"  - {col['column']} ({col['type']})")
    
    print(f"\nRenamed columns: {len(result['changes']['renamed_columns'])}")
    for rename in result['changes']['renamed_columns']:
        print(f"  ~ {rename['old_column']} -> {rename['new_column']} (similarity: {rename['similarity']:.2f})")
    
    print(f"\nType changes: {len(result['changes']['type_changes'])}")
    for tc in result['changes']['type_changes']:
        print(f"  ! {tc['column']}: {tc['old_type']} -> {tc['new_type']}")
    
    print("\n" + "=" * 50)
    print("CLASSIFICATION")
    print("=" * 50)
    print(f"Severity: {result['classification']['severity']}")
    print(f"Migration required: {result['classification']['migration_required']}")
    print(f"\nBreaking changes: {len(result['classification']['breaking_changes'])}")
    for bc in result['classification']['breaking_changes']:
        print(f"  ⚠ {bc}")
    print(f"\nNon-breaking changes: {len(result['classification']['non_breaking_changes'])}")
    for nbc in result['classification']['non_breaking_changes']:
        print(f"  ✓ {nbc}")

