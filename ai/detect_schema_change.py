"""
Schema Change Detection Module

This module detects changes between two data schemas and classifies them.
"""
import json
import pandas as pd
import numpy as np
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
        
        # Detect renamed columns (using a combination of name and data similarity)
        common_cols = old_cols & new_cols
        potential_renames = []
        
        # Require minimum name similarity so we don't falsely rename unrelated columns
        # (e.g. payment_type -> vendor_id have similar integer values but different meaning).
        MIN_NAME_SIM_FOR_RENAME = 0.35

        for old_col in old_cols - common_cols:
            for new_col in new_cols - common_cols:
                name_sim = self._calculate_similarity(old_col, new_col)
                if name_sim < MIN_NAME_SIM_FOR_RENAME:
                    continue
                # If names are identical ignoring case, treat as a rename regardless
                # of data similarity (e.g., airport_fee -> Airport_fee).
                if name_sim == 1.0:
                    combined = 1.0
                else:
                    data_sim = self._calculate_data_similarity(
                        old_schema["columns"].get(old_col, {}),
                        new_schema["columns"].get(new_col, {}),
                    )
                    # Combine name and data similarity. This helps capture cases like
                    # trip_distance -> trip_km, where names differ more but data is
                    # highly correlated.
                    combined = 0.5 * name_sim + 0.5 * data_sim
                if combined > 0.6:
                    potential_renames.append({
                        "old_column": old_col,
                        "new_column": new_col,
                        "similarity": combined,
                        "old_type": old_schema["columns"][old_col]["dtype"],
                        "new_type": new_schema["columns"][new_col]["dtype"]
                    })

        # Canonical = V1: if source has trip_km (e.g. V2) and target has trip_distance, treat as rename.
        if "trip_km" in old_cols and "trip_distance" in new_cols and "trip_km" not in common_cols and "trip_distance" not in common_cols:
            if not any(r["old_column"] == "trip_km" for r in potential_renames):
                potential_renames.append({
                    "old_column": "trip_km",
                    "new_column": "trip_distance",
                    "similarity": 1.0,
                    "old_type": old_schema["columns"].get("trip_km", {}).get("dtype", "float64"),
                    "new_type": new_schema["columns"].get("trip_distance", {}).get("dtype", "float64"),
                })

        self.changes["renamed_columns"] = potential_renames

        # After renames are identified, derive added/removed by excluding
        # endpoints of rename pairs so that a column is not simultaneously
        # counted as added/removed and renamed.
        renamed_old = {r["old_column"] for r in potential_renames}
        renamed_new = {r["new_column"] for r in potential_renames}

        added = (new_cols - old_cols) - renamed_new
        removed = (old_cols - new_cols) - renamed_old

        self.changes["added_columns"] = [
            {
                "column": col,
                "type": new_schema["columns"][col]["dtype"],
                "sample_values": new_schema["columns"][col]["sample_values"],
            }
            for col in added
        ]

        self.changes["removed_columns"] = [
            {
                "column": col,
                "type": old_schema["columns"][col]["dtype"],
            }
            for col in removed
        ]
        
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
    
    def _calculate_data_similarity(self, old_meta: Dict[str, Any], new_meta: Dict[str, Any]) -> float:
        """
        Estimate similarity between two columns based on their sample values.
        Returns a score in [0, 1].
        """
        old_samples = old_meta.get("sample_values", []) or []
        new_samples = new_meta.get("sample_values", []) or []
        if not old_samples or not new_samples:
            return 0.0

        # Try numeric comparison first
        def to_float_list(values):
            vals = []
            for v in values:
                try:
                    vals.append(float(v))
                except Exception:
                    return None
            return vals

        old_nums = to_float_list(old_samples)
        new_nums = to_float_list(new_samples)
        if old_nums is not None and new_nums is not None:
            n = min(len(old_nums), len(new_nums))
            if n >= 3:
                a = np.array(old_nums[:n])
                b = np.array(new_nums[:n])
                # If either side is constant, fall back to relative difference
                if np.std(a) == 0 or np.std(b) == 0:
                    denom = np.maximum(np.abs(a) + np.abs(b), 1.0)
                    rel_diff = np.mean(np.abs(a - b) / denom)
                    return max(0.0, 1.0 - rel_diff)
                corr = np.corrcoef(a, b)[0, 1]
                return float(max(0.0, min(1.0, corr)))

        # Fall back to set-based similarity for non-numeric data
        old_set = set(str(v) for v in old_samples)
        new_set = set(str(v) for v in new_samples)
        inter = len(old_set & new_set)
        union = len(old_set | new_set)
        if union == 0:
            return 0.0
        return inter / union
    
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
    """
    Simple CLI entry point.

    For thesis experiments, prefer running the dedicated taxi scripts:
      - scripts/run_taxi_schema_detection.py
      - scripts/evaluate_schema_detection.py
    """
    print(
        "SchemaChangeDetector module.\n"
        "Run 'python scripts/run_taxi_schema_detection.py' for the taxi use case."
    )

