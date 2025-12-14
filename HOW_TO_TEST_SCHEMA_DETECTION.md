# How to Test Schema Detection - Step by Step Guide

This guide will teach you how to test the schema detection system yourself.

## Understanding What Schema Detection Does

Schema detection compares two data files and identifies:
- **Added columns** - New columns in the new file
- **Removed columns** - Columns that existed in old file but not in new
- **Renamed columns** - Columns that were renamed (detected by similarity)
- **Type changes** - Columns where data types changed
- **Severity classification** - Whether changes are breaking or non-breaking

## Method 1: Using the Command Line (Easiest)

### Step 1: Check Your Data Files

First, let's see what data files you have:

```bash
cd /Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema
ls -la data/raw/
```

You should see:
- `users_v1.csv` - Original schema
- `users_v2.csv` - Schema with changes
- `users_v3.csv` - More complex changes

### Step 2: Look at the Actual Data

Let's see what's in these files:

```bash
# View first few lines of v1
head -5 data/raw/users_v1.csv

# View first few lines of v2
head -5 data/raw/users_v2.csv
```

**What to notice:**
- v1 has: `user_id`, `first_name`, `last_name`, `email`, `age`, `created_at`
- v2 has: `user_id`, `first_name`, `last_name`, `email`, `user_age`, `phone_number`, `country`, `created_at`

**Key differences:**
- `age` → `user_age` (renamed)
- Added: `phone_number`, `country`

### Step 3: Run Schema Detection

Now let's run the detection:

```bash
# Activate your virtual environment
source venv/bin/activate

# Set the Airflow home (optional, but good practice)
export AIRFLOW_HOME=/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema/airflow_home

# Run the schema detection
python ai/detect_schema_change.py
```

### Step 4: Understand the Output

You should see output like this:

```
Extracting schema from .../users_v1.csv...
Extracting schema from .../users_v2.csv...
✓ Schema saved to .../old_schema.json
✓ Schema saved to .../new_schema.json
Comparing schemas...

==================================================
SCHEMA CHANGE DETECTION RESULTS
==================================================

Added columns: 3
  + country (object)
  + user_age (int64)
  + phone_number (object)

Removed columns: 1
  - age (int64)

Renamed columns: 0

Type changes: 0

==================================================
CLASSIFICATION
==================================================
Severity: high
Migration required: True

Breaking changes: 1
  ⚠ Column removed: age

Non-breaking changes: 3
  ✓ Column added: country
  ✓ Column added: user_age
  ✓ Column added: phone_number
```

**What this means:**
- The system found 3 new columns and 1 removed column
- It classified this as "high severity" because a column was removed
- It says "migration required" because changes need to be handled

### Step 5: Check the Generated Schema Files

The detection also saved schema files. Let's look at them:

```bash
# View the old schema
cat schemas/old_schema.json | python -m json.tool | head -30

# View the new schema
cat schemas/new_schema.json | python -m json.tool | head -30
```

These JSON files contain detailed information about each column:
- Column name
- Data type
- Whether it can be null
- Sample values

## Method 2: Using Python Interactively

This method gives you more control and lets you explore the results.

### Step 1: Start Python

```bash
cd /Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema
source venv/bin/activate
python
```

### Step 2: Import and Use the Module

```python
# Import the detection module
from ai.detect_schema_change import SchemaChangeDetector, detect_changes
import os

# Set up paths
project_root = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"
old_file = os.path.join(project_root, "data/raw/users_v1.csv")
new_file = os.path.join(project_root, "data/raw/users_v2.csv")

# Run detection
result = detect_changes(old_file, new_file)

# Explore the results
print("Added columns:", len(result['changes']['added_columns']))
for col in result['changes']['added_columns']:
    print(f"  - {col['column']} ({col['type']})")

print("\nRemoved columns:", len(result['changes']['removed_columns']))
for col in result['changes']['removed_columns']:
    print(f"  - {col['column']} ({col['type']})")

print("\nSeverity:", result['classification']['severity'])
print("Migration required:", result['classification']['migration_required'])
```

### Step 3: Inspect Individual Schemas

```python
# Create a detector instance
detector = SchemaChangeDetector()

# Extract schema from a single file
schema_v1 = detector.extract_schema(old_file)
schema_v2 = detector.extract_schema(new_file)

# Look at column names
print("v1 columns:", list(schema_v1['columns'].keys()))
print("v2 columns:", list(schema_v2['columns'].keys()))

# Look at details of a specific column
print("\nDetails of 'age' column in v1:")
print(schema_v1['columns']['age'])

print("\nDetails of 'user_age' column in v2:")
print(schema_v2['columns']['user_age'])
```

## Method 3: Test Different Scenarios

### Test 1: v1 vs v2 (Basic Changes)

```bash
python -c "
from ai.detect_schema_change import detect_changes
import os
project_root = '/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema'
result = detect_changes(
    os.path.join(project_root, 'data/raw/users_v1.csv'),
    os.path.join(project_root, 'data/raw/users_v2.csv')
)
print(f'v1→v2: {len(result[\"changes\"][\"added_columns\"])} added, {len(result[\"changes\"][\"removed_columns\"])} removed')
"
```

### Test 2: v2 vs v3 (More Complex)

```bash
python -c "
from ai.detect_schema_change import detect_changes
import os
project_root = '/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema'
result = detect_changes(
    os.path.join(project_root, 'data/raw/users_v2.csv'),
    os.path.join(project_root, 'data/raw/users_v3.csv')
)
print(f'v2→v3: {len(result[\"changes\"][\"added_columns\"])} added, {len(result[\"changes\"][\"removed_columns\"])} removed')
print(f'Renamed: {len(result[\"changes\"][\"renamed_columns\"])}')
"
```

### Test 3: v1 vs v3 (Complex Combination)

```bash
python -c "
from ai.detect_schema_change import detect_changes
import os
project_root = '/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema'
result = detect_changes(
    os.path.join(project_root, 'data/raw/users_v1.csv'),
    os.path.join(project_root, 'data/raw/users_v3.csv')
)
print('v1→v3 Results:')
print(f'  Added: {len(result[\"changes\"][\"added_columns\"])}')
print(f'  Removed: {len(result[\"changes\"][\"removed_columns\"])}')
print(f'  Renamed: {len(result[\"changes\"][\"renamed_columns\"])}')
print(f'  Type changes: {len(result[\"changes\"][\"type_changes\"])}')
print(f'  Severity: {result[\"classification\"][\"severity\"]}')
"
```

## Method 4: Create Your Own Test Script

Create a file `test_schema_detection.py`:

```python
#!/usr/bin/env python3
"""Test script for schema detection - customize this!"""

from ai.detect_schema_change import detect_changes
import os
import json

# Your project root
PROJECT_ROOT = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"

def test_detection(old_file, new_file, test_name):
    """Test schema detection between two files."""
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print(f"{'='*60}")
    
    old_path = os.path.join(PROJECT_ROOT, "data/raw", old_file)
    new_path = os.path.join(PROJECT_ROOT, "data/raw", new_file)
    
    result = detect_changes(old_path, new_path)
    
    print(f"\n📊 Summary:")
    print(f"  Added columns: {len(result['changes']['added_columns'])}")
    print(f"  Removed columns: {len(result['changes']['removed_columns'])}")
    print(f"  Renamed columns: {len(result['changes']['renamed_columns'])}")
    print(f"  Type changes: {len(result['changes']['type_changes'])}")
    
    print(f"\n⚠️  Classification:")
    print(f"  Severity: {result['classification']['severity']}")
    print(f"  Migration required: {result['classification']['migration_required']}")
    
    if result['changes']['added_columns']:
        print(f"\n➕ Added columns:")
        for col in result['changes']['added_columns']:
            print(f"    - {col['column']} ({col['type']})")
    
    if result['changes']['removed_columns']:
        print(f"\n➖ Removed columns:")
        for col in result['changes']['removed_columns']:
            print(f"    - {col['column']} ({col['type']})")
    
    return result

if __name__ == "__main__":
    # Test different scenarios
    test_detection("users_v1.csv", "users_v2.csv", "v1 → v2")
    test_detection("users_v2.csv", "users_v3.csv", "v2 → v3")
    test_detection("users_v1.csv", "users_v3.csv", "v1 → v3")
    
    print("\n" + "="*60)
    print("✅ All tests completed!")
    print("="*60)
```

Run it:
```bash
python test_schema_detection.py
```

## What to Look For

### ✅ Good Results (Working Correctly)

- Detects added columns correctly
- Detects removed columns correctly
- Identifies type changes
- Classifies severity appropriately
- Generates valid JSON schema files

### ⚠️ Things to Check

1. **Renamed columns detection:**
   - The system uses similarity matching
   - If `age` → `user_age` isn't detected as rename, that's okay (similarity threshold)
   - You can adjust the threshold in the code (line ~150)

2. **Type changes:**
   - Should detect when `int64` → `object` (breaking)
   - Should detect when `object` → `string` (usually non-breaking)

3. **Severity classification:**
   - Removed columns = high severity
   - Breaking type changes = medium/high severity
   - Added columns = low severity

## Troubleshooting

### Problem: "File not found"
**Solution:** Check your file paths:
```bash
ls -la data/raw/*.csv
```

### Problem: "Module not found"
**Solution:** Make sure you're in the project directory and virtual environment is activated:
```bash
cd /Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema
source venv/bin/activate
```

### Problem: "JSON serialization error"
**Solution:** This was fixed, but if you see it, make sure you have the latest version of `detect_schema_change.py`

## Next Steps After Testing

Once you've verified schema detection works:

1. **Test with your own data** - Create CSV files with your own schema changes
2. **Modify the detection logic** - Adjust similarity thresholds, add new checks
3. **Integrate with Airflow** - Create a DAG that runs detection automatically
4. **Test edge cases** - Missing data, empty files, very large files

## Quick Reference Commands

```bash
# Quick test
python ai/detect_schema_change.py

# View schemas
cat schemas/old_schema.json | python -m json.tool

# Compare two specific files
python -c "from ai.detect_schema_change import detect_changes; import os; result = detect_changes('data/raw/users_v1.csv', 'data/raw/users_v2.csv'); print(result['classification'])"
```

---

**Now try it yourself!** Start with Method 1 (command line) - it's the easiest way to see it in action.

