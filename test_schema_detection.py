#!/usr/bin/env python3
"""
Test script for schema detection - customize this!

Run this to test schema detection between different file versions.
"""
from ai.detect_schema_change import detect_changes
import os
import sys

# Your project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

def test_detection(old_file, new_file, test_name):
    """Test schema detection between two files."""
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print(f"{'='*60}")
    
    old_path = os.path.join(PROJECT_ROOT, "data/raw", old_file)
    new_path = os.path.join(PROJECT_ROOT, "data/raw", new_file)
    
    # Check if files exist
    if not os.path.exists(old_path):
        print(f"❌ Error: {old_path} not found!")
        return None
    if not os.path.exists(new_path):
        print(f"❌ Error: {new_path} not found!")
        return None
    
    print(f"Comparing:\n  Old: {old_file}\n  New: {new_file}\n")
    
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
    
    if result['changes']['renamed_columns']:
        print(f"\n🔄 Renamed columns:")
        for rename in result['changes']['renamed_columns']:
            print(f"    - {rename['old_column']} → {rename['new_column']} (similarity: {rename['similarity']:.2f})")
    
    if result['changes']['type_changes']:
        print(f"\n🔀 Type changes:")
        for tc in result['changes']['type_changes']:
            print(f"    - {tc['column']}: {tc['old_type']} → {tc['new_type']}")
    
    if result['classification']['breaking_changes']:
        print(f"\n⚠️  Breaking changes:")
        for bc in result['classification']['breaking_changes']:
            print(f"    - {bc}")
    
    return result

if __name__ == "__main__":
    print("=" * 60)
    print("SCHEMA DETECTION TEST SUITE (Taxi only)")
    print("=" * 60)

    results = []

    try:
        # NYC taxi schema evolution (if data present) – only V1 → V2 is supported.
        if os.path.exists(os.path.join(PROJECT_ROOT, "data/raw/yellow_base_v1.csv")):
            results.append(test_detection("yellow_base_v1.csv", "yellow_base_v2.csv", "Taxi V1 → V2"))
        else:
            print("⚠️ Taxi CSV files not found under data/raw/. Skipping tests.")
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if results:
        print("\n" + "=" * 60)
        print("✅ All taxi tests completed!")
        print("=" * 60)

        print("\n📈 Overall Summary:")
        total_added = sum(len(r["changes"]["added_columns"]) for r in results if r)
        total_removed = sum(len(r["changes"]["removed_columns"]) for r in results if r)
        print(f"  Total columns added across all tests: {total_added}")
        print(f"  Total columns removed across all tests: {total_removed}")

