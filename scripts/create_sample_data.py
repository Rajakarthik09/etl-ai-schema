#!/usr/bin/env python3
"""
Create sample data files with different schema versions for testing.
This simulates real-world schema evolution scenarios.
"""
import pandas as pd
import os

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

# Ensure directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def create_users_v1():
    """Original schema: basic user information"""
    data = {
        "user_id": [1, 2, 3, 4, 5],
        "first_name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        "last_name": ["Doe", "Smith", "Johnson", "Williams", "Brown"],
        "email": ["john@example.com", "jane@example.com", "bob@example.com", 
                 "alice@example.com", "charlie@example.com"],
        "age": [25, 30, 35, 28, 32],
        "created_at": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
    }
    df = pd.DataFrame(data)
    filepath = os.path.join(RAW_DATA_DIR, "users_v1.csv")
    df.to_csv(filepath, index=False)
    print(f"✓ Created {filepath}")
    return df

def create_users_v2():
    """
    Schema changes:
    - Added: phone_number, country
    - Renamed: age -> user_age
    - Changed type: created_at format
    """
    data = {
        "user_id": [1, 2, 3, 4, 5],
        "first_name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        "last_name": ["Doe", "Smith", "Johnson", "Williams", "Brown"],
        "email": ["john@example.com", "jane@example.com", "bob@example.com", 
                 "alice@example.com", "charlie@example.com"],
        "user_age": [25, 30, 35, 28, 32],  # Renamed from 'age'
        "phone_number": ["+1-555-0101", "+1-555-0102", "+1-555-0103", 
                        "+1-555-0104", "+1-555-0105"],  # New column
        "country": ["USA", "USA", "Canada", "USA", "Mexico"],  # New column
        "created_at": ["2023-01-01T00:00:00", "2023-01-02T00:00:00", 
                      "2023-01-03T00:00:00", "2023-01-04T00:00:00", 
                      "2023-01-05T00:00:00"]  # Changed format
    }
    df = pd.DataFrame(data)
    filepath = os.path.join(RAW_DATA_DIR, "users_v2.csv")
    df.to_csv(filepath, index=False)
    print(f"✓ Created {filepath}")
    return df

def create_users_v3():
    """
    Schema changes:
    - Removed: phone_number
    - Added: address (structured), subscription_tier
    - Renamed: user_age -> age (back to original name)
    - Changed: country -> country_code (different name, similar purpose)
    """
    data = {
        "user_id": [1, 2, 3, 4, 5],
        "first_name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        "last_name": ["Doe", "Smith", "Johnson", "Williams", "Brown"],
        "email": ["john@example.com", "jane@example.com", "bob@example.com", 
                 "alice@example.com", "charlie@example.com"],
        "age": [25, 30, 35, 28, 32],  # Renamed back from user_age
        "country_code": ["US", "US", "CA", "US", "MX"],  # Renamed from country
        "address": ["123 Main St, City, State", "456 Oak Ave, City, State",
                   "789 Pine Rd, City, State", "321 Elm St, City, State",
                   "654 Maple Dr, City, State"],  # New column
        "subscription_tier": ["premium", "basic", "premium", "basic", "premium"],  # New column
        "created_at": ["2023-01-01T00:00:00", "2023-01-02T00:00:00", 
                      "2023-01-03T00:00:00", "2023-01-04T00:00:00", 
                      "2023-01-05T00:00:00"]
    }
    df = pd.DataFrame(data)
    filepath = os.path.join(RAW_DATA_DIR, "users_v3.csv")
    df.to_csv(filepath, index=False)
    print(f"✓ Created {filepath}")
    return df

if __name__ == "__main__":
    print("Creating sample data files...")
    print("=" * 50)
    
    df1 = create_users_v1()
    df2 = create_users_v2()
    df3 = create_users_v3()
    
    print("=" * 50)
    print("\nSchema Summary:")
    print(f"\nusers_v1.csv columns: {list(df1.columns)}")
    print(f"users_v2.csv columns: {list(df2.columns)}")
    print(f"users_v3.csv columns: {list(df3.columns)}")
    print("\n✓ All sample data files created successfully!")

