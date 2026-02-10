# Thesis Work Roadmap: AI-Powered ETL Schema Change Detection

## Project Overview
This thesis focuses on building an intelligent ETL system that can:
1. **Detect schema changes** in data sources automatically
2. **Use AI to regenerate** ETL pipeline mappings when schemas change
3. **Integrate with Airflow** for automated pipeline orchestration

## Current Status ✅
- [x] Airflow setup and running
- [x] Basic ETL pipeline (extract, transform, load)
- [x] Project structure established
- [ ] Schema detection implementation
- [ ] AI-powered mapping regeneration
- [ ] Integration with Airflow DAGs
- [ ] Testing and validation
- [ ] Documentation and results

---

## Phase 1: Foundation & Data Preparation (Week 1-2)

### 1.1 Create Sample Data with Schema Variations
**Goal:** Create test datasets that simulate real-world schema changes

**Tasks:**
- [ ] Create `data/raw/users_v1.csv` (original schema)
- [ ] Create `data/raw/users_v2.csv` (schema with changes: new columns, renamed columns, type changes)
- [ ] Create `data/raw/users_v3.csv` (more complex changes: nested structures, missing columns)

**Deliverable:** Multiple CSV files representing schema evolution

### 1.2 Implement Schema Extraction
**Goal:** Extract and store schema information from data sources

**Tasks:**
- [ ] Implement schema extraction from CSV files
- [ ] Store schemas in JSON format (column names, types, constraints)
- [ ] Create schema comparison utilities

**Files to implement:**
- `schemas/old_schema.json` - Original schema
- `schemas/new_schema.json` - New schema after changes
- Schema extraction utilities

---

## Phase 2: Schema Change Detection (Week 3-4)

### 2.1 Implement Schema Detection Module
**Goal:** Detect differences between old and new schemas

**Tasks:**
- [ ] Implement `ai/detect_schema_change.py`
  - Compare two schemas
  - Identify: added columns, removed columns, renamed columns, type changes
  - Generate change report

**Key Features:**
- Column addition detection
- Column removal detection
- Column rename detection (using similarity matching)
- Data type change detection
- Constraint change detection

### 2.2 Schema Change Classification
**Goal:** Classify changes by severity and impact

**Tasks:**
- [ ] Categorize changes (breaking vs. non-breaking)
- [ ] Assess impact on existing ETL pipeline
- [ ] Generate migration recommendations

---

## Phase 3: AI-Powered Mapping Regeneration (Week 5-7)

### 3.1 Implement AI Mapping Generator
**Goal:** Use AI to automatically generate new ETL mappings

**Tasks:**
- [ ] Implement `ai/regenerate_mapping.py`
  - Use LLM (OpenAI/Claude) to analyze schema changes
  - Generate new transform logic
  - Suggest data migration strategies

**Key Features:**
- Prompt engineering for schema mapping
- Code generation for transform functions
- Validation of generated code
- Fallback strategies for AI failures

### 3.2 Integration with LLM APIs
**Goal:** Connect to AI services for intelligent mapping

**Tasks:**
- [ ] Set up OpenAI/Claude API integration
- [ ] Create prompt templates for schema mapping
- [ ] Implement response parsing and code extraction
- [ ] Add error handling and retries

---

## Phase 4: Airflow Integration (Week 8-9)

### 4.1 Create Schema Monitoring DAG
**Goal:** Automate schema change detection in Airflow

**Tasks:**
- [ ] Create `airflow_home/dags/schema_monitor_dag.py`
  - Scheduled task to check for schema changes
  - Trigger alerts when changes detected
  - Automatically invoke mapping regeneration

### 4.2 Dynamic DAG Generation
**Goal:** Generate Airflow DAGs dynamically based on schema

**Tasks:**
- [ ] Create DAG generator that uses detected schemas
- [ ] Update ETL pipeline DAG based on new mappings
- [ ] Implement version control for DAGs

### 4.3 Pipeline Orchestration
**Goal:** Orchestrate the complete workflow

**Tasks:**
- [ ] Create master DAG that:
  1. Monitors schema changes
  2. Detects changes
  3. Regenerates mappings using AI
  4. Updates ETL pipeline
  5. Runs validation tests
  6. Deploys new pipeline

---

## Phase 5: Testing & Validation (Week 10-11)

**NYC taxi experiment:** Data and scripts are in place for controlled schema evolution (V1/V2/V3). See [docs/phase5_evaluation_protocol.md](docs/phase5_evaluation_protocol.md) for the manual-vs-AI timing protocol and results table. See [docs/nyc_taxi_experiment_notes.md](docs/nyc_taxi_experiment_notes.md) for schema change documentation.

### 5.1 Unit Tests
**Tasks:**
- [ ] Test schema detection accuracy
- [ ] Test AI mapping generation
- [ ] Test ETL pipeline with new mappings

### 5.2 Integration Tests
**Tasks:**
- [ ] Test end-to-end workflow
- [ ] Test with various schema change scenarios
- [ ] Performance testing

### 5.3 Validation Framework
**Tasks:**
- [ ] Data quality checks
- [ ] Schema validation
- [ ] Pipeline correctness verification

---

## Phase 6: Documentation & Results (Week 12)

### 6.1 Code Documentation
**Tasks:**
- [ ] Add docstrings to all functions
- [ ] Create API documentation
- [ ] Document configuration options

### 6.2 Thesis Documentation
**Tasks:**
- [ ] Document methodology
- [ ] Create results and analysis
- [ ] Prepare presentation materials

---

## Immediate Next Steps (Start Here!)

### Step 1: Create Sample Data
Create test CSV files to work with:

```bash
# Create sample data files
python scripts/create_sample_data.py
```

### Step 2: Implement Schema Detection
Start with basic schema extraction and comparison.

### Step 3: Test with Airflow
Run your first schema detection task in Airflow.

---

## Research Questions to Address

1. **Accuracy:** How accurately can AI detect and map schema changes?
2. **Efficiency:** How much time does automated regeneration save vs. manual updates?
3. **Reliability:** What's the success rate of AI-generated mappings?
4. **Scalability:** How does the system perform with complex schemas?

---

## Success Metrics

- [ ] Schema change detection accuracy > 95%
- [ ] AI mapping generation success rate > 80%
- [ ] Time reduction: 70% faster than manual mapping
- [ ] Pipeline correctness: 100% of generated pipelines pass validation

---

## Tools & Technologies

- **Orchestration:** Apache Airflow
- **AI/LLM:** OpenAI GPT-4 / Anthropic Claude
- **Data Processing:** Pandas, SQLAlchemy
- **Testing:** pytest
- **Documentation:** Sphinx / MkDocs

---

## Notes

- Keep detailed logs of all experiments
- Document all schema change scenarios tested
- Track AI API usage and costs
- Maintain version control for all code and schemas

