---
sidebar_position: 1
---

# Your First Pipeline

This guide walks through creating, validating, and executing a Teckel pipeline from scratch.

## 1. Write the Pipeline YAML

Create a file `pipeline.yaml`:

```yaml
version: "3.0"

pipeline:
  name: first-pipeline
  description: Filter and transform a CSV file

assets:
  - asset_ref: raw_data
    source:
      input:
        format: csv
        path: data/employees.csv
        options:
          header: "true"
          inferSchema: "true"

  - asset_ref: active_employees
    source:
      where:
        from: raw_data
        filter: "status = 'active'"

  - asset_ref: selected
    source:
      select:
        from: active_employees
        columns:
          - id
          - name
          - department
          - salary

  - asset_ref: by_department
    source:
      group_by:
        from: selected
        by:
          - department
        agg:
          - "COUNT(*) AS employee_count"
          - "AVG(salary) AS avg_salary"

  - asset_ref: result
    source:
      output:
        from: by_department
        format: parquet
        path: output/department_summary.parquet
        mode: overwrite
```

## 2. Create Sample Data

Create `data/employees.csv`:

```csv
id,name,department,salary,status
1,Alice,Engineering,95000,active
2,Bob,Marketing,75000,active
3,Charlie,Engineering,105000,active
4,Diana,Marketing,80000,inactive
5,Eve,Engineering,90000,active
6,Frank,Sales,70000,active
```

## 3. Validate the Pipeline

Before executing, validate the pipeline to catch errors:

### Using the Rust API

```rust
use teckel_api;
use std::collections::BTreeMap;

fn main() {
    let yaml = std::fs::read_to_string("pipeline.yaml").unwrap();
    let variables = BTreeMap::new();

    match teckel_api::validate(&yaml, &variables) {
        Ok(()) => println!("Pipeline is valid!"),
        Err(e) => eprintln!("Validation error: {e}"),
    }
}
```

### Using the HTTP Server

```bash
curl -X POST http://localhost:8080/api/validate \
  -H "Content-Type: application/json" \
  -d "{\"yaml\": $(cat pipeline.yaml | jq -Rs .), \"variables\": {}}"
```

## 4. Explain the Execution Plan

Generate an execution plan to understand what will happen without running anything:

### Using the Rust API

```rust
let plan = teckel_api::explain(&yaml, &variables)?;
println!("{plan}");
```

Output:
```
=== Pipeline Execution Plan ===

--- Inputs ---
  [raw_data] format=csv, path=data/employees.csv

--- Transformations ---
  [active_employees] WHERE (from: raw_data)
  [selected] SELECT (from: active_employees)
  [by_department] GROUP BY (from: selected)

--- Outputs ---
  [result] from=by_department, format=parquet, mode=Overwrite, path=output/department_summary.parquet

Total: 1 inputs, 3 transformations, 1 outputs
Execution waves: 5
  Wave 0: raw_data
  Wave 1: active_employees
  Wave 2: selected
  Wave 3: by_department
  Wave 4: result
```

### Using the HTTP Server

```bash
curl -X POST http://localhost:8080/api/explain \
  -H "Content-Type: application/json" \
  -d "{\"yaml\": $(cat pipeline.yaml | jq -Rs .), \"variables\": {}}"
```

## 5. Execute the Pipeline

### Synchronous (Rust API)

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string("pipeline.yaml")?;
    let variables = std::collections::BTreeMap::new();

    teckel_api::etl(&yaml, &variables).await?;
    println!("Pipeline completed!");
    Ok(())
}
```

### Asynchronous (HTTP Server)

```bash
# Submit the job
JOB_ID=$(curl -s -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d "{\"yaml\": $(cat pipeline.yaml | jq -Rs .), \"variables\": {}}" \
  | jq -r '.job_id')

echo "Job submitted: $JOB_ID"

# Wait for completion
curl -s "http://localhost:8080/api/jobs/$JOB_ID/wait?timeout=60" | jq .
```

## 6. Check the Output

The pipeline writes a Parquet file to `output/department_summary.parquet`. You can inspect it with any Parquet-compatible tool:

```bash
# Using Python
python3 -c "import pandas; print(pandas.read_parquet('output/department_summary.parquet'))"
```

Expected output:
```
    department  employee_count  avg_salary
0  Engineering               3     96666.7
1    Marketing               1     75000.0
2        Sales               1     70000.0
```

## Using Variables

Pipelines support variable substitution with `${VAR:default}` syntax:

```yaml
assets:
  - asset_ref: raw_data
    source:
      input:
        format: csv
        path: "${DATA_DIR:data}/employees.csv"

  - asset_ref: result
    source:
      output:
        from: by_department
        format: parquet
        path: "${OUTPUT_DIR:output}/department_summary.parquet"
        mode: overwrite
```

Pass variables at runtime:

```rust
let mut variables = BTreeMap::new();
variables.insert("DATA_DIR".to_string(), "/mnt/data".to_string());
variables.insert("OUTPUT_DIR".to_string(), "/mnt/output".to_string());
teckel_api::etl(&yaml, &variables).await?;
```

Or via the HTTP API:

```json
{
  "yaml": "...",
  "variables": {
    "DATA_DIR": "/mnt/data",
    "OUTPUT_DIR": "/mnt/output"
  }
}
```

## Choosing a Backend

By default, `teckel_api::etl()` uses the DataFusion backend. To use a different backend:

```rust
use teckel_polars::PolarsBackend;

let backend = PolarsBackend::new();
teckel_api::etl_with(&yaml, &variables, backend).await?;
```

Or for Spark Connect:

```rust
use teckel_spark::SparkConnectBackend;

let backend = SparkConnectBackend::new("sc://spark-master:15002/").await?;
teckel_api::etl_with(&yaml, &variables, backend).await?;
```
