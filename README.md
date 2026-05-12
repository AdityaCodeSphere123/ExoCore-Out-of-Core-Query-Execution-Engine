# Exocore
**High-Performance Out-of-Core Query Execution Engine**

Exocore is a high-performance query execution engine written in Rust, designed to handle massive datasets that exceed physical memory (RAM). By implementing specialized external-memory algorithms and a high-fidelity HDD simulator, Exocore masters the art of moving data between disk and processor with minimal latency.

**Link**: [Assignment Specification](https://messy-circle-aa4.notion.site/COL362-632-Assignment-3-Out-of-Core-Query-Execution-3236f1adfa0a80b9a9e4ca3492cb5efb)  
**Course**: COL362 (Database Management Systems)  
**Authors**: Aditya Anand and Ahilaan Saxena (IIT Delhi)

---

## 🏗 System Architecture

The project is structured as a multi-process system to isolate concerns and accurately simulate physical hardware constraints.

- **`database`**: The core query engine. It implements operators like Scan, Filter, Project, Sort, and Cross-Join. It operates under strict memory limits enforced via `setrlimit`.
- **`disk`**: A physical disk simulator. It models a Hard Disk Drive (HDD) with configurable parameters for seek time, rotational latency, and transfer rates, providing a realistic benchmark for I/O-bound queries.
- **`monitor`**: The orchestrator. It spawns the database and disk processes, facilitates communication via Unix pipes, collects performance metrics, and validates the output against expected results.
- **`generator`**: A utility to transform raw datasets (like TPC-H) into the engine's optimized binary format. It also generates a SQLite database for ground-truth validation.
- **`demo_query_printer`**: A DSL for constructing query plans in JSON format.

---

## 🚀 Getting Started

### 1. Prerequisites
- Rust (latest stable)
- SQLite3 (for generating validation data)
- TPC-H dataset (CSV/TBL format)

### 2. Data Generation
First, you need to compile your raw data into binary format and generate the necessary configurations.
```bash
cd my-awesome-db
cargo run -r --bin generator -- all \
    --dataset-folder ../path/to/tpch/raw \
    --compiled-dataset-folder ../scratch/compiled_datasets/tpch \
    --runtime-folder ../scratch/runtimes/tpch \
    --build-path ./target/release \
    --block-size 4096
```

### 3. Define a Query
Use the `demo_query_printer` to build your query plan. Modify `demo_query_printer/src/main.rs` to define your desired plan:
```rust
let query = QueryOp::scan("customer")
    .cross(QueryOp::scan("orders"))
    .filter("c_custkey", ComparisionOperator::EQ, ComparisionValue::Column("o_custkey".to_string()))
    .sort("c_custkey", true)
    .build();
```
Then run it to get the JSON:
```bash
cargo run -r --bin demo_query_printer
```

### 4. Configuration
Copy the printed JSON into the `query` field of `scratch/runtimes/tpch/monitor_config.json`. Ensure the query is enabled:
```json
{
  "execution_name": "Join Query",
  "disabled": false,
  "query": { ... your query json ... },
  "expected_output_file": "scratch/runtimes/tpch/expected_1.csv",
  "memory_limit_mb": 64
}
```

### 5. Generate Expected Output (Ground Truth)
Use SQLite to generate the correct results for validation:
```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < my_query.sql > scratch/runtimes/tpch/expected_1.csv
```

### 6. Run and Monitor
Execute the query through the monitor to see performance metrics:
```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```
Detailed metrics will be saved to `disk_io_metrics.csv`.

---

## ⚡ Performance Optimizations

The engine implements several advanced techniques to minimize Disk I/O and CPU overhead:

- **Scan Prefetching**: Background threads fetch data blocks before they are requested by the execution operators, hiding disk latency.
- **Batched Temp Storage**: Intermediate runs during external sorting are written in large, contiguous blocks to reduce disk head movement.
- **Optimized Join Algorithms**: Implements memory-efficient join strategies suitable for out-of-core execution.
- **Custom Buffer Management**: A specialized buffer manager designed to interface with the disk simulator's block-based API.

---

## 📊 Benchmarking Results

The following table summarizes the improvement across various optimization stages (measured in total I/O time):

| Optimization Stage | Total Reads | Total Writes | Cylinders Traveled | Total I/O Time (us) |
| :--- | :--- | :--- | :--- | :--- |
| Baseline | 24,107 | 19,178 | 21,411,854 | 100,997,961 |
| Batched Temp Storage | 9,867 | 4,938 | 6,218,286 | 35,326,175 |
| Scan Prefetch | 5,247 | 4,938 | 1,422,962 | 25,204,602 |
| Optimized Join | 2,677 | 2,368 | 1,855,391 | 15,335,025 |
| Full Optimizations | 932 | 330 | 237,680 | 3,423,203 |

> [!NOTE]
> Metrics were collected using the custom Disk Simulator, modeling a disk with 7200 RPM and 8.5ms average seek time.