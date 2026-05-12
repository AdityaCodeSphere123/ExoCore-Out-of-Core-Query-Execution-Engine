# Exocore
**High-Performance Out-of-Core Query Execution Engine**

Exocore is a Rust query engine built to run relational workloads that exceed RAM. The codebase is split into a database process, a disk simulator, and a monitor that wires them together, enforces resource limits, and validates the final output.

**Link**: [Assignment Specification](https://messy-circle-aa4.notion.site/COL362-632-Assignment-3-Out-of-Core-Query-Execution-3236f1adfa0a80b9a9e4ca3492cb5efb)  
**Course**: COL362 (Database Management Systems)  
**Authors**: Aditya Anand and Ahilaan Saxena (IIT Delhi)

## System Architecture

- `database`: the query runtime. It reads a JSON query plan, builds a physical operator tree, streams results back to the monitor, and runs under a strict memory cap.
- `disk`: the HDD simulator. It exposes a tiny text protocol over stdin/stdout and tracks read/write timing metrics.
- `monitor`: the orchestrator. It launches `disk` and `database`, remaps file descriptors, sets process limits, and checks results against the expected output.
- `generator`: the data and config builder. It converts raw datasets into the binary page format used by the engine and also produces the runtime JSON files.
- `demo_query_printer`: a small helper that prints a serialized query plan from the Rust DSL in `common/src/query.rs`.

## How to Run

### Prerequisites

- Rust stable
- SQLite3
- A TPC-H style dataset in CSV/TBL form
- A Linux environment or Linux-compatible layer such as WSL or a container

This project is not fully native-macOS friendly. The database reads `/proc/self/status`, and the monitor uses Unix process limits and file-descriptor remapping to launch the child processes.

### 1. Build the workspace

```bash
cd my-awesome-db
cargo build --release
```

### 2. Generate binary data and runtime configs

Use the generator to convert the raw dataset into the binary page layout and to create the runtime configuration files.

```bash
cd my-awesome-db
cargo run --release --bin generator -- all \
  --dataset-folder ../path/to/tpch/raw \
  --compiled-dataset-folder ../scratch/compiled_datasets/tpch \
  --runtime-folder ../scratch/runtimes/tpch \
  --build-path ./target/release \
  --block-size 4096
```

The `generator` binary also supports individual subcommands if you only want part of the setup:

- `disk`: writes the binary table files and disk config
- `database`: writes the database config
- `monitor`: writes the monitor config
- `sqlite`: creates the SQLite validation database
- `all`: does everything above

### 3. Define the query plan

Edit `demo_query_printer/src/main.rs`, build the query with the Rust DSL, and print it as JSON. For example:

```rust
let query = QueryOp::scan("customer")
    .cross(QueryOp::scan("orders"))
    .filter(
        "c_custkey",
        ComparisionOperator::EQ,
        ComparisionValue::Column("o_custkey".to_string()),
    )
    .sort("c_custkey", true)
    .build();
```

Then print the JSON:

```bash
cd my-awesome-db
cargo run --release --bin demo_query_printer
```

### 4. Wire the query into the monitor config

Copy the printed query JSON into the `query` field of the relevant entry in `scratch/runtimes/tpch/monitor_config.json` and make sure the query is enabled.

```json
{
  "execution_name": "Join Query",
  "disabled": false,
  "query": { "root": { "...": "..." } },
  "expected_output_file": "scratch/runtimes/tpch/expected_1.csv",
  "memory_limit_mb": 64
}
```

### 5. Generate expected output with SQLite

Use SQLite to produce the ground-truth output used by the monitor.

```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < my_query.sql > scratch/runtimes/tpch/expected_1.csv
```

### 6. Run the end-to-end pipeline

The monitor starts the disk simulator and database process, sends the query plan, enforces the memory limit, and checks the output.

```bash
cd my-awesome-db
cargo run --release --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

The disk process prints its I/O summary to stderr, and the database prints memory metrics to stderr as well.

## Implementation Details

### Logical query model

The shared query DSL is in `common/src/query.rs`. A query is a `Query` whose root is a tree of `QueryOp` nodes:

- `Scan` selects a table by id
- `Filter` applies one or more predicates
- `Project` renames or keeps specific columns
- `Cross` combines two child relations
- `Sort` sorts by one or more columns

The builder helpers in that file are just conveniences for composing those trees and serializing them to JSON.

### Query planning and execution

The main runtime path is `db_main()` in `database/src/main.rs`. It loads the database context, extracts table statistics from the same JSON, opens the disk and monitor pipes, receives the query JSON from the monitor, asks for the memory limit, queries the disk block size, creates the buffer manager and temp storage manager, and then calls `executor::execute_query`.

`execute_query` in `database/src/executor.rs` turns the logical tree into a physical operator tree and streams output rows back to the monitor in batches.

There is also a small optimizer in the executor:

- It tries to flatten select-project-join shapes.
- It estimates selectivity from the stats JSON.
- It chooses a left-deep join order when it can.
- It pushes required columns and residual predicates downward.
- It uses ordered-scan bounds when a table column is physically ordered and the predicates can be turned into ranges.

### Scan path

The scan operator is in `database/src/disk.rs`.

- It prefetches 512 blocks at a time.
- It can prune columns before decode.
- It can apply scan predicates during row decoding.
- It can restrict the read range when statistics say a column is physically ordered.

That means a scan can avoid reading and decoding data that the query does not actually need.

### Filter

`database/src/filter.rs` resolves predicates once into schema indices and then evaluates them row by row.

- Column references are converted to integer indices up front.
- Type-aware comparisons are used for numeric and string values.
- Join residual predicates can be evaluated against two input rows with `eval_resolved_two_parts`.

### Project

`database/src/project.rs` performs projection by storing the input indices once and then cloning only the requested columns into a new row.

### Join

The join implementation is in `database/src/join.rs`.

The important part is that the code does not use one single join strategy for everything:

- If the predicate contains one or more equi-join conditions, it uses a hash join.
- If there are no equi-join predicates, it falls back to block nested-loop join.
- For `Cross`, the executor calls the same join builder with no predicates, so a cross join is handled by the block nested-loop path.

The hash join itself is a hybrid implementation:

- It first tries a bounded in-memory hash join when the estimated build side is small enough.
- If that overflows, it partitions both inputs into temp runs using a Grace-hash-style scheme.
- A Bloom filter is used during partitioning to drop rows that definitely cannot match.
- If a partition is still too large, the code can repartition it up to a fixed depth.
- If repartitioning still does not make the partition small enough, it degrades to nested-loop processing for that partition.

So the join you are using is not a plain hash join. It is a hybrid hash join with Grace-style partitioning, Bloom filtering, repartitioning, and a final nested-loop fallback for pathological partitions.

The non-equi fallback is block nested-loop join:

- The inner relation is spilled to temp storage.
- The outer side is read in batches sized from the memory budget.
- Each outer batch is scanned against the inner temp file.

### Sort

`database/src/sort.rs` implements an external merge sort.

- Input rows are accumulated until the in-memory budget is reached.
- Each chunk is sorted with `sort_unstable_by` and spilled as a temp run.
- If everything fits in memory, the operator stays purely in-memory.
- If there are many runs, it first collapses groups of runs into intermediate temp runs.
- Final merging uses a binary heap priority queue over `TempRunReader`s.

So the sort is a multi-pass external merge sort, not an in-memory sort with an implicit spill.

### Buffer manager

`database/src/buffer_manager.rs` is a small FIFO page cache.

- It stores fixed-size blocks.
- On a hit it returns the cached block.
- On a miss it asks the disk simulator for the block.
- When full, it evicts the oldest cached block.

There is no LRU or write-back policy here. The engine relies on the disk simulator and temp-storage layer for the I/O model.

### Temporary storage

`database/src/temp_storage.rs` owns the anonymous temp region used by sort and join.

- Temp files are backed by extents allocated from the disk simulator’s anonymous region.
- Allocation uses a best-fit free-list strategy.
- Temp runs are written in batches of pages rather than one page at a time.
- Each page stores a row count in the last 2 bytes of the page.
- Rows are encoded with a compact custom binary format: field count, type tags, and per-field payloads.

This is the main mechanism that makes the out-of-core operators practical.

### Disk simulator

The disk process lives under `disk/src/disk_simulation`.

- It supports commands like `get block`, `put block`, `get file start-block`, `get file num-blocks`, `get block-size`, and `get anon-start-block`.
- It maps named files to contiguous block ranges.
- It reserves the anonymous writable region after the file region.
- It tracks reads, writes, cylinder movement, and estimated seek, rotational latency, and transfer time.

The default disk model uses 4096-byte blocks, 7200 RPM, 1024 blocks per track, 4 heads per cylinder, and 150 MB/s transfer rate.

### Monitor

`monitor/src/main.rs` is the orchestrator.

- It spawns the disk process first.
- It spawns the database process next.
- It remaps pipes into the file descriptors expected by the database.
- It sets `RLIMIT_AS`, `RLIMIT_STACK`, `RLIMIT_FSIZE`, and `RLIMIT_NPROC` before the database starts.
- It sends the query JSON to the database.
- It validates the output either by exact order or by sorting both sides first, depending on `sort_before_check`.

The monitor config also enforces that the expected output file exists and that the memory limit is at least 64 MB.

## Performance Notes

The engine is designed to reduce disk I/O in a few specific ways:

- scan prefetching to hide latency
- column pruning before decode
- ordered-scan range restriction when stats allow it
- Grace-style hash join partitioning with Bloom filtering
- batched temp writes and reads
- external merge sort with grouped merging

## Benchmarking Results

The table below summarizes the measured effect of the main optimization stages in terms of total I/O time.

| Optimization Stage | Total Reads | Total Writes | Cylinders Traveled | Total I/O Time (us) |
| :--- | ---: | ---: | ---: | ---: |
| Baseline | 24,107 | 19,178 | 21,411,854 | 100,997,961 |
| Batched Temp Storage | 9,867 | 4,938 | 6,218,286 | 35,326,175 |
| Scan Prefetch | 5,247 | 4,938 | 1,422,962 | 25,204,602 |
| Optimized Join | 2,677 | 2,368 | 1,855,391 | 15,335,025 |
| Full Optimizations | 932 | 330 | 237,680 | 3,423,203 |

> [!NOTE]
> Metrics were collected with the custom disk simulator.