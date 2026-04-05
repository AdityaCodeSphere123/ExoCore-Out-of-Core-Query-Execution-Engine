# Out-of-Core Query Execution

Link: https://messy-circle-aa4.notion.site/COL362-632-Assignment-3-Out-of-Core-Query-Execution-3236f1adfa0a80b9a9e4ca3492cb5efb

This assignment is for COL362.

Authors: Aditya Anand and Ahilaan Saxena (IIT Delhi)

First build your query in demo_query_printer/src/main.rs then run:
cargo run -r --bin demo_query_printer

Copy the printed query in the terminal to the "query" field of scratch/runtimes/tpch/monitor_config.json and set disabled to false and change expected output file accordingly

Write your query in my_query.sql and run:
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < my_query.sql > scratch/runtimes/tpch/expected_1.csv

SELECT *,''
FROM customer
CROSS JOIN orders
WHERE c_custkey = o_custkey ORDER BY c_custkey;

Disk IO metrics DiskIOMetricsResult {
    total_reads: 24107,
    total_writes: 19178,
    total_blocks_processed: 43285,
    total_cylinders_traveled: 21411854,
    total_io_time_us: 100997961,
    total_seek_time_us: 20457638,
    total_rotational_latency_us: 79358340,
    total_transfer_time_us: 1181983,
}

# batched temp storage 
Disk IO metrics DiskIOMetricsResult {
    total_reads: 9867,
    total_writes: 4938,
    total_blocks_processed: 43285,
    total_cylinders_traveled: 6218286,
    total_io_time_us: 35326175,
    total_seek_time_us: 7294199,
    total_rotational_latency_us: 26850002,
    total_transfer_time_us: 1181974,
}

# scan prefetch
Disk IO metrics DiskIOMetricsResult {
    total_reads: 5247,
    total_writes: 4938,
    total_blocks_processed: 43285,
    total_cylinders_traveled: 1422962,
    total_io_time_us: 25204602,
    total_seek_time_us: 3872628,
    total_rotational_latency_us: 20150002,
    total_transfer_time_us: 1181972,
} 

# claude optimizations
Disk IO metrics DiskIOMetricsResult {
    total_reads: 2854,
    total_writes: 2545,
    total_blocks_processed: 43285,
    total_cylinders_traveled: 1420404,
    total_io_time_us: 15979846,
    total_seek_time_us: 2368707,
    total_rotational_latency_us: 12429168,
    total_transfer_time_us: 1181971,
}

# optimized join
Disk IO metrics DiskIOMetricsResult {
    total_reads: 2677,
    total_writes: 2368,
    total_blocks_processed: 42855,
    total_cylinders_traveled: 1855391,
    total_io_time_us: 15335025,
    total_seek_time_us: 2018962,
    total_rotational_latency_us: 12145834,
    total_transfer_time_us: 1170229,
} 