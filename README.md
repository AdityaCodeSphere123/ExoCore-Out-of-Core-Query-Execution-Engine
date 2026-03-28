# Out-of-Core Query Execution

Link: https://messy-circle-aa4.notion.site/COL362-632-Assignment-3-Out-of-Core-Query-Execution-3236f1adfa0a80b9a9e4ca3492cb5efb

This assignment is for COL362.

Authors: Aditya Anand and Ahilaan Saxena (IIT Delhi)

First build your query in demo_query_printer/src/main.rs then run:
cargo run -r --bin demo_query_printer

Copy the printed query in the terminal to the "query" field of scratch/runtimes/tpch/monitor_config.json and set disabled to false and change expected output file accordingly

Write your query in my_query.sql and run:
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < my_query.sql > scratch/runtimes/tpch/expected_1.csv