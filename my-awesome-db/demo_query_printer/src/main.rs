use common::query::{
    ComparisionOperator, ComparisionValue, MultiProjectBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("lineitem")
        .sort("l_partkey", true)
        .build();

    let query_json = serde_json::to_string_pretty(&query).unwrap();
    println!("{}", query_json);
}