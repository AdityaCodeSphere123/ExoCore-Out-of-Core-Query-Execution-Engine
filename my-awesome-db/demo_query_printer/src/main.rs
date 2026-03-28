use common::query::{
    ComparisionOperator, ComparisionValue, MultiProjectBuilder, MultiSortBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("nation")
        .build();

    let query_json = serde_json::to_string_pretty(&query).unwrap();

    println!("{}", query_json);
}
