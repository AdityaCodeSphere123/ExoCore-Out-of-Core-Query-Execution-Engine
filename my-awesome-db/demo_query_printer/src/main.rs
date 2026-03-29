use common::query::{
    ComparisionOperator, ComparisionValue, MultiProjectBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("customer")
        .cross(QueryOp::scan("orders"))
        .filter(
            "c_custkey",
            ComparisionOperator::EQ,
            ComparisionValue::Column("o_custkey".to_string()),
        )
        .sort("c_custkey", true)
        .build();

    let query_json = serde_json::to_string_pretty(&query).unwrap();
    println!("{}", query_json);
}