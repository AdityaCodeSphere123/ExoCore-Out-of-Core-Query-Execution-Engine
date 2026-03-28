use common::query::{
    ComparisionOperator, ComparisionValue, MultiProjectBuilder, QueryOp,
};

fn main() {
    let query = QueryOp::scan("nation")
        .filter("n_regionkey", ComparisionOperator::GTE, ComparisionValue::I32(3))
        .filter("n_nationkey", ComparisionOperator::LT, ComparisionValue::I32(23))
        .filter(
            "n_name",
            ComparisionOperator::NE,
            ComparisionValue::String(String::from("IRAQ")),
        )
        .project_multiple(
            MultiProjectBuilder::new("n_name", "country")
                .add("n_regionkey", "region"),
        )
        .build();

    let query_json = serde_json::to_string_pretty(&query).unwrap();
    println!("{}", query_json);
}