use serde::{Deserialize, Serialize};

pub mod query;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Data {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int32(l0), Self::Int32(r0)) => l0.partial_cmp(r0),
            (Self::Int64(l0), Self::Int64(r0)) => l0.partial_cmp(r0),
            (Self::Float32(l0), Self::Float32(r0)) => l0.partial_cmp(r0),
            (Self::Float64(l0), Self::Float64(r0)) => l0.partial_cmp(r0),
            (Self::String(l0), Self::String(r0)) => l0.partial_cmp(r0),

            // Integer cross-comparison
            (Self::Int32(l), Self::Int64(r)) => (*l as i64).partial_cmp(r),
            (Self::Int64(l), Self::Int32(r)) => l.partial_cmp(&(*r as i64)),

            // Integer to Float comparison (upcasting to f64 for maximum precision)
            (Self::Int32(l), Self::Float32(r)) => (*l as f32).partial_cmp(r), // f32 is enough for i32? No, but i32 to f32 can lose precision.
            (Self::Float32(l), Self::Int32(r)) => l.partial_cmp(&(*r as f32)),

            (Self::Int32(l), Self::Float64(r)) => (*l as f64).partial_cmp(r),
            (Self::Float64(l), Self::Int32(r)) => l.partial_cmp(&(*r as f64)),

            (Self::Int64(l), Self::Float32(r)) => (*l as f64).partial_cmp(&(*r as f64)),
            (Self::Float32(l), Self::Int64(r)) => (*l as f64).partial_cmp(&(*r as f64)),

            (Self::Int64(l), Self::Float64(r)) => (*l as f64).partial_cmp(r),
            (Self::Float64(l), Self::Int64(r)) => l.partial_cmp(&(*r as f64)),

            // Float cross-comparison
            (Self::Float32(l), Self::Float64(r)) => (*l as f64).partial_cmp(r),
            (Self::Float64(l), Self::Float32(r)) => l.partial_cmp(&(*r as f64)),

            _ => None,
        }
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}