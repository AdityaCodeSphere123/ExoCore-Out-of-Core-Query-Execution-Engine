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

impl Data {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int32(v) => Some(*v as f64),
            Self::Int64(v) => Some(*v as f64),
            Self::Float32(v) => Some(*v as f64),
            Self::Float64(v) => Some(*v),
            Self::String(_) => None,
        }
    }
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::String(l), Self::String(r)) => l.partial_cmp(r),
            (l, r) => {
                if let (Some(lf), Some(rf)) = (l.as_f64(), r.as_f64()) {
                    lf.partial_cmp(&rf)
                } else {
                    None
                }
            }
        }
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(l), Self::String(r)) => l == r,
            (l, r) => {
                if let (Some(lf), Some(rf)) = (l.as_f64(), r.as_f64()) {
                    lf == rf
                } else {
                    false
                }
            }
        }
    }
}
