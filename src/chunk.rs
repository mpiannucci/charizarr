use ndarray::prelude::*;
use num::Complex;

use crate::{data_type::CoreDataType, metadata::DataType};

pub enum Chunk {
    Bool(ArrayD<bool>),
    Int8(ArrayD<i8>),
    Int16(ArrayD<i16>),
    Int32(ArrayD<i32>),
    Int64(ArrayD<i64>),
    UInt8(ArrayD<u8>),
    UInt16(ArrayD<u16>),
    UInt32(ArrayD<u32>),
    UInt64(ArrayD<u64>),
    Float32(ArrayD<f32>),
    Float64(ArrayD<f64>),
    Complex64(ArrayD<Complex<f32>>),
    Complex128(ArrayD<Complex<f64>>),
    Raw8(ArrayD<u8>),
    Raw16(ArrayD<u16>),
}

macro_rules! into_chunk {
    ($d_name:expr, $d_type:ty) => {
        impl From<Vec<$d_type>> for Chunk {
            fn from(value: Vec<$d_type>) -> Self {
                let arr = Array::from_vec(value);
                $d_name(arr.into_dyn())
            }
        }
    };
}

into_chunk!(Chunk::Bool, bool);
into_chunk!(Chunk::Int8, i8);
into_chunk!(Chunk::Int16, i16);
into_chunk!(Chunk::Int32, i32);
into_chunk!(Chunk::Int64, i64);
into_chunk!(Chunk::UInt8, u8);
into_chunk!(Chunk::UInt16, u16);
into_chunk!(Chunk::UInt32, u32);
into_chunk!(Chunk::UInt64, u64);
into_chunk!(Chunk::Float32, f32);
into_chunk!(Chunk::Float64, f64);
into_chunk!(Chunk::Complex64, Complex<f32>);
into_chunk!(Chunk::Complex128, Complex<f64>);
