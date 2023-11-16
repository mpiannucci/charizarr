use ndarray::prelude::*;
use num::Complex;

use crate::{metadata::DataType, data_type::CoreDataType};

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