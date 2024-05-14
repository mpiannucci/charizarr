use ndarray::prelude::*;
use num::Complex;

use crate::{
    data_type::CoreDataType, error::CharizarrError, index::ChunkProjection, metadata::DataType,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ZArray {
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

impl ZArray {
    pub fn zeros(dtype: &DataType, shape: &[usize]) -> Result<Self, CharizarrError> {
        let DataType::Core(dtype) = dtype else {
            return Err(CharizarrError::TypeError(dtype.to_string()));
        };

        let zarray = match dtype {
            CoreDataType::Bool => ZArray::Bool(ArrayD::<u8>::zeros(IxDyn(shape)).mapv(|_| false)),
            CoreDataType::Int8 => ZArray::Int8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int16 => ZArray::Int16(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int32 => ZArray::Int32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int64 => ZArray::Int64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt8 => ZArray::UInt8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt16 => ZArray::UInt16(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt32 => ZArray::UInt32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt64 => ZArray::UInt64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Float32 => ZArray::Float32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Float64 => ZArray::Float64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Complex64 => ZArray::Complex64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Complex128 => ZArray::Complex128(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Raw8 => ZArray::Raw8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Raw16 => ZArray::Raw16(ArrayD::zeros(IxDyn(shape))),
        };

        Ok(zarray)
    }

    pub fn reshape(self, shape: &[usize]) -> Self {
        match self {
            ZArray::Bool(arr) => ZArray::Bool(arr.into_shape(shape).unwrap()),
            ZArray::Int8(arr) => ZArray::Int8(arr.into_shape(shape).unwrap()),
            ZArray::Int16(arr) => ZArray::Int16(arr.into_shape(shape).unwrap()),
            ZArray::Int32(arr) => ZArray::Int32(arr.into_shape(shape).unwrap()),
            ZArray::Int64(arr) => ZArray::Int64(arr.into_shape(shape).unwrap()),
            ZArray::UInt8(arr) => ZArray::UInt8(arr.into_shape(shape).unwrap()),
            ZArray::UInt16(arr) => ZArray::UInt16(arr.into_shape(shape).unwrap()),
            ZArray::UInt32(arr) => ZArray::UInt32(arr.into_shape(shape).unwrap()),
            ZArray::UInt64(arr) => ZArray::UInt64(arr.into_shape(shape).unwrap()),
            ZArray::Float32(arr) => ZArray::Float32(arr.into_shape(shape).unwrap()),
            ZArray::Float64(arr) => ZArray::Float64(arr.into_shape(shape).unwrap()),
            ZArray::Complex64(arr) => ZArray::Complex64(arr.into_shape(shape).unwrap()),
            ZArray::Complex128(arr) => ZArray::Complex128(arr.into_shape(shape).unwrap()),
            ZArray::Raw8(arr) => ZArray::Raw8(arr.into_shape(shape).unwrap()),
            ZArray::Raw16(arr) => ZArray::Raw16(arr.into_shape(shape).unwrap()),
        }
    }

    /// Set the value of a chunk at a given selection.
    /// TODO: MAKE THIS WAY CLEANER
    pub fn set(&mut self, sel: &ChunkProjection, value: &Self) -> Result<(), CharizarrError> {
        match self {
            ZArray::Bool(arr) => {
                let target_chunk: ArrayViewD<bool> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Int8(arr) => {
                let target_chunk: ArrayViewD<i8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Int16(arr) => {
                let target_chunk: ArrayViewD<i16> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Int32(arr) => {
                let target_chunk: ArrayViewD<i32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Int64(arr) => {
                let target_chunk: ArrayViewD<i64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::UInt8(arr) => {
                let target_chunk: ArrayViewD<u8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::UInt16(arr) => {
                let target_chunk: ArrayViewD<u16> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::UInt32(arr) => {
                let target_chunk: ArrayViewD<u32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::UInt64(arr) => {
                let target_chunk: ArrayViewD<u64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Float32(arr) => {
                let target_chunk: ArrayViewD<f32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Float64(arr) => {
                let target_chunk: ArrayViewD<f64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Complex64(arr) => {
                let target_chunk: ArrayViewD<Complex<f32>> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Complex128(arr) => {
                let target_chunk: ArrayViewD<Complex<f64>> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Raw8(arr) => {
                let target_chunk: ArrayViewD<u8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            ZArray::Raw16(arr) => {
                let target_chunk: ArrayViewD<u16> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
        }

        Ok(())
    }
}

macro_rules! into_array {
    ($d_name:path, $d_type:ty) => {
        impl TryInto<ArrayD<$d_type>> for ZArray {
            type Error = CharizarrError;

            fn try_into(self) -> Result<ArrayD<$d_type>, Self::Error> {
                if let $d_name(arr) = self {
                    Ok(arr)
                } else {
                    Err(CharizarrError::TypeError(stringify!($d_type).to_string()))
                }
            }
        }
    };
}

into_array!(ZArray::Bool, bool);
into_array!(ZArray::Int8, i8);
into_array!(ZArray::Int16, i16);
into_array!(ZArray::Int32, i32);
into_array!(ZArray::Int64, i64);
into_array!(ZArray::UInt8, u8);
into_array!(ZArray::UInt16, u16);
into_array!(ZArray::UInt32, u32);
into_array!(ZArray::UInt64, u64);
into_array!(ZArray::Float32, f32);
into_array!(ZArray::Float64, f64);
into_array!(ZArray::Complex64, Complex<f32>);
into_array!(ZArray::Complex128, Complex<f64>);

macro_rules! into_array_view {
    ($d_name:path, $d_type:ty) => {
        impl<'a> TryInto<ArrayViewD<'a, $d_type>> for &'a ZArray {
            type Error = CharizarrError;

            fn try_into(self) -> Result<ArrayViewD<'a, $d_type>, Self::Error> {
                if let $d_name(arr) = self {
                    Ok(arr.view())
                } else {
                    Err(CharizarrError::TypeError(stringify!($d_type).to_string()))
                }
            }
        }
    };
}

into_array_view!(ZArray::Bool, bool);
into_array_view!(ZArray::Int8, i8);
into_array_view!(ZArray::Int16, i16);
into_array_view!(ZArray::Int32, i32);
into_array_view!(ZArray::Int64, i64);
into_array_view!(ZArray::UInt8, u8);
into_array_view!(ZArray::UInt16, u16);
into_array_view!(ZArray::UInt32, u32);
into_array_view!(ZArray::UInt64, u64);
into_array_view!(ZArray::Float32, f32);
into_array_view!(ZArray::Float64, f64);
into_array_view!(ZArray::Complex64, Complex<f32>);
into_array_view!(ZArray::Complex128, Complex<f64>);

macro_rules! into_array_view_mut {
    ($d_name:path, $d_type:ty) => {
        impl<'a> TryInto<ArrayViewMutD<'a, $d_type>> for &'a mut ZArray {
            type Error = CharizarrError;

            fn try_into(self) -> Result<ArrayViewMutD<'a, $d_type>, Self::Error> {
                if let $d_name(arr) = self {
                    Ok(arr.view_mut())
                } else {
                    Err(CharizarrError::TypeError(stringify!($d_type).to_string()))
                }
            }
        }
    };
}

into_array_view_mut!(ZArray::Bool, bool);
into_array_view_mut!(ZArray::Int8, i8);
into_array_view_mut!(ZArray::Int16, i16);
into_array_view_mut!(ZArray::Int32, i32);
into_array_view_mut!(ZArray::Int64, i64);
into_array_view_mut!(ZArray::UInt8, u8);
into_array_view_mut!(ZArray::UInt16, u16);
into_array_view_mut!(ZArray::UInt32, u32);
into_array_view_mut!(ZArray::UInt64, u64);
into_array_view_mut!(ZArray::Float32, f32);
into_array_view_mut!(ZArray::Float64, f64);
into_array_view_mut!(ZArray::Complex64, Complex<f32>);
into_array_view_mut!(ZArray::Complex128, Complex<f64>);

macro_rules! into_chunk {
    ($d_name:expr, $d_type:ty) => {
        impl From<Vec<$d_type>> for ZArray {
            fn from(value: Vec<$d_type>) -> Self {
                let arr = Array::from_vec(value);
                $d_name(arr.into_dyn())
            }
        }
    };
}

into_chunk!(ZArray::Bool, bool);
into_chunk!(ZArray::Int8, i8);
into_chunk!(ZArray::Int16, i16);
into_chunk!(ZArray::Int32, i32);
into_chunk!(ZArray::Int64, i64);
into_chunk!(ZArray::UInt8, u8);
into_chunk!(ZArray::UInt16, u16);
into_chunk!(ZArray::UInt32, u32);
into_chunk!(ZArray::UInt64, u64);
into_chunk!(ZArray::Float32, f32);
into_chunk!(ZArray::Float64, f64);
into_chunk!(ZArray::Complex64, Complex<f32>);
into_chunk!(ZArray::Complex128, Complex<f64>);
