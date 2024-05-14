use ndarray::prelude::*;
use num::Complex;

use crate::{
    codec::Codec,
    codec_registry::CodecRegistry,
    data_type::CoreDataType,
    error::CharizarrError,
    index::ChunkProjection,
    metadata::{DataType, Extension},
};

#[derive(Debug, Clone, PartialEq)]
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

impl Chunk {
    pub fn zeros(dtype: &DataType, shape: &[usize]) -> Result<Self, CharizarrError> {
        let DataType::Core(dtype) = dtype else {
            return Err(CharizarrError::TypeError(dtype.to_string()));
        };

        let chunk = match dtype {
            CoreDataType::Bool => Chunk::Bool(ArrayD::<u8>::zeros(IxDyn(shape)).mapv(|_| false)),
            CoreDataType::Int8 => Chunk::Int8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int16 => Chunk::Int16(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int32 => Chunk::Int32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Int64 => Chunk::Int64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt8 => Chunk::UInt8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt16 => Chunk::UInt16(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt32 => Chunk::UInt32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::UInt64 => Chunk::UInt64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Float32 => Chunk::Float32(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Float64 => Chunk::Float64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Complex64 => Chunk::Complex64(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Complex128 => Chunk::Complex128(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Raw8 => Chunk::Raw8(ArrayD::zeros(IxDyn(shape))),
            CoreDataType::Raw16 => Chunk::Raw16(ArrayD::zeros(IxDyn(shape))),
        };

        Ok(chunk)
    }

    pub fn reshape(self, shape: &[usize]) -> Self {
        match self {
            Chunk::Bool(arr) => Chunk::Bool(arr.into_shape(shape).unwrap()),
            Chunk::Int8(arr) => Chunk::Int8(arr.into_shape(shape).unwrap()),
            Chunk::Int16(arr) => Chunk::Int16(arr.into_shape(shape).unwrap()),
            Chunk::Int32(arr) => Chunk::Int32(arr.into_shape(shape).unwrap()),
            Chunk::Int64(arr) => Chunk::Int64(arr.into_shape(shape).unwrap()),
            Chunk::UInt8(arr) => Chunk::UInt8(arr.into_shape(shape).unwrap()),
            Chunk::UInt16(arr) => Chunk::UInt16(arr.into_shape(shape).unwrap()),
            Chunk::UInt32(arr) => Chunk::UInt32(arr.into_shape(shape).unwrap()),
            Chunk::UInt64(arr) => Chunk::UInt64(arr.into_shape(shape).unwrap()),
            Chunk::Float32(arr) => Chunk::Float32(arr.into_shape(shape).unwrap()),
            Chunk::Float64(arr) => Chunk::Float64(arr.into_shape(shape).unwrap()),
            Chunk::Complex64(arr) => Chunk::Complex64(arr.into_shape(shape).unwrap()),
            Chunk::Complex128(arr) => Chunk::Complex128(arr.into_shape(shape).unwrap()),
            Chunk::Raw8(arr) => Chunk::Raw8(arr.into_shape(shape).unwrap()),
            Chunk::Raw16(arr) => Chunk::Raw16(arr.into_shape(shape).unwrap()),
        }
    }

    /// Set the value of a chunk at a given selection.
    /// TODO: MAKE THIS WAY CLEANER
    pub fn set(&mut self, sel: &ChunkProjection, value: &Self) -> Result<(), CharizarrError> {
        match self {
            Chunk::Bool(arr) => {
                let target_chunk: ArrayViewD<bool> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Int8(arr) => {
                let target_chunk: ArrayViewD<i8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Int16(arr) => {
                let target_chunk: ArrayViewD<i16> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Int32(arr) => {
                let target_chunk: ArrayViewD<i32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Int64(arr) => {
                let target_chunk: ArrayViewD<i64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::UInt8(arr) => {
                let target_chunk: ArrayViewD<u8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::UInt16(arr) => {
                let target_chunk: ArrayViewD<u16> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::UInt32(arr) => {
                let target_chunk: ArrayViewD<u32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::UInt64(arr) => {
                let target_chunk: ArrayViewD<u64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Float32(arr) => {
                let target_chunk: ArrayViewD<f32> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Float64(arr) => {
                let target_chunk: ArrayViewD<f64> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Complex64(arr) => {
                let target_chunk: ArrayViewD<Complex<f32>> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Complex128(arr) => {
                let target_chunk: ArrayViewD<Complex<f64>> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Raw8(arr) => {
                let target_chunk: ArrayViewD<u8> = value.try_into()?;
                let target = target_chunk.slice_each_axis(|a| sel.chunk_sel[a.axis.0]);
                let mut arr_view = arr.slice_each_axis_mut(|a| sel.out_sel[a.axis.0]);
                arr_view.assign(&target);
            }
            Chunk::Raw16(arr) => {
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
        impl TryInto<ArrayD<$d_type>> for Chunk {
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

into_array!(Chunk::Bool, bool);
into_array!(Chunk::Int8, i8);
into_array!(Chunk::Int16, i16);
into_array!(Chunk::Int32, i32);
into_array!(Chunk::Int64, i64);
into_array!(Chunk::UInt8, u8);
into_array!(Chunk::UInt16, u16);
into_array!(Chunk::UInt32, u32);
into_array!(Chunk::UInt64, u64);
into_array!(Chunk::Float32, f32);
into_array!(Chunk::Float64, f64);
into_array!(Chunk::Complex64, Complex<f32>);
into_array!(Chunk::Complex128, Complex<f64>);

macro_rules! into_array_view {
    ($d_name:path, $d_type:ty) => {
        impl<'a> TryInto<ArrayViewD<'a, $d_type>> for &'a Chunk {
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

into_array_view!(Chunk::Bool, bool);
into_array_view!(Chunk::Int8, i8);
into_array_view!(Chunk::Int16, i16);
into_array_view!(Chunk::Int32, i32);
into_array_view!(Chunk::Int64, i64);
into_array_view!(Chunk::UInt8, u8);
into_array_view!(Chunk::UInt16, u16);
into_array_view!(Chunk::UInt32, u32);
into_array_view!(Chunk::UInt64, u64);
into_array_view!(Chunk::Float32, f32);
into_array_view!(Chunk::Float64, f64);
into_array_view!(Chunk::Complex64, Complex<f32>);
into_array_view!(Chunk::Complex128, Complex<f64>);

macro_rules! into_array_view_mut {
    ($d_name:path, $d_type:ty) => {
        impl<'a> TryInto<ArrayViewMutD<'a, $d_type>> for &'a mut Chunk {
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

into_array_view_mut!(Chunk::Bool, bool);
into_array_view_mut!(Chunk::Int8, i8);
into_array_view_mut!(Chunk::Int16, i16);
into_array_view_mut!(Chunk::Int32, i32);
into_array_view_mut!(Chunk::Int64, i64);
into_array_view_mut!(Chunk::UInt8, u8);
into_array_view_mut!(Chunk::UInt16, u16);
into_array_view_mut!(Chunk::UInt32, u32);
into_array_view_mut!(Chunk::UInt64, u64);
into_array_view_mut!(Chunk::Float32, f32);
into_array_view_mut!(Chunk::Float64, f64);
into_array_view_mut!(Chunk::Complex64, Complex<f32>);
into_array_view_mut!(Chunk::Complex128, Complex<f64>);

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

pub fn decode_chunk(
    codec_registry: &CodecRegistry,
    codecs: &[Extension],
    data_type: &DataType,
    bytes: Vec<u8>,
) -> Result<Chunk, CharizarrError> {
    let mut btb_codecs = vec![];
    let mut bta_codecs = vec![];
    let mut ata_codecs = vec![];

    codecs.iter().rev().for_each(|codec| {
        let config = codec.configuration.clone();
        let Some(codec) = codec_registry.get(&codec.name) else {
            return;
        };
        match codec {
            Codec::ByteToByte(codec) => btb_codecs.push((codec, config)),
            Codec::ByteToArray(codec) => bta_codecs.push((codec, config)),
            Codec::ArrayToArray(codec) => ata_codecs.push((codec, config)),
        }
    });

    // byte to byte
    let bytes = btb_codecs.iter().try_fold(bytes, |bytes, codec| {
        let (codec, config) = codec;
        codec.decode(data_type, config, &bytes)
    })?;

    // byte to array
    let Some((bta_codec, bta_config)) = bta_codecs.first() else {
        return Err(CharizarrError::CodecError(
            "No ByteToArray codec found".to_string(),
        ));
    };
    let arr = bta_codec.decode(data_type, bta_config, &bytes)?;

    // array to array
    let arr = ata_codecs.iter().fold(arr, |arr, (codec, config)| {
        codec.decode(data_type, config, &arr).unwrap()
    });

    Ok(arr)
}

pub fn encode_chunk(
    codec_registry: &CodecRegistry,
    codecs: &[Extension],
    data_type: &DataType,
    arr: &Chunk,
) -> Result<Vec<u8>, CharizarrError> {
    let mut ata_codecs = vec![];
    let mut bta_codecs = vec![];
    let mut btb_codecs = vec![];

    codecs.iter().for_each(|codec| {
        let config = codec.configuration.clone();
        let Some(codec) = codec_registry.get(&codec.name) else {
            println!("Codec not found: {}", codec.name);
            return;
        };
        match codec {
            Codec::ByteToByte(codec) => btb_codecs.push((codec, config)),
            Codec::ByteToArray(codec) => bta_codecs.push((codec, config)),
            Codec::ArrayToArray(codec) => ata_codecs.push((codec, config)),
        }
    });

    // array to array
    let new_arr = ata_codecs
        .iter()
        .try_fold(arr.clone(), |arr, (codec, config)| {
            codec.encode(data_type, config, &arr)
        })?;

    // array to byte
    let (bta_codec, bta_config) = bta_codecs.first().unwrap();
    let bytes = bta_codec.encode(data_type, bta_config, &new_arr)?;

    // byte to byte
    let bytes = btb_codecs
        .iter()
        .try_fold(bytes, |bytes, (codec, config)| {
            codec.encode(data_type, config, &bytes)
        })?;

    Ok(bytes)
}
