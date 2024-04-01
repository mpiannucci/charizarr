use ndarray::prelude::*;
use num::Complex;

use crate::{
    codec::Codec,
    codec_registry::CodecRegistry,
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
) -> Result<Chunk, String> {
    let mut btb_codecs = vec![];
    let mut bta_codecs = vec![];
    let mut ata_codecs = vec![];

    codecs.iter().rev().for_each(|codec| {
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

    // byte to byte
    let bytes = btb_codecs.iter().fold(bytes, |bytes, codec| {
        let (codec, config) = codec;
        println!("Decoding with codec: {}", codec.resolve_name());
        codec.decode(data_type, config, &bytes).unwrap()
    });

    // byte to array
    let (bta_codec, bta_config) = bta_codecs.first().unwrap();
    let arr = bta_codec.decode(data_type, bta_config, &bytes).unwrap();

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
    arr: Chunk,
) -> Result<Vec<u8>, String> {
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
    let arr = ata_codecs.iter().fold(arr, |arr, (codec, config)| {
        codec.encode(data_type, config, &arr).unwrap()
    });

    // array to byte
    let (bta_codec, bta_config) = bta_codecs.first().unwrap();
    let bytes = bta_codec.encode(data_type, bta_config, &arr).unwrap();

    // byte to byte
    let bytes = btb_codecs.iter().fold(bytes, |bytes, (codec, config)| {
        codec.encode(data_type, config, &bytes).unwrap()
    });

    Ok(bytes)
}
