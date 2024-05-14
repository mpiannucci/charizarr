use crate::{codec::Codec, codec_registry::CodecRegistry, error::CharizarrError, metadata::{DataType, Extension}, zarray::ZArray};



pub fn decode_chunk(
    codec_registry: &CodecRegistry,
    codecs: &[Extension],
    data_type: &DataType,
    bytes: Vec<u8>,
) -> Result<ZArray, CharizarrError> {
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
    arr: &ZArray,
) -> Result<Vec<u8>, CharizarrError> {
    let mut ata_codecs = vec![];
    let mut bta_codecs = vec![];
    let mut btb_codecs = vec![];

    codecs.iter().for_each(|codec| {
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
