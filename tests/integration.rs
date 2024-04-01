use std::{collections::HashMap, sync::Arc};

use charizarr::{
    array::ArrayMetadata,
    chunk::Chunk,
    codec::Codec,
    codecs::{blosc::BloscCodec, gzip::GZipCodec},
    metadata::{DataType, Extension, NodeType, ZarrFormat},
};
use ndarray::{Array, ArrayD, IxDyn};

#[tokio::test]
async fn test_roundtrip() {
    // Create the codec registry
    let codecs = Some(
        charizarr::codec_registry::CodecRegistry::default()
            .register(Codec::ByteToByte(Arc::new(GZipCodec::new())))
            .register(Codec::ByteToByte(Arc::new(BloscCodec::new()))),
    );

    // Create the store
    let path = std::path::PathBuf::from("tests/roundtrip.zarr");
    let store = charizarr::stores::FileSystemStore::create(path.clone())
        .await
        .unwrap();
    let group = charizarr::group::Group::create(&store, None, None)
        .await
        .unwrap();
    assert_eq!(group.name(), "roundtrip.zarr");

    // Create an array
    let metadata = ArrayMetadata {
        zarr_format: ZarrFormat::V3,
        node_type: NodeType::Group,
        shape: vec![3, 2],
        data_type: DataType::Core(charizarr::data_type::CoreDataType::UInt8),
        chunk_grid: Extension {
            name: "regular".to_string(),
            configuration: serde_json::json!({ "chunk_shape": [3, 2] }),
        },
        chunk_key_encoding: Extension {
            name: "default".to_string(),
            configuration: serde_json::json!({ "separator": "/" }),
        },
        fill_value: serde_json::json!(0),
        codecs: vec![Extension {
            name: "bytes".to_string(),
            configuration: serde_json::json!({"endian": "little"}),
        }],
        attributes: Some(HashMap::new()),
        storage_transformers: None,
        dimension_names: Some(vec!["y".into(), "x".into()]),
    };

    let array =
        charizarr::array::Array::create(&store, Some("rect".into()), metadata, codecs.clone())
            .await
            .unwrap();

    let set_raw_data = vec![3u8, 2, 4, 5, 6, 7];
    let set_array_data = ArrayD::from_shape_vec(IxDyn(&[3, 2]), set_raw_data).unwrap();
    let chunk = Chunk::UInt8(set_array_data.clone());
    let write_chunk = array.set_chunk("0/0", chunk).await;
    assert!(write_chunk.is_ok());

    // Open the store
    let store2 = charizarr::stores::FileSystemStore::open(path).await;

    assert!(store2.is_ok());
    let store2 = store2.unwrap();

    // Open the group
    let group2 = charizarr::group::Group::open(&store2, None).await;
    assert!(group2.is_ok());
    let group2 = group2.unwrap();

    // Read the array
    let array2 = group2.get_array("rect", codecs).await;
    assert!(array2.is_ok());
    let array2 = array2.unwrap();
    let chunk = array2.get_chunk("0/0").await;
    assert!(chunk.is_ok());
    let chunk = chunk.unwrap();
    let array_chunk: ArrayD<u8> = chunk.try_into().unwrap();
    assert_eq!(array_chunk, set_array_data);

    // Cleanup
    std::fs::remove_dir_all("tests/roundtrip.zarr").unwrap();
}

#[tokio::test]
async fn test_read() {
    // Create the codec registry
    let codecs = Some(
        charizarr::codec_registry::CodecRegistry::default()
            .register(Codec::ByteToByte(Arc::new(GZipCodec::new())))
            .register(Codec::ByteToByte(Arc::new(BloscCodec::new()))),
    );

    // Open the store
    let path = std::path::PathBuf::from("tests/data.zarr");
    let store = charizarr::stores::FileSystemStore::open(path)
        .await
        .unwrap();

    // Read in a group
    let group = charizarr::group::Group::open(&store, None).await.unwrap();

    assert_eq!(&group.metadata.zarr_format, &ZarrFormat::V3);
    assert_eq!(&group.name(), &"data.zarr");

    // Read in an array
    let array = charizarr::array::Array::open(&store, Some("3d.contiguous.i2".to_string()), None)
        .await
        .unwrap();

    assert_eq!(&array.metadata.zarr_format, &ZarrFormat::V3);
    let data_type = array.metadata.data_type.clone();
    assert_eq!(
        data_type,
        DataType::Core(charizarr::data_type::CoreDataType::Int16)
    );

    // We can also get arrays from the group
    let array = group
        .get_array("1d.contiguous.raw.i2", codecs.clone())
        .await
        .unwrap();
    assert_eq!(&array.metadata.zarr_format, &ZarrFormat::V3);
    assert_eq!(&array.metadata.codecs[0].name, &"bytes");

    // Lets read a chunk manually for now
    let array_chunk: ArrayD<i16> = array.get_chunk("c/0").await.unwrap().try_into().unwrap();
    let expected = Array::from_vec(vec![1i16, 2, 3, 4]).into_dyn();
    assert_eq!(array_chunk, expected);

    // We can also read compressed chunks
    // First lets try gzip
    let array = group
        .get_array("1d.contiguous.gzip.i2", codecs.clone())
        .await
        .unwrap();
    assert_eq!(&array.metadata.codecs.len(), &2);
    assert_eq!(array.shape(), vec![4]);
    assert_eq!(array.chunk_shape(), vec![4]);

    let array_chunk: ArrayD<i16> = array.get_chunk("c/0").await.unwrap().try_into().unwrap();
    assert_eq!(array_chunk, expected);

    // Then we'll try blosc
    let array = group
        .get_array("1d.contiguous.blosc.i2", codecs)
        .await
        .unwrap();
    assert_eq!(&array.metadata.codecs.len(), &2);

    let array_chunk: ArrayD<i16> = array.get_chunk("c/0").await.unwrap().try_into().unwrap();
    assert_eq!(array_chunk, expected);
}
