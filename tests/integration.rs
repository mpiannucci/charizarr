use std::{collections::HashMap, sync::Arc};

use charizarr::{
    codec::Codec,
    codecs::{blosc::BloscCodec, gzip::GZipCodec},
    metadata::{DataType, Extension, ZarrFormat},
    zarray::ZArray,
};
use ndarray::{Array, ArrayD, IxDyn};
use object_store::{local::LocalFileSystem, path::Path};
use serde_json::Value;

#[tokio::test]
async fn test_roundtrip() {
    // Create the codec registry
    let codecs = Some(
        charizarr::codec_registry::CodecRegistry::default()
            .register(Codec::ByteToByte(Arc::new(GZipCodec::new())))
            .register(Codec::ByteToByte(Arc::new(BloscCodec::new()))),
    );

    // Create the store
    let local_store = Box::new(LocalFileSystem::new());
    let path = Path::from_absolute_path(std::env::current_dir().unwrap())
        .expect("Failed to create store in current directory")
        .child("tests")
        .child("roundtrip.zarr");
    let store = charizarr::stores::ZarrObjectStore::create(local_store, path.clone());

    let group = charizarr::group::Group::create(
        &store,
        None,
        Some(HashMap::from([(
            "name".to_string(),
            Value::String("roundtrip".to_string()),
        )])),
    )
    .await
    .unwrap();
    assert_eq!(group.name(), "roundtrip");

    // Create an array
    let array = charizarr::array::Array::create(
        &store,
        Some("rect".into()),
        codecs.clone(),
        vec![3, 2],
        vec![3, 2],
        None,
        DataType::Core(charizarr::data_type::CoreDataType::UInt8),
        serde_json::json!(0),
        vec![Extension {
            name: "bytes".to_string(),
            configuration: serde_json::json!({"endian": "little"}),
        }],
        Some(vec!["y".into(), "x".into()]),
        Some(HashMap::new()),
    )
    .await
    .unwrap();

    let set_raw_data = vec![3u8, 2, 4, 5, 6, 7];
    let set_array_data = ArrayD::from_shape_vec(IxDyn(&[3, 2]), set_raw_data).unwrap();
    let chunk = ZArray::UInt8(set_array_data.clone());
    let write_chunk = array.set_chunk(&[0, 0], &chunk).await;
    assert!(write_chunk.is_ok());

    // Open the store
    let local_store2 = Box::new(LocalFileSystem::new());
    let store2 = charizarr::stores::ZarrObjectStore::create(local_store2, path);

    // Open the group
    let group2 = charizarr::group::Group::open(&store2, None).await;
    assert!(group2.is_ok());
    let group2 = group2.unwrap();

    // Read the array
    let array2 = group2.get_array("rect", codecs).await;
    assert!(array2.is_ok());
    let array2 = array2.unwrap();

    let array_data: ArrayD<u8> = array2.get(None).await.unwrap().try_into().unwrap();
    assert_eq!(array_data, set_array_data);

    // Set some custom part of the array
    let new_values = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![25, 26, 27, 28]).unwrap();
    let new_value = ZArray::UInt8(new_values);
    let sel = vec![0usize..2, 0..2];
    let set_opt = array2.set(Some(sel), &new_value).await;
    assert!(set_opt.is_ok());

    // Read the array
    let first_col: ArrayD<u8> = array2
        .get(Some(vec![0usize..3, 0..1]))
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let truth = ArrayD::from_shape_vec(IxDyn(&[3, 1]), vec![25u8, 27, 6]).unwrap();
    assert_eq!(first_col, truth);

    let second_col: ArrayD<u8> = array2
        .get(Some(vec![0usize..3, 1..2]))
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let truth = ArrayD::from_shape_vec(IxDyn(&[3, 1]), vec![26u8, 28, 7]).unwrap();
    assert_eq!(second_col, truth);

    // Lets write again to the entire array
    let new_set_raw_data = vec![10u8, 11, 12, 13, 14, 15];
    let new_set_array_data = ArrayD::from_shape_vec(IxDyn(&[3, 2]), new_set_raw_data).unwrap();
    let new_array_values = ZArray::UInt8(new_set_array_data.clone());
    let result = array2.set(None, &new_array_values).await;
    assert!(result.is_ok());

    // Read the array
    let updated_array_data: ArrayD<u8> = array2.get(None).await.unwrap().try_into().unwrap();
    assert_eq!(updated_array_data, new_set_array_data);

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
    let local_store = Box::new(LocalFileSystem::new());
    let path = Path::from_absolute_path(std::env::current_dir().unwrap())
        .expect("Failed to create store in current directory")
        .child("tests")
        .child("data.zarr");
    let store = charizarr::stores::ZarrObjectStore::create(local_store, path);

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
    let array_chunk: ArrayD<i16> = array.get_chunk(&[0]).await.unwrap().try_into().unwrap();
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

    let array_chunk: ArrayD<i16> = array.get_chunk(&[0]).await.unwrap().try_into().unwrap();
    assert_eq!(array_chunk, expected);

    // Then we'll try blosc
    let array = group
        .get_array("1d.contiguous.blosc.i2", codecs.clone())
        .await
        .unwrap();
    assert_eq!(&array.metadata.codecs.len(), &2);

    let array_chunk: ArrayD<i16> = array.get_chunk(&[0]).await.unwrap().try_into().unwrap();
    assert_eq!(array_chunk, expected);

    // 3d
    let array = group
        .get_array("3d.contiguous.i2", codecs.clone())
        .await
        .unwrap();
    assert_eq!(&array.metadata.zarr_format, &ZarrFormat::V3);
    assert_eq!(&array.metadata.codecs[0].name, &"bytes");
    assert_eq!(&array.metadata.codecs[1].name, &"blosc");

    // Test getting a specific slice of the array
    let array_slice = array
        .get(Some(vec![0usize..1, 0usize..1, 0usize..1]))
        .await
        .unwrap();
    let array_data: ArrayD<i16> = array_slice.try_into().unwrap();
    let expected = Array::from_vec(vec![0i16])
        .into_dyn()
        .into_shape(IxDyn(&[1, 1, 1]))
        .unwrap();
    assert_eq!(array_data, expected);
}
