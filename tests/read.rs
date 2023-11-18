use charizarr::{metadata::{DataType, ZarrFormat}, codecs::endian::EndianCodec, codec::ByteToArrayCodec};

#[tokio::test]
async fn test_read() {
    // Create the store
    let path = std::path::PathBuf::from("tests/data.zarr");
    let store = charizarr::stores::FileSystemStore::new(path);

    // Read in a group
    let group = charizarr::hierarchy::Group::open(&store, None)
        .await
        .unwrap();

    assert_eq!(&group.meta.zarr_format, &ZarrFormat::V3);

    let name = &group
        .meta
        .attributes
        .as_ref()
        .unwrap()
        .get("name")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(name, &"data.zarr");

    // Read in an array
    let array = charizarr::hierarchy::Array::open(&store, Some("3d.contiguous.i2".to_string()))
        .await
        .unwrap();

    assert_eq!(&array.meta.zarr_format, &ZarrFormat::V3);
    let data_type = array.meta.data_type.clone();
    assert_eq!(
        data_type,
        DataType::Core(charizarr::data_type::CoreDataType::Int16)
    );

    // We can also get arrays from the group
    let array = group.get_array("1d.contiguous.raw.i2").await.unwrap();
    assert_eq!(&array.meta.zarr_format, &ZarrFormat::V3);

    // Lets read a chunk manually for now
    let chunk = array.get_raw_chunk("c/0").await.unwrap();
    let decoder = EndianCodec::new();
    //let decoded = decoder.decode(&array.meta.data_type, serde_json::json!({"endian": "little"}), &chunk).unwrap();
}
