use charizarr::metadata::ZarrFormat;

#[tokio::test]
async fn test_read() {
    let path = std::path::PathBuf::from("tests/data.zarr");
    let store = charizarr::stores::FileSystemStore::new(path);
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
}
