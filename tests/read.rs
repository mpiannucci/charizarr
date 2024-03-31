use charizarr::{metadata::{DataType, ZarrFormat}, codecs::{bytes::BytesCodec, gzip::GZipCodec, blosc::BloscCodec}, codec::{ByteToArrayCodec, ByteToByteCodec}, chunk::Chunk};
use ndarray::Array;

#[tokio::test]
async fn test_read() {
    // Create the store
    let path = std::path::PathBuf::from("tests/data.zarr");
    let store = charizarr::stores::FileSystemStore::new(path);

    // Read in a group
    let group = charizarr::group::Group::open(&store, None)
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
    let array = charizarr::array::Array::open(&store, Some("3d.contiguous.i2".to_string()), None)
        .await
        .unwrap();

    assert_eq!(&array.meta.zarr_format, &ZarrFormat::V3);
    let data_type = array.meta.data_type.clone();
    assert_eq!(
        data_type,
        DataType::Core(charizarr::data_type::CoreDataType::Int16)
    );

    // We can also get arrays from the group
    let array = group.get_array("1d.contiguous.raw.i2", None).await.unwrap();
    assert_eq!(&array.meta.zarr_format, &ZarrFormat::V3);
    assert_eq!(&array.meta.codecs[0].name, &"bytes");

    // Lets read a chunk manually for now
    let chunk = array.get_raw_chunk("c/0").await.unwrap();
    let decoder = BytesCodec::new();

    // At the moment this is the only way to extract the core data type... needs to improve
    let DataType::Core(data_type) = &array.meta.data_type else {
        assert!(false);
        return;
    };

    let config = array.meta.codecs.first().as_ref().unwrap().configuration.clone().unwrap();
    let decoded = decoder.decode(data_type, config, &chunk).unwrap();
    let Chunk::Int16(array_chunk) = decoded else {
        assert!(false);
        return;
    };

    let expected = Array::from_vec(vec![1i16, 2, 3, 4]).into_dyn();
    assert_eq!(array_chunk, expected);

    // We can also read compressed chunks
    // First lets try gzip
    let array = group.get_array("1d.contiguous.gzip.i2", None).await.unwrap();
    assert_eq!(&array.meta.codecs.len(), &2);
    assert_eq!(array.shape(), vec![4]);
    assert_eq!(array.chunk(), vec![4]);

    let chunk = array.get_raw_chunk("c/0").await.unwrap();
    let gzip_decoder = GZipCodec::new();
    let config = array.meta.codecs.last().as_ref().unwrap().configuration.clone().unwrap();
    let raw_chunk = gzip_decoder.decode(data_type, config, &chunk).unwrap();
    let config = array.meta.codecs.first().as_ref().unwrap().configuration.clone().unwrap();
    let decoded_chunk = decoder.decode(data_type, config, &raw_chunk).unwrap();
    let Chunk::Int16(array_chunk) = decoded_chunk else {
        assert!(false);
        return;
    };
    assert_eq!(array_chunk, expected);

    // Then we'll try blosc
    let array = group.get_array("1d.contiguous.blosc.i2", None).await.unwrap();
    assert_eq!(&array.meta.codecs.len(), &2);

    let chunk = array.get_raw_chunk("c/0").await.unwrap();
    let blosc_decoder = BloscCodec::new();
    let config = array.meta.codecs.last().as_ref().unwrap().configuration.clone().unwrap();
    let raw_chunk = blosc_decoder.decode(data_type, config, &chunk).unwrap();
    let config = array.meta.codecs.first().as_ref().unwrap().configuration.clone().unwrap();
    let decoded_chunk = decoder.decode(data_type, config, &raw_chunk).unwrap();
    let Chunk::Int16(array_chunk) = decoded_chunk else {
        assert!(false);
        return;
    };
    assert_eq!(array_chunk, expected);
}
