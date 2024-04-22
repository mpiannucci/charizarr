# charizarr

A small, opinionated, async zarr 3.0 implementation written in rust.

This should not be used for anything real yet (probably), it is going to change a lot and is primarily a thought excecise.

## Development

This crate requires rust 1.75 or later because it uses async traits.

### Progress

- [x] Create filesystem zarr store
- [ ] Create cloud zarr store
- [ ] Virtual zarr store (kerchunk)
- [x] Read zarr group hierarchy
- [x] Write zarr group hierarchy
- [x] Read zarr array hierarchy
- [x] Write zarr array hierarchy
- [x] Read zarr array chunks
- [x] Write zarr array chunks
- [x] Read zarr array data
- [ ] Write zarr array data
- [x] bytes codec
- [x] blosc codec
- [x] gzip codec
- [ ] transpose codec
- [ ] grib codec
- [ ] error handling

## Features

**gzip**

This feature enables gzip compression support. This is enabled by default.

**blosc**

This feature enables blosc compression support. This is enabled by default. It requires that the blosc library is installed on your system and is not available for wasm targets.

For macos:
```bash
brew install c-blosc
export RUSTFLAGS="-L/opt/homebrew/lib"
```
