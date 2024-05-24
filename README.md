# charizarr

A small, opinionated, async zarr 3.0 implementation written in rust.

This should not be used for anything real yet (probably), it is going to change a lot and is primarily a thought excecise.

## Development

This crate requires rust 1.75 or later because it uses async traits.

### Progress

- [x] create object store zarr store
- [ ] virtual zarr store (kerchunk, reference manifest)
- [x] read zarr group hierarchy
- [x] write zarr group hierarchy
- [x] read zarr array hierarchy
- [x] write zarr array hierarchy
- [x] read zarr array chunks
- [x] write zarr array chunks
- [x] read zarr array data
- [x] write zarr array data
- [x] custom chunk encoding support
- [ ] fill values
- [x] bytes codec
- [x] blosc codec
- [x] gzip codec
- [ ] sharding codec
- [ ] transpose codec
- [ ] grib codec
- [x] error handling
- [ ] way more tests
- [ ] optimization
    - [x] dont clone data arrays
    - [ ] ergonomic API for using arrays
    - [ ] get rid of generics on hierarchies if possible

## Features

**gzip**

This feature enables gzip compression support. This is enabled by default.

**blosc**

This feature enables blosc compression support. This is enabled by default. It requires that the blosc library is installed on your system and is not available for wasm targets.

*macos*:
```bash
brew install c-blosc
export RUSTFLAGS="-L/opt/homebrew/lib"
```

*ubuntu*:
```bash
apt-get install libblosc-dev
```
