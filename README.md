# charizarr

A small, opinionated, async zarr 3.0 implementation written in rust

This is beginning as a way to deep dive into zarr and to push whats possible
with rust and async.

## Development

This crate requires nightly or beta rust to build until async/await is released to the stable channel.

```bash
# For beta
rustup default beta

# For nightly
rustup default nightly
```

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
