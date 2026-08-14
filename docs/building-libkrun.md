# Building libkrun

`libkrun` is built from the pinned `libkrun` submodule. The macOS
`lib/libkrun.dylib` output is generated locally and is intentionally not
committed or stored in Git LFS.

## macOS

Install the host dependencies and the matching guest-init target:

```bash
brew install llvm virglrenderer
rustup target add aarch64-unknown-linux-musl  # arm64 macOS
git submodule update --init libkrun
```

Build the block, networking, and GPU-enabled library:

```bash
cargo make build-libkrun
```

This writes the generated library to `lib/libkrun.dylib` and updates
`lib/libkrun.provenance`. The generated dylib is ignored by Git.

For the graphics-free Swift SDK variant, use
`scripts/build-libkrun-swift-sdk-macos.sh` instead.

## Linux

Use the platform build script, which builds or reuses `libkrunfw` as needed and
writes the result under `lib/linux-<arch>/`:

```bash
./scripts/build-libkrun-linux.sh
```
