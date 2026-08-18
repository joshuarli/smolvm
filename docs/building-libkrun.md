# Building libkrun

`libkrun` is built from the pinned `libkrun` submodule. The macOS
`lib/libkrun.dylib` output is generated locally and is intentionally not
committed or stored in Git LFS.

## macOS

For the supported Apple-Silicon local setup, run:

```bash
make setup
```

It initializes all pinned submodules recursively, installs LLVM when absent,
installs the matching guest-init target, and builds the headless block-and-
network library needed by ordinary VMs. Equivalently, install the prerequisites
manually:

```bash
brew install llvm
rustup target add aarch64-unknown-linux-musl  # arm64 macOS
git submodule update --init --recursive
```

GPU support is an explicit build because it additionally needs a compatible
`virglrenderer` installation visible to `pkg-config`. After providing that
dependency, build the block, networking, and GPU-enabled library with:

```bash
cargo make build-libkrun
```

This writes the generated library to `lib/libkrun.dylib` and updates
`lib/libkrun.provenance`. The generated dylib is ignored by Git.

## Linux

Use the platform build script, which builds or reuses `libkrunfw` as needed and
writes the result under `lib/linux-<arch>/`:

```bash
./scripts/build-libkrun-linux.sh
```
