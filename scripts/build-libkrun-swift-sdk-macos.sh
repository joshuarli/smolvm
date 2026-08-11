#!/usr/bin/env bash

# Build the smallest libkrun variant consumed by the embedded Swift SDK.
#
# Docker/Compose needs virtio block storage, virtio-net, and vsock. It does not
# need virtio-gpu, virglrenderer, MoltenVK, or libepoxy. libkrun 2.0 builds its
# guest PID 1 in `src/init_blob`; `libkrun/init/init` is a deliberately empty
# legacy placeholder, so KRUN_INIT_BINARY_PATH must be absent here.

set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPOSITORY_ROOT"

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "This script builds the macOS Swift SDK libkrun bundle; use scripts/build-libkrun-linux.sh on Linux." >&2
    exit 1
fi

case "$(uname -m)" in
    arm64) MUSL_TARGET="aarch64-unknown-linux-musl" ;;
    x86_64) MUSL_TARGET="x86_64-unknown-linux-musl" ;;
    *)
        echo "Unsupported macOS architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

command -v rustup >/dev/null 2>&1 || {
    echo "rustup is required to install the guest init target: ${MUSL_TARGET}" >&2
    exit 1
}
if ! rustup target list --installed | grep -qx "$MUSL_TARGET"; then
    rustup target add "$MUSL_TARGET"
fi

# `env -u` prevents a shell-level legacy override from embedding the zero-byte
# libkrun/init/init placeholder. The default init_blob build compiles a static
# Linux guest init for the target above.
env -u KRUN_INIT_BINARY_PATH make -C libkrun BLK=1 NET=1

BUILT_LIBRARY="$REPOSITORY_ROOT/libkrun/target/release/libkrun.2.0.0.dylib"
[[ -f "$BUILT_LIBRARY" ]] || {
    echo "libkrun build produced no macOS dylib: $BUILT_LIBRARY" >&2
    exit 1
}
if otool -L "$BUILT_LIBRARY" | grep -Eq 'libvirglrenderer|libMoltenVK|libepoxy'; then
    echo "Refusing to stage a graphics-enabled libkrun for the Swift SDK." >&2
    exit 1
fi
nm -gU "$BUILT_LIBRARY" | grep -q ' _krun_add_net_unixstream$' || {
    echo "libkrun is missing krun_add_net_unixstream; rebuild with NET=1." >&2
    exit 1
}

cp -f "$BUILT_LIBRARY" "$REPOSITORY_ROOT/lib/libkrun.dylib"
"$REPOSITORY_ROOT/scripts/stamp-libkrun-provenance.sh" "$REPOSITORY_ROOT/lib" --skip-libkrunfw

echo "Built non-graphics libkrun for the Swift SDK: lib/libkrun.dylib"
