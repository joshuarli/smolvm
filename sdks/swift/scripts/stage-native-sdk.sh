#!/usr/bin/env bash

# Build and stage the native assets consumed by SmolVMSDK. This intentionally
# ships the C ABI library and the small `_boot-vm` helper, never the `smolvm`
# CLI or daemon binary.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPOSITORY_ROOT="$(cd "$SDK_ROOT/../.." && pwd)"
OUTPUT_DIRECTORY="${1:-$SDK_ROOT/.build-assets}"
ROOTFS_SOURCE="${SMOLVM_AGENT_ROOTFS_SOURCE:-$REPOSITORY_ROOT/target/agent-rootfs}"
LIBRARY_SOURCE="${SMOLVM_EMBEDDED_LIB_BUNDLE:-$REPOSITORY_ROOT/lib}"
SIGN_IDENTITY="${SMOLVM_SIGN_IDENTITY:--}"

if [[ ! -d "$ROOTFS_SOURCE" ]]; then
    echo "Missing agent rootfs: $ROOTFS_SOURCE" >&2
    echo "Build it first with scripts/build-agent-rootfs.sh, or set SMOLVM_AGENT_ROOTFS_SOURCE." >&2
    exit 1
fi
if [[ ! -d "$LIBRARY_SOURCE" ]]; then
    echo "Missing libkrun bundle: $LIBRARY_SOURCE" >&2
    echo "Set SMOLVM_EMBEDDED_LIB_BUNDLE to a directory containing matching libkrun/libkrunfw artifacts." >&2
    exit 1
fi

required_libraries=(
    libkrun.dylib
    libkrunfw.5.dylib
)
for library in "${required_libraries[@]}"; do
    if [[ ! -f "$LIBRARY_SOURCE/$library" ]]; then
        echo "Missing required macOS VMM library $library in $LIBRARY_SOURCE" >&2
        exit 1
    fi
done
if [[ ! -f "$LIBRARY_SOURCE/libkrun.dylib" || ! -f "$LIBRARY_SOURCE/libkrunfw.5.dylib" ]]; then
    echo "Missing matching macOS libkrun/libkrunfw artifacts in $LIBRARY_SOURCE" >&2
    exit 1
fi
if [[ "$(uname -s)" == "Darwin" ]] && otool -L "$LIBRARY_SOURCE/libkrun.dylib" | grep -Eq 'libvirglrenderer|libMoltenVK|libepoxy'; then
    echo "The Swift SDK bundle intentionally excludes graphics. Rebuild libkrun with scripts/build-libkrun-swift-sdk-macos.sh first." >&2
    exit 1
fi
if head -n 1 "$LIBRARY_SOURCE/libkrun.dylib" | grep -q '^version https://git-lfs.github.com/spec/'; then
    echo "libkrun is a Git LFS pointer, not a native library. Run 'git lfs install --local && git lfs pull'." >&2
    exit 1
fi

(
    cd "$REPOSITORY_ROOT"
    # Use precisely the libkrun pair copied into the SDK bundle.  The runtime
    # is still CLI-free: these commands build only the C ABI library and the
    # narrow _boot-vm helper, never the smolvm command or daemon binary.
    LIBKRUN_DIR="$LIBRARY_SOURCE" cargo build --release --package smolvm-swift-ffi
    LIBKRUN_DIR="$LIBRARY_SOURCE" cargo build --release --package smolvm --bin smolvm-boot
)

mkdir -p "$OUTPUT_DIRECTORY/lib" "$OUTPUT_DIRECTORY/agent-rootfs"
# Delete only legacy generated artifacts. This keeps a rerun from preserving
# unused graphics libraries or obsolete compatibility aliases after the SDK's
# dependency cutover.
rm -f \
    "$OUTPUT_DIRECTORY/lib/libepoxy.0.dylib" \
    "$OUTPUT_DIRECTORY/lib/libvirglrenderer.1.dylib" \
    "$OUTPUT_DIRECTORY/lib/libMoltenVK.dylib" \
    "$OUTPUT_DIRECTORY/lib/libkrun.1.dylib" \
    "$OUTPUT_DIRECTORY/lib/libkrunfw.dylib"
cp -f "$REPOSITORY_ROOT/target/release/libsmolvm_swift_ffi.dylib" "$OUTPUT_DIRECTORY/lib/"
cp -f "$REPOSITORY_ROOT/target/release/smolvm-boot" "$OUTPUT_DIRECTORY/"
cp -a "$ROOTFS_SOURCE/." "$OUTPUT_DIRECTORY/agent-rootfs/"

# The loader resolves exactly these two names. Do not glob the source directory:
# it may contain historical compatibility aliases that refer to an older or
# graphics-linked build.
cp -f "$LIBRARY_SOURCE/libkrun.dylib" "$OUTPUT_DIRECTORY/lib/libkrun.dylib"
cp -f "$LIBRARY_SOURCE/libkrunfw.5.dylib" "$OUTPUT_DIRECTORY/lib/libkrunfw.5.dylib"

# The checked-in convenience filename is `libkrun.dylib`, while libkrun's
# Mach-O install name is currently `libkrun.2.dylib`.  The boot helper is
# deliberately weak-linked against that install name, so preserve a matching
# sibling alias in the staged bundle rather than relying on a Homebrew copy.
krun_install_name="$(otool -D "$LIBRARY_SOURCE/libkrun.dylib" 2>/dev/null | sed -n '2p' | xargs basename || true)"
if [[ -n "$krun_install_name" && "$krun_install_name" != "libkrun.dylib" ]]; then
    ln -sfn "libkrun.dylib" "$OUTPUT_DIRECTORY/lib/$krun_install_name"
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
    # The helper is the process that enters Hypervisor.framework, so it carries
    # the entitlement. Libraries are signed as part of the same SDK bundle.
    codesign --force --sign "$SIGN_IDENTITY" --entitlements "$REPOSITORY_ROOT/smolvm.entitlements" \
        "$OUTPUT_DIRECTORY/smolvm-boot"
    codesign --force --sign "$SIGN_IDENTITY" "$OUTPUT_DIRECTORY/lib/libsmolvm_swift_ffi.dylib"
    codesign --force --sign "$SIGN_IDENTITY" "$OUTPUT_DIRECTORY/lib/libkrun.dylib"
    codesign --force --sign "$SIGN_IDENTITY" "$OUTPUT_DIRECTORY/lib/libkrunfw.5.dylib"
fi

printf '%s\n' "Staged SmolVMSDK assets in $OUTPUT_DIRECTORY"
