SMOLVM_UPSTREAM_URL ?= git@github.com:smol-machines/smolvm
LIBKRUN_UPSTREAM_URL ?= git@github.com:smol-machines/libkrun
LIBKRUNFW_UPSTREAM_URL ?= https://github.com/smol-machines/libkrunfw

.PHONY: setup sync push

# Prepare the supported Apple-Silicon headless development runtime. The
# generated lib/libkrun.dylib intentionally stays out of Git; this target
# builds its required block and network support from the libkrun submodule.
# GPU support is an explicit separate build because it needs virglrenderer.
setup:
	@set -eu; \
	if test "$$(uname -s)" != Darwin; then \
		echo "smolvm: make setup supports macOS only; see docs/building-libkrun.md for Linux" >&2; \
		exit 1; \
	fi; \
	command -v brew >/dev/null || { echo "smolvm: make setup requires Homebrew" >&2; exit 1; }; \
	command -v cargo >/dev/null || { echo "smolvm: make setup requires Cargo" >&2; exit 1; }; \
	command -v rustup >/dev/null || { echo "smolvm: make setup requires rustup" >&2; exit 1; }; \
	git submodule update --init --recursive; \
	brew list --versions llvm >/dev/null 2>&1 || brew install llvm; \
	llvm_prefix="$$(brew --prefix llvm)"; \
	export LIBCLANG_PATH="$$llvm_prefix/lib"; \
	export DYLD_FALLBACK_LIBRARY_PATH="$$LIBCLANG_PATH$${DYLD_FALLBACK_LIBRARY_PATH:+:$$DYLD_FALLBACK_LIBRARY_PATH}"; \
	export RUSTFLAGS="$${RUSTFLAGS:+$$RUSTFLAGS }-C link-arg=-Wl,-rpath,$$LIBCLANG_PATH"; \
	export PATH="$$llvm_prefix/bin:$$PATH"; \
	rustup target list --installed | grep -qx 'aarch64-unknown-linux-musl' || rustup target add aarch64-unknown-linux-musl; \
	env -u KRUN_INIT_BINARY_PATH make -C libkrun BLK=1 NET=1; \
	cp libkrun/target/release/libkrun.2.0.0.dylib lib/libkrun.dylib; \
	./scripts/stamp-libkrun-provenance.sh lib --skip-libkrunfw

# Fetch the explicit upstream main tips into local rebase references. This does
# not check out or rebase any branch; `make setup` initializes the submodules.
sync:
	@set -eu; \
	test -e libkrun/.git || { echo "smolvm: libkrun is not initialized; run make setup first" >&2; exit 1; }; \
	test -e libkrunfw/.git || { echo "smolvm: libkrunfw is not initialized; run make setup first" >&2; exit 1; }; \
	git fetch --no-tags "$(SMOLVM_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/smolvm-main"; \
	git -C libkrun fetch --no-tags "$(LIBKRUN_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/libkrun-main"; \
	git -C libkrunfw fetch --no-tags "$(LIBKRUNFW_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/libkrunfw-main"

# Push the libkrun commit referenced by this checkout before publishing the
# parent smolvm commit. libkrun is commonly checked out detached by Git
# submodule, so LIBKRUN_PUSH_BRANCH names the destination branch in that case.
push:
	@set -eu; \
	parent_branch="$$(git branch --show-current)"; \
	test -n "$$parent_branch" || { echo "smolvm: push requires an attached branch" >&2; exit 1; }; \
	test -z "$$(git status --porcelain)" || { echo "smolvm: worktree is dirty; commit or stash it before make push" >&2; exit 1; }; \
	test -z "$$(git -C libkrun status --porcelain)" || { echo "smolvm: libkrun worktree is dirty; commit or stash it before make push" >&2; exit 1; }; \
	submodule_branch="$$(git -C libkrun symbolic-ref --quiet --short HEAD || true)"; \
	if test -n "$$submodule_branch"; then \
		echo "smolvm: pushing libkrun branch $$submodule_branch"; \
		git -C libkrun push origin "$$submodule_branch"; \
	else \
		destination="$${LIBKRUN_PUSH_BRANCH:-main}"; \
		echo "smolvm: pushing detached libkrun HEAD to origin/$$destination"; \
		git -C libkrun push origin "HEAD:$$destination"; \
	fi; \
	echo "smolvm: pushing branch $$parent_branch"; \
	git push origin "$$parent_branch"
