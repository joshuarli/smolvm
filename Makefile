SMOLVM_FORK ?= git@github.com:joshuarli
SMOLVM_UPSTREAM ?= git@github.com:smol-machines
SUBMODULES := libkrun libkrunfw smolvm-sdk

SMOLVM_UPSTREAM_URL ?= git@github.com:smol-machines/smolvm
LIBKRUN_UPSTREAM_URL ?= git@github.com:smol-machines/libkrun
LIBKRUNFW_UPSTREAM_URL ?= https://github.com/smol-machines/libkrunfw

.PHONY: setup sync push remotes

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
	$(MAKE) --no-print-directory remotes; \
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
sync: remotes
	@set -eu; \
	test -e libkrun/.git || { echo "smolvm: libkrun is not initialized; run make setup first" >&2; exit 1; }; \
	test -e libkrunfw/.git || { echo "smolvm: libkrunfw is not initialized; run make setup first" >&2; exit 1; }; \
	git fetch --no-tags "$(SMOLVM_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/smolvm-main"; \
	git -C libkrun fetch --no-tags "$(LIBKRUN_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/libkrun-main"; \
	git -C libkrunfw fetch --no-tags "$(LIBKRUNFW_UPSTREAM_URL)" "+refs/heads/main:refs/smolvm-sync/libkrunfw-main"

# Point every submodule at the joshuarli fork (origin) with the smol-machines
# repository as upstream, so make push publishes to the fork and upstream refs
# are fetched from smol-machines. Idempotent; automatically run by setup, sync
# and push.
remotes:
	@set -eu; \
	for sub in $(SUBMODULES); do \
		test -e "$$sub/.git" || { echo "smolvm: $$sub is not initialized; run make setup first" >&2; exit 1; }; \
		if git -C "$$sub" remote get-url origin >/dev/null 2>&1; then \
			git -C "$$sub" remote set-url origin "$(SMOLVM_FORK)/$$sub.git"; \
		else \
			git -C "$$sub" remote add origin "$(SMOLVM_FORK)/$$sub.git"; \
		fi; \
		if git -C "$$sub" remote get-url upstream >/dev/null 2>&1; then \
			git -C "$$sub" remote set-url upstream "$(SMOLVM_UPSTREAM)/$$sub.git"; \
		else \
			git -C "$$sub" remote add upstream "$(SMOLVM_UPSTREAM)/$$sub.git"; \
		fi; \
		echo "smolvm: $$sub: origin=$(SMOLVM_FORK)/$$sub.git upstream=$(SMOLVM_UPSTREAM)/$$sub.git"; \
	done

# Publish the submodule commits referenced by this checkout before pushing the
# parent smolvm commit. libkrun, libkrunfw and smolvm-sdk are commonly checked
# out detached by Git submodule, so $(NAME)_PUSH_BRANCH names the destination
# branch in that case (defaulting to main). Each push is policy-driven so the
# target "just works" regardless of history shape: an already-published commit
# is a no-op, a commit that is strictly behind its branch is skipped rather
# than rewinding the remote, a fast-forward uses a plain push, and a divergent
# (e.g. rebased) history is published with --force-with-lease, which refuses to
# clobber remote commits we have not seen.
push: remotes
	@set -eu; \
	parent_branch="$$(git branch --show-current)"; \
	test -n "$$parent_branch" || { echo "smolvm: push requires an attached branch" >&2; exit 1; }; \
	test -z "$$(git status --porcelain)" || { echo "smolvm: worktree is dirty; commit or stash it before make push" >&2; exit 1; }; \
	publish() { \
		name="$$1"; subpath="$$2"; default_dst="$$3"; \
		test -z "$$(git -C "$$subpath" status --porcelain)" || { echo "smolvm: $$subpath worktree is dirty; commit or stash it before make push" >&2; exit 1; }; \
		sub_branch="$$(git -C "$$subpath" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"; \
		if test -n "$$sub_branch"; then \
			src="$$sub_branch"; dst="$$sub_branch"; \
		else \
			src="HEAD"; dst="$$default_dst"; \
		fi; \
		rt="refs/remotes/origin/$$dst"; \
		rt_sha="$$(git -C "$$subpath" rev-parse --verify "$$rt" 2>/dev/null || true)"; \
		if test -z "$$rt_sha"; then \
			echo "smolvm: $$name: no tracked origin/$$dst; pushing $$src"; \
			git -C "$$subpath" push origin "$$src:$$dst"; \
		elif test "$$(git -C "$$subpath" rev-parse "$$src")" = "$$rt_sha"; then \
			echo "smolvm: $$name: $$src already in origin/$$dst"; \
		elif git -C "$$subpath" merge-base --is-ancestor "$$src" "$$rt_sha"; then \
			echo "smolvm: $$name: $$src is behind origin/$$dst; skipping"; \
		elif git -C "$$subpath" merge-base --is-ancestor "$$rt_sha" "$$src"; then \
			echo "smolvm: $$name: pushing $$src to origin/$$dst"; \
			git -C "$$subpath" push origin "$$src:$$dst"; \
		else \
			echo "smolvm: $$name: pushing $$src to origin/$$dst (--force-with-lease)"; \
			git -C "$$subpath" push --force-with-lease origin "$$src:$$dst"; \
		fi; \
	}; \
	publish libkrun libkrun "$${LIBKRUN_PUSH_BRANCH:-main}"; \
	publish libkrunfw libkrunfw "$${LIBKRUNFW_PUSH_BRANCH:-main}"; \
	publish smolvm-sdk smolvm-sdk "$${SMOLVM_SDK_PUSH_BRANCH:-main}"; \
	echo "smolvm: pushing branch $$parent_branch"; \
	rt="refs/remotes/origin/$$parent_branch"; \
	rt_sha="$$(git rev-parse --verify "$$rt" 2>/dev/null || true)"; \
	if test -z "$$rt_sha"; then \
		git push origin "$$parent_branch"; \
	elif test "$$(git rev-parse HEAD)" = "$$rt_sha"; then \
		echo "smolvm: origin/$$parent_branch already up to date"; \
	elif git merge-base --is-ancestor HEAD "$$rt_sha"; then \
		echo "smolvm: $$parent_branch is behind origin/$$parent_branch; skipping"; \
	elif git merge-base --is-ancestor "$$rt_sha" HEAD; then \
		echo "smolvm: pushing $$parent_branch to origin/$$parent_branch"; \
		git push origin "$$parent_branch"; \
	else \
		echo "smolvm: pushing $$parent_branch to origin/$$parent_branch (--force-with-lease)"; \
		git push --force-with-lease origin "$$parent_branch"; \
	fi
