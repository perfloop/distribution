#!/usr/bin/env bash
# Runs the repository's lint gate. When Docker is unavailable, use the exact
# golangci-lint release pinned by dockerfiles/lint.Dockerfile.
set -euo pipefail

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
	make lint
	exit 0
fi

readonly VERSION='2.9.0'
readonly TOOLS_DIR='/workspace/deps/perfloop/golangci-lint-v2.9.0'
readonly BINARY="$TOOLS_DIR/golangci-lint"

case "$(uname -m)" in
x86_64)
	readonly ARCH='amd64'
	;;
aarch64|arm64)
	readonly ARCH='arm64'
	;;
*)
	printf 'unsupported architecture for golangci-lint: %s\n' "$(uname -m)" >&2
	exit 2
	;;
esac

if [[ ! -x "$BINARY" ]]; then
	mkdir -p "$TOOLS_DIR"
	temporary_dir=$(mktemp -d "$TOOLS_DIR/download.XXXXXX")
	trap 'rm -rf "$temporary_dir"' EXIT
	archive="$temporary_dir/golangci-lint.tar.gz"
	curl --fail --location --silent --show-error \
		-o "$archive" \
		"https://github.com/golangci/golangci-lint/releases/download/v${VERSION}/golangci-lint-${VERSION}-linux-${ARCH}.tar.gz"
	tar -xzf "$archive" --strip-components=1 -C "$temporary_dir"
	mv "$temporary_dir/golangci-lint" "$BINARY"
fi

"$BINARY" --timeout 5m --build-tags "${BUILDTAGS:-}" run
