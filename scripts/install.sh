#!/usr/bin/env sh
set -eu

REPO_SLUG="${GALACTICA_INSTALL_REPO:-digiterialabs/galactica}"
INSTALL_ROOT="${GALACTICA_INSTALL_ROOT:-$HOME/.local}"
REPO_ROOT="${GALACTICA_REPO_ROOT:-}"
RELEASE_API_URL="${GALACTICA_CLI_RELEASE_API_URL:-}"

usage() {
  cat <<'EOF'
Install galactica-cli from the latest GitHub Release and create a repo-bound galactica launcher.

Usage:
  install.sh [--repo-root PATH] [--install-root PATH] [--repo OWNER/REPO]

Environment:
  GALACTICA_REPO_ROOT         Override the Galactica checkout to bind the launcher to.
  GALACTICA_INSTALL_ROOT      Override the install root. The launcher is placed in <root>/bin.
  GALACTICA_INSTALL_REPO      Override the GitHub repository slug. Default: digiterialabs/galactica
  GALACTICA_CLI_RELEASE_API_URL
                              Override the GitHub release API endpoint.
  GITHUB_TOKEN                Optional token for authenticated GitHub API requests.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --repo-root)
      [ $# -ge 2 ] || {
        echo "missing value for --repo-root" >&2
        exit 1
      }
      REPO_ROOT="$2"
      shift 2
      ;;
    --install-root)
      [ $# -ge 2 ] || {
        echo "missing value for --install-root" >&2
        exit 1
      }
      INSTALL_ROOT="$2"
      shift 2
      ;;
    --repo)
      [ $# -ge 2 ] || {
        echo "missing value for --repo" >&2
        exit 1
      }
      REPO_SLUG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

find_repo_root() {
  dir="$1"
  while [ -n "$dir" ] && [ "$dir" != "/" ]; do
    if [ -f "$dir/Cargo.toml" ] && [ -d "$dir/rust/galactica-cli" ]; then
      printf '%s\n' "$dir"
      return 0
    fi
    parent=$(dirname "$dir")
    [ "$parent" != "$dir" ] || break
    dir="$parent"
  done
  if [ -f "/Cargo.toml" ] && [ -d "/rust/galactica-cli" ]; then
    printf '/\n'
    return 0
  fi
  return 1
}

if [ -z "$REPO_ROOT" ]; then
  REPO_ROOT=$(find_repo_root "$PWD" || true)
fi

[ -n "$REPO_ROOT" ] || {
  echo "failed to find the Galactica repo root; run this from the checkout or set GALACTICA_REPO_ROOT" >&2
  exit 1
}

[ -f "$REPO_ROOT/Cargo.toml" ] && [ -d "$REPO_ROOT/rust/galactica-cli" ] || {
  echo "repo root does not look like a Galactica checkout: $REPO_ROOT" >&2
  exit 1
}

detect_target() {
  os_name=$(uname -s)
  arch_name=$(uname -m)
  case "$os_name:$arch_name" in
    Darwin:arm64|Darwin:aarch64)
      printf 'aarch64-apple-darwin .tar.gz\n'
      ;;
    Darwin:x86_64)
      printf 'x86_64-apple-darwin .tar.gz\n'
      ;;
    Linux:x86_64|Linux:amd64)
      printf 'x86_64-unknown-linux-gnu .tar.gz\n'
      ;;
    *)
      echo "unsupported host: $os_name $arch_name" >&2
      exit 1
      ;;
  esac
}

set -- $(detect_target)
TARGET_TRIPLE="$1"
ARCHIVE_SUFFIX="$2"

if [ -z "$RELEASE_API_URL" ]; then
  RELEASE_API_URL="https://api.github.com/repos/$REPO_SLUG/releases/latest"
fi

curl_release() {
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      "$1"
  else
    curl -fsSL -H "Accept: application/vnd.github+json" "$1"
  fi
}

extract_json_urls() {
  tr ',' '\n' | grep -o 'https://[^"]*'
}

release_json=$(curl_release "$RELEASE_API_URL")
asset_url=$(
  printf '%s' "$release_json" \
    | extract_json_urls \
    | grep 'galactica-cli' \
    | grep "$TARGET_TRIPLE" \
    | grep "${ARCHIVE_SUFFIX}$" \
    | head -n1
)
checksum_url=$(
  printf '%s' "$release_json" \
    | extract_json_urls \
    | grep 'SHA256SUMS.txt' \
    | head -n1
)
release_tag=$(
  printf '%s' "$release_json" \
    | tr ',' '\n' \
    | sed -n 's/.*"tag_name":"\([^"]*\)".*/\1/p' \
    | head -n1
)

[ -n "$asset_url" ] || {
  echo "failed to resolve a galactica-cli release asset for $TARGET_TRIPLE" >&2
  exit 1
}

archive_name=$(basename "$asset_url")
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/galactica-install.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT INT TERM
archive_path="$tmp_dir/$archive_name"
checksum_path="$tmp_dir/SHA256SUMS.txt"

echo "Downloading $archive_name${release_tag:+ from $release_tag}..."
curl -fsSL "$asset_url" -o "$archive_path"
if [ -n "$checksum_url" ]; then
  curl -fsSL "$checksum_url" -o "$checksum_path"
fi

verify_archive() {
  [ -f "$checksum_path" ] || return 0
  expected=$(
    grep " ${archive_name}\$" "$checksum_path" | awk '{print $1}' | head -n1
  )
  [ -n "$expected" ] || return 0
  if command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$archive_path" | awk '{print $1}')
  elif command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$archive_path" | awk '{print $1}')
  else
    echo "warning: no SHA256 tool found; skipping checksum verification" >&2
    return 0
  fi
  [ "$actual" = "$expected" ] || {
    echo "checksum mismatch for $archive_name" >&2
    exit 1
  }
}

verify_archive

extract_dir="$tmp_dir/extract"
mkdir -p "$extract_dir"
tar -xzf "$archive_path" -C "$extract_dir"
extracted_binary=$(find "$extract_dir" -type f -name 'galactica-cli' | head -n1)
[ -n "$extracted_binary" ] || {
  echo "failed to find galactica-cli in $archive_name" >&2
  exit 1
}

bin_dir="$INSTALL_ROOT/bin"
binary_path="$bin_dir/galactica-cli"
launcher_path="$bin_dir/galactica"
mkdir -p "$bin_dir"
cp "$extracted_binary" "$binary_path"
chmod 755 "$binary_path"

cat >"$launcher_path" <<EOF
#!/usr/bin/env sh
exec "$binary_path" --repo-root "$REPO_ROOT" "\$@"
EOF
chmod 755 "$launcher_path"

on_path=0
case ":${PATH:-}:" in
  *:"$bin_dir":*)
    on_path=1
    ;;
esac

echo "Installed galactica-cli to $binary_path"
echo "Installed galactica launcher to $launcher_path"
if [ "$on_path" -eq 0 ]; then
  echo "Add $bin_dir to PATH:"
  echo "  export PATH=\"$bin_dir:\$PATH\""
fi
echo "Next:"
echo "  galactica detect"
echo "  galactica doctor"
echo "  galactica install"
