#!/bin/sh
set -eu

REPO="${ELARK_REPO:-hachiwii/exec-over-lark}"
RELEASE_API_URL="${ELARK_RELEASE_API_URL:-https://api.github.com/repos/${REPO}/releases/latest}"
INSTALL_DIR="${ELARK_INSTALL_DIR:-}"
SYSTEM_INSTALL=0
AUTO_INSTALL=1

log() {
	printf '%s\n' "$*"
}

warn() {
	printf 'elark install: %s\n' "$*" >&2
}

die() {
	warn "$*"
	exit 1
}

command_exists() {
	command -v "$1" >/dev/null 2>&1
}

usage() {
	cat <<'EOF'
Usage:
  install.sh [--system] [--no-install]

Options:
  --system      install elarkd as a system daemon
  --no-install  install binaries only; do not run elarkd install
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--system)
			SYSTEM_INSTALL=1
			shift
			;;
		--no-install)
			AUTO_INSTALL=0
			shift
			;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			die "unknown option: $1"
			;;
	esac
done

download_to() {
	url=$1
	destination=$2

	if command_exists curl; then
		curl -fsSL "$url" -o "$destination" || die "failed to download ${url}"
		return
	fi
	if command_exists wget; then
		wget -q -O "$destination" "$url" || die "failed to download ${url}"
		return
	fi
	die "missing dependency: install curl or wget"
}

detect_os() {
	case "$(uname -s)" in
		Darwin)
			printf 'darwin'
			;;
		Linux)
			printf 'linux'
			;;
		*)
			die "unsupported operating system: $(uname -s)"
			;;
	esac
}

detect_arch() {
	case "$(uname -m)" in
		x86_64 | amd64)
			printf 'amd64'
			;;
		arm64 | aarch64)
			printf 'arm64'
			;;
		*)
			die "unsupported CPU architecture: $(uname -m)"
			;;
	esac
}

path_contains_dir() {
	dir=$1
	case ":${PATH:-}:" in
		*":${dir}:"*)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

ensure_writable_dir() {
	dir=$1
	mkdir -p "$dir" 2>/dev/null || return 1
	probe="${dir}/.elark-install-test.$$"
	if (: >"$probe") 2>/dev/null; then
		rm -f "$probe"
		return 0
	fi
	rm -f "$probe" 2>/dev/null || true
	return 1
}

select_install_dir() {
	if [ -n "$INSTALL_DIR" ]; then
		ensure_writable_dir "$INSTALL_DIR" || die "install directory is not writable: ${INSTALL_DIR}"
		printf '%s\n' "$INSTALL_DIR"
		return
	fi

	if [ -n "${HOME:-}" ]; then
		home_local="${HOME}/.local/bin"
		if path_contains_dir "$home_local" && ensure_writable_dir "$home_local"; then
			printf '%s\n' "$home_local"
			return
		fi
	fi

	old_ifs=$IFS
	IFS=:
	for dir in ${PATH:-}; do
		IFS=$old_ifs
		[ -n "$dir" ] || continue
		if [ -d "$dir" ] && ensure_writable_dir "$dir"; then
			printf '%s\n' "$dir"
			return
		fi
		IFS=:
	done
	IFS=$old_ifs

	if [ -n "${HOME:-}" ]; then
		home_local="${HOME}/.local/bin"
		ensure_writable_dir "$home_local" || die "no writable PATH directory found and ${home_local} is not writable"
		printf '%s\n' "$home_local"
		return
	fi

	die "no writable PATH directory found; set ELARK_INSTALL_DIR to a writable directory"
}

release_urls() {
	sed -n 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$1"
}

find_asset_url() {
	release_json=$1
	goos=$2
	goarch=$3

	release_urls "$release_json" \
		| grep -E "exec-over-lark_[^/]*_${goos}_${goarch}\.(tar\.gz|zip)$" \
		| head -n 1
}

sha256_hex() {
	file=$1
	if command_exists sha256sum; then
		sha256sum "$file" | awk '{print $1}'
		return
	fi
	if command_exists shasum; then
		shasum -a 256 "$file" | awk '{print $1}'
		return
	fi
	printf ''
}

verify_checksum() {
	checksum_file=$1
	archive=$2

	expected=$(sed -n '1s/^\([0-9a-fA-F][0-9a-fA-F]*\).*/\1/p' "$checksum_file")
	if [ ${#expected} -ne 64 ]; then
		warn "could not parse checksum file; skipping checksum verification"
		return
	fi

	actual=$(sha256_hex "$archive")
	if [ -z "$actual" ]; then
		warn "sha256sum or shasum not found; skipping checksum verification"
		return
	fi

	[ "$actual" = "$expected" ] || die "checksum mismatch for $(basename "$archive")"
}

find_extracted_binary() {
	extract_dir=$1
	name=$2

	found=$(find "$extract_dir" -type f -name "$name" | head -n 1)
	[ -n "$found" ] || die "release archive did not contain ${name}"
	printf '%s\n' "$found"
}

install_binary() {
	source=$1
	name=$2
	destination="${install_dir}/${name}"
	staged="${tmpdir}/${name}"

	cp "$source" "$staged" || die "failed to stage ${name}"
	chmod 0755 "$staged" || die "failed to mark ${name} executable"
	mv "$staged" "$destination" || die "failed to install ${name} to ${destination}"
	[ -x "$destination" ] || die "installed ${destination} is not executable"
}

run_daemon_install() {
	elarkd_bin="${install_dir}/elarkd"
	[ -x "$elarkd_bin" ] || die "installed ${elarkd_bin} is not executable"
	if [ "$SYSTEM_INSTALL" -eq 1 ]; then
		if [ "$(id -u)" -eq 0 ]; then
			"$elarkd_bin" install --system || die "elarkd system install failed"
		else
			command_exists sudo || die "--system requires sudo"
			sudo "$elarkd_bin" install --system || die "elarkd system install failed"
		fi
		return
	fi
	"$elarkd_bin" install || die "elarkd install failed"
}

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/elark-install.XXXXXX") || die "failed to create temporary directory"
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM

goos=$(detect_os)
goarch=$(detect_arch)
install_dir=$(select_install_dir)

release_json="${tmpdir}/release.json"
download_to "$RELEASE_API_URL" "$release_json"

asset_url=$(find_asset_url "$release_json" "$goos" "$goarch" || true)
[ -n "$asset_url" ] || die "latest GitHub release has no asset for ${goos}/${goarch}"

archive_name=${asset_url##*/}
archive="${tmpdir}/${archive_name}"
download_to "$asset_url" "$archive"

checksum_url=$(release_urls "$release_json" | grep -F "${archive_name}.sha256" | head -n 1 || true)
if [ -n "$checksum_url" ]; then
	checksum_file="${tmpdir}/${archive_name}.sha256"
	download_to "$checksum_url" "$checksum_file"
	verify_checksum "$checksum_file" "$archive"
fi

extract_dir="${tmpdir}/extract"
mkdir -p "$extract_dir" || die "failed to create extraction directory"
case "$archive_name" in
	*.tar.gz | *.tgz)
		command_exists tar || die "missing dependency: tar"
		tar -xzf "$archive" -C "$extract_dir" || die "failed to extract ${archive_name}"
		;;
	*.zip)
		command_exists unzip || die "missing dependency: unzip"
		unzip -q "$archive" -d "$extract_dir" || die "failed to extract ${archive_name}"
		;;
	*)
		die "unsupported release archive: ${archive_name}"
		;;
esac

elark_source=$(find_extracted_binary "$extract_dir" elark)
elarkd_source=$(find_extracted_binary "$extract_dir" elarkd)

install_binary "$elark_source" elark
install_binary "$elarkd_source" elarkd

if [ "$AUTO_INSTALL" -eq 1 ]; then
	run_daemon_install
fi

log ""
log "exec-over-lark installed successfully."
log ""
log "Installation directory:"
log "  ${install_dir}"
log ""
log "Generate initial configuration:"
log "  ${install_dir}/elarkd init --client"
log "  ${install_dir}/elarkd init --server"
log ""
if [ "$AUTO_INSTALL" -eq 1 ]; then
	log "Daemon service:"
	if [ "$SYSTEM_INSTALL" -eq 1 ]; then
		log "  installed with: sudo ${install_dir}/elarkd install --system"
	else
		log "  installed with: ${install_dir}/elarkd install"
	fi
else
	log "Daemon service:"
	if [ "$SYSTEM_INSTALL" -eq 1 ]; then
		log "  sudo ${install_dir}/elarkd install --system"
	else
		log "  ${install_dir}/elarkd install"
	fi
fi
log ""
log "Start background process:"
if [ "$SYSTEM_INSTALL" -eq 1 ]; then
	log "  sudo ${install_dir}/elarkd start --system"
else
	log "  ${install_dir}/elarkd start"
fi

if ! path_contains_dir "$install_dir"; then
	log ""
	log "PATH note:"
	log "  add ${install_dir} to PATH before running elark by name"
fi
