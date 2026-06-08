#!/bin/sh
set -eu

export LC_ALL=C
export LANG=C

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "${script_dir}/.." && pwd)

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/elark-install-test.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM

case "$(uname -s)" in
	Darwin)
		goos=darwin
		;;
	Linux)
		goos=linux
		;;
	*)
		echo "unsupported test OS: $(uname -s)" >&2
		exit 1
		;;
esac

case "$(uname -m)" in
	x86_64 | amd64)
		goarch=amd64
		;;
	arm64 | aarch64)
		goarch=arm64
		;;
	*)
		echo "unsupported test arch: $(uname -m)" >&2
		exit 1
		;;
esac

assert_contains() {
	haystack=$1
	needle=$2
	printf '%s' "$haystack" | grep -F "$needle" >/dev/null || {
		echo "missing expected output: $needle" >&2
		exit 1
	}
}

mockbin="${tmpdir}/mockbin"
release_dir="${tmpdir}/release"
payload_root="${tmpdir}/payload"
install_dir="${tmpdir}/install"
install_dir_no_install="${tmpdir}/install-no-install"
install_dir_system="${tmpdir}/install-system"
mkdir -p "$mockbin" "$release_dir" "$payload_root"

cat >"${mockbin}/curl" <<'EOF'
#!/bin/sh
set -eu
output=
url=

while [ "$#" -gt 0 ]; do
	case "$1" in
		-o)
			output=$2
			shift 2
			;;
		-H)
			shift 2
			;;
		-*)
			shift
			;;
		*)
			url=$1
			shift
			;;
	esac
done

[ -n "$output" ] || exit 2
case "$url" in
	file://*)
		cp "${url#file://}" "$output"
		;;
	*)
		echo "unexpected URL: $url" >&2
		exit 3
		;;
esac
EOF
chmod 0755 "${mockbin}/curl"

cat >"${mockbin}/sudo" <<'EOF'
#!/bin/sh
set -eu
echo "sudo $*" >>"${ELARK_INSTALL_TEST_CALLS:?}"
exec "$@"
EOF
chmod 0755 "${mockbin}/sudo"

asset_dir="${payload_root}/exec-over-lark_v9.9.9_${goos}_${goarch}"
mkdir -p "$asset_dir"

cat >"${asset_dir}/elark" <<'EOF'
#!/bin/sh
echo "fake elark $*"
EOF

cat >"${asset_dir}/elarkd" <<'EOF'
#!/bin/sh
echo "elarkd $*" >>"${ELARK_INSTALL_TEST_CALLS:?}"
echo "fake elarkd $*"
EOF

chmod 0755 "${asset_dir}/elark" "${asset_dir}/elarkd"

archive_name="exec-over-lark_v9.9.9_${goos}_${goarch}.tar.gz"
tar -C "$payload_root" -czf "${release_dir}/${archive_name}" "$(basename "$asset_dir")"

if command -v sha256sum >/dev/null 2>&1; then
	sha256sum "${release_dir}/${archive_name}" >"${release_dir}/${archive_name}.sha256"
elif command -v shasum >/dev/null 2>&1; then
	shasum -a 256 "${release_dir}/${archive_name}" >"${release_dir}/${archive_name}.sha256"
else
	echo "sha256 tool not available for test fixture" >&2
	exit 1
fi

release_json="${release_dir}/latest.json"
cat >"$release_json" <<EOF
{
  "assets": [
    {
      "browser_download_url": "file://${release_dir}/exec-over-lark_v9.9.9_${goos}_386.tar.gz"
    },
    {
      "browser_download_url": "file://${release_dir}/${archive_name}"
    },
    {
      "browser_download_url": "file://${release_dir}/${archive_name}.sha256"
    }
  ]
}
EOF

output=$(
	PATH="${mockbin}:${PATH}" \
	ELARK_RELEASE_API_URL="file://${release_json}" \
	ELARK_INSTALL_DIR="$install_dir" \
	ELARK_INSTALL_TEST_CALLS="${tmpdir}/calls-default" \
	sh "${repo_root}/scripts/install.sh" 2>&1
)

[ -x "${install_dir}/elark" ] || {
	echo "elark was not installed as an executable" >&2
	exit 1
}
[ -x "${install_dir}/elarkd" ] || {
	echo "elarkd was not installed as an executable" >&2
	exit 1
}

assert_contains "$output" "Installation directory:"
assert_contains "$output" "$install_dir"
assert_contains "$output" "elark install: starting installer for hachiwii/exec-over-lark"
assert_contains "$output" "elark install: detected platform: ${goos}/${goarch}"
assert_contains "$output" "elark install: downloading latest release metadata"
assert_contains "$output" "elark install: selected release asset: ${archive_name}"
assert_contains "$output" "elark install: installing elark to ${install_dir}/elark"
assert_contains "$output" "elark install: registering elarkd user service"
assert_contains "$output" "elark install: install complete"
assert_contains "$output" "Generate initial configuration:"
assert_contains "$output" "${install_dir}/elarkd init --client"
assert_contains "$output" "${install_dir}/elarkd init --server"
assert_contains "$output" "Daemon service:"
assert_contains "$output" "${install_dir}/elarkd install"
assert_contains "$output" "Start background process:"
assert_contains "$output" "${install_dir}/elarkd start"
assert_contains "$(cat "${tmpdir}/calls-default")" "elarkd install"

output_no_install=$(
	PATH="${mockbin}:${PATH}" \
	ELARK_RELEASE_API_URL="file://${release_json}" \
	ELARK_INSTALL_DIR="$install_dir_no_install" \
	ELARK_INSTALL_TEST_CALLS="${tmpdir}/calls-no-install" \
	sh "${repo_root}/scripts/install.sh" --no-install 2>&1
)

[ -x "${install_dir_no_install}/elarkd" ] || {
	echo "elarkd was not installed for --no-install run" >&2
	exit 1
}
if [ -e "${tmpdir}/calls-no-install" ] && grep -F "elarkd install" "${tmpdir}/calls-no-install" >/dev/null; then
	echo "--no-install still called elarkd install" >&2
	exit 1
fi
assert_contains "$output_no_install" "elark install: skipping daemon service registration (--no-install)"
assert_contains "$output_no_install" "${install_dir_no_install}/elarkd install"
assert_contains "$output_no_install" "${install_dir_no_install}/elarkd start"

output_system=$(
	PATH="${mockbin}:${PATH}" \
	ELARK_RELEASE_API_URL="file://${release_json}" \
	ELARK_INSTALL_DIR="$install_dir_system" \
	ELARK_INSTALL_TEST_CALLS="${tmpdir}/calls-system" \
	sh "${repo_root}/scripts/install.sh" --system 2>&1
)

assert_contains "$output_system" "elark install: registering elarkd as a system service"
assert_contains "$output_system" "sudo ${install_dir_system}/elarkd install --system"
assert_contains "$output_system" "sudo ${install_dir_system}/elarkd start --system"
assert_contains "$(cat "${tmpdir}/calls-system")" "sudo ${install_dir_system}/elarkd install --system"
assert_contains "$(cat "${tmpdir}/calls-system")" "elarkd install --system"
