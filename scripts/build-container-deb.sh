#!/bin/bash
# build-container-deb.sh — Build DEB packages inside a container
#
# Expected env vars:
#   PLATFORM_ID       — e.g. "debian-12", "ubuntu-22.04"
#   PLATFORM_CODENAME — e.g. "bookworm", "jammy"
#   EXPECTED_ARCH     — "amd64" or "arm64"
#   MODULE_VERSION    — e.g. "0.1.0" or "0.1.0-dev+abc1234"
#
# Expected mounts:
#   /source    — source tarball (valkey-luajit-VERSION.tar.gz)
#   /packaging — packaging/ directory from repo
#   /scripts   — scripts/ directory from repo
#   /output    — directory for built DEBs
set -euo pipefail

echo "==> Building DEB for valkey-luajit ${MODULE_VERSION} on ${PLATFORM_ID} (${EXPECTED_ARCH})"

export DEBIAN_FRONTEND=noninteractive

# ── Step 1: Install build dependencies ──
apt-get update
apt-get install -y --no-install-recommends \
    build-essential \
    debhelper-compat \
    devscripts \
    cmake \
    fakeroot

# ── Step 2: Extract source and set up debian/ ──
DEB_VERSION=$(echo "$MODULE_VERSION" | tr - '~')

BUILDDIR="/tmp/build"
mkdir -p "$BUILDDIR"
cd "$BUILDDIR"

tar xzf /source/valkey-luajit-${MODULE_VERSION}.tar.gz
SRCDIR="$BUILDDIR/valkey-luajit-${MODULE_VERSION}"
cd "$SRCDIR"

# Copy debian packaging into source
cp -r /packaging/debian .

# On older distros that need the compat file, keep it; on newer ones
# debhelper-compat in Build-Depends is sufficient
DH_VERSION=$(dpkg-query -W -f='${Version}' debhelper 2>/dev/null || echo "0")
DH_MAJOR=$(echo "$DH_VERSION" | cut -d. -f1)
if [ "$DH_MAJOR" -ge 13 ] 2>/dev/null; then
    rm -f debian/compat
fi

# ── Step 3: Update changelog with correct version and codename ──
DISTRO_TAG=$(echo "$PLATFORM_ID" | tr '-' '.')
dch --newversion "${DEB_VERSION}-1~${DISTRO_TAG}" \
    --distribution "$PLATFORM_CODENAME" \
    --urgency medium \
    "Build for ${PLATFORM_ID} — version ${MODULE_VERSION}"

# Create orig tarball (required by quilt format)
cp /source/valkey-luajit-${MODULE_VERSION}.tar.gz \
   "$BUILDDIR/valkey-luajit_${DEB_VERSION}.orig.tar.gz"

# ── Step 4: Build ──
echo "==> Running dpkg-buildpackage"
dpkg-buildpackage -b -us -uc

# ── Step 5: Sanity checks ──
echo "==> Sanity checks"
# Match only the main package, not the -dbgsym package
DEB_FILE=$(find "$BUILDDIR" -maxdepth 1 -name "valkey-luajit_*.deb" ! -name "*-dbgsym*" | head -1)
if [ -z "$DEB_FILE" ]; then
    echo "ERROR: No DEB produced!" >&2
    exit 1
fi

# Check the .so is inside (capture output to avoid SIGPIPE with pipefail)
DEB_CONTENTS=$(dpkg-deb -c "$DEB_FILE" || true)
if ! echo "$DEB_CONTENTS" | grep -q 'libvalkeyluajit.so'; then
    echo "ERROR: libvalkeyluajit.so not found in DEB!" >&2
    exit 1
fi

# Check architecture
DEB_ARCH=$(dpkg-deb --info "$DEB_FILE" | grep '^ Architecture:' | awk '{print $2}')
if [ "$DEB_ARCH" != "$EXPECTED_ARCH" ]; then
    echo "ERROR: Expected arch ${EXPECTED_ARCH}, got ${DEB_ARCH}" >&2
    exit 1
fi

echo "==> DEB built successfully: $(basename "$DEB_FILE")"

# ── Step 6: Copy DEBs to output ──
cp "$BUILDDIR"/valkey-luajit_*.deb /output/
cp "$BUILDDIR"/valkey-luajit_*.changes /output/ 2>/dev/null || true
cp "$BUILDDIR"/valkey-luajit_*.buildinfo /output/ 2>/dev/null || true

echo "==> Output:"
ls -la /output/valkey-luajit*
