#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APP_NAME="${APP_NAME:-ViewDB}"
BUNDLE_ID="${BUNDLE_ID:-com.viewdb.app}"
VERSION="${VERSION:-0.1.0}"
VOL_NAME="${VOL_NAME:-$APP_NAME}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/dist}"
CREATE_APPS_LINK="${CREATE_APPS_LINK:-1}"
ICON_PATH="${ICON_PATH:-}"
SIGN_IDENTITY="${SIGN_IDENTITY:-}"

STAGE_DIR="$OUTPUT_DIR/stage"
APP_BUNDLE="$STAGE_DIR/$APP_NAME.app"
APP_CONTENTS="$APP_BUNDLE/Contents"
MACOS_DIR="$APP_CONTENTS/MacOS"
RESOURCES_DIR="$APP_CONTENTS/Resources"
DMG_PATH="$OUTPUT_DIR/$APP_NAME-$VERSION.dmg"

echo "Building release binary..."
swift build --package-path "$ROOT_DIR" -c release --product "$APP_NAME"

BIN_DIR="$(swift build --package-path "$ROOT_DIR" -c release --product "$APP_NAME" --show-bin-path)"
BIN_PATH="$BIN_DIR/$APP_NAME"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "error: expected binary not found at $BIN_PATH" >&2
  exit 1
fi

echo "Preparing app bundle..."
rm -rf "$STAGE_DIR"
mkdir -p "$MACOS_DIR" "$RESOURCES_DIR"
cp "$BIN_PATH" "$MACOS_DIR/$APP_NAME"
chmod +x "$MACOS_DIR/$APP_NAME"

cat >"$APP_CONTENTS/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundleDisplayName</key>
  <string>$APP_NAME</string>
  <key>CFBundleExecutable</key>
  <string>$APP_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleVersion</key>
  <string>$VERSION</string>
  <key>CFBundleShortVersionString</key>
  <string>$VERSION</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>LSMinimumSystemVersion</key>
  <string>26.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
EOF

if [[ -n "$ICON_PATH" ]]; then
  if [[ ! -f "$ICON_PATH" ]]; then
    echo "error: ICON_PATH file not found: $ICON_PATH" >&2
    exit 1
  fi
  cp "$ICON_PATH" "$RESOURCES_DIR/AppIcon.icns"
  /usr/libexec/PlistBuddy -c "Add :CFBundleIconFile string AppIcon" "$APP_CONTENTS/Info.plist" >/dev/null
fi

if [[ "$CREATE_APPS_LINK" == "1" ]]; then
  ln -s /Applications "$STAGE_DIR/Applications"
fi

if [[ -n "$SIGN_IDENTITY" ]]; then
  echo "Code signing app bundle..."
  codesign --force --deep --options runtime --sign "$SIGN_IDENTITY" "$APP_BUNDLE"
fi

echo "Building DMG..."
mkdir -p "$OUTPUT_DIR"
rm -f "$DMG_PATH"
hdiutil create -volname "$VOL_NAME" -srcfolder "$STAGE_DIR" -format UDZO -ov "$DMG_PATH" >/dev/null

if [[ -n "$SIGN_IDENTITY" ]]; then
  echo "Code signing DMG..."
  codesign --force --sign "$SIGN_IDENTITY" "$DMG_PATH"
fi

echo "Done: $DMG_PATH"
