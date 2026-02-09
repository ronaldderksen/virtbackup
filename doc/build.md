# Build & Distribution

This document describes what ends up in the packaged TGZ created by `tools/build_linux.sh` and `tools/build_macos.sh`.

## Included

- App bundle from the Flutter build output.
- `virtbackup-agent` binary built from `bin/agent.dart`.
- Platform packaging scripts under `package/linux` or `package/macos`.
- `etc/` from the project root (copied into the distribution as `etc/`).
  The Google OAuth client config is expected at `etc/google_oauth_client.json` next to the executable or under the current working directory.
- `hashblocks` binary (when available).
- Native library output under `native/linux` or `native/macos`.

## Excluded

- Top-level `assets/` in the project root is not included in the distribution.
