# Build & Distribution

This document describes what ends up in the packaged TGZ created by `tools/build_linux.sh` and `tools/build_macos.sh`.

The TGZ filename includes the target platform and architecture before the version: `virtbackup-linux-x64-<version>.tgz`, `virtbackup-macos-arm64-<version>.tgz`, or `virtbackup-macos-x64-<version>.tgz`.
The top-level directory inside the archive remains `virtbackup-<version>`.

`tools/local_build.sh --release` builds the local platform archive, copies the TGZ to `tools/../../virtbackup_backend/public/downloads`, and copies `CHANGELOG.md` to `tools/../../virtbackup_backend/public/CHANGELOG.md` for the public release notes page. The TGZ copy fails when the release file already exists.

## Included

- App bundle from the Flutter build output.
- `virtbackup-agent` binary built from `bin/agent.dart`.
- Platform packaging scripts under `package/linux` or `package/macos`.
- `CHANGELOG.md`, `TERMS.txt`, and `LICENSE` from the project root.
- `etc/` from the project root (copied into the distribution as `etc/`).
  The Google OAuth client config is expected at `etc/google_oauth_client.json` next to the executable or under the current working directory.
- `hashblocks` binary (when available).
- Native library output under `native/linux` or `native/macos`.

## macOS requirements

- Homebrew `libssh2`.
- Homebrew `openssl@3`.

## Excluded

- Top-level `assets/` in the project root is not included in the distribution.

## User service host binding

The Linux and macOS user-service installers bind startup to the full hostname
reported by `hostname` when installing. Re-run the installer on the intended
host after changing its hostname or intentionally moving the installation.
Manual execution of `virtbackup-agent` is independent of this service binding.

Linux uses systemd `ConditionHost` (requires systemd 244 or newer), so a mismatched
host skips the service before log rotation or agent startup. macOS generates a
host-checking shell launcher beside the LaunchAgent plist. A mismatched host
exits successfully; `KeepAlive.SuccessfulExit=false` restarts failures without
restarting the rejected launch. A clean agent exit is also not restarted.
The macOS uninstaller removes the generated launcher as well as the plist.
