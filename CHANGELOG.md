# Changelog

## 0.9.2 - 2026-05-13

- Added VM rename support for stopped SSH-managed virtual machines, including disk file renaming.
- Added automatic restore renaming when the target VM name or disk paths already exist.
- Improved restore handling for VMs with multiple disks.
- Added disk SHA-256 metadata to backups for stronger restore validation.
- Improved restore manifest validation and user notifications.
- Hardened remote tool checks and overlay cleanup checks.
- Improved VM rename diagnostics in the agent log and console output.
- Blocked VM rename earlier when libvirt snapshots, checkpoints, active overlays, or backing chains are present.

## 0.9.0 - 2026-05-10

- Initial release.
