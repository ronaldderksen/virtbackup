# Changelog

## 0.9.3 - 2026-05-14

- Schedules can now back up all VMs on a server. The VM list is fixed when the schedule starts, so VMs added during the run are picked up next time.
- Restores are safer: disk files are first uploaded as temporary files and are only moved into place when the VM is ready to be defined.
- Automatic restore renaming is more reliable when the VM name or disk path already exists.
- SFTP backups now handle "file already exists" conflicts more safely by checking the existing backup block before continuing.
- Backups with many repeated blocks now avoid uploading the same new block more than once during a single run.
- Improved SFTP retry handling to prevent one temporary write problem from being reported twice.
- Added job result email notifications through the Virt Backup backend and Mailgun, including a GUI test email button.

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
