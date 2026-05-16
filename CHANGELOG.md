# Changelog

## 0.9.4 - 2026-05-16

- Added restore schedules that can restore the latest complete XML for every VM in the selected storage.
- Manual schedule runs can now wait in the schedule queue when concurrency guards are active, and the GUI includes a Queue view for running and waiting schedule runs.
- Improved cancellation handling for backups, restores, checks, and schedule runs, including stuck restore workers and slow storage operations.
- Hardened Google Drive writes with more reliable folder caching, transient retry handling, and better retry diagnostics.
- Improved job notifications with richer error, warning, source, target, and storage context.
- Restore definitions now strip stale libvirt `backingStore` metadata, and VM rename is blocked when inactive XML still contains backing metadata.
- Server event refreshes reuse the latest known required-tool state instead of repeating required-tool checks each time.
- Email test failures now show backend status and response body details in a dialog that the user must close.
- The browser account callback page now uses styled HTML and redirects successful sign-ins to the configured Virt Backup website.
- Restore job result notifications now report meaningful restore throughput and byte totals instead of zero-valued physical transfer fields.
- Google Drive restores now acknowledge Drive abuse warnings for backup blob downloads and stop retrying permanent abusive-file download failures.
- VM rename preview now logs expected blockers without stack traces, and Manage keeps action progress inside the affected VM row.

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
