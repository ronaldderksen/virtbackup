# Virt Backup

Website: [virtbackup.net](https://virtbackup.net/)

Virt Backup is a Flutter app plus a local agent for backing up and restoring libvirt/QEMU virtual machines over SSH/SFTP.

## What It Does

- Discovers VM inventory on a remote hypervisor (via `virsh` over SSH).
- Creates consistent backups (snapshot when the VM is running).
- Deduplicates disk data into a shared, content-addressed blob store (SHA-256).
- Restores VMs from manifests + blobs back to remote disks via SFTP.
- Exposes an HTTPS API for orchestration, job tracking, and Server-Sent Events (SSE).

## Storage Backends (Drivers)

- `filesystem`: stores manifests/blobs on a local path you choose.
- `gdrive` (preview): stores manifests/blobs in Google Drive (with a local cache).
- `dummy`: discards writes (useful for testing backpressure/progress behavior).

## Components

- GUI: Flutter desktop UI.
- Agent: local HTTPS server that runs backup/restore jobs and talks to hypervisors via SSH/SFTP.
- `hashblocks`: small helper binary uploaded to the hypervisor to hash disk blocks remotely (used when available).

## Docs

- Agent internals: `doc/agent.md`
- Agent API: `doc/api.md`
- Dedup details (current implementation): `doc/dedup_plan.md`

## Security Notes

- The agent uses a self-signed TLS certificate generated on first start.
- The agent requires an auth token on every request.
- SSH host key verification is currently a known TODO (see `doc/TODO.md`).

## Disclaimer (Data Loss / No Warranty)

This project can delete, overwrite, or corrupt virtual machine disk data if misconfigured or if a bug is triggered. Use it at your own risk.

- Always test restores before relying on it.
- Keep independent backups and verify integrity.
- The software is provided "AS IS", without warranty of any kind, and the authors/copyright holders are not liable for any claim or damages.

## Attribution

The Virt Backup concept and product design were conceived by Ronald Derksen.
The code in this repository was produced with OpenAI Codex under Ronald's direction.
