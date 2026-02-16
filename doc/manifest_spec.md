# Unified Backup Manifest Specification (Version 1)

Status: agreed design specification.

This document defines the required manifest format for backups and restore validation.

## Goals

- A single manifest file is sufficient for restore context.
- No separate sidecar files are required for restore metadata.
- Strict end-of-file validation is required to detect incomplete manifests.

## Versioning

- Manifest `version` remains `1`.
- No fallback behavior to an older structure is allowed.

## Required Top-Level Fields

The following fields are required in each manifest:

- `version`
- `block_size`
- `server_id`
- `vm_name`
- `disk_id`
- `source_path`
- `timestamp`
- `meta`
- `domain_xml_b64_gz`
- `chain`
- `blocks`
- `EOF` terminator line

`file_size` remains optional when unknown.

## Metadata Embedding

The manifest embeds the metadata that was previously stored outside the manifest.

### `domain_xml_b64_gz`

- Contains the VM domain XML as `base64(gzip(xml_bytes))`.
- Restore must decode base64 and gunzip successfully.
- Decode/decompress failure is a hard restore error.

### `chain`

- Contains disk chain data directly in the manifest.
- YAML list format is required.
- Empty list is allowed when no chain is present.
- Invalid structure is a hard restore error.

Example chain item:

```yaml
- order: 0
  disk_id: vda.overlay
  path: /var/lib/libvirt/images/vm.overlay.qcow2
```

## Block Section

`blocks:` contains deduplicated block mappings, for example:

```yaml
blocks:
  0 -> <sha256>
  1 -> <sha256>
  2-10 -> ZERO
```

Rules:

- Hash entries map block index to blob hash.
- `ZERO` run entries represent zero-filled ranges.
- `block_size` is stored in bytes.

## Mandatory File Completion Marker

After the final block entry:

1. A literal `EOF` line is required.
2. A trailing empty line is optional and intended for readability.

Example ending:

```text
120-140 -> ZERO
EOF

```

## Restore Validation Rules

Restore must fail hard if any of the following is true:

- `EOF` line is missing.
- Embedded `domain_xml_b64_gz` is missing or invalid.
- Embedded `chain` is missing or invalid.
- Manifest shape deviates from this specification.

Error message for missing terminator condition:

- `manifest incomplete`

## No Fallback Policy

- No fallback to sidecar `domain.xml` or chain file is allowed.
- No fallback to pre-existing legacy manifest interpretation is allowed.
