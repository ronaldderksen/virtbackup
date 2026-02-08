# hashblocks

Fast block-hash tool used by the VirtBackup agent.

## Usage

```bash
./hashblocks --read /path/to/vm.qcow2 --block-size 1048576
```

## Output format

- Normal blocks:
  ```
  0 -> <sha256>
  1 -> <sha256>
  ```
- Zero blocks:
  ```
  6 -> ZERO
  100-120 -> ZERO
  ```
- End of file:
  ```
  EOF
  ```

Long ZERO runs may be emitted in chunks (e.g. `0-4095 -> ZERO`, `4096-8191 -> ZERO`) to keep progress visible.

## Notes

- Reads the file sequentially.
- Block index is **0-based**.
- Default block size is **8192** bytes (the VirtBackup agent passes its own block size).
- Hash algorithm: **SHA-256** (OpenSSL EVP).

## Control (stdin)

`hashblocks` can be controlled via stdin while it is running:
- `LIMIT <index>`: throttle reading/hashing so the tool will not advance past the given 0-based block index.
- `STOP`: stop immediately (also triggered when stdin reaches EOF).

This is used by the VirtBackup agent to apply backpressure (for example when the writer backlog grows).

## Build

Dynamic build (default):

```bash
cd hashblocks
make
cd ..
```

Static build (if `libcrypto.a` is available):

```bash
cd hashblocks
make STATIC=1
cd ..
```

## Dependencies (Rocky 9)

```bash
sudo dnf install -y gcc make openssl-devel
```

Static build requires additional packages (if available in your repos):

```bash
sudo dnf install -y openssl-static glibc-static
```
