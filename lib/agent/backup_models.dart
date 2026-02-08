part of 'backup.dart';

class _MissingRun {
  _MissingRun(this.startIndex, this.hashes);

  final int startIndex;
  final List<String> hashes;
}

class _MissingEntry {
  const _MissingEntry(this.index, this.hash);

  final int index;
  final String hash;
}

class _MissingBlock {
  const _MissingBlock(this.hash, this.bytes);

  final String hash;
  final Uint8List bytes;
}

class _WriteResult {
  const _WriteResult(this.future, this.wrote);

  final Future<bool> future;
  final bool wrote;
}

class _BackupCanceled implements Exception {
  const _BackupCanceled();

  @override
  String toString() => 'Canceled';
}

class _DiskChainItem {
  const _DiskChainItem({required this.sourcePath, required this.diskId});

  final String sourcePath;
  final String diskId;
}

class _DiskBackupPlan {
  const _DiskBackupPlan({required this.topDiskId, required this.target, required this.chain});

  final String topDiskId;
  final String target;
  final List<_DiskChainItem> chain;
}
