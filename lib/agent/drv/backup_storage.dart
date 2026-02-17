import 'dart:io';

class BackupDriverCapabilities {
  const BackupDriverCapabilities({
    required this.supportsRangeRead,
    required this.supportsBatchDelete,
    required this.supportsMultipartUpload,
    required this.supportsServerSideCopy,
    required this.supportsConditionalWrite,
    required this.supportsVersioning,
    required this.maxConcurrentWrites,
    this.params = const <DriverParamDefinition>[],
  });

  final bool supportsRangeRead;
  final bool supportsBatchDelete;
  final bool supportsMultipartUpload;
  final bool supportsServerSideCopy;
  final bool supportsConditionalWrite;
  final bool supportsVersioning;
  final int maxConcurrentWrites;
  final List<DriverParamDefinition> params;
}

enum DriverParamType { number, text, boolean }

class DriverParamDefinition {
  const DriverParamDefinition({required this.key, required this.label, required this.type, this.defaultValue, this.min, this.max, this.step, this.unit, this.help});

  final String key;
  final String label;
  final DriverParamType type;
  final dynamic defaultValue;
  final num? min;
  final num? max;
  final num? step;
  final String? unit;
  final String? help;
}

abstract class BackupDriver {
  BackupDriverCapabilities get capabilities;
  String get storage;
  bool get discardWrites;
  int get bufferedBytes;

  Future<void> ensureReady();
  Future<void> prepareBackup(String serverId, String vmName);
  Future<void> uploadFile({required String relativePath, required File localFile});
  Future<List<String>> listRelativeFiles(String relativeDir);
  Future<List<int>?> readFileBytes(String relativePath);
  Future<void> freshCleanup();
  Future<void> ensureBlobDir(String hash);
  // Hard rule: always blind-write in every driver implementation.
  // No exists/list/stat/mkdir checks are allowed in this write path.
  // Existence/dir decisions are owned by agent-side cache/workers.
  Future<void> writeBlob(String hash, List<int> bytes);
  String backupCompletedMessage(String outputPath);

  Directory blobsDir();
  Directory tmpDir();
  File blobFile(String hash);

  String baseName(String path);
  String sanitizeFileName(String name);
  Future<void> cleanupInProgressFiles();
  Future<void> closeConnections();
}

abstract class RemoteBlobDriver {
  Future<bool> blobExistsRemote(String hash);
  Future<int?> blobLength(String hash);
  Stream<List<int>> openBlobStream(String hash, {int? length});
  Future<List<int>?> readBlobBytes(String hash);
}

abstract class BlobDirectoryLister {
  Future<Set<String>> listBlobShards();
  Future<Set<String>> listBlobNames(String shard);
}
