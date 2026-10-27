import 'dart:io';

import 'package:virtbackup/common/models.dart' show BackupDriverCapabilities;

export 'package:virtbackup/common/models.dart' show BackupDriverCapabilities, DriverParamDefinition, DriverParamType;

abstract class BackupDriver {
  BackupDriverCapabilities get capabilities;
  String get storage;
  bool get discardWrites;
  int get bufferedBytes;

  Future<void> ensureReady();
  Future<void> prepareBackup(String serverId, String vmName);
  Future<void> uploadFile({required String relativePath, required File localFile});
  Future<bool> deleteFile(String relativePath);
  Future<List<String>> listRelativeFiles(String relativeDir);
  Future<List<int>?> readFileBytes(String relativePath);
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

  void setWriteConcurrencyLimit(int concurrency) {}

  void setReadConcurrencyLimit(int concurrency) {}
}

class BackupWriteConflictMismatch implements Exception {
  const BackupWriteConflictMismatch(this.message);

  final String message;

  @override
  String toString() => message;
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
