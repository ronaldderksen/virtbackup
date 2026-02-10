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
  String get destination;
  bool get discardWrites;
  int get bufferedBytes;

  Future<void> ensureReady();
  Future<void> prepareBackup(String serverId, String vmName);
  DriverFileWrite startXmlWrite(String serverId, String vmName, String timestamp);
  DriverFileWrite startChainWrite(String serverId, String vmName, String timestamp, String diskId);
  DriverManifestWrite startManifestWrite(String serverId, String vmName, String diskId, String timestamp);
  Future<void> finalizeManifest(DriverManifestWrite write);
  Future<void> freshCleanup();
  Future<void> ensureBlobDir(String hash);
  Future<bool> writeBlobIfMissing(String hash, List<int> bytes);
  Future<bool> blobExists(String hash);
  String backupCompletedMessage(String manifestsPath);

  Directory manifestsDir(String serverId, String vmName);
  Directory blobsDir();
  Directory tmpDir();

  File xmlFile(String serverId, String vmName, String timestamp, {required bool inProgress});
  File chainFile(String serverId, String vmName, String timestamp, String diskId, {required bool inProgress});
  File manifestFile(String serverId, String vmName, String diskId, String timestamp, {required bool inProgress});
  File manifestGzFile(String serverId, String vmName, String diskId, String timestamp);
  File blobFile(String hash);

  Stream<File> listXmlFiles();
  RestoreLocation? restoreLocationFromXml(File xmlFile);
  Future<File?> findManifestForTimestamp(Directory vmDir, String timestamp, String diskBaseName);
  Future<Directory?> findDiskDirForTimestamp(Directory vmDir, String timestamp, String diskBaseName);
  File? findChainFileForTimestamp(Directory vmDir, Directory? diskDir, String timestamp, String diskBaseName);
  Future<List<File>> listManifestsForTimestamp(Directory vmDir, String timestamp);
  Future<File?> findManifestBySourcePath(Directory vmDir, String timestamp, String sourcePath, Future<String?> Function(File manifest) readSourcePath);
  Future<File?> findManifestForChainEntry(Directory vmDir, String timestamp, String diskId, String sourcePath, Future<String?> Function(File manifest) readSourcePath);
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
  Future<Set<String>> listBlobShard1();
  Future<Set<String>> listBlobShard2(String shard1);
  Future<Set<String>> listBlobNames(String shard1, String shard2);
}

class DriverFileWrite {
  DriverFileWrite({required this.sink, required this.commit});

  final IOSink sink;
  final Future<void> Function() commit;
}

class DriverManifestWrite {
  DriverManifestWrite({required this.sink, required this.commit});

  final IOSink sink;
  final Future<void> Function() commit;
}

class RestoreLocation {
  const RestoreLocation({required this.vmDir, required this.serverId, required this.vmName});

  final Directory vmDir;
  final String serverId;
  final String vmName;
}
