import 'dart:io';

import 'package:virtbackup/agent/drv/backup_storage.dart';

class DummyBackupDriver implements BackupDriver, BlobDirectoryLister {
  DummyBackupDriver(this._destination, {required bool tmpWritesEnabled, required int blockSizeMB, Map<String, dynamic> driverParams = const <String, dynamic>{}})
    : _tmpWritesEnabled = tmpWritesEnabled,
      _blockSizeMB = blockSizeMB,
      _throttleBytesPerSecond = _resolveThrottleBytesPerSecond(driverParams);
  final String _destination;
  final int _blockSizeMB;
  static const String _appFolderName = 'VirtBackup';
  final bool _tmpWritesEnabled;
  final int? _throttleBytesPerSecond;
  final Stopwatch _throttleClock = Stopwatch();
  int _throttleBytes = 0;

  @override
  BackupDriverCapabilities get capabilities => const BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: true,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: 1,
    params: [DriverParamDefinition(key: 'throttleMbps', label: 'Dummy throttle (MB/s)', type: DriverParamType.number, min: 0, unit: 'MB/s', help: 'Leave empty or 0 for unlimited.')],
  );

  @override
  String get destination => _rootDir().path;

  @override
  bool get discardWrites => true;

  @override
  int get bufferedBytes => 0;

  Directory _rootDir() => Directory('$_destination${Platform.pathSeparator}$_appFolderName');

  Directory _manifestsRoot() => Directory('${_rootDir().path}${Platform.pathSeparator}manifests');

  @override
  Directory manifestsDir(String serverId, String vmName) {
    return Directory('${_manifestsRoot().path}${Platform.pathSeparator}$serverId${Platform.pathSeparator}$vmName');
  }

  @override
  Directory blobsDir() {
    return Directory('${_rootDir().path}${Platform.pathSeparator}blobs${Platform.pathSeparator}$_blockSizeMB');
  }

  @override
  Directory tmpDir() {
    return Directory('${_rootDir().path}${Platform.pathSeparator}tmp');
  }

  @override
  File xmlFile(String serverId, String vmName, String timestamp, {required bool inProgress}) {
    final suffix = inProgress ? '.inprogress' : '';
    final base = manifestsDir(serverId, vmName).path;
    return File('$base${Platform.pathSeparator}${timestamp}__domain.xml$suffix');
  }

  @override
  File chainFile(String serverId, String vmName, String timestamp, String diskId, {required bool inProgress}) {
    final suffix = inProgress ? '.inprogress' : '';
    final base = manifestsDir(serverId, vmName).path;
    return File('$base${Platform.pathSeparator}${timestamp}__$diskId.chain$suffix');
  }

  @override
  File manifestFile(String serverId, String vmName, String diskId, String timestamp, {required bool inProgress}) {
    final suffix = inProgress ? '.inprogress' : '';
    final base = '${manifestsDir(serverId, vmName).path}${Platform.pathSeparator}$diskId';
    return File('$base${Platform.pathSeparator}$timestamp.manifest$suffix');
  }

  @override
  File manifestGzFile(String serverId, String vmName, String diskId, String timestamp) {
    final base = '${manifestsDir(serverId, vmName).path}${Platform.pathSeparator}$diskId';
    return File('$base${Platform.pathSeparator}$timestamp.manifest.gz');
  }

  @override
  File blobFile(String hash) {
    if (hash.length < 2) {
      return File('${blobsDir().path}${Platform.pathSeparator}$hash');
    }
    final shard = hash.substring(0, 2);
    return File('${blobsDir().path}${Platform.pathSeparator}$shard${Platform.pathSeparator}$hash');
  }

  @override
  Stream<File> listXmlFiles() async* {
    final root = _rootDir();
    if (!await root.exists()) {
      return;
    }
    await for (final entity in root.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = baseName(entity.path);
      if (name.endsWith('.xml') && !name.endsWith('.xml.inprogress')) {
        yield entity;
      }
    }
  }

  @override
  RestoreLocation? restoreLocationFromXml(File xmlFile) {
    final vmDir = xmlFile.parent;
    final serverDir = vmDir.parent;
    final serverId = baseName(serverDir.path);
    final vmName = baseName(vmDir.path);
    if (serverId.isEmpty || vmName.isEmpty) {
      return null;
    }
    return RestoreLocation(vmDir: vmDir, serverId: serverId, vmName: vmName);
  }

  @override
  Future<File?> findManifestForTimestamp(Directory vmDir, String timestamp, String diskBaseName) async {
    final diskId = sanitizeFileName(diskBaseName);
    await for (final entity in vmDir.list(followLinks: false)) {
      if (entity is! Directory && entity is! Link) {
        continue;
      }
      final name = baseName(entity.path);
      if (name != diskId && name != diskBaseName) {
        continue;
      }
      final manifest = await _findManifestFileForTimestamp(entity.path, timestamp);
      if (manifest != null) {
        return manifest;
      }
    }
    return null;
  }

  @override
  Future<Directory?> findDiskDirForTimestamp(Directory vmDir, String timestamp, String diskBaseName) async {
    final diskId = sanitizeFileName(diskBaseName);
    await for (final entity in vmDir.list(followLinks: false)) {
      if (entity is! Directory && entity is! Link) {
        continue;
      }
      final name = baseName(entity.path);
      if (name != diskId && name != diskBaseName) {
        continue;
      }
      final diskDir = entity is Directory ? entity : Directory(entity.path);
      final manifest = await _findManifestFileForTimestamp(diskDir.path, timestamp);
      final chainFile = findChainFileForTimestamp(vmDir, diskDir, timestamp, diskBaseName);
      if (manifest != null || chainFile != null) {
        return diskDir;
      }
    }
    return null;
  }

  @override
  File? findChainFileForTimestamp(Directory vmDir, Directory? diskDir, String timestamp, String diskBaseName) {
    final diskId = sanitizeFileName(diskBaseName);
    final vmChainFile = File('${vmDir.path}${Platform.pathSeparator}${timestamp}__$diskId.chain');
    if (vmChainFile.existsSync()) {
      return vmChainFile;
    }
    return null;
  }

  @override
  Future<List<File>> listManifestsForTimestamp(Directory vmDir, String timestamp) async {
    final manifests = <File>[];
    await for (final entity in vmDir.list(followLinks: false)) {
      if (entity is! Directory && entity is! Link) {
        continue;
      }
      final manifest = await _findManifestFileForTimestamp(entity.path, timestamp);
      if (manifest != null) {
        manifests.add(manifest);
      }
    }
    return manifests;
  }

  @override
  Future<File?> findManifestBySourcePath(Directory vmDir, String timestamp, String sourcePath, Future<String?> Function(File manifest) readSourcePath) async {
    await for (final entity in vmDir.list(followLinks: false)) {
      if (entity is! Directory && entity is! Link) {
        continue;
      }
      final manifest = await _findManifestFileForTimestamp(entity.path, timestamp);
      if (manifest == null) {
        continue;
      }
      final storedPath = await readSourcePath(manifest);
      if (storedPath != null && storedPath.trim() == sourcePath.trim()) {
        return manifest;
      }
    }
    return null;
  }

  @override
  Future<File?> findManifestForChainEntry(Directory vmDir, String timestamp, String diskId, String sourcePath, Future<String?> Function(File manifest) readSourcePath) async {
    final byDiskId = await findManifestForTimestamp(vmDir, timestamp, diskId);
    if (byDiskId != null) {
      final storedPath = await readSourcePath(byDiskId);
      if (storedPath != null && storedPath.trim() == sourcePath.trim()) {
        return byDiskId;
      }
    }
    return findManifestBySourcePath(vmDir, timestamp, sourcePath, readSourcePath);
  }

  @override
  String baseName(String path) {
    final parts = path.split(RegExp(r'[\\/]')).where((part) => part.isNotEmpty).toList();
    return parts.isEmpty ? path : parts.last;
  }

  @override
  String sanitizeFileName(String name) {
    return name.trim().replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
  }

  @override
  Future<void> ensureReady() async {
    await _rootDir().create(recursive: true);
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    await manifestsDir(serverId, vmName).create(recursive: true);
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);
  }

  @override
  DriverFileWrite startXmlWrite(String serverId, String vmName, String timestamp) {
    final file = xmlFile(serverId, vmName, timestamp, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverFileWrite(
      sink: sink,
      commit: () async {
        if (await file.exists()) {
          await file.delete();
        }
      },
    );
  }

  @override
  DriverFileWrite startChainWrite(String serverId, String vmName, String timestamp, String diskId) {
    final file = chainFile(serverId, vmName, timestamp, diskId, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverFileWrite(
      sink: sink,
      commit: () async {
        if (await file.exists()) {
          await file.delete();
        }
      },
    );
  }

  @override
  DriverManifestWrite startManifestWrite(String serverId, String vmName, String diskId, String timestamp) {
    final file = manifestFile(serverId, vmName, diskId, timestamp, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverManifestWrite(
      sink: sink,
      commit: () async {
        if (await file.exists()) {
          await file.delete();
        }
      },
    );
  }

  @override
  Future<void> finalizeManifest(DriverManifestWrite write) async {
    await write.commit();
  }

  @override
  Future<void> freshCleanup() async {
    await _deleteDirIfExists(_manifestsRoot());
    await _deleteDirIfExists(blobsDir());
  }

  @override
  Future<void> ensureBlobDir(String hash) async {}

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2) {
      return;
    }
    await _simulateWriteDelay(bytes.length);
    if (_tmpWritesEnabled) {
      final tempPath = '${tmpDir().path}${Platform.pathSeparator}$hash.inprogress.${DateTime.now().microsecondsSinceEpoch}';
      final tempFile = File(tempPath);
      await tempFile.writeAsBytes(bytes);
      try {
        if (await tempFile.exists()) {
          await tempFile.delete();
        }
      } catch (_) {}
    }
  }

  Future<void> _simulateWriteDelay(int byteCount) async {
    if (byteCount <= 0) {
      return;
    }
    final throttleBytesPerSecond = _throttleBytesPerSecond;
    if (throttleBytesPerSecond == null) {
      return;
    }
    final micros = (byteCount * 1000000 / throttleBytesPerSecond).round();
    if (micros <= 0) {
      return;
    }
    if (!_throttleClock.isRunning) {
      _throttleClock.start();
    }
    _throttleBytes += byteCount;
    final elapsedMicros = _throttleClock.elapsedMicroseconds;
    final targetMicros = (_throttleBytes * 1000000 / throttleBytesPerSecond).round();
    final remainingMicros = targetMicros - elapsedMicros;
    if (remainingMicros > 0) {
      await Future.delayed(Duration(microseconds: remainingMicros));
    }
  }

  static int? _resolveThrottleBytesPerSecond(Map<String, dynamic> driverParams) {
    final raw = driverParams['throttleMbps'];
    if (raw == null) {
      return null;
    }
    final parsed = raw is num ? raw.toDouble() : double.tryParse(raw.toString());
    if (parsed == null || parsed <= 0) {
      return null;
    }
    return (parsed * 1024 * 1024).round();
  }

  @override
  Future<Set<String>> listBlobShards() async {
    return <String>{};
  }

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    return <String>{};
  }

  @override
  String backupCompletedMessage(String manifestsPath) => 'Backup completed (dummy driver)';

  @override
  Future<void> cleanupInProgressFiles() async {
    await _deleteInProgressInDir(_manifestsRoot());
    await _deleteInProgressInDir(tmpDir());
    await _deleteInProgressInDir(blobsDir());
  }

  @override
  Future<void> closeConnections() async {
    return;
  }

  Future<void> _deleteDirIfExists(Directory dir) async {
    if (!await dir.exists()) {
      return;
    }
    await dir.delete(recursive: true);
  }

  Future<void> _deleteInProgressInDir(Directory dir) async {
    if (!await dir.exists()) {
      return;
    }
    await for (final entity in dir.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      if (!entity.path.endsWith('.inprogress')) {
        continue;
      }
      try {
        await entity.delete();
      } catch (_) {}
    }
  }

  Future<File?> _findManifestFileForTimestamp(String directoryPath, String timestamp) async {
    final manifest = File('$directoryPath${Platform.pathSeparator}$timestamp.manifest');
    if (await manifest.exists()) {
      return manifest;
    }
    final gzManifest = File('$directoryPath${Platform.pathSeparator}$timestamp.manifest.gz');
    if (await gzManifest.exists()) {
      return gzManifest;
    }
    return null;
  }
}
