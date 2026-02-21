import 'dart:io';

import 'package:virtbackup/agent/drv/backup_storage.dart';

class DummyBackupDriver implements BackupDriver, BlobDirectoryLister {
  DummyBackupDriver(this._storageRoot, {required bool tmpWritesEnabled, required int blockSizeMB, Map<String, dynamic> driverParams = const <String, dynamic>{}})
    : _tmpWritesEnabled = tmpWritesEnabled,
      _blockSizeMB = blockSizeMB,
      _throttleBytesPerSecond = _resolveThrottleBytesPerSecond(driverParams);
  final String _storageRoot;
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
  String get storage => _rootDir().path;

  @override
  bool get discardWrites => true;

  @override
  int get bufferedBytes => 0;

  Directory _rootDir() => Directory('$_storageRoot${Platform.pathSeparator}$_appFolderName');

  @override
  Directory blobsDir() {
    return Directory('${_rootDir().path}${Platform.pathSeparator}blobs${Platform.pathSeparator}$_blockSizeMB');
  }

  @override
  Directory tmpDir() {
    return Directory('${_rootDir().path}${Platform.pathSeparator}tmp');
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
    await _rootDir().create(recursive: true);
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);
  }

  @override
  Future<void> uploadFile({required String relativePath, required File localFile}) async {
    if (!_tmpWritesEnabled) {
      return;
    }
    final finalFile = _relativeFile(relativePath);
    final tempFile = File('${finalFile.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}');
    await tempFile.parent.create(recursive: true);
    await tempFile.writeAsBytes(await localFile.readAsBytes());
    try {
      await tempFile.delete();
    } catch (_) {}
  }

  @override
  Future<List<String>> listRelativeFiles(String relativeDir) async {
    return <String>[];
  }

  @override
  Future<List<int>?> readFileBytes(String relativePath) async {
    return null;
  }

  @override
  Future<bool> deleteFile(String relativePath) async {
    final file = _relativeFile(relativePath);
    if (!await file.exists()) {
      return false;
    }
    await file.delete();
    return true;
  }

  @override
  Future<void> freshCleanup() async {
    await _deleteDirIfExists(Directory('${_rootDir().path}${Platform.pathSeparator}manifests'));
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
  String backupCompletedMessage(String outputPath) => 'Backup completed (dummy driver)';

  @override
  Future<void> cleanupInProgressFiles() async {
    await _deleteInProgressInDir(_rootDir());
    await _deleteInProgressInDir(tmpDir());
    await _deleteInProgressInDir(blobsDir());
  }

  File _relativeFile(String relativePath) {
    final normalized = relativePath
        .replaceAll('\\', Platform.pathSeparator)
        .replaceAll('/', Platform.pathSeparator)
        .split(Platform.pathSeparator)
        .where((part) => part.isNotEmpty)
        .join(Platform.pathSeparator);
    return File('${_rootDir().path}${Platform.pathSeparator}$normalized');
  }

  @override
  Future<void> closeConnections() async {
    return;
  }

  @override
  void setWriteConcurrencyLimit(int concurrency) {
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
}
