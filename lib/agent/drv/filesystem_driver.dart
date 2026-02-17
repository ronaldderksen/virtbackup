import 'dart:io';

import 'package:virtbackup/agent/drv/backup_storage.dart';

class FilesystemBackupDriver implements BackupDriver, BlobDirectoryLister {
  FilesystemBackupDriver(this._storageRoot, {required int blockSizeMB}) : _blockSizeMB = blockSizeMB;

  final String _storageRoot;
  final int _blockSizeMB;
  static const String _appFolderName = 'VirtBackup';

  @override
  BackupDriverCapabilities get capabilities => const BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: true,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: 16,
  );

  @override
  String get storage => _rootDir().path;

  @override
  bool get discardWrites => false;

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
  Future<Set<String>> listBlobShards() async {
    final root = blobsDir();
    if (!await root.exists()) {
      return <String>{};
    }
    final names = <String>{};
    await for (final entity in root.list(followLinks: false)) {
      if (entity is Directory) {
        names.add(baseName(entity.path));
      }
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    final path = '${blobsDir().path}${Platform.pathSeparator}$shard';
    final dir = Directory(path);
    if (!await dir.exists()) {
      return <String>{};
    }
    final names = <String>{};
    await for (final entity in dir.list(followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = baseName(entity.path);
      if (name.endsWith('.inprogress')) {
        continue;
      }
      names.add(name);
    }
    return names;
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
  }

  @override
  Future<void> uploadFile({required String relativePath, required File localFile}) async {
    final finalFile = _relativeFile(relativePath);
    final tempFile = File('${finalFile.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}');
    await tempFile.parent.create(recursive: true);
    await tempFile.writeAsBytes(await localFile.readAsBytes());
    if (await finalFile.exists()) {
      await finalFile.delete();
    }
    await tempFile.rename(finalFile.path);
  }

  @override
  Future<List<String>> listRelativeFiles(String relativeDir) async {
    final normalized = _normalizeRelativePath(relativeDir);
    final root = Directory('${_rootDir().path}${Platform.pathSeparator}${normalized.replaceAll('/', Platform.pathSeparator)}');
    if (!await root.exists()) {
      return <String>[];
    }
    final result = <String>[];
    await for (final entity in root.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final rel = _relativePath(from: root.path, to: entity.path);
      if (rel.isEmpty) {
        continue;
      }
      result.add('$normalized/$rel');
    }
    result.sort();
    return result;
  }

  @override
  Future<List<int>?> readFileBytes(String relativePath) async {
    final file = _relativeFile(relativePath);
    if (!await file.exists()) {
      return null;
    }
    return file.readAsBytes();
  }

  @override
  Future<void> freshCleanup() async {
    await _deleteDirIfExists(Directory('${_rootDir().path}${Platform.pathSeparator}manifests'));
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 2) {
      return;
    }
    final shardKey = hash.substring(0, 2);
    final dir = Directory('${blobsDir().path}${Platform.pathSeparator}$shardKey');
    await dir.create(recursive: true);
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2) {
      return;
    }
    final blob = blobFile(hash);
    final tempPath = '${blob.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final tempFile = File(tempPath);
    await tempFile.writeAsBytes(bytes);
    try {
      await tempFile.rename(blob.path);
    } catch (_) {
      try {
        await tempFile.delete();
      } catch (_) {}
    }
  }

  @override
  String backupCompletedMessage(String outputPath) {
    return 'Backup saved to $outputPath';
  }

  @override
  Future<void> cleanupInProgressFiles() async {
    await _deleteInProgressInDir(_rootDir());
    await _deleteInProgressInDir(tmpDir());
    await _deleteInProgressInDir(blobsDir());
  }

  File _relativeFile(String relativePath) {
    final normalized = _normalizeRelativePath(relativePath).replaceAll('/', Platform.pathSeparator);
    return File('${_rootDir().path}${Platform.pathSeparator}$normalized');
  }

  String _normalizeRelativePath(String relativePath) {
    return relativePath.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/');
  }

  String _relativePath({required String from, required String to}) {
    final fromAbs = Directory(from).absolute.path;
    final toAbs = File(to).absolute.path;
    final prefix = fromAbs.endsWith(Platform.pathSeparator) ? fromAbs : '$fromAbs${Platform.pathSeparator}';
    if (!toAbs.startsWith(prefix)) {
      return '';
    }
    return toAbs.substring(prefix.length).replaceAll('\\', '/');
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
}
