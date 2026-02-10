import 'dart:io';

import 'package:virtbackup/agent/drv/backup_storage.dart';

class FilesystemBackupDriver implements BackupDriver, BlobDirectoryLister {
  FilesystemBackupDriver(this._destination);

  final String _destination;
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
  String get destination => _rootDir().path;

  @override
  bool get discardWrites => false;

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
    return Directory('${_rootDir().path}${Platform.pathSeparator}blobs');
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
    if (hash.length < 4) {
      return File('${blobsDir().path}${Platform.pathSeparator}$hash');
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    return File('${blobsDir().path}${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2${Platform.pathSeparator}$hash');
  }

  @override
  Future<Set<String>> listBlobShard1() async {
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
  Future<Set<String>> listBlobShard2(String shard1) async {
    final path = '${blobsDir().path}${Platform.pathSeparator}$shard1';
    final dir = Directory(path);
    if (!await dir.exists()) {
      return <String>{};
    }
    final names = <String>{};
    await for (final entity in dir.list(followLinks: false)) {
      if (entity is Directory) {
        names.add(baseName(entity.path));
      }
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobNames(String shard1, String shard2) async {
    final path = '${blobsDir().path}${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2';
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
  Stream<File> listXmlFiles() async* {
    final root = _manifestsRoot();
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
    if (diskDir != null) {
      final legacy = File('${diskDir.path}${Platform.pathSeparator}$timestamp.chain');
      if (legacy.existsSync()) {
        return legacy;
      }
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
  }

  @override
  DriverFileWrite startXmlWrite(String serverId, String vmName, String timestamp) {
    final file = xmlFile(serverId, vmName, timestamp, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverFileWrite(
      sink: sink,
      commit: () async {
        final finalPath = file.path.substring(0, file.path.length - '.inprogress'.length);
        final finalFile = File(finalPath);
        if (await finalFile.exists()) {
          await finalFile.delete();
        }
        if (await file.exists()) {
          await file.rename(finalPath);
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
        final finalPath = file.path.substring(0, file.path.length - '.inprogress'.length);
        final finalFile = File(finalPath);
        if (await finalFile.exists()) {
          await finalFile.delete();
        }
        if (await file.exists()) {
          await file.rename(finalPath);
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
        final finalFile = manifestFile(serverId, vmName, diskId, timestamp, inProgress: false);
        final finalGzFile = manifestGzFile(serverId, vmName, diskId, timestamp);
        if (await finalFile.exists()) {
          await finalFile.delete();
        }
        if (await file.exists()) {
          await file.rename(finalFile.path);
        }
        await _gzipManifest(finalFile, finalGzFile);
        try {
          await finalFile.delete();
        } catch (_) {}
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
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 4) {
      return;
    }
    final dir = blobFile(hash).parent;
    await dir.create(recursive: true);
  }

  @override
  Future<bool> writeBlobIfMissing(String hash, List<int> bytes) async {
    if (hash.length < 4) {
      return false;
    }
    final blob = blobFile(hash);
    await blob.parent.create(recursive: true);
    final tempPath = '${blob.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final tempFile = File(tempPath);
    await tempFile.writeAsBytes(bytes);
    try {
      await tempFile.rename(blob.path);
      return true;
    } catch (_) {
      try {
        await tempFile.delete();
      } catch (_) {}
    }
    return false;
  }

  @override
  Future<bool> blobExists(String hash) async {
    if (hash.length < 4) {
      return false;
    }
    return blobFile(hash).exists();
  }

  @override
  String backupCompletedMessage(String manifestsPath) {
    return 'Backup saved to $manifestsPath';
  }

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

  Future<void> _gzipManifest(File source, File target) async {
    final input = source.openRead();
    final output = target.openWrite();
    try {
      await output.addStream(input.transform(gzip.encoder));
    } finally {
      await output.close();
    }
  }
}
