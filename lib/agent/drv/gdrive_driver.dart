import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/google_oauth_client.dart';
import 'package:virtbackup/common/settings.dart';

class GdrivePrefillStats {
  const GdrivePrefillStats({required this.shard1Count, required this.shard2Count, required this.blobCount, required this.durationMs});

  final int shard1Count;
  final int shard2Count;
  final int blobCount;
  final int durationMs;
}

class GdriveBackupDriver implements BackupDriver, RemoteBlobDriver {
  GdriveBackupDriver({required AppSettings settings, required Future<void> Function(AppSettings) persistSettings, Directory? settingsDir, void Function(String message)? logInfo})
    : _settings = settings,
      _persistSettings = persistSettings,
      _settingsDir = settingsDir,
      _cacheRoot = Directory('${_tempBase()}${Platform.pathSeparator}virtbackup_gdrive_cache'),
      _logInfo = logInfo ?? ((_) {});

  final Directory? _settingsDir;
  GoogleOAuthInstalledClient? _oauthClient;
  static const int _uploadConcurrency = 8;
  static const String _driveFolderMime = 'application/vnd.google-apps.folder';
  static const String _driveRootId = 'root';

  AppSettings _settings;
  final Future<void> Function(AppSettings) _persistSettings;
  final Directory _cacheRoot;
  final void Function(String message) _logInfo;
  final Map<String, String> _folderCache = {};
  final Map<String, Future<String>> _folderInFlight = {};
  final Set<String> _folderChildrenLoaded = {};
  final Map<String, String> _blobShardFolderIds = {};
  final Set<String> _blobShard1Names = {};
  final Map<String, Set<String>> _blobShard2Names = {};
  final Map<String, _DriveFileRef> _blobFiles = {};
  final Set<String> _blobNames = {};
  bool _blobCachePrefilled = false;
  http.Client _client = IOClient(_createHttpClient());
  final List<http.Client> _uploadClients = List<http.Client>.generate(_uploadConcurrency, (_) => IOClient(_createHttpClient()));
  int _uploadClientIndex = 0;
  int _inFlightUploads = 0;
  String? _tmpFolderId;
  String? _blobsRootId;
  void Function(GdrivePrefillStats stats, bool done)? onPrefillProgress;

  @override
  BackupDriverCapabilities get capabilities => const BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: true,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: 8,
  );

  @override
  String get destination => _cacheRoot.path;

  @override
  bool get discardWrites => false;

  @override
  int get bufferedBytes => 0;

  static const bool _cacheBlobsEnabled = true;
  GdrivePrefillStats _prefillStats = const GdrivePrefillStats(shard1Count: 0, shard2Count: 0, blobCount: 0, durationMs: 0);

  bool get cacheBlobsEnabled => _cacheBlobsEnabled;
  GdrivePrefillStats get prefillStats => _prefillStats;

  Future<GoogleOAuthInstalledClient> _ensureOAuthClient() async {
    final existing = _oauthClient;
    if (existing != null) {
      return existing;
    }
    final sep = Platform.pathSeparator;
    final overrideFile = _settingsDir == null ? null : File('${_settingsDir.path}${sep}etc${sep}google_oauth_client.json');
    final locator = GoogleOAuthClientLocator(overrideFile: overrideFile);
    final loaded = await locator.load(requireSecret: true);
    _oauthClient = loaded;
    return loaded;
  }

  @override
  Directory manifestsDir(String serverId, String vmName) {
    return Directory('${_cacheRoot.path}${Platform.pathSeparator}manifests${Platform.pathSeparator}$serverId${Platform.pathSeparator}$vmName');
  }

  @override
  Directory blobsDir() {
    return Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs');
  }

  @override
  Directory tmpDir() {
    return Directory('${_cacheRoot.path}${Platform.pathSeparator}tmp');
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
    final path = _blobCachePath(hash);
    return File(path);
  }

  @override
  Future<void> ensureReady() async {
    await _cacheRoot.create(recursive: true);
    await manifestsDir('tmp', 'tmp').parent.create(recursive: true);
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureBlobsRoot();
    await _warmFolderCache();
    _logInfo('gdrive: ready (cache=${_cacheRoot.path})');
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    await manifestsDir(serverId, vmName).create(recursive: true);
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureTmpFolder();
    await _ensureManifestFolder(serverId, vmName);
    await _ensureBlobsRoot();
    final prefillStart = DateTime.now();
    await _prefillBlobShardCache();
    final prefillMs = DateTime.now().difference(prefillStart).inMilliseconds;
    _logInfo('gdrive: prefill blob cache durationMs=$prefillMs');
    _logInfo('gdrive: prepare backup for $serverId/$vmName');
  }

  @override
  DriverFileWrite startXmlWrite(String serverId, String vmName, String timestamp) {
    final file = xmlFile(serverId, vmName, timestamp, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverFileWrite(
      sink: sink,
      commit: () async {
        _logInfo('gdrive: commit xml $timestamp for $serverId/$vmName');
        await _commitManifestFile(localFile: file, finalName: '${timestamp}__domain.xml', remoteFolder: await _ensureManifestFolder(serverId, vmName));
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
        _logInfo('gdrive: commit chain $timestamp for $serverId/$vmName ($diskId)');
        await _commitManifestFile(localFile: file, finalName: '${timestamp}__$diskId.chain', remoteFolder: await _ensureManifestFolder(serverId, vmName));
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
        _logInfo('gdrive: commit manifest $timestamp for $serverId/$vmName ($diskId)');
        final finalFile = manifestFile(serverId, vmName, diskId, timestamp, inProgress: false);
        if (await finalFile.exists()) {
          await finalFile.delete();
        }
        if (await file.exists()) {
          await file.rename(finalFile.path);
        }
        final gzFile = manifestGzFile(serverId, vmName, diskId, timestamp);
        await _gzipManifest(finalFile, gzFile);
        try {
          await finalFile.delete();
        } catch (_) {}
        await _commitManifestFile(localFile: gzFile, finalName: '$timestamp.manifest.gz', remoteFolder: await _ensureDiskManifestFolder(serverId, vmName, diskId));
      },
    );
  }

  @override
  Future<void> finalizeManifest(DriverManifestWrite write) async {
    await write.commit();
  }

  @override
  Future<void> freshCleanup() async {
    final rootId = await _resolveDriveRootId();
    if (rootId == null || rootId.isEmpty) {
      _logInfo('gdrive: fresh cleanup skipped (missing root)');
    } else {
      _logInfo('gdrive: fresh cleanup trashing VirtBackup root (id=$rootId)');
      await _trashFile(rootId);
    }
    _folderCache.clear();
    _folderChildrenLoaded.clear();
    _blobsRootId = null;
    _blobShardFolderIds.clear();
    _blobShard1Names.clear();
    _blobShard2Names.clear();
    _blobNames.clear();
    _blobFiles.clear();
    _blobCachePrefilled = false;
    _prefillStats = const GdrivePrefillStats(shard1Count: 0, shard2Count: 0, blobCount: 0, durationMs: 0);
    await _deleteDirIfExists(Directory('${_cacheRoot.path}${Platform.pathSeparator}manifests'));
    await _deleteDirIfExists(Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs'));
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 4) {
      return;
    }
    final shardKey = hash.substring(0, 4);
    if (_blobShardFolderIds.containsKey(shardKey)) {
      return;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final folderId = await _ensureFolderByPath(['blobs', shard1, shard2]);
    _blobShardFolderIds[shardKey] = folderId;
    _blobShard1Names.add(shard1);
    _blobShard2Names.putIfAbsent(shard1, () => <String>{}).add(shard2);
  }

  @override
  Future<bool> writeBlobIfMissing(String hash, List<int> bytes) async {
    if (hash.length < 4 || bytes.isEmpty) {
      return false;
    }
    final shardKey = hash.substring(0, 4);
    final parentId = _blobShardFolderIds[shardKey];
    if (parentId == null) {
      throw 'Blob shard folder not ready for $shardKey';
    }
    final ref = await _uploadSimple(name: hash, parentId: parentId, bytes: bytes, contentType: 'application/octet-stream');
    if (_cacheBlobsEnabled) {
      _blobNames.add(hash);
      _blobFiles[hash] = ref;
    }
    return true;
  }

  @override
  Future<bool> blobExists(String hash) async {
    if (hash.length < 4) {
      return false;
    }
    if (_cacheBlobsEnabled) {
      if (_blobNames.contains(hash)) {
        return true;
      }
      if (_blobCachePrefilled) {
        return false;
      }
    }
    final shardFolder = await _findBlobShardFolderId(hash);
    if (shardFolder == null) {
      return false;
    }
    final found = await _findFileByName(shardFolder, hash);
    if (found == null) {
      return false;
    }
    if (_cacheBlobsEnabled) {
      _blobNames.add(hash);
      _blobFiles[hash] = found;
    }
    return true;
  }

  @override
  Future<bool> blobExistsRemote(String hash) async {
    return blobExists(hash);
  }

  @override
  Future<int?> blobLength(String hash) async {
    if (hash.length < 4) {
      return null;
    }
    if (_cacheBlobsEnabled) {
      final cached = _blobFiles[hash];
      if (cached != null) {
        return cached.size;
      }
      if (_blobCachePrefilled) {
        return null;
      }
    }
    final shardFolder = await _findBlobShardFolderId(hash);
    if (shardFolder == null) {
      return null;
    }
    final found = await _findFileByName(shardFolder, hash, includeSize: true);
    if (found == null) {
      return null;
    }
    if (_cacheBlobsEnabled) {
      _blobNames.add(hash);
      _blobFiles[hash] = found;
    }
    return found.size;
  }

  @override
  Stream<List<int>> openBlobStream(String hash, {int? length}) async* {
    final data = await readBlobBytes(hash);
    if (data == null) {
      return;
    }
    yield data;
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 4) {
      return null;
    }
    final cacheFile = File(_blobCachePath(hash));
    if (await cacheFile.exists()) {
      return cacheFile.readAsBytes();
    }
    _DriveFileRef? fileRef;
    if (_cacheBlobsEnabled) {
      fileRef = _blobFiles[hash];
      if (fileRef == null && !_blobCachePrefilled) {
        final shardFolder = await _findBlobShardFolderId(hash);
        if (shardFolder == null) {
          throw 'Missing blob folder for hash: $hash';
        }
        fileRef = await _findFileByName(shardFolder, hash);
        if (fileRef != null) {
          _blobNames.add(hash);
          _blobFiles[hash] = fileRef;
        }
      }
      if (fileRef == null) {
        throw 'Missing blob in cache: $hash';
      }
    } else {
      final shardFolder = await _findBlobShardFolderId(hash);
      if (shardFolder == null) {
        throw 'Missing blob folder for hash: $hash';
      }
      fileRef = await _findFileByName(shardFolder, hash);
      if (fileRef == null) {
        throw 'Missing blob in Drive: $hash';
      }
    }
    final bytes = await _downloadFile(fileRef.id);
    if (!await cacheFile.exists()) {
      await cacheFile.parent.create(recursive: true);
      await cacheFile.writeAsBytes(bytes, flush: true);
    }
    return bytes;
  }

  @override
  String backupCompletedMessage(String manifestsPath) {
    return 'Backup saved to Google Drive';
  }

  @override
  Stream<File> listXmlFiles() async* {
    _logInfo('gdrive: listing xml files');
    final manifestsRootId = await _ensureFolderByPath(['manifests']);
    final serverFolders = await _listChildFolders(manifestsRootId);
    for (final serverFolder in serverFolders) {
      final vmFolders = await _listChildFolders(serverFolder.id);
      for (final vmFolder in vmFolders) {
        final xmlFiles = await _listFiles(vmFolder.id, nameContains: '__domain.xml');
        for (final file in xmlFiles) {
          if (file.name.endsWith('.inprogress')) {
            continue;
          }
          final localPath =
              '${_cacheRoot.path}${Platform.pathSeparator}manifests${Platform.pathSeparator}${serverFolder.name}${Platform.pathSeparator}${vmFolder.name}${Platform.pathSeparator}${file.name}';
          final localFile = File(localPath);
          if (!await localFile.exists()) {
            await localFile.parent.create(recursive: true);
            final bytes = await _downloadFile(file.id);
            await localFile.writeAsBytes(bytes, flush: true);
          }
          yield localFile;
        }
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

  Future<void> ensureXmlOnDrive(File xmlFile) async {
    final location = restoreLocationFromXml(xmlFile);
    if (location == null) {
      return;
    }
    final fileName = baseName(xmlFile.path);
    final folderId = await _ensureManifestFolder(location.serverId, location.vmName);
    final files = await _listFiles(folderId, nameEquals: fileName);
    if (files.isEmpty) {
      throw 'Restore XML not found on Drive: $fileName';
    }
    final bytes = await _downloadFile(files.first.id);
    await xmlFile.parent.create(recursive: true);
    await xmlFile.writeAsBytes(bytes, flush: true);
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
    final location = _resolveServerVm(vmDir);
    if (location != null) {
      final downloaded = await _downloadManifestIfMissing(location.serverId, location.vmName, diskId, timestamp);
      if (downloaded != null) {
        return downloaded;
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
    final location = _resolveServerVm(vmDir);
    if (location != null) {
      final diskId = sanitizeFileName(diskBaseName);
      final diskFolderId = await _ensureDiskManifestFolder(location.serverId, location.vmName, diskId);
      final downloaded = await _downloadManifestFromFolder(diskFolderId, vmDir, diskId, timestamp);
      if (downloaded != null) {
        return Directory('${vmDir.path}${Platform.pathSeparator}$diskId');
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

  Future<File?> ensureChainFile(Directory vmDir, String timestamp, String diskBaseName) async {
    final existing = findChainFileForTimestamp(vmDir, null, timestamp, diskBaseName);
    if (existing != null) {
      return existing;
    }
    final location = _resolveServerVm(vmDir);
    if (location == null) {
      return null;
    }
    final diskId = sanitizeFileName(diskBaseName);
    final vmChainFile = File('${vmDir.path}${Platform.pathSeparator}${timestamp}__$diskId.chain');
    return _downloadChainIfMissing(location.serverId, location.vmName, diskId, timestamp, vmChainFile);
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
    if (manifests.isNotEmpty) {
      return manifests;
    }
    final location = _resolveServerVm(vmDir);
    if (location != null) {
      final vmFolderId = await _ensureManifestFolder(location.serverId, location.vmName);
      final diskFolders = await _listChildFolders(vmFolderId);
      for (final diskFolder in diskFolders) {
        final diskId = diskFolder.name;
        final downloaded = await _downloadManifestFromFolder(diskFolder.id, vmDir, diskId, timestamp);
        if (downloaded != null) {
          manifests.add(downloaded);
        }
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
    final location = _resolveServerVm(vmDir);
    if (location != null) {
      final vmFolderId = await _ensureManifestFolder(location.serverId, location.vmName);
      final diskFolders = await _listChildFolders(vmFolderId);
      for (final diskFolder in diskFolders) {
        final diskId = diskFolder.name;
        final downloaded = await _downloadManifestFromFolder(diskFolder.id, vmDir, diskId, timestamp);
        if (downloaded == null) {
          continue;
        }
        final storedPath = await readSourcePath(downloaded);
        if (storedPath != null && storedPath.trim() == sourcePath.trim()) {
          return downloaded;
        }
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
  Future<void> cleanupInProgressFiles() async {
    await _deleteInProgressInDir(manifestsDir('tmp', 'tmp').parent);
    await _deleteInProgressInDir(tmpDir());
  }

  Future<void> _ensureBlobsRoot() async {
    if (_blobsRootId != null) {
      return;
    }
    _blobsRootId = await _ensureFolderByPath(['blobs']);
  }

  Future<String> _ensureTmpFolder() async {
    if (_tmpFolderId != null) {
      return _tmpFolderId!;
    }
    final root = await _ensureDriveRoot();
    _tmpFolderId = await _ensureFolder(root, 'tmp');
    return _tmpFolderId!;
  }

  Future<String?> _findBlobShardFolderId(String hash) async {
    final shardKey = hash.substring(0, 4);
    final cached = _blobShardFolderIds[shardKey];
    if (cached != null) {
      return cached;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    if (_blobCachePrefilled) {
      if (!_blobShard1Names.contains(shard1)) {
        return null;
      }
      final shard2Names = _blobShard2Names[shard1];
      if (shard2Names == null || !shard2Names.contains(shard2)) {
        return null;
      }
    }
    final folderId = await _findFolderByPath(['blobs', shard1, shard2]);
    if (folderId != null) {
      _blobShardFolderIds[shardKey] = folderId;
      _blobShard1Names.add(shard1);
      _blobShard2Names.putIfAbsent(shard1, () => <String>{}).add(shard2);
    }
    return folderId;
  }

  Future<void> _prefillBlobShardCache() async {
    if (_blobCachePrefilled) {
      return;
    }
    final startedAt = DateTime.now();
    DateTime? lastEmitAt;
    _logInfo('gdrive: prefill blob cache start (cacheBlobs=$_cacheBlobsEnabled)');
    await _ensureBlobsRoot();
    final rootId = _blobsRootId;
    if (rootId == null || rootId.isEmpty) {
      _prefillStats = GdrivePrefillStats(shard1Count: 0, shard2Count: 0, blobCount: 0, durationMs: DateTime.now().difference(startedAt).inMilliseconds);
      onPrefillProgress?.call(_prefillStats, true);
      _logInfo('gdrive: prefill blob cache skipped (missing blobs root)');
      return;
    }
    _blobShardFolderIds.clear();
    _blobShard1Names.clear();
    _blobShard2Names.clear();
    if (_cacheBlobsEnabled) {
      _blobNames.clear();
      _blobFiles.clear();
    }
    var shard1Count = 0;
    var shard2Count = 0;
    var blobCount = 0;
    void emitPrefill({required bool done}) {
      final now = DateTime.now();
      if (!done && lastEmitAt != null && now.difference(lastEmitAt!) < const Duration(seconds: 1)) {
        return;
      }
      lastEmitAt = now;
      final durationMs = now.difference(startedAt).inMilliseconds;
      _prefillStats = GdrivePrefillStats(shard1Count: shard1Count, shard2Count: shard2Count, blobCount: blobCount, durationMs: durationMs);
      onPrefillProgress?.call(_prefillStats, done);
    }

    final shard1Folders = await _listChildFolders(rootId);
    final shard2Folders = <(String shard1, _DriveFileRef shard2)>[];
    for (final shard1 in shard1Folders) {
      _blobShard1Names.add(shard1.name);
      shard1Count++;
      final shard2Children = await _listChildFolders(shard1.id);
      if (shard2Children.isEmpty) {
        continue;
      }
      final shard2Names = _blobShard2Names.putIfAbsent(shard1.name, () => <String>{});
      for (final shard2 in shard2Children) {
        shard2Names.add(shard2.name);
        _blobShardFolderIds['${shard1.name}${shard2.name}'] = shard2.id;
        shard2Count++;
        shard2Folders.add((shard1.name, shard2));
      }
      emitPrefill(done: false);
    }
    emitPrefill(done: false);

    if (_cacheBlobsEnabled) {
      const maxConcurrentLists = 8;
      var nextShard2Index = 0;
      final shard2Fetchers = List<Future<void>>.generate(maxConcurrentLists, (_) async {
        while (true) {
          final currentIndex = nextShard2Index;
          if (currentIndex >= shard2Folders.length) {
            break;
          }
          nextShard2Index += 1;
          final entry = shard2Folders[currentIndex];
          final shard1 = entry.$1;
          final shard2 = entry.$2;
          final files = await _listFilesRaw("mimeType!='$_driveFolderMime' and '${shard2.id}' in parents and trashed=false", fields: 'nextPageToken,files(id,name,parents,size)');
          for (final file in files) {
            if (file.name.endsWith('.inprogress')) {
              continue;
            }
            _blobNames.add(file.name);
            _blobFiles[file.name] = file;
            blobCount += 1;
            _blobShard2Names.putIfAbsent(shard1, () => <String>{}).add(shard2.name);
          }
          emitPrefill(done: false);
        }
      });
      await Future.wait(shard2Fetchers);
    }

    _blobCachePrefilled = true;
    final elapsedMs = DateTime.now().difference(startedAt).inMilliseconds;
    _prefillStats = GdrivePrefillStats(shard1Count: shard1Count, shard2Count: shard2Count, blobCount: blobCount, durationMs: elapsedMs);
    onPrefillProgress?.call(_prefillStats, true);
    _logInfo('gdrive: prefill blob cache done shard1=$shard1Count shard2=$shard2Count blobs=$blobCount durationMs=$elapsedMs');
  }

  Future<String?> _resolveDriveRootId() async {
    final rootPath = _settings.gdriveRootPath.trim();
    final parts = rootPath.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = _driveFolderId(_driveRootId);
    for (final part in parts) {
      final child = await _findChildFolder(current, part.trim());
      if (child == null) {
        return null;
      }
      current = child.id;
    }
    final virtBackup = await _findChildFolder(current, 'VirtBackup');
    return virtBackup?.id;
  }

  Future<void> _commitManifestFile({required File localFile, required String finalName, required String remoteFolder}) async {
    if (!await localFile.exists()) {
      return;
    }
    final bytes = await localFile.readAsBytes();
    _logInfo('gdrive: upload manifest $finalName size=${bytes.length}');
    await _uploadSimple(name: finalName, parentId: remoteFolder, bytes: bytes, contentType: 'application/octet-stream');
  }

  Future<String> _ensureManifestFolder(String serverId, String vmName) async {
    return _ensureFolderByPath(['manifests', serverId, vmName]);
  }

  Future<String> _ensureDiskManifestFolder(String serverId, String vmName, String diskId) async {
    return _ensureFolderByPath(['manifests', serverId, vmName, diskId]);
  }

  Future<String> _ensureFolderByPath(List<String> parts) async {
    final root = await _ensureDriveRoot();
    var current = root;
    for (final part in parts) {
      current = await _ensureFolder(current, part);
    }
    return current;
  }

  Future<String?> _findFolderByPath(List<String> parts) async {
    final rootId = await _resolveDriveRootId();
    if (rootId == null) {
      return null;
    }
    var current = rootId;
    for (final part in parts) {
      final parentId = current;
      final child = await _findChildFolder(parentId, part);
      if (child == null) {
        return null;
      }
      current = child.id;
      _folderCache['$parentId/$part'] = child.id;
    }
    return current;
  }

  Future<String> _ensureDriveRoot() async {
    final rootPath = _settings.gdriveRootPath.trim();
    final parts = rootPath.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = _driveFolderId(_driveRootId);
    for (final part in parts) {
      current = _driveFolderId(await _ensureFolder(current, part.trim()));
    }
    final virtBackupId = await _ensureFolder(current, 'VirtBackup');
    return virtBackupId;
  }

  Future<void> _warmFolderCache() async {
    final rootId = await _ensureDriveRoot();
    if (_folderChildrenLoaded.contains(rootId)) {
      return;
    }
    final children = await _listChildFolders(rootId);
    for (final child in children) {
      _folderCache['$rootId/${child.name}'] = child.id;
    }
    _folderChildrenLoaded.add(rootId);
  }

  String _driveFolderId(String id) => id;

  Future<String> _ensureFolder(String parentId, String name) async {
    final key = '$parentId/$name';
    final cached = _folderCache[key];
    if (cached != null) {
      return cached;
    }
    final inFlight = _folderInFlight[key];
    if (inFlight != null) {
      return inFlight;
    }
    final future = () async {
      final existing = await _findChildFolder(parentId, name);
      if (existing != null) {
        _folderCache[key] = existing.id;
        return existing.id;
      }
      final created = await _createFolder(parentId, name);
      _folderCache[key] = created.id;
      return created.id;
    }();
    _folderInFlight[key] = future;
    try {
      return await future;
    } finally {
      _folderInFlight.remove(key);
    }
  }

  Future<_DriveFileRef?> _findChildFolder(String parentId, String name) async {
    final query = "mimeType='$_driveFolderMime' and name='${_escapeQuery(name)}' and '$parentId' in parents and trashed=false";
    final files = await _listFilesRaw(query, fields: 'nextPageToken,files(id,name,parents)');
    return files.isEmpty ? null : files.first;
  }

  Future<_DriveFileRef> _createFolder(String parentId, String name) async {
    return _withRetry('create folder "$name"', () async {
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files');
      final body = jsonEncode({
        'name': name,
        'mimeType': _driveFolderMime,
        'parents': [parentId],
      });
      final response = await _client.post(uri, headers: _authHeaders(token)..['Content-Type'] = 'application/json', body: body);
      if (response.statusCode >= 300) {
        throw 'Drive folder create failed: ${response.statusCode} ${response.body}';
      }
      final decoded = jsonDecode(response.body);
      return _DriveFileRef(id: decoded['id'].toString(), name: name, parentId: parentId);
    });
  }

  Future<List<_DriveFileRef>> _listChildFolders(String parentId) async {
    final query = "mimeType='$_driveFolderMime' and '$parentId' in parents and trashed=false";
    return _listFilesRaw(query, fields: 'nextPageToken,files(id,name,parents)');
  }

  Future<List<_DriveFileRef>> _listFiles(String parentId, {String? nameContains, String? nameEquals}) async {
    var query = "'$parentId' in parents and trashed=false";
    if (nameContains != null && nameContains.isNotEmpty) {
      query += " and name contains '${_escapeQuery(nameContains)}'";
    }
    if (nameEquals != null && nameEquals.isNotEmpty) {
      query += " and name='${_escapeQuery(nameEquals)}'";
    }
    return _listFilesRaw(query, fields: 'nextPageToken,files(id,name,parents)');
  }

  Future<List<_DriveFileRef>> _listFilesRaw(String query, {required String fields}) async {
    final results = <_DriveFileRef>[];
    String? pageToken;
    do {
      final params = <String, String>{'q': query, 'fields': fields, 'pageSize': '1000'};
      if (pageToken != null && pageToken.isNotEmpty) {
        params['pageToken'] = pageToken;
      }
      final uri = Uri.https('www.googleapis.com', '/drive/v3/files', params);
      final response = await _withRetry('list files', () async {
        final token = await _ensureAccessToken();
        final response = await _client.get(uri, headers: _authHeaders(token));
        if (response.statusCode >= 300) {
          throw 'Drive list failed: ${response.statusCode} ${response.body}';
        }
        return response;
      });
      final decoded = jsonDecode(response.body);
      final files = decoded['files'];
      if (files is List) {
        for (final entry in files) {
          final map = Map<String, dynamic>.from(entry as Map);
          final parents = map['parents'];
          final parentId = parents is List && parents.isNotEmpty ? parents.first.toString() : '';
          final size = map['size'] == null ? null : int.tryParse(map['size'].toString());
          results.add(_DriveFileRef(id: map['id'].toString(), name: map['name'].toString(), parentId: parentId, size: size));
        }
      }
      pageToken = decoded['nextPageToken']?.toString();
    } while (pageToken != null && pageToken.isNotEmpty);
    return results;
  }

  Future<void> _trashFile(String fileId) async {
    await _withRetry('trash file', () async {
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files/$fileId');
      final response = await _client.patch(uri, headers: _authHeaders(token)..['Content-Type'] = 'application/json', body: jsonEncode({'trashed': true}));
      if (response.statusCode >= 300) {
        throw 'Drive trash failed: ${response.statusCode} ${response.body}';
      }
    });
  }

  Future<void> _deleteDirIfExists(Directory dir) async {
    if (!await dir.exists()) {
      return;
    }
    await dir.delete(recursive: true);
  }

  Future<_DriveFileRef> _uploadSimple({required String name, required String parentId, required List<int> bytes, required String contentType}) async {
    final stopwatch = Stopwatch()..start();
    _inFlightUploads += 1;
    final lease = _leaseUploadClient();
    try {
      final uri = Uri.parse('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable');
      return await _uploadSimpleWithClient(lease: lease, uri: uri, bytes: bytes, name: name, parentId: parentId, stopwatch: stopwatch);
    } catch (error, stackTrace) {
      stopwatch.stop();
      _logInfo('gdrive: uploadSimple durationMs=${stopwatch.elapsedMilliseconds} error=$error');
      _logInfo('gdrive: uploadSimple failed name=$name parent=$parentId bytes=${bytes.length} error=$error');
      final traceText = stackTrace.toString();
      if (traceText.trim().isEmpty) {
        _logInfo(StackTrace.current.toString());
      } else {
        _logInfo(traceText);
      }
      rethrow;
    } finally {
      _inFlightUploads = max(0, _inFlightUploads - 1);
    }
  }

  Future<_DriveFileRef> _uploadSimpleWithClient({
    required _UploadClientLease lease,
    required Uri uri,
    required List<int> bytes,
    required String name,
    required String parentId,
    required Stopwatch stopwatch,
  }) async {
    try {
      Future<http.Response> startSession(String token) {
        return _withRetry('upload session $name', () async {
          final headers = _authHeaders(token)
            ..['Content-Type'] = 'application/json; charset=UTF-8'
            ..['X-Upload-Content-Type'] = 'application/octet-stream'
            ..['X-Upload-Content-Length'] = bytes.length.toString();
          final response = await lease.client.post(
            uri,
            headers: headers,
            body: jsonEncode({
              'name': name,
              'parents': [parentId],
            }),
          );
          if (response.statusCode >= 300 && response.statusCode != 401) {
            throw _DriveHttpException('Drive upload session failed', response.statusCode, response.body);
          }
          return response;
        }, onRetry: () => _resetUploadClient(lease.index));
      }

      Future<http.Response> uploadData(Uri sessionUri, String token) {
        return _withRetry('upload data $name', () async {
          final headers = _authHeaders(token)
            ..['Content-Type'] = 'application/octet-stream'
            ..['Content-Length'] = bytes.length.toString();
          final response = await lease.client.put(sessionUri, headers: headers, body: bytes);
          if (response.statusCode >= 300 && response.statusCode != 401) {
            throw _DriveHttpException('Drive upload failed', response.statusCode, response.body);
          }
          return response;
        }, onRetry: () => _resetUploadClient(lease.index));
      }

      var token = await _ensureAccessToken();
      var response = await startSession(token);
      if (response.statusCode == 401) {
        _logInfo('gdrive: uploadSimple unauthorized, refreshing token name=$name parent=$parentId');
        token = await _refreshAccessToken();
        response = await startSession(token);
      }
      if (response.statusCode >= 300) {
        stopwatch.stop();
        _logInfo('gdrive: uploadSimple session response headers=${response.headers}');
        _logInfo('gdrive: uploadSimple session response body=${response.body}');
        throw _DriveHttpException('Drive upload session failed', response.statusCode, response.body);
      }

      final location = response.headers['location'];
      if (location == null || location.isEmpty) {
        stopwatch.stop();
        _logInfo('gdrive: uploadSimple missing session location for name=$name parent=$parentId');
        throw _DriveHttpException('Drive upload failed', 0, 'missing resumable session location');
      }
      final sessionUri = Uri.parse(location);
      var uploadResponse = await uploadData(sessionUri, token);
      if (uploadResponse.statusCode == 401) {
        _logInfo('gdrive: uploadSimple unauthorized during upload, refreshing token name=$name parent=$parentId');
        token = await _refreshAccessToken();
        uploadResponse = await uploadData(sessionUri, token);
      }
      if (uploadResponse.statusCode >= 300) {
        stopwatch.stop();
        _logInfo('gdrive: uploadSimple upload response headers=${uploadResponse.headers}');
        _logInfo('gdrive: uploadSimple upload response body=${uploadResponse.body}');
        throw _DriveHttpException('Drive upload failed', uploadResponse.statusCode, uploadResponse.body);
      }

      final decoded = jsonDecode(uploadResponse.body);
      stopwatch.stop();
      return _DriveFileRef(id: decoded['id'].toString(), name: name, parentId: parentId);
    } catch (error) {
      _resetUploadClient(lease.index);
      rethrow;
    }
  }

  Future<List<int>> _downloadFile(String fileId) async {
    return _withRetry('download file', () async {
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files/$fileId?alt=media');
      final headers = _authHeaders(token);
      final response = await _client.get(uri, headers: headers);
      if (response.statusCode >= 300) {
        _logInfo('gdrive: downloadFile request headers=${_redactHeaders(headers)}');
        _logInfo('gdrive: downloadFile response headers=${response.headers}');
        _logInfo('gdrive: downloadFile response body=${response.body}');
        throw 'Drive download failed: ${response.statusCode} ${response.body}';
      }
      return response.bodyBytes;
    });
  }

  Future<String> _ensureAccessToken() async {
    final now = DateTime.now().toUtc();
    final expiresAt = _settings.gdriveExpiresAt;
    final token = _settings.gdriveAccessToken.trim();
    if (token.isNotEmpty && expiresAt != null && expiresAt.isAfter(now.add(const Duration(minutes: 1)))) {
      return token;
    }
    _logInfo('gdrive: refreshing access token');
    final refresh = _settings.gdriveRefreshToken.trim();
    if (refresh.isEmpty) {
      throw 'Google Drive refresh token is missing.';
    }
    final oauth = await _ensureOAuthClient();
    final uri = Uri.parse('https://oauth2.googleapis.com/token');
    final response = await _withRetry('refresh token', () async {
      final body = <String, String>{'client_id': oauth.clientId, 'client_secret': oauth.clientSecret, 'refresh_token': refresh, 'grant_type': 'refresh_token'};
      final response = await _client.post(uri, headers: {'Content-Type': 'application/x-www-form-urlencoded'}, body: body);
      if (response.statusCode >= 300) {
        throw 'Token refresh failed: ${response.statusCode} ${response.body}';
      }
      return response;
    });
    final decoded = jsonDecode(response.body);
    if (decoded is! Map) {
      throw 'Token refresh failed: invalid response.';
    }
    final accessToken = decoded['access_token']?.toString() ?? '';
    final expiresIn = decoded['expires_in'];
    final newExpiresAt = _calculateExpiresAt(expiresIn);
    _settings = _settings.copyWith(gdriveAccessToken: accessToken, gdriveExpiresAt: newExpiresAt);
    await _persistSettings(_settings);
    _logInfo('gdrive: access token refreshed, expiresAt=${newExpiresAt?.toIso8601String() ?? 'unknown'}');
    return accessToken;
  }

  Future<String> _refreshAccessToken() async {
    _logInfo('gdrive: refreshing access token (forced)');
    final refresh = _settings.gdriveRefreshToken.trim();
    if (refresh.isEmpty) {
      throw 'Google Drive refresh token is missing.';
    }
    final oauth = await _ensureOAuthClient();
    final uri = Uri.parse('https://oauth2.googleapis.com/token');
    final response = await _withRetry('refresh token (forced)', () async {
      final body = <String, String>{'client_id': oauth.clientId, 'client_secret': oauth.clientSecret, 'refresh_token': refresh, 'grant_type': 'refresh_token'};
      final response = await _client.post(uri, headers: {'Content-Type': 'application/x-www-form-urlencoded'}, body: body);
      if (response.statusCode >= 300) {
        throw 'Token refresh failed: ${response.statusCode} ${response.body}';
      }
      return response;
    });
    final decoded = jsonDecode(response.body);
    if (decoded is! Map) {
      throw 'Token refresh failed: invalid response.';
    }
    final accessToken = decoded['access_token']?.toString() ?? '';
    final expiresIn = decoded['expires_in'];
    final newExpiresAt = _calculateExpiresAt(expiresIn);
    _settings = _settings.copyWith(gdriveAccessToken: accessToken, gdriveExpiresAt: newExpiresAt);
    await _persistSettings(_settings);
    _logInfo('gdrive: access token refreshed, expiresAt=${newExpiresAt?.toIso8601String() ?? 'unknown'}');
    return accessToken;
  }

  DateTime? _calculateExpiresAt(Object? expiresIn) {
    if (expiresIn is num) {
      return DateTime.now().toUtc().add(Duration(seconds: expiresIn.toInt()));
    }
    final parsed = int.tryParse(expiresIn?.toString() ?? '');
    if (parsed == null || parsed <= 0) {
      return null;
    }
    return DateTime.now().toUtc().add(Duration(seconds: parsed));
  }

  Map<String, String> _authHeaders(String token) {
    return {'Authorization': 'Bearer $token'};
  }

  Map<String, String> _redactHeaders(Map<String, String> headers) {
    final copy = Map<String, String>.from(headers);
    if (copy.containsKey(HttpHeaders.authorizationHeader)) {
      copy[HttpHeaders.authorizationHeader] = 'Bearer <redacted>';
    } else if (copy.containsKey('Authorization')) {
      copy['Authorization'] = 'Bearer <redacted>';
    }
    return copy;
  }

  String _escapeQuery(String value) {
    return value.replaceAll("'", "\\'");
  }

  String _blobCachePath(String hash) {
    if (hash.length < 4) {
      return '${_cacheRoot.path}${Platform.pathSeparator}blobs${Platform.pathSeparator}$hash';
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    return '${_cacheRoot.path}${Platform.pathSeparator}blobs${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2${Platform.pathSeparator}$hash';
  }

  _ServerVmLocation? _resolveServerVm(Directory vmDir) {
    final vmName = baseName(vmDir.path);
    final serverId = baseName(vmDir.parent.path);
    if (vmName.isEmpty || serverId.isEmpty) {
      return null;
    }
    return _ServerVmLocation(serverId: serverId, vmName: vmName);
  }

  Future<File?> _downloadManifestIfMissing(String serverId, String vmName, String diskId, String timestamp) async {
    final folderId = await _ensureDiskManifestFolder(serverId, vmName, diskId);
    return _downloadManifestFromFolder(folderId, manifestsDir(serverId, vmName), diskId, timestamp);
  }

  Future<File?> _downloadManifestFromFolder(String folderId, Directory vmDir, String diskId, String timestamp) async {
    final nameGz = '$timestamp.manifest.gz';
    final namePlain = '$timestamp.manifest';
    final file = await _findFileByName(folderId, nameGz) ?? await _findFileByName(folderId, namePlain);
    if (file == null) {
      return null;
    }
    final localDir = Directory('${vmDir.path}${Platform.pathSeparator}$diskId');
    await localDir.create(recursive: true);
    final localPath = '${localDir.path}${Platform.pathSeparator}${file.name}';
    final localFile = File(localPath);
    if (!await localFile.exists()) {
      final bytes = await _downloadFile(file.id);
      await localFile.writeAsBytes(bytes, flush: true);
    }
    return localFile;
  }

  Future<File?> _downloadChainIfMissing(String serverId, String vmName, String diskId, String timestamp, File localFile) async {
    if (await localFile.exists()) {
      return localFile;
    }
    final folderId = await _ensureManifestFolder(serverId, vmName);
    final name = '${timestamp}__$diskId.chain';
    final file = await _findFileByName(folderId, name);
    if (file == null) {
      return null;
    }
    await localFile.parent.create(recursive: true);
    final bytes = await _downloadFile(file.id);
    await localFile.writeAsBytes(bytes, flush: true);
    return localFile;
  }

  Future<_DriveFileRef?> _findFileByName(String parentId, String name, {bool includeSize = false}) async {
    final fields = includeSize ? 'nextPageToken,files(id,name,parents,size)' : 'nextPageToken,files(id,name,parents)';
    final files = await _listFilesRaw("'$parentId' in parents and name='${_escapeQuery(name)}' and trashed=false", fields: fields);
    return files.isEmpty ? null : files.first;
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

  Future<T> _withRetry<T>(String label, Future<T> Function() action, {void Function()? onRetry}) async {
    var delaySeconds = 2;
    var attempt = 0;
    const maxRetries = 5;
    while (true) {
      try {
        return await action();
      } catch (error, stackTrace) {
        if (attempt >= maxRetries) {
          _logInfo('gdrive: $label failed after retry: $error');
          _logInfo(stackTrace.toString());
          throw 'gdrive $label failed after retry: $error';
        }
        attempt += 1;
        _logInfo('gdrive: $label failed: $error');
        _logInfo(stackTrace.toString());
        _logInfo('gdrive: $label retrying after error: $error');
        if (onRetry != null) {
          onRetry();
        } else {
          _resetHttpClient();
        }
        await Future.delayed(Duration(seconds: delaySeconds));
        delaySeconds *= 2;
      }
    }
  }

  static HttpClient _createHttpClient() {
    final client = HttpClient();
    client.connectionTimeout = const Duration(seconds: 10);
    return client;
  }

  void _resetHttpClient() {
    _client.close();
    _client = IOClient(_createHttpClient());
  }

  static String _tempBase() {
    if (Platform.isWindows) {
      return Directory.systemTemp.path;
    }
    return '${Platform.pathSeparator}var${Platform.pathSeparator}tmp';
  }

  _UploadClientLease _leaseUploadClient() {
    if (_uploadClients.isEmpty) {
      return _UploadClientLease(index: 0, client: IOClient(_createHttpClient()));
    }
    final index = _uploadClientIndex % _uploadClients.length;
    _uploadClientIndex += 1;
    return _UploadClientLease(index: index, client: _uploadClients[index]);
  }

  void _resetUploadClient(int index) {
    if (index < 0 || index >= _uploadClients.length) {
      return;
    }
    try {
      _uploadClients[index].close();
    } catch (_) {}
    _uploadClients[index] = IOClient(_createHttpClient());
  }
}

class _UploadClientLease {
  _UploadClientLease({required this.index, required this.client});

  final int index;
  final http.Client client;
}

class _DriveFileRef {
  _DriveFileRef({required this.id, required this.name, required this.parentId, this.size});

  final String id;
  final String name;
  final String parentId;
  final int? size;
}

class _DriveHttpException implements Exception {
  _DriveHttpException(this.message, this.statusCode, this.body);

  final String message;
  final int statusCode;
  final String body;
}

class _ServerVmLocation {
  _ServerVmLocation({required this.serverId, required this.vmName});

  final String serverId;
  final String vmName;
}
