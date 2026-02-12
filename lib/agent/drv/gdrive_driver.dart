import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/debug_log_writer.dart';
import 'package:virtbackup/common/google_oauth_client.dart';
import 'package:virtbackup/common/settings.dart';

class GdrivePrefillStats {
  const GdrivePrefillStats({required this.shard1Count, required this.shard2Count, required this.blobCount, required this.durationMs});

  final int shard1Count;
  final int shard2Count;
  final int blobCount;
  final int durationMs;
}

class GdriveBackupDriver implements BackupDriver, RemoteBlobDriver, BlobDirectoryLister {
  GdriveBackupDriver({required AppSettings settings, required Future<void> Function(AppSettings) persistSettings, Directory? settingsDir, void Function(String message)? logInfo})
    : _settings = settings,
      _persistSettings = persistSettings,
      _settingsDir = settingsDir,
      _cacheRoot = _cacheRootForSettings(settings),
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
  final Map<String, String> _folderPathById = {};
  final Map<String, Future<String>> _folderInFlight = {};
  final Set<String> _folderChildrenLoaded = {};
  final Map<String, String> _blobShardFolderIds = {};
  final Map<String, String> _blobShard1FolderIds = {};
  final Set<String> _blobShard1Names = {};
  final Map<String, Set<String>> _blobShard2Names = {};
  final Map<String, _DriveFileRef> _blobFiles = {};
  final Set<String> _blobNames = {};
  final _NamedAsyncLock _folderLocks = _NamedAsyncLock();
  bool _debugLogPrepared = false;
  http.Client _client = IOClient(_createHttpClient());
  final _HttpClientPool _uploadClientPool = _HttpClientPool(maxClients: _uploadConcurrency);
  int _inFlightUploads = 0;
  String? _tmpFolderId;
  String? _blobsRootId;
  String? _resolvedDriveRootId;
  void Function(GdrivePrefillStats stats, bool done)? onPrefillProgress;

  static Directory _cacheRootForSettings(AppSettings settings) {
    final basePath = settings.backupPath.trim();
    if (basePath.isEmpty) {
      // Fallback for misconfiguration; callers should configure backup.base_path.
      return Directory('${_tempBase()}${Platform.pathSeparator}virtbackup_gdrive_cache');
    }
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}cache${Platform.pathSeparator}gdrive');
  }

  @override
  BackupDriverCapabilities get capabilities => const BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: true,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: 4,
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
    final basePath = _settings.backupPath.trim();
    if (basePath.isEmpty) {
      return Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs');
    }
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}blobs');
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
  Future<Set<String>> listBlobShard1() async {
    await _ensureBlobsRoot();
    final rootId = _blobsRootId;
    if (rootId == null || rootId.isEmpty) {
      return <String>{};
    }
    final folders = await _listChildFolders(rootId);
    final names = <String>{};
    for (final folder in folders) {
      names.add(folder.name);
      _blobShard1FolderIds[folder.name] = folder.id;
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobShard2(String shard1) async {
    final shard1Id = await _findFolderByPath(['blobs', shard1]);
    if (shard1Id == null || shard1Id.isEmpty) {
      return <String>{};
    }
    _blobShard1FolderIds[shard1] = shard1Id;
    final folders = await _listChildFolders(shard1Id);
    final names = <String>{};
    for (final folder in folders) {
      names.add(folder.name);
      _blobShardFolderIds['$shard1${folder.name}'] = folder.id;
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobNames(String shard1, String shard2) async {
    final shard2Id = await _findFolderByPath(['blobs', shard1, shard2]);
    if (shard2Id == null || shard2Id.isEmpty) {
      return <String>{};
    }
    _blobShardFolderIds['$shard1$shard2'] = shard2Id;
    final files = await _listFilesRaw("mimeType!='$_driveFolderMime' and '$shard2Id' in parents and trashed=false", fields: 'nextPageToken,files(id,name,parents)');
    final names = <String>{};
    for (final file in files) {
      if (file.name.endsWith('.inprogress')) {
        continue;
      }
      names.add(file.name);
    }
    return names;
  }

  @override
  Future<void> ensureReady() async {
    await _cacheRoot.create(recursive: true);
    await _prepareDebugLogFile();
    await manifestsDir('tmp', 'tmp').parent.create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureBlobsRoot();
    await _warmFolderCache();
    _logInfo('gdrive: ready (cache=${_cacheRoot.path})');
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    await manifestsDir(serverId, vmName).create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureTmpFolder();
    await _ensureManifestFolder(serverId, vmName);
    await _ensureBlobsRoot();
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
    _logInfo('gdrive: fresh cleanup clearing local cache and in-memory state');
    _folderCache.clear();
    _folderChildrenLoaded.clear();
    _blobsRootId = null;
    _resolvedDriveRootId = null;
    _blobShardFolderIds.clear();
    _blobShard1FolderIds.clear();
    _blobShard1Names.clear();
    _blobShard2Names.clear();
    _blobNames.clear();
    _blobFiles.clear();
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
    await _ensureBlobsRoot();
    final rootId = _blobsRootId;
    if (rootId == null || rootId.isEmpty) {
      throw 'Missing blobs root for shard $shardKey';
    }
    final shard1Ref = _blobShard1FolderIds[shard1] ?? (await _createFolderBlind(rootId, shard1)).id;
    _blobShard1FolderIds[shard1] = shard1Ref;
    final shard1Path = _folderPathById[rootId] == null || _folderPathById[rootId]!.isEmpty ? shard1 : '${_folderPathById[rootId]}/$shard1';
    _folderPathById[shard1Ref] = shard1Path;
    final folderRef = await _createFolderBlind(shard1Ref, shard2);
    _blobShardFolderIds[shardKey] = folderRef.id;
    _folderPathById[folderRef.id] = '$shard1Path/$shard2';
    _blobShard1Names.add(shard1);
    _blobShard2Names.putIfAbsent(shard1, () => <String>{}).add(shard2);
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 4 || bytes.isEmpty) {
      return;
    }
    final shardKey = hash.substring(0, 4);
    final parentId = _blobShardFolderIds[shardKey];
    if (parentId == null || parentId.isEmpty) {
      throw 'Blob shard folder not ready for $shardKey';
    }
    final ref = await _uploadSimple(name: hash, parentId: parentId, bytes: bytes, contentType: 'application/octet-stream');
    if (_cacheBlobsEnabled) {
      _blobNames.add(hash);
      _blobFiles[hash] = ref;
    }
  }

  @override
  Future<bool> blobExists(String hash) async {
    if (hash.length < 4) {
      return false;
    }
    return _blobNames.contains(hash);
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
    final local = blobFile(hash);
    if (await local.exists()) {
      return local.length();
    }
    final cached = _blobFiles[hash];
    if (cached != null) {
      return cached.size;
    }
    final found = await _findBlobByHash(hash, includeSize: true);
    if (found == null) {
      return null;
    }
    _blobNames.add(hash);
    _blobFiles[hash] = found;
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
    final cacheFile = blobFile(hash);
    if (await cacheFile.exists()) {
      return cacheFile.readAsBytes();
    }
    _DriveFileRef? fileRef = _blobFiles[hash];
    fileRef ??= await _findBlobByHash(hash, includeSize: true);
    if (fileRef == null) {
      return null;
    }
    _blobNames.add(hash);
    _blobFiles[hash] = fileRef;
    final bytes = await _downloadFile(fileRef.id);
    if (!await cacheFile.exists()) {
      await cacheFile.parent.create(recursive: true);
      await cacheFile.writeAsBytes(bytes, flush: true);
    }
    return bytes;
  }

  Future<_DriveFileRef?> _findBlobByHash(String hash, {bool includeSize = false}) async {
    if (hash.length < 4) {
      return null;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final shardKey = '$shard1$shard2';
    final folderId = _blobShardFolderIds[shardKey] ?? await _findFolderByPath(['blobs', shard1, shard2]);
    if (folderId == null || folderId.isEmpty) {
      return null;
    }
    _blobShardFolderIds[shardKey] = folderId;
    _blobShard1Names.add(shard1);
    _blobShard2Names.putIfAbsent(shard1, () => <String>{}).add(shard2);
    return _findFileByName(folderId, hash, includeSize: includeSize);
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

  @override
  Future<void> closeConnections() async {
    try {
      _client.close();
    } catch (_) {}
    _uploadClientPool.closeAll();
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

  Future<String?> _resolveDriveRootId() async {
    final cachedRoot = _resolvedDriveRootId;
    if (cachedRoot != null && cachedRoot.isNotEmpty) {
      return cachedRoot;
    }
    final rootPath = _settings.gdriveRootPath.trim();
    final parts = rootPath.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = _driveFolderId(_driveRootId);
    for (final part in parts) {
      final name = part.trim();
      final cachedId = _folderCache['$current/$name'];
      _DriveFileRef? child;
      if (cachedId != null && cachedId.isNotEmpty) {
        child = _DriveFileRef(id: cachedId, name: name, parentId: current, mimeType: _driveFolderMime);
      } else {
        child = await _findChildFolder(current, name);
      }
      if (child == null) {
        return null;
      }
      current = child.id;
    }
    final cachedVirtBackupId = _folderCache['$current/VirtBackup'];
    _DriveFileRef? virtBackup;
    if (cachedVirtBackupId != null && cachedVirtBackupId.isNotEmpty) {
      virtBackup = _DriveFileRef(id: cachedVirtBackupId, name: 'VirtBackup', parentId: current, mimeType: _driveFolderMime);
    } else {
      virtBackup = await _findChildFolder(current, 'VirtBackup');
    }
    final resolved = virtBackup?.id;
    if (resolved != null && resolved.isNotEmpty) {
      _resolvedDriveRootId = resolved;
    }
    return resolved;
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
    _folderPathById[root] = '';
    var current = root;
    var currentPath = '';
    for (final part in parts) {
      current = await _ensureFolder(current, part);
      currentPath = currentPath.isEmpty ? part : '$currentPath/$part';
      _folderPathById[current] = currentPath;
    }
    return current;
  }

  Future<String?> _findFolderByPath(List<String> parts) async {
    final rootId = await _resolveDriveRootId();
    if (rootId == null) {
      return null;
    }
    _folderPathById[rootId] = '';
    var current = rootId;
    var currentPath = '';
    for (final part in parts) {
      final parentId = current;
      final cachedId = _folderCache['$parentId/$part'];
      _DriveFileRef? child;
      if (cachedId != null && cachedId.isNotEmpty) {
        child = _DriveFileRef(id: cachedId, name: part, parentId: parentId, mimeType: _driveFolderMime);
      } else {
        child = await _findChildFolder(parentId, part);
      }
      if (child == null) {
        return null;
      }
      current = child.id;
      currentPath = currentPath.isEmpty ? part : '$currentPath/$part';
      _folderPathById[current] = currentPath;
      _folderCache['$parentId/$part'] = child.id;
    }
    return current;
  }

  Future<String> _ensureDriveRoot() async {
    final cachedRoot = _resolvedDriveRootId;
    if (cachedRoot != null && cachedRoot.isNotEmpty) {
      return cachedRoot;
    }
    final rootPath = _settings.gdriveRootPath.trim();
    final parts = rootPath.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = _driveFolderId(_driveRootId);
    for (final part in parts) {
      current = _driveFolderId(await _ensureFolder(current, part.trim()));
    }
    final virtBackupId = await _ensureFolder(current, 'VirtBackup');
    _folderPathById[virtBackupId] = '';
    _resolvedDriveRootId = virtBackupId;
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
    final future = _withFolderCreateLock(parentId, name, () async {
      final existing = await _findChildFolder(parentId, name);
      if (existing != null) {
        _folderCache[key] = existing.id;
        return existing.id;
      }
      final created = await _createFolder(parentId, name);
      _folderCache[key] = created.id;
      return created.id;
    });
    _folderInFlight[key] = future;
    try {
      return await future;
    } finally {
      _folderInFlight.remove(key);
    }
  }

  Future<_DriveFileRef?> _findChildFolder(String parentId, String name) async {
    final files = await _findChildFolders(parentId, name);
    return files.isEmpty ? null : files.first;
  }

  Future<List<_DriveFileRef>> _findChildFolders(String parentId, String name) async {
    final query = "mimeType='$_driveFolderMime' and name='${_escapeQuery(name)}' and '$parentId' in parents and trashed=false";
    final files = await _listFilesRaw(query, fields: 'nextPageToken,files(id,name,parents,mimeType,createdTime)');
    if (files.length > 1) {
      final warnPath = _folderDisplayPath(parentId, name);
      _logInfo('gdrive: duplicate folders detected for $warnPath (count=${files.length}); using deterministic first folder');
    }
    files.sort((a, b) => a.id.compareTo(b.id));
    return files;
  }

  String _folderDisplayPath(String parentId, String name) {
    final parentPath = _folderPathById[parentId];
    if (parentPath == null || parentPath.isEmpty) {
      return name;
    }
    return '$parentPath/$name';
  }

  Future<_DriveFileRef> _createFolder(String parentId, String name) async {
    return _withRetry('create folder "$name"', () async {
      final existing = await _findChildFolder(parentId, name);
      if (existing != null) {
        return existing;
      }
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files');
      final body = jsonEncode({
        'name': name,
        'mimeType': _driveFolderMime,
        'parents': [parentId],
      });
      final response = await _requestWithApiLog(
        action: 'mkdir',
        target: _folderDisplayPath(parentId, name),
        method: 'POST',
        uri: uri,
        send: () => _client.post(uri, headers: _authHeaders(token)..['Content-Type'] = 'application/json', body: body),
      );
      if (response.statusCode >= 300) {
        throw 'Drive folder create failed: ${response.statusCode} ${response.body}';
      }
      final decoded = jsonDecode(response.body);
      final created = _DriveFileRef(id: decoded['id'].toString(), name: name, parentId: parentId);
      final reconciled = await _findChildFolder(parentId, name);
      return reconciled ?? created;
    });
  }

  Future<_DriveFileRef> _createFolderBlind(String parentId, String name) async {
    return _withRetry('create folder blind "$name"', () async {
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files');
      final body = jsonEncode({
        'name': name,
        'mimeType': _driveFolderMime,
        'parents': [parentId],
      });
      final response = await _requestWithApiLog(
        action: 'mkdir',
        target: _folderDisplayPath(parentId, name),
        detail: 'blind',
        method: 'POST',
        uri: uri,
        send: () => _client.post(uri, headers: _authHeaders(token)..['Content-Type'] = 'application/json', body: body),
      );
      if (response.statusCode >= 300) {
        throw 'Drive folder create failed: ${response.statusCode} ${response.body}';
      }
      final decoded = jsonDecode(response.body);
      return _DriveFileRef(id: decoded['id'].toString(), name: name, parentId: parentId);
    });
  }

  Future<T> _withFolderCreateLock<T>(String parentId, String name, Future<T> Function() action) async {
    final key = '$parentId/$name';
    return _folderLocks.withLock(key, action);
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
    final listTarget = _listTargetFromQuery(query);
    final isFolderQuery = query.contains("mimeType='$_driveFolderMime'");
    String? pageToken;
    do {
      final params = <String, String>{'q': query, 'fields': fields, 'pageSize': '1000'};
      if (pageToken != null && pageToken.isNotEmpty) {
        params['pageToken'] = pageToken;
      }
      final uri = Uri.https('www.googleapis.com', '/drive/v3/files', params);
      final response = await _withRetry('list files', () async {
        final token = await _ensureAccessToken();
        final response = await _requestWithApiLog(
          action: 'list',
          target: listTarget,
          method: 'GET',
          uri: uri,
          send: () => _client.get(uri, headers: _authHeaders(token)),
        );
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
          final mimeType = map['mimeType']?.toString();
          final id = map['id'].toString();
          final name = map['name'].toString();
          results.add(_DriveFileRef(id: id, name: name, parentId: parentId, size: size, mimeType: mimeType));
          if (isFolderQuery || mimeType == _driveFolderMime) {
            _folderPathById[id] = _folderDisplayPath(parentId, name);
          }
        }
      }
      pageToken = decoded['nextPageToken']?.toString();
    } while (pageToken != null && pageToken.isNotEmpty);
    return results;
  }

  String _listTargetFromQuery(String query) {
    final match = RegExp("'([^']+)'\\s+in\\s+parents").firstMatch(query);
    if (match == null) {
      return 'unknown';
    }
    final parentId = match.group(1);
    if (parentId == null || parentId.isEmpty) {
      return 'unknown';
    }
    return _displayPathForId(parentId);
  }

  String _displayPathForId(String id) {
    if (id == _driveRootId) {
      return 'root';
    }
    if (!_folderPathById.containsKey(id)) {
      return 'unknown';
    }
    final path = _folderPathById[id] ?? '';
    if (path.isEmpty) {
      return 'VirtBackup';
    }
    return path;
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
    final lease = await _leaseUploadClient();
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
      lease.release();
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
          final response = await _requestWithApiLog(
            action: 'upload',
            target: name,
            detail: 'session',
            method: 'POST',
            uri: uri,
            send: () => lease.client.post(
              uri,
              headers: headers,
              body: jsonEncode({
                'name': name,
                'parents': [parentId],
              }),
            ),
          );
          if (response.statusCode >= 300 && response.statusCode != 401) {
            throw _DriveHttpException('Drive upload session failed', response.statusCode, response.body);
          }
          return response;
        }, onRetry: lease.resetClient);
      }

      Future<http.Response> uploadData(Uri sessionUri, String token) {
        return _withRetry('upload data $name', () async {
          final headers = _authHeaders(token)
            ..['Content-Type'] = 'application/octet-stream'
            ..['Content-Length'] = bytes.length.toString();
          final response = await _requestWithApiLog(
            action: 'upload',
            target: name,
            detail: 'data',
            method: 'PUT',
            uri: sessionUri,
            send: () => lease.client.put(sessionUri, headers: headers, body: bytes),
          );
          if (response.statusCode >= 300 && response.statusCode != 401) {
            throw _DriveHttpException('Drive upload failed', response.statusCode, response.body);
          }
          return response;
        }, onRetry: lease.resetClient);
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
      lease.resetClient();
      rethrow;
    }
  }

  Future<List<int>> _downloadFile(String fileId) async {
    return _withRetry('download file', () async {
      final token = await _ensureAccessToken();
      final uri = Uri.parse('https://www.googleapis.com/drive/v3/files/$fileId?alt=media');
      final headers = _authHeaders(token);
      final response = await _requestWithApiLog(
        action: 'download',
        target: _displayPathForId(fileId),
        method: 'GET',
        uri: uri,
        send: () => _client.get(uri, headers: headers),
      );
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
      final response = await _requestWithApiLog(
        action: 'auth.refresh',
        method: 'POST',
        uri: uri,
        send: () => _client.post(uri, headers: {'Content-Type': 'application/x-www-form-urlencoded'}, body: body),
      );
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
      final response = await _requestWithApiLog(
        action: 'auth.refresh',
        detail: 'forced',
        method: 'POST',
        uri: uri,
        send: () => _client.post(uri, headers: {'Content-Type': 'application/x-www-form-urlencoded'}, body: body),
      );
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
      return '${blobsDir().path}${Platform.pathSeparator}$hash';
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    return '${blobsDir().path}${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2${Platform.pathSeparator}$hash';
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

  File _debugLogFile() {
    final basePath = _settings.backupPath.trim();
    if (basePath.isNotEmpty) {
      return File('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}logs${Platform.pathSeparator}debug.log');
    }
    return File('${_cacheRoot.path}${Platform.pathSeparator}logs${Platform.pathSeparator}debug.log');
  }

  Future<void> _prepareDebugLogFile() async {
    final path = _debugLogFile().path;
    if (_debugLogPrepared) {
      await Directory(_debugLogFile().parent.path).create(recursive: true);
      return;
    }
    await DebugLogWriter.truncate(path);
    _debugLogPrepared = true;
  }

  Future<http.Response> _requestWithApiLog({required String action, required String method, required Uri uri, required Future<http.Response> Function() send, String? target, String? detail}) async {
    final stopwatch = Stopwatch()..start();
    final targetText = target == null || target.isEmpty ? '' : ' target=$target';
    final detailText = detail == null || detail.isEmpty ? '' : ' detail=$detail';
    try {
      final response = await send();
      stopwatch.stop();
      await _appendApiLogLine(
        '${DateTime.now().toIso8601String()} action=$action status=${response.statusCode} durationMs=${stopwatch.elapsedMilliseconds}$targetText$detailText method=${method.toUpperCase()}',
      );
      return response;
    } catch (error) {
      stopwatch.stop();
      await _appendApiLogLine(
        '${DateTime.now().toIso8601String()} action=$action status=error durationMs=${stopwatch.elapsedMilliseconds}$targetText$detailText method=${method.toUpperCase()} error=$error',
      );
      rethrow;
    }
  }

  Future<void> _appendApiLogLine(String line) async {
    try {
      await DebugLogWriter.appendLine(_debugLogFile().path, line);
    } catch (_) {}
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

  Future<_UploadClientLease> _leaseUploadClient() async {
    return _uploadClientPool.lease(() => IOClient(_createHttpClient()));
  }
}

class _UploadClientLease {
  _UploadClientLease(this._pool, this._slot, this._createClient);

  final _HttpClientPool _pool;
  final _PooledHttpClient _slot;
  final http.Client Function() _createClient;
  bool _released = false;

  http.Client get client => _slot.client;

  void resetClient() {
    _pool.reset(_slot, _createClient);
  }

  void release() {
    if (_released) {
      return;
    }
    _released = true;
    _pool.release(_slot, _createClient);
  }
}

class _HttpClientPool {
  _HttpClientPool({required this.maxClients});

  final int maxClients;
  final List<_PooledHttpClient> _idle = <_PooledHttpClient>[];
  final List<_PooledHttpClient> _all = <_PooledHttpClient>[];
  final List<Completer<_UploadClientLease>> _waiters = <Completer<_UploadClientLease>>[];
  var _connecting = 0;
  bool _closed = false;

  Future<_UploadClientLease> lease(http.Client Function() createClient) async {
    if (_closed) {
      throw 'GDrive upload client pool is closed.';
    }
    if (_idle.isNotEmpty) {
      final slot = _idle.removeLast();
      return _UploadClientLease(this, slot, createClient);
    }
    if (_all.length + _connecting < maxClients) {
      _connecting += 1;
      try {
        final slot = _PooledHttpClient(client: createClient());
        _all.add(slot);
        return _UploadClientLease(this, slot, createClient);
      } finally {
        _connecting -= 1;
      }
    }
    final waiter = Completer<_UploadClientLease>();
    _waiters.add(waiter);
    return waiter.future;
  }

  void reset(_PooledHttpClient slot, http.Client Function() createClient) {
    if (_closed) {
      return;
    }
    try {
      slot.client.close();
    } catch (_) {}
    slot.client = createClient();
  }

  void release(_PooledHttpClient slot, http.Client Function() createClient) {
    if (_closed) {
      try {
        slot.client.close();
      } catch (_) {}
      return;
    }
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.isCompleted) {
        waiter.complete(_UploadClientLease(this, slot, createClient));
        return;
      }
    }
    _idle.add(slot);
  }

  void closeAll() {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.isCompleted) {
        waiter.completeError('GDrive upload client pool is closed.');
      }
    }
    _waiters.clear();
    for (final slot in _all) {
      try {
        slot.client.close();
      } catch (_) {}
    }
    _all.clear();
    _idle.clear();
    _connecting = 0;
  }
}

class _PooledHttpClient {
  _PooledHttpClient({required this.client});

  http.Client client;
}

class _NamedAsyncLock {
  final Map<String, _AsyncLockQueue> _queues = <String, _AsyncLockQueue>{};

  Future<T> withLock<T>(String key, Future<T> Function() action) async {
    final queue = _queues.putIfAbsent(key, () => _AsyncLockQueue());
    queue.pending += 1;
    final previous = queue.tail;
    final release = Completer<void>();
    queue.tail = release.future;
    await previous;
    try {
      return await action();
    } finally {
      if (!release.isCompleted) {
        release.complete();
      }
      queue.pending -= 1;
      if (queue.pending <= 0) {
        _queues.remove(key);
      }
    }
  }
}

class _AsyncLockQueue {
  Future<void> tail = Future<void>.value();
  int pending = 0;
}

class _DriveFileRef {
  _DriveFileRef({required this.id, required this.name, required this.parentId, this.size, this.mimeType});

  final String id;
  final String name;
  final String parentId;
  final int? size;
  final String? mimeType;
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
