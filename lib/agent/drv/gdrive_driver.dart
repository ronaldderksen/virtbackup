import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/google_oauth_client.dart';
import 'package:virtbackup/common/models.dart' show BackupStorage;
import 'package:virtbackup/common/settings.dart';

class GdrivePrefillStats {
  const GdrivePrefillStats({required this.shardCount, required this.blobCount, required this.durationMs});

  final int shardCount;
  final int blobCount;
  final int durationMs;
}

class _PermanentGdriveConfigError implements Exception {
  _PermanentGdriveConfigError(this.message);

  final String message;

  @override
  String toString() => message;
}

class GdriveBackupDriver implements BackupDriver, RemoteBlobDriver, BlobDirectoryLister {
  GdriveBackupDriver({required AppSettings settings, required Future<void> Function(AppSettings) persistSettings, Directory? settingsDir, void Function(String message)? logInfo})
    : _settings = settings,
      _persistSettings = persistSettings,
      _settingsDir = settingsDir,
      _cacheRoot = _cacheRootForSettings(settings),
      _logInfo = ((message) => (logInfo ?? ((_) {}))('driver=gdrive $message'));

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
  final _NamedAsyncLock _folderLocks = _NamedAsyncLock();
  bool _agentLogConfigured = false;
  Future<String>? _tokenRefreshInFlight;
  http.Client _client = IOClient(_createHttpClient());
  final _HttpClientPool _uploadClientPool = _HttpClientPool(maxClients: _uploadConcurrency);
  int _inFlightUploads = 0;
  String? _blobsRootId;
  String? _resolvedDriveRootId;
  void Function(GdrivePrefillStats stats, bool done)? onPrefillProgress;

  static Directory _cacheRootForSettings(AppSettings settings) {
    final basePath = settings.backupPath.trim();
    if (basePath.isEmpty) {
      throw StateError('GDrive cache root requires backupPath in settings.');
    }
    final storageId = _cacheKeyFromSettings(settings);
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}cache${Platform.pathSeparator}$storageId');
  }

  static String _cacheKeyFromSettings(AppSettings settings) {
    final storageId = settings.backupStorageId?.trim() ?? '';
    if (storageId.isEmpty) {
      throw StateError('GDrive cache root requires backupStorageId in settings.');
    }
    return storageId.replaceAll(RegExp(r'[^A-Za-z0-9._-]'), '_');
  }

  BackupStorage get _gdriveStorage => _resolveSelectedGdriveStorage(_settings);
  Map<String, dynamic> get _params => _gdriveStorage.params;
  String get _rootPath => (_params['rootPath'] ?? '').toString().trim();
  String get _accessToken => (_params['accessToken'] ?? '').toString().trim();
  String get _refreshToken => (_params['refreshToken'] ?? '').toString().trim();
  DateTime? get _expiresAt => AppSettings.parseDateTimeOrNull(_params['expiresAt']);

  static BackupStorage _resolveSelectedGdriveStorage(AppSettings settings) {
    final selectedId = settings.backupStorageId?.trim() ?? '';
    if (selectedId.isEmpty) {
      throw StateError('Google Drive settings require backupStorageId.');
    }
    for (final storage in settings.storage) {
      if (storage.id == selectedId) {
        if (storage.driverId != 'gdrive') {
          throw StateError('Selected storage "$selectedId" is not a Google Drive storage.');
        }
        return storage;
      }
    }
    throw StateError('Google Drive storage "$selectedId" not found.');
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
  String get storage => _cacheRoot.path;

  @override
  bool get discardWrites => false;

  @override
  int get bufferedBytes => 0;

  GdrivePrefillStats _prefillStats = const GdrivePrefillStats(shardCount: 0, blobCount: 0, durationMs: 0);

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
  Directory blobsDir() {
    final basePath = _settings.backupPath.trim();
    if (basePath.isEmpty) {
      return Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs${Platform.pathSeparator}${_settings.blockSizeMB}');
    }
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}blobs${Platform.pathSeparator}${_settings.blockSizeMB}');
  }

  @override
  Directory tmpDir() {
    return Directory('${_cacheRoot.path}${Platform.pathSeparator}tmp');
  }

  @override
  File blobFile(String hash) {
    final path = _blobCachePath(hash);
    return File(path);
  }

  @override
  Future<Set<String>> listBlobShards() async {
    await _ensureBlobsRoot();
    final rootId = _blobsRootId;
    if (rootId == null || rootId.isEmpty) {
      return <String>{};
    }
    final folders = await _listChildFolders(rootId);
    final names = <String>{};
    for (final folder in folders) {
      names.add(folder.name);
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    final shardId = await _findFolderByPath(<String>[..._blobsPathPrefix(), shard]);
    if (shardId == null || shardId.isEmpty) {
      return <String>{};
    }
    final files = await _listFilesRaw("mimeType!='$_driveFolderMime' and '$shardId' in parents and trashed=false", fields: 'nextPageToken,files(id,name,parents)');
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
    await _configureAgentLogPath();
    await _cacheRoot.create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureBlobsRoot();
    await _warmFolderCache();
    _logInfo('gdrive: ready (cache=${_cacheRoot.path})');
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    await _cacheRoot.create(recursive: true);
    await tmpDir().create(recursive: true);
    await _ensureBlobsRoot();
    _logInfo('gdrive: prepare backup for $serverId/$vmName');
  }

  @override
  Future<void> uploadFile({required String relativePath, required File localFile}) async {
    final normalizedPath = _normalizeRelativePath(relativePath);
    _logInfo('gdrive: upload file $normalizedPath');
    await _commitRelativeFile(localFile: localFile, relativePath: normalizedPath);
  }

  @override
  Future<List<String>> listRelativeFiles(String relativeDir) async {
    final normalizedDir = _normalizeRelativePath(relativeDir);
    if (normalizedDir.isEmpty) {
      return <String>[];
    }
    final parts = _splitRelativePath(normalizedDir);
    final folderId = await _findFolderByPath(parts);
    if (folderId == null || folderId.isEmpty) {
      return <String>[];
    }
    final result = <String>[];
    await _listDriveFilesRecursively(folderId: folderId, relativePrefix: normalizedDir, out: result);
    result.sort();
    return result;
  }

  @override
  Future<List<int>?> readFileBytes(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return null;
    }
    final parts = _splitRelativePath(normalized);
    if (parts.isEmpty) {
      return null;
    }
    final name = parts.last;
    final folderParts = parts.take(parts.length - 1).toList();
    final folderId = folderParts.isEmpty ? await _resolveDriveRootId() : await _findFolderByPath(folderParts);
    if (folderId == null || folderId.isEmpty) {
      return null;
    }
    final fileRef = await _findFileByName(folderId, name, includeSize: true);
    if (fileRef == null) {
      return null;
    }
    return _downloadFile(fileRef.id);
  }

  @override
  Future<bool> deleteFile(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return false;
    }
    final parts = _splitRelativePath(normalized);
    if (parts.isEmpty) {
      return false;
    }
    final name = parts.last;
    final folderParts = parts.take(parts.length - 1).toList();
    final folderId = folderParts.isEmpty ? await _resolveDriveRootId() : await _findFolderByPath(folderParts);
    if (folderId == null || folderId.isEmpty) {
      return false;
    }
    final fileRef = await _findFileByName(folderId, name, includeSize: false);
    if (fileRef == null) {
      return false;
    }
    await _deleteDriveFile(fileRef.id, normalized);
    final localFile = _relativeCacheFile(normalized);
    if (await localFile.exists()) {
      await localFile.delete();
    }
    return true;
  }

  @override
  Future<void> freshCleanup() async {
    _logInfo('gdrive: fresh cleanup clearing local cache and in-memory state');
    _folderCache.clear();
    _folderChildrenLoaded.clear();
    _blobsRootId = null;
    _resolvedDriveRootId = null;
    _prefillStats = const GdrivePrefillStats(shardCount: 0, blobCount: 0, durationMs: 0);
    await _deleteDirIfExists(Directory('${_cacheRoot.path}${Platform.pathSeparator}manifests'));
    await _deleteDirIfExists(Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs'));
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 2) {
      return;
    }
    final shardKey = hash.substring(0, 2);
    await _ensureFolderByPath(<String>[..._blobsPathPrefix(), shardKey]);
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2 || bytes.isEmpty) {
      return;
    }
    final shardKey = hash.substring(0, 2);
    final parentId = await _findFolderByPath(<String>[..._blobsPathPrefix(), shardKey]);
    if (parentId == null || parentId.isEmpty) {
      throw 'Blob shard folder not ready for $shardKey';
    }
    await _uploadSimple(name: hash, parentId: parentId, bytes: bytes, contentType: 'application/octet-stream');
  }

  @override
  Future<bool> blobExistsRemote(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    return (await _findBlobByHash(hash, includeSize: false)) != null;
  }

  @override
  Future<int?> blobLength(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    final found = await _findBlobByHash(hash, includeSize: true);
    if (found == null) {
      return null;
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
    if (hash.length < 2) {
      return null;
    }
    final fileRef = await _findBlobByHash(hash, includeSize: true);
    if (fileRef == null) {
      return null;
    }
    return _downloadFile(fileRef.id);
  }

  Future<_DriveFileRef?> _findBlobByHash(String hash, {bool includeSize = false}) async {
    if (hash.length < 2) {
      return null;
    }
    final shardKey = hash.substring(0, 2);
    final folderId = await _findFolderByPath(<String>[..._blobsPathPrefix(), shardKey]);
    if (folderId == null || folderId.isEmpty) {
      return null;
    }
    return _findFileByName(folderId, hash, includeSize: includeSize);
  }

  @override
  String backupCompletedMessage(String outputPath) {
    return 'Backup saved to Google Drive';
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
    await _deleteInProgressInDir(_cacheRoot);
    await _deleteInProgressInDir(tmpDir());
  }

  @override
  Future<void> closeConnections() async {
    try {
      _client.close();
    } catch (_) {}
    _uploadClientPool.closeAll();
  }

  @override
  void setWriteConcurrencyLimit(int concurrency) {
    return;
  }

  Future<void> _ensureBlobsRoot() async {
    if (_blobsRootId != null) {
      return;
    }
    _blobsRootId = await _ensureFolderByPath(_blobsPathPrefix());
  }

  List<String> _blobsPathPrefix() => <String>['blobs', _settings.blockSizeMB.toString()];

  Future<String?> _resolveDriveRootId() async {
    final cachedRoot = _resolvedDriveRootId;
    if (cachedRoot != null && cachedRoot.isNotEmpty) {
      return cachedRoot;
    }
    final rootPath = _rootPath;
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

  Future<void> _commitRelativeFile({required File localFile, required String relativePath}) async {
    if (!await localFile.exists()) {
      return;
    }
    final bytes = await localFile.readAsBytes();
    final parts = _splitRelativePath(relativePath);
    if (parts.isEmpty) {
      return;
    }
    final finalName = parts.last;
    final parentParts = <String>[...parts.take(parts.length - 1)];
    final remoteFolder = await _ensureFolderByPath(parentParts);
    _logInfo('gdrive: upload file $relativePath size=${bytes.length}');
    await _uploadSimple(name: finalName, parentId: remoteFolder, bytes: bytes, contentType: 'application/octet-stream');
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
    final rootPath = _rootPath;
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

  Future<T> _withFolderCreateLock<T>(String parentId, String name, Future<T> Function() action) async {
    final key = '$parentId/$name';
    return _folderLocks.withLock(key, action);
  }

  Future<List<_DriveFileRef>> _listChildFolders(String parentId) async {
    final query = "mimeType='$_driveFolderMime' and '$parentId' in parents and trashed=false";
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
        final headers = _authHeaders(token)
          ..['Content-Type'] = 'application/json; charset=UTF-8'
          ..['X-Upload-Content-Type'] = 'application/octet-stream'
          ..['X-Upload-Content-Length'] = bytes.length.toString();
        return _requestWithApiLog(
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
      }

      Future<http.Response> uploadData(Uri sessionUri, String token) {
        final headers = _authHeaders(token)
          ..['Content-Type'] = 'application/octet-stream'
          ..['Content-Length'] = bytes.length.toString();
        return _requestWithApiLog(
          action: 'upload',
          target: name,
          detail: 'data',
          method: 'PUT',
          uri: sessionUri,
          send: () => lease.client.put(sessionUri, headers: headers, body: bytes),
        );
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
    final expiresAt = _expiresAt;
    final token = _accessToken;
    if (token.isNotEmpty && expiresAt != null && expiresAt.isAfter(now.add(const Duration(minutes: 1)))) {
      return token;
    }
    final inFlight = _tokenRefreshInFlight;
    if (inFlight != null) {
      return inFlight;
    }
    final refresh = _refreshToken;
    if (refresh.isEmpty) {
      throw _PermanentGdriveConfigError('Google Drive refresh token is missing.');
    }
    _logInfo('gdrive: refreshing access token');
    final refreshFuture = _refreshAccessTokenInternal(refresh, forced: false);
    _tokenRefreshInFlight = refreshFuture;
    try {
      return await refreshFuture;
    } finally {
      if (identical(_tokenRefreshInFlight, refreshFuture)) {
        _tokenRefreshInFlight = null;
      }
    }
  }

  Future<String> _refreshAccessToken() async {
    final inFlight = _tokenRefreshInFlight;
    if (inFlight != null) {
      return inFlight;
    }
    final refresh = _refreshToken;
    if (refresh.isEmpty) {
      throw _PermanentGdriveConfigError('Google Drive refresh token is missing.');
    }
    _logInfo('gdrive: refreshing access token (forced)');
    final refreshFuture = _refreshAccessTokenInternal(refresh, forced: true);
    _tokenRefreshInFlight = refreshFuture;
    try {
      return await refreshFuture;
    } finally {
      if (identical(_tokenRefreshInFlight, refreshFuture)) {
        _tokenRefreshInFlight = null;
      }
    }
  }

  Future<String> _refreshAccessTokenInternal(String refresh, {required bool forced}) async {
    final oauth = await _ensureOAuthClient();
    final uri = Uri.parse('https://oauth2.googleapis.com/token');
    final response = await _withRetry(forced ? 'refresh token (forced)' : 'refresh token', () async {
      final body = <String, String>{'client_id': oauth.clientId, 'client_secret': oauth.clientSecret, 'refresh_token': refresh, 'grant_type': 'refresh_token'};
      final response = await _requestWithApiLog(
        action: 'auth.refresh',
        detail: forced ? 'forced' : null,
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
    await _persistRefreshedToken(refreshToken: refresh, accessToken: accessToken, expiresAt: newExpiresAt);
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

  Future<void> _persistRefreshedToken({required String refreshToken, required String accessToken, required DateTime? expiresAt}) async {
    final storage = _gdriveStorage;
    final updatedParams = Map<String, dynamic>.from(storage.params);
    updatedParams['refreshToken'] = refreshToken;
    updatedParams['accessToken'] = accessToken;
    if (expiresAt == null) {
      updatedParams.remove('expiresAt');
    } else {
      updatedParams['expiresAt'] = expiresAt.toUtc().toIso8601String();
    }

    final updatedStorages = <BackupStorage>[];
    for (final candidate in _settings.storage) {
      if (candidate.id != storage.id) {
        updatedStorages.add(candidate);
        continue;
      }
      updatedStorages.add(
        BackupStorage(
          id: candidate.id,
          name: candidate.name,
          driverId: candidate.driverId,
          enabled: candidate.enabled,
          params: updatedParams,
          disableFresh: candidate.disableFresh,
          storeBlobs: candidate.storeBlobs,
          useBlobs: candidate.useBlobs,
          uploadConcurrency: candidate.uploadConcurrency,
          downloadConcurrency: candidate.downloadConcurrency,
        ),
      );
    }

    _settings = _settings.copyWith(storage: updatedStorages);
    await _persistSettings(_settings);
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
    if (hash.length < 2) {
      return '${blobsDir().path}${Platform.pathSeparator}$hash';
    }
    final shard = hash.substring(0, 2);
    return '${blobsDir().path}${Platform.pathSeparator}$shard${Platform.pathSeparator}$hash';
  }

  File _relativeCacheFile(String relativePath) {
    final normalized = _normalizeRelativePath(relativePath).replaceAll('/', Platform.pathSeparator);
    return File('${_cacheRoot.path}${Platform.pathSeparator}$normalized');
  }

  String _normalizeRelativePath(String relativePath) {
    return _splitRelativePath(relativePath).join('/');
  }

  List<String> _splitRelativePath(String relativePath) {
    return relativePath.replaceAll('\\', '/').split('/').map((part) => part.trim()).where((part) => part.isNotEmpty && part != '.').toList();
  }

  Future<_DriveFileRef?> _findFileByName(String parentId, String name, {bool includeSize = false}) async {
    final fields = includeSize ? 'nextPageToken,files(id,name,parents,size)' : 'nextPageToken,files(id,name,parents)';
    final files = await _listFilesRaw("'$parentId' in parents and name='${_escapeQuery(name)}' and trashed=false", fields: fields);
    return files.isEmpty ? null : files.first;
  }

  Future<void> _deleteDriveFile(String fileId, String relativePath) async {
    await _withRetry('delete file', () async {
      var token = await _ensureAccessToken();
      var uri = Uri.parse('https://www.googleapis.com/drive/v3/files/$fileId');
      var response = await _requestWithApiLog(
        action: 'delete',
        target: relativePath,
        method: 'DELETE',
        uri: uri,
        send: () => _client.delete(uri, headers: _authHeaders(token)),
      );
      if (response.statusCode == 401) {
        token = await _refreshAccessToken();
        uri = Uri.parse('https://www.googleapis.com/drive/v3/files/$fileId');
        response = await _requestWithApiLog(
          action: 'delete',
          target: relativePath,
          detail: 'retry-auth',
          method: 'DELETE',
          uri: uri,
          send: () => _client.delete(uri, headers: _authHeaders(token)),
        );
      }
      if (response.statusCode == 404) {
        return;
      }
      if (response.statusCode >= 300) {
        throw 'Drive delete failed: ${response.statusCode} ${response.body}';
      }
    });
  }

  Future<void> _listDriveFilesRecursively({required String folderId, required String relativePrefix, required List<String> out}) async {
    final files = await _listFilesRaw("mimeType!='$_driveFolderMime' and '$folderId' in parents and trashed=false", fields: 'nextPageToken,files(id,name,parents)');
    for (final file in files) {
      out.add('$relativePrefix/${file.name}');
    }
    final folders = await _listChildFolders(folderId);
    for (final folder in folders) {
      final childPrefix = '$relativePrefix/${folder.name}';
      await _listDriveFilesRecursively(folderId: folder.id, relativePrefix: childPrefix, out: out);
    }
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

  Future<T> _withRetry<T>(String label, Future<T> Function() action, {void Function()? onRetry}) async {
    var delaySeconds = 2;
    var attempt = 0;
    const maxRetries = 5;
    while (true) {
      try {
        return await action();
      } catch (error, stackTrace) {
        if (error is _PermanentGdriveConfigError) {
          _logInfo('gdrive: $label failed: $error');
          rethrow;
        }
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

  Future<http.Response> _requestWithApiLog({required String action, required String method, required Uri uri, required Future<http.Response> Function() send, String? target, String? detail}) async {
    final stopwatch = Stopwatch()..start();
    final targetText = target == null || target.isEmpty ? '' : ' target=$target';
    final detailText = detail == null || detail.isEmpty ? '' : ' detail=$detail';
    try {
      final response = await send();
      stopwatch.stop();
      await _appendApiLogLine('action=$action status=${response.statusCode} durationMs=${stopwatch.elapsedMilliseconds}$targetText$detailText method=${method.toUpperCase()}');
      return response;
    } catch (error) {
      stopwatch.stop();
      await _appendApiLogLine('action=$action status=error durationMs=${stopwatch.elapsedMilliseconds}$targetText$detailText method=${method.toUpperCase()} error=$error');
      rethrow;
    }
  }

  Future<void> _appendApiLogLine(String line) async {
    try {
      await _configureAgentLogPath();
      LogWriter.logAgentSync(level: 'debug', message: 'driver=gdrive $line');
    } catch (_) {}
  }

  Future<void> _configureAgentLogPath() async {
    if (_agentLogConfigured) {
      return;
    }
    await LogWriter.configureSourcePath(
      source: 'agent',
      path: LogWriter.defaultPathForSource('agent', basePath: _settings.backupPath.trim()),
    );
    LogWriter.configureSourceLevel(source: 'agent', level: _settings.logLevel);
    _agentLogConfigured = true;
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
