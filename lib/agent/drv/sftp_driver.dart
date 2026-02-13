import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:ffi/ffi.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/settings.dart';

class SftpBackupDriver implements BackupDriver, RemoteBlobDriver, BlobDirectoryLister {
  SftpBackupDriver({required AppSettings settings}) : _settings = settings, _cacheRoot = _cacheRootForSettings(settings);

  final AppSettings _settings;
  final Directory _cacheRoot;
  final _NativeSftpBindings? _nativeSftp = _NativeSftpBindings.tryLoad();

  static const int _writeConcurrency = 8;
  static const int _blobCacheReservedSessions = 2;
  static const int _generalPoolSessions = _writeConcurrency > _blobCacheReservedSessions ? _writeConcurrency - _blobCacheReservedSessions : 1;
  // Temporary safety switch: keep upload code intact but disable remote blob uploads.
  // Set to true when cache behavior is verified and uploads should be active again.
  static const bool _remoteBlobUploadEnabled = true;
  static const String _remoteAppFolderName = 'VirtBackup';
  final _SftpPool _pool = _SftpPool(maxSessions: _generalPoolSessions);
  final _SftpPool _blobCachePool = _SftpPool(maxSessions: _blobCacheReservedSessions);
  final _NativeSftpPool _nativePool = _NativeSftpPool(maxSessions: _writeConcurrency);
  final Set<String> _knownRemoteShardKeys = <String>{};
  final Set<String> _remoteBlobNames = <String>{};
  final Map<String, int> _remoteBlobSizes = <String, int>{};

  String get _host => _settings.sftpHost.trim();
  int get _port => _settings.sftpPort;
  String get _username => _settings.sftpUsername.trim();
  String get _password => _settings.sftpPassword;
  String get _basePath => _settings.sftpBasePath.trim();

  static Directory _cacheRootForSettings(AppSettings settings) {
    final basePath = settings.backupPath.trim();
    if (basePath.isEmpty) {
      // Fallback for misconfiguration; callers should configure backup.base_path.
      return Directory('${_tempBase()}${Platform.pathSeparator}virtbackup_sftp_cache');
    }
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}cache${Platform.pathSeparator}sftp');
  }

  @override
  BackupDriverCapabilities get capabilities => const BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: false,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: _writeConcurrency,
    params: [],
  );

  @override
  String get destination => _cacheRoot.path;

  @override
  bool get discardWrites => false;

  @override
  int get bufferedBytes => 0;

  @override
  Future<void> ensureReady() async {
    await _cacheRoot.create(recursive: true);
    await manifestsDir('tmp', 'tmp').parent.create(recursive: true);
    await tmpDir().create(recursive: true);
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    _validateConfig();
    await manifestsDir(serverId, vmName).create(recursive: true);
    await tmpDir().create(recursive: true);

    await _withSftp('prepare remote layout', (sftp) async {
      await _ensureRemoteDir(sftp, _remoteRoot());
      await _ensureRemoteDir(sftp, _remoteManifestsRoot());
      await _ensureRemoteDir(sftp, _remoteBlobsRoot());
      await _ensureRemoteDir(sftp, _remoteTmpRoot());
      await _ensureRemoteDir(sftp, _remoteManifestFolder(serverId, vmName));
    });
  }

  void _validateConfig() {
    if (_host.isEmpty) {
      throw 'SFTP host is missing.';
    }
    if (_username.isEmpty) {
      throw 'SFTP username is missing.';
    }
    if (_password.isEmpty) {
      throw 'SFTP password is missing.';
    }
    if (_basePath.isEmpty) {
      throw 'SFTP base path is missing.';
    }
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
  Directory tmpDir() => Directory('${_cacheRoot.path}${Platform.pathSeparator}tmp');

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
  Future<Set<String>> listBlobShards() async {
    return _withSftp('list blobs', (sftp) async {
      final entries = await sftp.listdir(_remoteBlobsRoot());
      final names = <String>{};
      for (final entry in entries) {
        final name = entry.filename;
        if (name == '.' || name == '..') {
          continue;
        }
        if (!entry.attr.isDirectory) {
          continue;
        }
        names.add(name);
      }
      return names;
    });
  }

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    return _withSftp('list blobs/$shard', (sftp) async {
      final entries = await sftp.listdir(_remoteJoin(_remoteBlobsRoot(), shard));
      final names = <String>{};
      for (final entry in entries) {
        final name = entry.filename;
        if (name == '.' || name == '..') {
          continue;
        }
        if (entry.attr.isDirectory) {
          continue;
        }
        if (name.endsWith('.inprogress')) {
          continue;
        }
        names.add(name);
      }
      return names;
    });
  }

  Future<void> _appendDebugLogLine(String line) async {
    if (line.isEmpty) {
      return;
    }
    return;
  }

  @override
  DriverFileWrite startXmlWrite(String serverId, String vmName, String timestamp) {
    final file = xmlFile(serverId, vmName, timestamp, inProgress: true);
    file.parent.createSync(recursive: true);
    final sink = file.openWrite();
    return DriverFileWrite(
      sink: sink,
      commit: () async {
        await _commitSmallRemoteFile(localFile: file, remotePath: _remoteJoin(_remoteManifestFolder(serverId, vmName), '${timestamp}__domain.xml'));
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
        await _commitSmallRemoteFile(localFile: file, remotePath: _remoteJoin(_remoteManifestFolder(serverId, vmName), '${timestamp}__$diskId.chain'));
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
        await _withSftp('ensure disk manifest folder', (sftp) async {
          await _ensureRemoteDir(sftp, _remoteDiskManifestFolder(serverId, vmName, diskId));
        });
        await _commitSmallRemoteFile(localFile: gzFile, remotePath: _remoteJoin(_remoteDiskManifestFolder(serverId, vmName, diskId), '$timestamp.manifest.gz'));
      },
    );
  }

  @override
  Future<void> finalizeManifest(DriverManifestWrite write) async {
    await write.commit();
  }

  @override
  Future<void> freshCleanup() async {
    _validateConfig();
    final base = _normalizeRemotePath(_basePath);
    final current = _remoteRoot();
    final stamp = sanitizeFileName(DateTime.now().toUtc().toIso8601String());
    final renamed = _remoteJoin(base, '${_remoteAppFolderName}__fresh_$stamp');
    final opStopwatch = Stopwatch();
    await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=fresh_cleanup status=running durationMs=0 target=VirtBackup method=RENAME detail=from:$current to:$renamed');

    await _withSftp('fresh cleanup rename remote root', (sftp) async {
      opStopwatch.start();
      try {
        await sftp.rename(current, renamed);
      } catch (error) {
        // If the folder doesn't exist yet, there is nothing to rename.
        try {
          await sftp.stat(current);
        } catch (_) {
          opStopwatch.stop();
          return;
        }
        rethrow;
      } finally {
        if (opStopwatch.isRunning) {
          opStopwatch.stop();
        }
      }
    });
    await _appendDebugLogLine(
      '${DateTime.now().toIso8601String()} action=fresh_cleanup status=200 durationMs=${opStopwatch.elapsedMilliseconds} target=VirtBackup method=RENAME detail=from:$current to:$renamed',
    );
    _knownRemoteShardKeys.clear();
    _remoteBlobNames.clear();
    _remoteBlobSizes.clear();
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 2) {
      return;
    }
    final shardKey = hash.substring(0, 2);
    if (_knownRemoteShardKeys.contains(shardKey)) {
      return;
    }
    final remoteDir = _remoteJoin(_remoteBlobsRoot(), shardKey);
    await _withSftp('ensure blob dir:$remoteDir', (sftp) async {
      await _ensureRemoteDirBlind(sftp, remoteDir);
    });
    _knownRemoteShardKeys.add(shardKey);
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2 || bytes.isEmpty) {
      return;
    }
    if (!_remoteBlobUploadEnabled) {
      _remoteBlobNames.add(hash);
      _remoteBlobSizes[hash] = bytes.length;
      await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=upload status=disabled durationMs=0 method=WRITE detail=remote-upload-disabled hash=$hash bytes=${bytes.length}');
      return;
    }
    final remotePath = _remoteBlobPath(hash);
    final remoteTemp = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final data = Uint8List.fromList(bytes);
    return _writeBlobWithTiming(hash: hash, remoteTemp: remoteTemp, remotePath: remotePath, data: data);
  }

  @override
  Future<bool> blobExists(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    return _remoteBlobNames.contains(hash);
  }

  @override
  Future<bool> blobExistsRemote(String hash) async {
    return blobExists(hash);
  }

  @override
  Future<int?> blobLength(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    final local = blobFile(hash);
    if (await local.exists()) {
      return local.length();
    }
    final cached = _remoteBlobSizes[hash];
    if (cached != null) {
      return cached;
    }
    return _withSftp('blob length', (sftp) async {
      try {
        final attrs = await sftp.stat(_remoteBlobPath(hash));
        final size = attrs.size;
        if (size != null) {
          _remoteBlobNames.add(hash);
          _remoteBlobSizes[hash] = size;
        }
        return size;
      } catch (_) {
        return null;
      }
    });
  }

  @override
  Stream<List<int>> openBlobStream(String hash, {int? length}) async* {
    if (hash.length < 2) {
      return;
    }
    final local = blobFile(hash);
    if (await local.exists()) {
      final stream = length == null ? local.openRead() : local.openRead(0, length);
      await for (final chunk in stream) {
        yield chunk;
      }
      return;
    }
    final bytes = await readBlobBytes(hash);
    if (bytes == null) {
      return;
    }
    if (length == null || length >= bytes.length) {
      yield bytes;
      return;
    }
    yield bytes.sublist(0, length);
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    final local = blobFile(hash);
    if (await local.exists()) {
      return local.readAsBytes();
    }
    final remotePath = _remoteBlobPath(hash);
    final bytes = await _withSftp('read blob', (sftp) async {
      final file = await sftp.open(remotePath, mode: SftpFileOpenMode.read);
      try {
        return await file.readBytes();
      } finally {
        await file.close();
      }
    });
    if (!await local.exists()) {
      await local.parent.create(recursive: true);
      await local.writeAsBytes(bytes, flush: true);
    }
    return bytes;
  }

  @override
  String backupCompletedMessage(String manifestsPath) => 'Backup saved to SFTP';

  @override
  Stream<File> listXmlFiles() async* {
    _validateConfig();
    final opStopwatch = Stopwatch();
    await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=list status=running durationMs=0 target=manifests method=LIST');
    final lease = await _pool.lease(_connect);
    try {
      final sftp = lease.sftp;
      opStopwatch.start();
      final manifestsRoot = _remoteManifestsRoot();
      List<SftpName> serverDirs;
      try {
        serverDirs = await sftp.listdir(manifestsRoot);
      } catch (_) {
        opStopwatch.stop();
        return;
      }
      for (final serverEntry in serverDirs) {
        final serverName = serverEntry.filename;
        if (serverName == '.' || serverName == '..') {
          continue;
        }
        if (!serverEntry.attr.isDirectory) {
          continue;
        }
        final serverPath = _remoteJoin(manifestsRoot, serverName);
        final vmDirs = await sftp.listdir(serverPath);
        for (final vmEntry in vmDirs) {
          final vmName = vmEntry.filename;
          if (vmName == '.' || vmName == '..') {
            continue;
          }
          if (!vmEntry.attr.isDirectory) {
            continue;
          }
          final vmPath = _remoteJoin(serverPath, vmName);
          final files = await sftp.listdir(vmPath);
          for (final file in files) {
            final name = file.filename;
            if (name == '.' || name == '..') {
              continue;
            }
            if (!name.endsWith('.xml') || name.endsWith('.xml.inprogress')) {
              continue;
            }
            final localPath = '${_cacheRoot.path}${Platform.pathSeparator}manifests${Platform.pathSeparator}$serverName${Platform.pathSeparator}$vmName${Platform.pathSeparator}$name';
            final localFile = File(localPath);
            if (!await localFile.exists()) {
              await localFile.parent.create(recursive: true);
              final bytes = await _downloadRemoteFile(sftp, _remoteJoin(vmPath, name));
              await localFile.writeAsBytes(bytes, flush: true);
            }
            yield localFile;
          }
        }
      }
      opStopwatch.stop();
    } finally {
      lease.release();
      final duration = opStopwatch.elapsedMilliseconds;
      await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=list status=200 durationMs=$duration target=manifests method=LIST');
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
    final location = _resolveServerVm(vmDir);
    if (location != null) {
      return _downloadManifestIfMissing(location.serverId, location.vmName, diskId, timestamp);
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
      final downloaded = await _downloadManifestIfMissing(location.serverId, location.vmName, diskId, timestamp);
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
    if (location == null) {
      return manifests;
    }
    final lease = await _pool.lease(_connect);
    try {
      final sftp = lease.sftp;
      final vmRemote = _remoteManifestFolder(location.serverId, location.vmName);
      final diskDirs = await sftp.listdir(vmRemote);
      for (final disk in diskDirs) {
        if (!disk.attr.isDirectory) {
          continue;
        }
        final diskId = disk.filename;
        if (diskId == '.' || diskId == '..') {
          continue;
        }
        final downloaded = await _downloadManifestFromFolder(sftp, _remoteJoin(vmRemote, diskId), vmDir, diskId, timestamp);
        if (downloaded != null) {
          manifests.add(downloaded);
        }
      }
    } finally {
      lease.release();
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
    if (location == null) {
      return null;
    }
    final lease = await _pool.lease(_connect);
    try {
      final sftp = lease.sftp;
      final vmRemote = _remoteManifestFolder(location.serverId, location.vmName);
      final diskDirs = await sftp.listdir(vmRemote);
      for (final disk in diskDirs) {
        if (!disk.attr.isDirectory) {
          continue;
        }
        final diskId = disk.filename;
        if (diskId == '.' || diskId == '..') {
          continue;
        }
        final downloaded = await _downloadManifestFromFolder(sftp, _remoteJoin(vmRemote, diskId), vmDir, diskId, timestamp);
        if (downloaded == null) {
          continue;
        }
        final storedPath = await readSourcePath(downloaded);
        if (storedPath != null && storedPath.trim() == sourcePath.trim()) {
          return downloaded;
        }
      }
    } finally {
      lease.release();
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
    await _deleteInProgressInDir(blobsDir());
  }

  @override
  Future<void> closeConnections() async {
    _pool.closeAll();
    _blobCachePool.closeAll();
    _nativePool.closeAll();
  }

  Future<_SshSftpSession> _connect() async {
    final socket = await SSHSocket.connect(_host, _port, timeout: const Duration(seconds: 10));
    final client = SSHClient(socket, username: _username, onPasswordRequest: () => _password);
    final sftp = await client.sftp();
    return _SshSftpSession(socket: socket, client: client, sftp: sftp);
  }

  Future<_NativeSftpSession> _connectNative() async {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final session = bindings.connect(_host, _port, _username, _password);
    if (session == nullptr) {
      throw 'Native SFTP connect failed.';
    }
    return _NativeSftpSession(bindings: bindings, session: session);
  }

  Future<void> _nativeWriteAll(String remotePath, Uint8List data, {required bool truncate}) async {
    final opStopwatch = Stopwatch();
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final lease = await _nativePool.lease(_connectNative);
    var invalidateLease = false;
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    try {
      file = bindings.openWrite(lease.session, remotePath, truncate: truncate);
      if (file == nullptr) {
        throw 'Native SFTP openWrite failed: $remotePath';
      }
      opStopwatch.start();
      buffer = calloc<Uint8>(data.length);
      buffer.asTypedList(data.length).setAll(0, data);
      final wrote = bindings.write(file, buffer, data.length);
      if (wrote != data.length) {
        throw 'Native SFTP write failed: wrote=$wrote expected=${data.length}';
      }
    } catch (_) {
      invalidateLease = true;
      rethrow;
    } finally {
      if (file != null && file != nullptr) {
        bindings.closeFile(file);
      }
      if (buffer != null) {
        calloc.free(buffer);
      }
      if (invalidateLease) {
        lease.invalidate();
      } else {
        lease.release();
      }
      if (opStopwatch.isRunning) {
        opStopwatch.stop();
      }
      final elapsedMs = opStopwatch.elapsedMilliseconds;
      if (elapsedMs > 250) {
        await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=upload status=200 durationMs=$elapsedMs method=WRITE detail=native bytes=${data.length}');
      }
    }
  }

  Future<void> _writeBlobWithTiming({required String hash, required String remoteTemp, required String remotePath, required Uint8List data}) async {
    final label = 'write blob $hash';
    return _withRetry(label, () async {
      final pool = _poolForLabel(label);
      final leaseWaitStopwatch = Stopwatch()..start();
      final lease = await pool.lease(_connect);
      leaseWaitStopwatch.stop();
      final operationStopwatch = Stopwatch()..start();
      var released = false;
      var writeMs = 0;
      var renameMs = 0;
      try {
        final writeStopwatch = Stopwatch()..start();
        if (_nativeSftp != null) {
          await _nativeWriteAll(remoteTemp, data, truncate: true);
        } else {
          final remoteFile = await lease.sftp.open(remoteTemp, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
          try {
            await remoteFile.writeBytes(data);
          } finally {
            await remoteFile.close();
          }
        }
        writeStopwatch.stop();
        writeMs = writeStopwatch.elapsedMilliseconds;

        await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=upload_rename status=start durationMs=0 method=RENAME');
        final renameStopwatch = Stopwatch()..start();
        try {
          await lease.sftp.rename(remoteTemp, remotePath);
        } catch (_) {
          renameStopwatch.stop();
          renameMs = renameStopwatch.elapsedMilliseconds;
          await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=upload_rename status=error durationMs=$renameMs method=RENAME');
          await _tryRemoveRemoteFile(lease.sftp, remoteTemp);
          rethrow;
        }
        renameStopwatch.stop();
        renameMs = renameStopwatch.elapsedMilliseconds;
        await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=upload_rename status=200 durationMs=$renameMs method=RENAME');

        _remoteBlobNames.add(hash);
        _remoteBlobSizes[hash] = data.length;

        await _appendDebugLogLine(
          '${DateTime.now().toIso8601String()} action=upload status=200 durationMs=${operationStopwatch.elapsedMilliseconds} '
          'method=WRITE '
          'detail=leaseWaitMs:${leaseWaitStopwatch.elapsedMilliseconds} writeMs:$writeMs renameMs:$renameMs bytes:${data.length}',
        );
        lease.release();
        released = true;
      } catch (error) {
        await _appendDebugLogLine(
          '${DateTime.now().toIso8601String()} action=upload status=error durationMs=${operationStopwatch.elapsedMilliseconds} '
          'method=WRITE '
          'detail=leaseWaitMs:${leaseWaitStopwatch.elapsedMilliseconds} writeMs:$writeMs renameMs:$renameMs bytes:${data.length} error=$error',
        );
        lease.invalidate();
        released = true;
        rethrow;
      } finally {
        if (!released) {
          lease.release();
        }
      }
    });
  }

  Future<T> _withSftp<T>(String label, Future<T> Function(SftpClient sftp) action) async {
    return _withRetry(label, () async {
      final pool = _poolForLabel(label);
      final lease = await pool.lease(_connect);
      final opStopwatch = Stopwatch()..start();
      var released = false;
      try {
        final result = await action(lease.sftp);
        opStopwatch.stop();
        lease.release();
        released = true;
        return result;
      } catch (_) {
        opStopwatch.stop();
        lease.invalidate();
        released = true;
        rethrow;
      } finally {
        if (!released) {
          lease.release();
        }
      }
    });
  }

  Future<T> _withRetry<T>(String label, Future<T> Function() action) async {
    var delaySeconds = 2;
    var attempt = 0;
    const maxRetries = 5;
    while (true) {
      try {
        return await action();
      } catch (error) {
        if (attempt >= maxRetries) {
          await _appendDebugLogLine('${DateTime.now().toIso8601String()} action=retry status=failed durationMs=0 target=$label method=RETRY error=$error');
          rethrow;
        }
        _pool.invalidateIdle();
        _blobCachePool.invalidateIdle();
        _nativePool.invalidateIdle();
        attempt += 1;
        await _appendDebugLogLine(
          '${DateTime.now().toIso8601String()} action=retry status=retry durationMs=0 target=$label method=RETRY detail=attempt:$attempt delaySec:$delaySeconds closeIdle:true error=$error',
        );
        await Future.delayed(Duration(seconds: delaySeconds));
        delaySeconds *= 2;
      }
    }
  }

  _SftpPool _poolForLabel(String label) {
    if (label == 'list blobs' || label.startsWith('list blobs/') || label.startsWith('ensure blob dir:')) {
      return _blobCachePool;
    }
    return _pool;
  }

  // Keep remote structure scoped under a dedicated folder to avoid cluttering user-provided base paths.
  String _remoteRoot() => _remoteJoin(_normalizeRemotePath(_basePath), _remoteAppFolderName);
  String _remoteManifestsRoot() => _remoteJoin(_remoteRoot(), 'manifests');
  String _remoteBlobsRoot() => _remoteJoin(_remoteRoot(), 'blobs');
  String _remoteTmpRoot() => _remoteJoin(_remoteRoot(), 'tmp');

  String _remoteManifestFolder(String serverId, String vmName) => _remoteJoin(_remoteManifestsRoot(), serverId, vmName);
  String _remoteDiskManifestFolder(String serverId, String vmName, String diskId) => _remoteJoin(_remoteManifestsRoot(), serverId, vmName, diskId);

  String _remoteBlobDir(String hash) => _remoteJoin(_remoteBlobsRoot(), hash.substring(0, 2));
  String _remoteBlobPath(String hash) => _remoteJoin(_remoteBlobDir(hash), hash);

  Future<void> _commitSmallRemoteFile({required File localFile, required String remotePath}) async {
    if (!await localFile.exists()) {
      return;
    }
    final bytes = await localFile.readAsBytes();
    await _withSftp('upload small file', (sftp) async {
      await _ensureRemoteDir(sftp, _remoteDirName(remotePath));
      final tmpRemote = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
      final remoteFile = await sftp.open(tmpRemote, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
      try {
        await remoteFile.writeBytes(Uint8List.fromList(bytes));
      } finally {
        await remoteFile.close();
      }
      try {
        await sftp.rename(tmpRemote, remotePath);
      } catch (_) {
        if (await _remoteExists(sftp, remotePath)) {
          await _tryRemoveRemoteFile(sftp, tmpRemote);
          return;
        }
        await _tryRemoveRemoteFile(sftp, tmpRemote);
        rethrow;
      }
    });
  }

  Future<void> _ensureRemoteDirBlind(SftpClient sftp, String remotePath) async {
    final normalized = _normalizeRemotePath(remotePath);
    if (normalized == '/' || normalized.isEmpty) {
      return;
    }
    final parts = normalized.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = normalized.startsWith('/') ? '/' : '';
    for (final part in parts) {
      current = current.isEmpty || current == '/' ? '$current$part' : '$current/$part';
      try {
        await sftp.mkdir(current);
      } catch (_) {
        // Blind mkdir: ignore if the directory already exists.
      }
    }
  }

  Future<List<int>> _downloadRemoteFile(SftpClient sftp, String remotePath) async {
    final remoteFile = await sftp.open(remotePath, mode: SftpFileOpenMode.read);
    try {
      return await remoteFile.readBytes();
    } finally {
      await remoteFile.close();
    }
  }

  Future<File?> _downloadManifestIfMissing(String serverId, String vmName, String diskId, String timestamp) async {
    return _withSftp('download manifest', (sftp) async => _downloadManifestFromFolder(sftp, _remoteDiskManifestFolder(serverId, vmName, diskId), manifestsDir(serverId, vmName), diskId, timestamp));
  }

  Future<File?> _downloadManifestFromFolder(SftpClient sftp, String folderPath, Directory vmDir, String diskId, String timestamp) async {
    final nameGz = '$timestamp.manifest.gz';
    final namePlain = '$timestamp.manifest';
    final remoteGz = _remoteJoin(folderPath, nameGz);
    final remotePlain = _remoteJoin(folderPath, namePlain);

    String? remotePath;
    String fileName = nameGz;
    if (await _remoteExists(sftp, remoteGz)) {
      remotePath = remoteGz;
      fileName = nameGz;
    } else if (await _remoteExists(sftp, remotePlain)) {
      remotePath = remotePlain;
      fileName = namePlain;
    } else {
      return null;
    }

    final localDir = Directory('${vmDir.path}${Platform.pathSeparator}$diskId');
    await localDir.create(recursive: true);
    final localFile = File('${localDir.path}${Platform.pathSeparator}$fileName');
    if (!await localFile.exists()) {
      final bytes = await _downloadRemoteFile(sftp, remotePath);
      await localFile.writeAsBytes(bytes, flush: true);
    }
    return localFile;
  }

  _ServerVmLocation? _resolveServerVm(Directory vmDir) {
    final vmName = baseName(vmDir.path);
    final serverId = baseName(vmDir.parent.path);
    if (vmName.isEmpty || serverId.isEmpty) {
      return null;
    }
    return _ServerVmLocation(serverId: serverId, vmName: vmName);
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

  Future<void> _ensureRemoteDir(SftpClient sftp, String remotePath) async {
    final normalized = _normalizeRemotePath(remotePath);
    if (normalized == '/' || normalized.isEmpty) {
      return;
    }
    final parts = normalized.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = normalized.startsWith('/') ? '/' : '';
    for (final part in parts) {
      current = current.isEmpty || current == '/' ? '$current$part' : '$current/$part';
      try {
        await sftp.mkdir(current);
      } catch (_) {
        try {
          final attrs = await sftp.stat(current);
          if (attrs.isDirectory) {
            continue;
          }
        } catch (_) {}
        rethrow;
      }
    }
  }

  Future<bool> _remoteExists(SftpClient sftp, String remotePath) async {
    try {
      await sftp.stat(remotePath);
      return true;
    } catch (_) {
      return false;
    }
  }

  Future<void> _tryRemoveRemoteFile(SftpClient sftp, String remotePath) async {
    try {
      await sftp.remove(remotePath);
    } catch (_) {}
  }

  String _remoteDirName(String remotePath) {
    final normalized = _normalizeRemotePath(remotePath);
    final index = normalized.lastIndexOf('/');
    if (index <= 0) {
      return normalized.startsWith('/') ? '/' : '';
    }
    return normalized.substring(0, index);
  }

  static String _normalizeRemotePath(String path) {
    final trimmed = path.trim();
    if (trimmed.isEmpty) {
      return '';
    }
    var value = trimmed.replaceAll('\\', '/');
    while (value.contains('//')) {
      value = value.replaceAll('//', '/');
    }
    if (value.length > 1 && value.endsWith('/')) {
      value = value.substring(0, value.length - 1);
    }
    return value;
  }

  static String _remoteJoin(String a, [String? b, String? c, String? d, String? e]) {
    final parts = <String>[];
    void add(String? value) {
      if (value == null) return;
      final trimmed = value.trim();
      if (trimmed.isEmpty) return;
      parts.add(trimmed);
    }

    add(a);
    add(b);
    add(c);
    add(d);
    add(e);

    final raw = parts.join('/');
    return _normalizeRemotePath(raw.startsWith('/') ? '/$raw' : raw);
  }

  static String _tempBase() {
    if (Platform.isWindows) {
      return Directory.systemTemp.path;
    }
    return '${Platform.pathSeparator}var${Platform.pathSeparator}tmp';
  }
}

class _SshSftpSession {
  _SshSftpSession({required this.socket, required this.client, required this.sftp});

  final SSHSocket socket;
  final SSHClient client;
  final SftpClient sftp;
  final DateTime createdAt = DateTime.now();

  void close() {
    try {
      sftp.close();
    } catch (_) {}
    try {
      client.close();
    } catch (_) {}
    try {
      socket.close();
    } catch (_) {}
  }
}

class _SftpLease {
  _SftpLease(this._pool, this._session, this.metrics);

  final _SftpPool _pool;
  final _SshSftpSession _session;
  final _SftpLeaseMetrics metrics;

  SftpClient get sftp => _session.sftp;

  void release() {
    _pool.release(_session);
  }

  void invalidate() {
    _pool.invalidate(_session);
  }
}

class _SftpPool {
  _SftpPool({required this.maxSessions});

  static const Duration _maxSessionAge = Duration(minutes: 10);

  final int maxSessions;
  final List<_SshSftpSession> _idle = <_SshSftpSession>[];
  final List<_SshSftpSession> _all = <_SshSftpSession>[];
  final List<_SftpLeaseWaiter> _waiters = <_SftpLeaseWaiter>[];
  bool _closed = false;
  var _connecting = 0;

  Future<_SftpLease> lease(Future<_SshSftpSession> Function() connect) async {
    final waitStartedAt = DateTime.now();
    if (_closed) {
      throw 'SFTP pool is closed.';
    }
    _pruneExpiredIdle();
    if (_idle.isNotEmpty) {
      final session = _idle.removeLast();
      final waitMs = DateTime.now().difference(waitStartedAt).inMilliseconds;
      return _SftpLease(this, session, _SftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: true, queued: false));
    }
    if (_all.length + _connecting < maxSessions) {
      _connecting += 1;
      final connectStartedAt = DateTime.now();
      try {
        final session = await connect();
        _all.add(session);
        final now = DateTime.now();
        final waitMs = now.difference(waitStartedAt).inMilliseconds;
        final connectMs = now.difference(connectStartedAt).inMilliseconds;
        return _SftpLease(this, session, _SftpLeaseMetrics(waitMs: waitMs, connectMs: connectMs, reused: false, queued: false));
      } finally {
        _connecting -= 1;
      }
    }
    final waiter = _SftpLeaseWaiter(Completer<_SftpLease>(), waitStartedAt, connect);
    _waiters.add(waiter);
    return waiter.completer.future;
  }

  void release(_SshSftpSession session) {
    if (_isExpired(session)) {
      invalidate(session);
      if (_waiters.isNotEmpty) {
        _fulfillWaiterWithConnect(_waiters.removeAt(0));
      }
      return;
    }
    if (_closed) {
      try {
        session.close();
      } catch (_) {}
      return;
    }
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.completer.isCompleted) {
        final waitMs = DateTime.now().difference(waiter.waitStartedAt).inMilliseconds;
        waiter.completer.complete(_SftpLease(this, session, _SftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: true, queued: true)));
        return;
      }
    }
    _idle.add(session);
  }

  void invalidate(_SshSftpSession session) {
    _idle.remove(session);
    _all.remove(session);
    try {
      session.close();
    } catch (_) {}
  }

  void invalidateIdle() {
    final idleSessions = List<_SshSftpSession>.from(_idle);
    _idle.clear();
    for (final session in idleSessions) {
      _all.remove(session);
      try {
        session.close();
      } catch (_) {}
    }
  }

  void closeAll() {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.completer.isCompleted) {
        waiter.completer.completeError('SFTP pool closed.');
      }
    }
    _waiters.clear();
    for (final session in _all) {
      try {
        session.close();
      } catch (_) {}
    }
    _all.clear();
    _idle.clear();
    _connecting = 0;
  }

  bool _isExpired(_SshSftpSession session) {
    return DateTime.now().difference(session.createdAt) >= _maxSessionAge;
  }

  void _pruneExpiredIdle() {
    if (_idle.isEmpty) {
      return;
    }
    final now = DateTime.now();
    final expired = _idle.where((session) => now.difference(session.createdAt) >= _maxSessionAge).toList();
    for (final session in expired) {
      _idle.remove(session);
      _all.remove(session);
      try {
        session.close();
      } catch (_) {}
    }
  }

  void _fulfillWaiterWithConnect(_SftpLeaseWaiter waiter) {
    final connect = waiter.connect;
    _connecting += 1;
    unawaited(
      connect()
          .then((session) {
            _all.add(session);
            final waitMs = DateTime.now().difference(waiter.waitStartedAt).inMilliseconds;
            if (!waiter.completer.isCompleted) {
              waiter.completer.complete(_SftpLease(this, session, _SftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: false, queued: true)));
            }
          })
          .catchError((Object error, StackTrace stackTrace) {
            if (!waiter.completer.isCompleted) {
              waiter.completer.completeError(error, stackTrace);
            }
          })
          .whenComplete(() {
            _connecting -= 1;
          }),
    );
  }
}

class _SftpLeaseWaiter {
  _SftpLeaseWaiter(this.completer, this.waitStartedAt, this.connect);

  final Completer<_SftpLease> completer;
  final DateTime waitStartedAt;
  final Future<_SshSftpSession> Function() connect;
}

class _SftpLeaseMetrics {
  _SftpLeaseMetrics({required this.waitMs, required this.connectMs, required this.reused, required this.queued});

  final int waitMs;
  final int connectMs;
  final bool reused;
  final bool queued;
}

class _NativeSftpBindings {
  _NativeSftpBindings(DynamicLibrary lib)
    : _connect = lib.lookupFunction<_SftpConnectC, _SftpConnectDart>('vb_sftp_connect'),
      _disconnect = lib.lookupFunction<_SftpDisconnectC, _SftpDisconnectDart>('vb_sftp_disconnect'),
      _openRead = lib.lookupFunction<_SftpOpenReadC, _SftpOpenReadDart>('vb_sftp_open_read'),
      _openWrite = lib.lookupFunction<_SftpOpenWriteC, _SftpOpenWriteDart>('vb_sftp_open_write'),
      _read = lib.lookupFunction<_SftpReadC, _SftpReadDart>('vb_sftp_read'),
      _write = lib.lookupFunction<_SftpWriteC, _SftpWriteDart>('vb_sftp_write'),
      _closeFile = lib.lookupFunction<_SftpCloseFileC, _SftpCloseFileDart>('vb_sftp_close_file');

  final _SftpConnectDart _connect;
  final _SftpDisconnectDart _disconnect;
  final _SftpOpenReadDart _openRead;
  final _SftpOpenWriteDart _openWrite;
  final _SftpReadDart _read;
  final _SftpWriteDart _write;
  final _SftpCloseFileDart _closeFile;

  static _NativeSftpBindings? tryLoad() {
    final candidates = <String>[];
    try {
      final exeDir = File(Platform.resolvedExecutable).parent.path;
      candidates.add('$exeDir/native/linux/libvirtbackup_native.so');
      candidates.add('$exeDir/native/macos/libvirtbackup_native.dylib');
    } catch (_) {}

    final cwd = Directory.current.path;
    candidates.add('$cwd/native/linux/libvirtbackup_native.so');
    candidates.add('$cwd/native/macos/libvirtbackup_native.dylib');

    for (final path in candidates) {
      if (!File(path).existsSync()) {
        continue;
      }
      try {
        return _NativeSftpBindings(DynamicLibrary.open(path));
      } catch (_) {}
    }
    return null;
  }

  Pointer<Void> connect(String host, int port, String user, String password) {
    final hostPtr = host.toNativeUtf8();
    final userPtr = user.toNativeUtf8();
    final passPtr = password.toNativeUtf8();
    final result = _connect(hostPtr, port, userPtr, passPtr);
    calloc.free(hostPtr);
    calloc.free(userPtr);
    calloc.free(passPtr);
    return result;
  }

  void disconnect(Pointer<Void> session) {
    _disconnect(session);
  }

  Pointer<Void> openRead(Pointer<Void> session, String path) {
    final pathPtr = path.toNativeUtf8();
    final result = _openRead(session, pathPtr);
    calloc.free(pathPtr);
    return result;
  }

  Pointer<Void> openWrite(Pointer<Void> session, String path, {required bool truncate}) {
    final pathPtr = path.toNativeUtf8();
    final result = _openWrite(session, pathPtr, truncate ? 1 : 0);
    calloc.free(pathPtr);
    return result;
  }

  int read(Pointer<Void> file, int offset, Pointer<Uint8> buffer, int length) {
    return _read(file, offset, buffer, length);
  }

  int write(Pointer<Void> file, Pointer<Uint8> buffer, int length) {
    return _write(file, buffer, length);
  }

  void closeFile(Pointer<Void> file) {
    _closeFile(file);
  }
}

class _NativeSftpSession {
  _NativeSftpSession({required this.bindings, required this.session});

  final _NativeSftpBindings bindings;
  final Pointer<Void> session;
  final DateTime createdAt = DateTime.now();

  void close() {
    bindings.disconnect(session);
  }
}

class _NativeSftpLease {
  _NativeSftpLease(this._pool, this._session);

  final _NativeSftpPool _pool;
  final _NativeSftpSession _session;

  Pointer<Void> get session => _session.session;

  void release() {
    _pool.release(_session);
  }

  void invalidate() {
    _pool.invalidate(_session);
  }
}

class _NativeSftpPool {
  _NativeSftpPool({required this.maxSessions});

  static const Duration _maxSessionAge = Duration(minutes: 10);

  final int maxSessions;
  final List<_NativeSftpSession> _idle = <_NativeSftpSession>[];
  final List<_NativeSftpSession> _all = <_NativeSftpSession>[];
  final List<_NativeSftpLeaseWaiter> _waiters = <_NativeSftpLeaseWaiter>[];
  bool _closed = false;
  var _connecting = 0;

  Future<_NativeSftpLease> lease(Future<_NativeSftpSession> Function() connect) async {
    if (_closed) {
      throw 'Native SFTP pool is closed.';
    }
    _pruneExpiredIdle();
    if (_idle.isNotEmpty) {
      final session = _idle.removeLast();
      return _NativeSftpLease(this, session);
    }
    if (_all.length + _connecting < maxSessions) {
      _connecting += 1;
      try {
        final session = await connect();
        _all.add(session);
        return _NativeSftpLease(this, session);
      } finally {
        _connecting -= 1;
      }
    }
    final waiter = _NativeSftpLeaseWaiter(Completer<_NativeSftpLease>(), connect);
    _waiters.add(waiter);
    return waiter.completer.future;
  }

  void release(_NativeSftpSession session) {
    if (_isExpired(session)) {
      invalidate(session);
      if (_waiters.isNotEmpty) {
        _fulfillWaiterWithConnect(_waiters.removeAt(0));
      }
      return;
    }
    if (_closed) {
      try {
        session.close();
      } catch (_) {}
      return;
    }
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.completer.isCompleted) {
        waiter.completer.complete(_NativeSftpLease(this, session));
        return;
      }
    }
    _idle.add(session);
  }

  bool _isExpired(_NativeSftpSession session) {
    return DateTime.now().difference(session.createdAt) >= _maxSessionAge;
  }

  void _pruneExpiredIdle() {
    if (_idle.isEmpty) {
      return;
    }
    final now = DateTime.now();
    final expired = _idle.where((session) => now.difference(session.createdAt) >= _maxSessionAge).toList();
    for (final session in expired) {
      _idle.remove(session);
      _all.remove(session);
      try {
        session.close();
      } catch (_) {}
    }
  }

  void _fulfillWaiterWithConnect(_NativeSftpLeaseWaiter waiter) {
    final connect = waiter.connect;
    _connecting += 1;
    unawaited(
      connect()
          .then((session) {
            _all.add(session);
            if (!waiter.completer.isCompleted) {
              waiter.completer.complete(_NativeSftpLease(this, session));
            }
          })
          .catchError((Object error, StackTrace stackTrace) {
            if (!waiter.completer.isCompleted) {
              waiter.completer.completeError(error, stackTrace);
            }
          })
          .whenComplete(() {
            _connecting -= 1;
          }),
    );
  }

  void invalidate(_NativeSftpSession session) {
    _idle.remove(session);
    _all.remove(session);
    try {
      session.close();
    } catch (_) {}
  }

  void invalidateIdle() {
    final idleSessions = List<_NativeSftpSession>.from(_idle);
    _idle.clear();
    for (final session in idleSessions) {
      _all.remove(session);
      try {
        session.close();
      } catch (_) {}
    }
  }

  void closeAll() {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.completer.isCompleted) {
        waiter.completer.completeError('Native SFTP pool closed.');
      }
    }
    _waiters.clear();
    for (final session in _all) {
      try {
        session.close();
      } catch (_) {}
    }
    _all.clear();
    _idle.clear();
    _connecting = 0;
  }
}

class _NativeSftpLeaseWaiter {
  _NativeSftpLeaseWaiter(this.completer, this.connect);

  final Completer<_NativeSftpLease> completer;
  final Future<_NativeSftpSession> Function() connect;
}

typedef _SftpConnectC = Pointer<Void> Function(Pointer<Utf8> host, Int32 port, Pointer<Utf8> user, Pointer<Utf8> password);
typedef _SftpConnectDart = Pointer<Void> Function(Pointer<Utf8> host, int port, Pointer<Utf8> user, Pointer<Utf8> password);
typedef _SftpDisconnectC = Void Function(Pointer<Void> session);
typedef _SftpDisconnectDart = void Function(Pointer<Void> session);
typedef _SftpOpenReadC = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpOpenReadDart = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpOpenWriteC = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path, Int32 truncate);
typedef _SftpOpenWriteDart = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path, int truncate);
typedef _SftpReadC = Int32 Function(Pointer<Void> file, Int64 offset, Pointer<Uint8> buffer, Int32 length);
typedef _SftpReadDart = int Function(Pointer<Void> file, int offset, Pointer<Uint8> buffer, int length);
typedef _SftpWriteC = Int32 Function(Pointer<Void> file, Pointer<Uint8> buffer, Int32 length);
typedef _SftpWriteDart = int Function(Pointer<Void> file, Pointer<Uint8> buffer, int length);
typedef _SftpCloseFileC = Void Function(Pointer<Void> file);
typedef _SftpCloseFileDart = void Function(Pointer<Void> file);

class _ServerVmLocation {
  const _ServerVmLocation({required this.serverId, required this.vmName});

  final String serverId;
  final String vmName;
}
