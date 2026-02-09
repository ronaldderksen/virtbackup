import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/settings.dart';

class SftpPrefillStats {
  const SftpPrefillStats({required this.shard1Count, required this.shard2Count, required this.blobCount, required this.durationMs});

  final int shard1Count;
  final int shard2Count;
  final int blobCount;
  final int durationMs;
}

class SftpBackupDriver implements BackupDriver, RemoteBlobDriver {
  SftpBackupDriver({required AppSettings settings, void Function(String message)? logInfo})
    : _settings = settings,
      _cacheRoot = Directory('${_tempBase()}${Platform.pathSeparator}virtbackup_sftp_cache'),
      _logInfo = logInfo ?? ((_) {});

  final AppSettings _settings;
  final Directory _cacheRoot;
  final void Function(String message) _logInfo;

  static const int _writeConcurrency = 8;
  static const String _remoteAppFolderName = 'VirtBackup';
  static const int _sftpStatusNoSuchFile = 2;

  void Function(SftpPrefillStats stats, bool done)? onPrefillProgress;
  SftpPrefillStats _prefillStats = const SftpPrefillStats(shard1Count: 0, shard2Count: 0, blobCount: 0, durationMs: 0);
  SftpPrefillStats get prefillStats => _prefillStats;

  final _SftpPool _pool = _SftpPool(maxSessions: _writeConcurrency);
  final Set<String> _knownRemoteShardKeys = <String>{};
  final Set<String> _remoteBlobNames = <String>{};
  final Map<String, int> _remoteBlobSizes = <String, int>{};
  bool _blobIndexFullyPrefilled = false;

  String get _host => _settings.sftpHost.trim();
  int get _port => _settings.sftpPort;
  String get _username => _settings.sftpUsername.trim();
  String get _password => _settings.sftpPassword;
  String get _basePath => _settings.sftpBasePath.trim();

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
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);
    _logInfo('sftp: ready (cache=${_cacheRoot.path})');
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    _validateConfig();
    await manifestsDir(serverId, vmName).create(recursive: true);
    await blobsDir().create(recursive: true);
    await tmpDir().create(recursive: true);

    await _withSftp('prepare remote layout', (sftp) async {
      await _ensureRemoteDir(sftp, _remoteRoot());
      await _ensureRemoteDir(sftp, _remoteManifestsRoot());
      await _ensureRemoteDir(sftp, _remoteBlobsRoot());
      await _ensureRemoteDir(sftp, _remoteTmpRoot());
      await _ensureRemoteDir(sftp, _remoteManifestFolder(serverId, vmName));
    });

    final prefillStart = DateTime.now();
    await _prefillRemoteBlobCache();
    final prefillMs = DateTime.now().difference(prefillStart).inMilliseconds;
    _logInfo('sftp: prefill blob cache durationMs=$prefillMs');
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
  Directory blobsDir() => Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs');

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
    if (hash.length < 4) {
      return File('${blobsDir().path}${Platform.pathSeparator}$hash');
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    return File('${blobsDir().path}${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2${Platform.pathSeparator}$hash');
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
    // Very conservative: never delete remote data in v1.
    throw 'SFTP fresh cleanup is not supported.';
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 4) {
      return;
    }
    final shardKey = hash.substring(0, 4);
    if (_knownRemoteShardKeys.contains(shardKey)) {
      return;
    }
    // If prefill fully indexed remote blobs/dirs, then:
    // - existing shard dirs are in `_knownRemoteShardKeys`
    // - missing shard dirs must be created as part of the write path (blind mkdir, no stat)
    // So we must not do remote ensure calls here, and we must not "mark as known" because that
    // would prevent the write path from creating the directory.
    if (_blobIndexFullyPrefilled) {
      return;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final remoteDir = _remoteJoin(_remoteBlobsRoot(), shard1, shard2);
    await _withSftp('ensure blob dir', (sftp) async {
      await _ensureRemoteDir(sftp, remoteDir);
    });
    _knownRemoteShardKeys.add(shardKey);
  }

  @override
  Future<bool> writeBlobIfMissing(String hash, List<int> bytes) async {
    if (hash.length < 4 || bytes.isEmpty) {
      return false;
    }
    if (_remoteBlobNames.contains(hash)) {
      return false;
    }
    final remotePath = _remoteBlobPath(hash);
    final remoteTemp = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final data = Uint8List.fromList(bytes);
    return _withSftp('write blob', (sftp) async {
      if (!_blobIndexFullyPrefilled) {
        if (await _remoteExists(sftp, remotePath)) {
          _remoteBlobNames.add(hash);
          return false;
        }
      }

      // If prefill fully indexed remote blobs, we must not do any remote `stat` calls here.
      // We either skip directory creation (known shard) or do a best-effort mkdir without checking.
      final shardKey = hash.substring(0, 4);
      if (!_blobIndexFullyPrefilled || !_knownRemoteShardKeys.contains(shardKey)) {
        if (_blobIndexFullyPrefilled) {
          await _ensureRemoteDirBlind(sftp, _remoteBlobDir(hash));
          _knownRemoteShardKeys.add(shardKey);
        } else {
          await _ensureRemoteDir(sftp, _remoteBlobDir(hash));
          _knownRemoteShardKeys.add(shardKey);
        }
      }
      final remoteFile = await sftp.open(remoteTemp, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
      try {
        await remoteFile.writeBytes(data);
      } finally {
        await remoteFile.close();
      }
      try {
        await sftp.rename(remoteTemp, remotePath);
        _remoteBlobNames.add(hash);
        _remoteBlobSizes[hash] = bytes.length;
        return true;
      } catch (error) {
        // We must not `stat` when prefill indexed all remote blobs, but we still want correct behavior.
        // If rename failed because the target already exists, treat it as "missing write" without failing the backup.
        if (_blobIndexFullyPrefilled) {
          final exists = await _remoteFileExistsByOpenRead(sftp, remotePath);
          if (exists) {
            await _tryRemoveRemoteFile(sftp, remoteTemp);
            _remoteBlobNames.add(hash);
            return false;
          }
        } else {
          if (await _remoteExists(sftp, remotePath)) {
            await _tryRemoveRemoteFile(sftp, remoteTemp);
            _remoteBlobNames.add(hash);
            return false;
          }
        }
        await _tryRemoveRemoteFile(sftp, remoteTemp);
        rethrow;
      }
    });
  }

  @override
  Future<bool> blobExists(String hash) async {
    if (hash.length < 4) {
      return false;
    }
    if (_remoteBlobNames.contains(hash)) {
      return true;
    }
    if (_blobIndexFullyPrefilled) {
      return false;
    }
    return _withSftp('blob exists', (sftp) async => _remoteExists(sftp, _remoteBlobPath(hash)));
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
    final cached = _remoteBlobSizes[hash];
    if (cached != null) {
      return cached;
    }
    if (_blobIndexFullyPrefilled) {
      return null;
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
    if (hash.length < 4) {
      return;
    }
    final lease = await _pool.lease(_connect);
    try {
      final sftp = lease.sftp;
      final file = await sftp.open(_remoteBlobPath(hash), mode: SftpFileOpenMode.read);
      try {
        await for (final chunk in file.read(length: length)) {
          yield chunk;
        }
      } finally {
        await file.close();
      }
    } finally {
      lease.release();
    }
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 4) {
      return null;
    }
    return _withSftp('read blob', (sftp) async {
      final file = await sftp.open(_remoteBlobPath(hash), mode: SftpFileOpenMode.read);
      try {
        return await file.readBytes();
      } finally {
        await file.close();
      }
    });
  }

  @override
  String backupCompletedMessage(String manifestsPath) => 'Backup saved to SFTP';

  @override
  Stream<File> listXmlFiles() async* {
    _validateConfig();
    final stopwatch = Stopwatch()..start();
    _logInfo('sftp: list xml files start');
    final lease = await _pool.lease(_connect);
    try {
      final sftp = lease.sftp;
      final manifestsRoot = _remoteManifestsRoot();
      List<SftpName> serverDirs;
      try {
        serverDirs = await sftp.listdir(manifestsRoot);
      } catch (_) {
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
    } finally {
      lease.release();
      _logInfo('sftp: list xml files done durationMs=${stopwatch.elapsedMilliseconds}');
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

  Future<_SshSftpSession> _connect() async {
    final socket = await SSHSocket.connect(_host, _port, timeout: const Duration(seconds: 10));
    final client = SSHClient(socket, username: _username, onPasswordRequest: () => _password);
    final sftp = await client.sftp();
    return _SshSftpSession(socket: socket, client: client, sftp: sftp);
  }

  Future<T> _withSftp<T>(String label, Future<T> Function(SftpClient sftp) action) async {
    final shouldLog = label != 'write blob';
    final stopwatch = Stopwatch()..start();
    if (shouldLog) {
      _logInfo('sftp: $label start');
    }
    final lease = await _pool.lease(_connect);
    try {
      final result = await action(lease.sftp);
      if (shouldLog) {
        _logInfo('sftp: $label done durationMs=${stopwatch.elapsedMilliseconds}');
      }
      return result;
    } catch (error) {
      if (shouldLog) {
        _logInfo('sftp: $label failed durationMs=${stopwatch.elapsedMilliseconds} error=$error');
      }
      rethrow;
    } finally {
      lease.release();
    }
  }

  // Keep remote structure scoped under a dedicated folder to avoid cluttering user-provided base paths.
  String _remoteRoot() => _remoteJoin(_normalizeRemotePath(_basePath), _remoteAppFolderName);
  String _remoteManifestsRoot() => _remoteJoin(_remoteRoot(), 'manifests');
  String _remoteBlobsRoot() => _remoteJoin(_remoteRoot(), 'blobs');
  String _remoteTmpRoot() => _remoteJoin(_remoteRoot(), 'tmp');

  String _remoteManifestFolder(String serverId, String vmName) => _remoteJoin(_remoteManifestsRoot(), serverId, vmName);
  String _remoteDiskManifestFolder(String serverId, String vmName, String diskId) => _remoteJoin(_remoteManifestsRoot(), serverId, vmName, diskId);

  String _remoteBlobDir(String hash) => _remoteJoin(_remoteBlobsRoot(), hash.substring(0, 2), hash.substring(2, 4));
  String _remoteBlobPath(String hash) => _remoteJoin(_remoteBlobDir(hash), hash);

  Future<void> _prefillRemoteBlobCache() async {
    if (_blobIndexFullyPrefilled) {
      return;
    }
    _logInfo('sftp: prefill blob cache start');
    _knownRemoteShardKeys.clear();
    _remoteBlobNames.clear();
    _remoteBlobSizes.clear();
    final startedAt = DateTime.now();
    DateTime? lastEmitAt;

    final blobsRoot = _remoteBlobsRoot();
    final shard2Folders = <String>[];
    var shard1Count = 0;
    var shard2Count = 0;
    var blobCount = 0;
    var prefillOk = false;
    var hadError = false;

    void emitPrefill({required bool done}) {
      final now = DateTime.now();
      if (!done && lastEmitAt != null && now.difference(lastEmitAt!) < const Duration(seconds: 1)) {
        return;
      }
      lastEmitAt = now;
      final durationMs = now.difference(startedAt).inMilliseconds;
      _prefillStats = SftpPrefillStats(shard1Count: shard1Count, shard2Count: shard2Count, blobCount: blobCount, durationMs: durationMs);
      onPrefillProgress?.call(_prefillStats, done);
    }

    await _withSftp('prefill shard folders', (sftp) async {
      List<SftpName> shard1Entries;
      try {
        shard1Entries = await sftp.listdir(blobsRoot);
      } catch (_) {
        // If the blobs root can't be listed, we keep caches empty and do not mark as prefetched.
        hadError = true;
        return;
      }
      prefillOk = true;
      for (final shard1 in shard1Entries) {
        final name1 = shard1.filename;
        if (name1 == '.' || name1 == '..') continue;
        if (!shard1.attr.isDirectory) continue;
        shard1Count += 1;
        final shard1Path = _remoteJoin(blobsRoot, name1);
        List<SftpName> shard2Entries;
        try {
          shard2Entries = await sftp.listdir(shard1Path);
        } catch (_) {
          hadError = true;
          continue;
        }
        for (final shard2 in shard2Entries) {
          final name2 = shard2.filename;
          if (name2 == '.' || name2 == '..') continue;
          if (!shard2.attr.isDirectory) continue;
          shard2Count += 1;
          if (name1.length == 2 && name2.length == 2) {
            _knownRemoteShardKeys.add('$name1$name2');
          }
          shard2Folders.add(_remoteJoin(shard1Path, name2));
        }
        emitPrefill(done: false);
      }
    });
    emitPrefill(done: false);

    const maxConcurrentLists = _writeConcurrency;
    var nextShard2Index = 0;
    final listers = List<Future<void>>.generate(maxConcurrentLists, (_) {
      return _withSftp('prefill blobs', (sftp) async {
        while (true) {
          final index = nextShard2Index;
          if (index >= shard2Folders.length) {
            break;
          }
          nextShard2Index += 1;
          final folder = shard2Folders[index];
          List<SftpName> entries;
          try {
            entries = await sftp.listdir(folder);
          } catch (_) {
            hadError = true;
            return;
          }
          for (final entry in entries) {
            final name = entry.filename;
            if (name == '.' || name == '..') continue;
            if (entry.attr.isDirectory) continue;
            if (name.endsWith('.inprogress')) continue;
            _remoteBlobNames.add(name);
            final size = entry.attr.size;
            if (size != null) {
              _remoteBlobSizes[name] = size;
            }
            blobCount += 1;
          }
          emitPrefill(done: false);
        }
      });
    });
    await Future.wait(listers);

    _blobIndexFullyPrefilled = prefillOk && !hadError;
    final elapsedMs = DateTime.now().difference(startedAt).inMilliseconds;
    _logInfo('sftp: prefill blob cache done shard1=$shard1Count shard2=$shard2Count blobs=$blobCount durationMs=$elapsedMs fullyPrefilled=$_blobIndexFullyPrefilled');
    _prefillStats = SftpPrefillStats(shard1Count: shard1Count, shard2Count: shard2Count, blobCount: blobCount, durationMs: elapsedMs);
    onPrefillProgress?.call(_prefillStats, true);
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
        // Don't check via `stat`: callers use this in "no remote stat" paths.
      }
    }
  }

  Future<bool> _remoteFileExistsByOpenRead(SftpClient sftp, String remotePath) async {
    try {
      final file = await sftp.open(remotePath, mode: SftpFileOpenMode.read);
      await file.close();
      return true;
    } on SftpStatusError catch (error) {
      if (error.code == _sftpStatusNoSuchFile) {
        return false;
      }
      rethrow;
    }
  }

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
  _SftpLease(this._pool, this._session);

  final _SftpPool _pool;
  final _SshSftpSession _session;

  SftpClient get sftp => _session.sftp;

  void release() {
    _pool.release(_session);
  }
}

class _SftpPool {
  _SftpPool({required this.maxSessions});

  final int maxSessions;
  final List<_SshSftpSession> _idle = <_SshSftpSession>[];
  final List<_SshSftpSession> _all = <_SshSftpSession>[];
  final List<Completer<_SftpLease>> _waiters = <Completer<_SftpLease>>[];

  Future<_SftpLease> lease(Future<_SshSftpSession> Function() connect) async {
    if (_idle.isNotEmpty) {
      final session = _idle.removeLast();
      return _SftpLease(this, session);
    }
    if (_all.length < maxSessions) {
      final session = await connect();
      _all.add(session);
      return _SftpLease(this, session);
    }
    final waiter = Completer<_SftpLease>();
    _waiters.add(waiter);
    return waiter.future;
  }

  void release(_SshSftpSession session) {
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.isCompleted) {
        waiter.complete(_SftpLease(this, session));
        return;
      }
    }
    _idle.add(session);
  }
}

class _ServerVmLocation {
  const _ServerVmLocation({required this.serverId, required this.vmName});

  final String serverId;
  final String vmName;
}
