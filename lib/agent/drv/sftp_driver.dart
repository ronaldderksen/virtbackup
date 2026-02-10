import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:ffi/ffi.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/settings.dart';

class SftpBackupDriver implements BackupDriver, RemoteBlobDriver, BlobDirectoryLister {
  SftpBackupDriver({required AppSettings settings, void Function(String message)? logInfo}) : _settings = settings, _cacheRoot = _cacheRootForSettings(settings), _logInfo = logInfo ?? ((_) {});

  final AppSettings _settings;
  final Directory _cacheRoot;
  final void Function(String message) _logInfo;
  final _NativeSftpBindings? _nativeSftp = _NativeSftpBindings.tryLoad();

  static const int _writeConcurrency = 8;
  static const String _remoteAppFolderName = 'VirtBackup';
  final _SftpPool _pool = _SftpPool(maxSessions: _writeConcurrency);
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
    _logInfo('sftp: ready (cache=${_cacheRoot.path})');
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
    if (hash.length < 4) {
      return File('${blobsDir().path}${Platform.pathSeparator}$hash');
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    return File('${blobsDir().path}${Platform.pathSeparator}$shard1${Platform.pathSeparator}$shard2${Platform.pathSeparator}$hash');
  }

  @override
  Future<Set<String>> listBlobShard1() async {
    return _withSftp('list blob shard1', (sftp) async {
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
  Future<Set<String>> listBlobShard2(String shard1) async {
    return _withSftp('list blob shard2', (sftp) async {
      final entries = await sftp.listdir(_remoteJoin(_remoteBlobsRoot(), shard1));
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
  Future<Set<String>> listBlobNames(String shard1, String shard2) async {
    return _withSftp('list blob names', (sftp) async {
      final entries = await sftp.listdir(_remoteJoin(_remoteBlobsRoot(), shard1, shard2));
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

    await _withSftp('fresh cleanup rename remote root', (sftp) async {
      try {
        await sftp.rename(current, renamed);
      } catch (error) {
        // If the folder doesn't exist yet, there is nothing to rename.
        try {
          await sftp.stat(current);
        } catch (_) {
          return;
        }
        rethrow;
      }
    });
    _knownRemoteShardKeys.clear();
    _remoteBlobNames.clear();
    _remoteBlobSizes.clear();
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
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final remoteDir = _remoteJoin(_remoteBlobsRoot(), shard1, shard2);
    await _withSftp('ensure blob dir:$remoteDir', (sftp) async {
      await _ensureRemoteDirBlind(sftp, remoteDir);
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
      if (await _remoteExists(sftp, remotePath)) {
        _remoteBlobNames.add(hash);
        return false;
      }

      final shardKey = hash.substring(0, 4);
      if (!_knownRemoteShardKeys.contains(shardKey)) {
        await _ensureRemoteDir(sftp, _remoteBlobDir(hash));
        _knownRemoteShardKeys.add(shardKey);
      }
      if (_nativeSftp != null) {
        await _nativeWriteAll(remoteTemp, data, truncate: true);
      } else {
        final remoteFile = await sftp.open(remoteTemp, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
        try {
          await remoteFile.writeBytes(data);
        } finally {
          await remoteFile.close();
        }
      }
      try {
        await sftp.rename(remoteTemp, remotePath);
        _remoteBlobNames.add(hash);
        _remoteBlobSizes[hash] = bytes.length;
        return true;
      } catch (error) {
        if (await _remoteExists(sftp, remotePath)) {
          await _tryRemoveRemoteFile(sftp, remoteTemp);
          _remoteBlobNames.add(hash);
          return false;
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
    if (hash.length < 4) {
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

    await local.parent.create(recursive: true);
    final tempFile = File('${local.path}.${DateTime.now().microsecondsSinceEpoch}.inprogress');
    final sink = tempFile.openWrite();
    final remotePath = _remoteBlobPath(hash);
    var success = false;
    try {
      final bindings = _nativeSftp;
      if (bindings != null) {
        final stopwatch = Stopwatch()..start();
        final lease = await _nativePool.lease(_connectNative);
        Pointer<Void>? file;
        Pointer<Uint8>? buffer;
        try {
          file = bindings.openRead(lease.session, remotePath);
          if (file == nullptr) {
            throw 'Native SFTP openRead failed: $remotePath';
          }
          const chunkSize = 256 * 1024;
          buffer = calloc<Uint8>(chunkSize);
          var offset = 0;
          var remaining = length;
          while (remaining == null || remaining > 0) {
            final toRead = remaining == null ? chunkSize : remaining.clamp(0, chunkSize);
            if (toRead == 0) {
              break;
            }
            final read = bindings.read(file, offset, buffer, toRead);
            if (read < 0) {
              throw 'Native SFTP read failed: $remotePath';
            }
            if (read == 0) {
              break;
            }
            final chunk = Uint8List.fromList(buffer.asTypedList(read));
            sink.add(chunk);
            yield chunk;
            offset += read;
            if (remaining != null) {
              remaining -= read;
            }
          }
          success = true;
        } finally {
          if (file != null && file != nullptr) {
            bindings.closeFile(file);
          }
          if (buffer != null) {
            calloc.free(buffer);
          }
          lease.release();
          final elapsedMs = stopwatch.elapsedMilliseconds;
          if (elapsedMs > 500) {
            _logInfo('sftp: read blob stream done durationMs=$elapsedMs');
          }
        }
        if (success) {
          return;
        }
      }

      final stopwatch = Stopwatch()..start();
      final lease = await _pool.lease(_connect);
      try {
        final sftp = lease.sftp;
        final file = await sftp.open(remotePath, mode: SftpFileOpenMode.read);
        try {
          await for (final chunk in file.read(length: length)) {
            sink.add(chunk);
            yield chunk;
          }
        } finally {
          await file.close();
        }
        success = true;
      } finally {
        lease.release();
        final elapsedMs = stopwatch.elapsedMilliseconds;
        if (elapsedMs > 500) {
          _logInfo('sftp: read blob stream done durationMs=$elapsedMs');
        }
      }
    } finally {
      try {
        await sink.close();
      } catch (_) {}
      if (!success) {
        try {
          await tempFile.delete();
        } catch (_) {}
      }
      try {
        final hasLocal = await local.exists();
        if (hasLocal) {
          await tempFile.delete();
        } else {
          await tempFile.rename(local.path);
        }
      } catch (_) {
        try {
          await tempFile.delete();
        } catch (_) {}
      }
    }
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 4) {
      return null;
    }
    final local = blobFile(hash);
    if (await local.exists()) {
      return local.readAsBytes();
    }
    final remotePath = _remoteBlobPath(hash);
    if (_nativeSftp != null) {
      final bytes = await _withSftpLogOnly('read blob', () => _nativeReadAll(remotePath));
      if (!await local.exists()) {
        await local.parent.create(recursive: true);
        await local.writeAsBytes(bytes, flush: true);
      }
      return bytes;
    }
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

  @override
  Future<void> closeConnections() async {
    _pool.closeAll();
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
    final stopwatch = Stopwatch()..start();
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final lease = await _nativePool.lease(_connectNative);
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    try {
      file = bindings.openWrite(lease.session, remotePath, truncate: truncate);
      if (file == nullptr) {
        throw 'Native SFTP openWrite failed: $remotePath';
      }
      buffer = calloc<Uint8>(data.length);
      buffer.asTypedList(data.length).setAll(0, data);
      final wrote = bindings.write(file, buffer, data.length);
      if (wrote != data.length) {
        throw 'Native SFTP write failed: wrote=$wrote expected=${data.length}';
      }
    } finally {
      if (file != null && file != nullptr) {
        bindings.closeFile(file);
      }
      if (buffer != null) {
        calloc.free(buffer);
      }
      lease.release();
      final elapsedMs = stopwatch.elapsedMilliseconds;
      if (elapsedMs > 250) {
        _logInfo('sftp: native write done durationMs=$elapsedMs path=$remotePath bytes=${data.length}');
      }
    }
  }

  Future<List<int>> _nativeReadAll(String remotePath) async {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final lease = await _nativePool.lease(_connectNative);
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    try {
      file = bindings.openRead(lease.session, remotePath);
      if (file == nullptr) {
        throw 'Native SFTP openRead failed: $remotePath';
      }
      const chunkSize = 256 * 1024;
      buffer = calloc<Uint8>(chunkSize);
      final out = BytesBuilder(copy: false);
      var offset = 0;
      while (true) {
        final read = bindings.read(file, offset, buffer, chunkSize);
        if (read < 0) {
          throw 'Native SFTP read failed: $remotePath';
        }
        if (read == 0) {
          break;
        }
        out.add(buffer.asTypedList(read));
        offset += read;
      }
      return out.takeBytes();
    } finally {
      if (file != null && file != nullptr) {
        bindings.closeFile(file);
      }
      if (buffer != null) {
        calloc.free(buffer);
      }
      lease.release();
    }
  }

  Future<T> _withSftp<T>(String label, Future<T> Function(SftpClient sftp) action) async {
    final shouldLog = label != 'write blob' && !label.startsWith('list blob ') && !label.startsWith('ensure blob dir');
    final stopwatch = Stopwatch()..start();
    if (shouldLog) {
      _logInfo('sftp: $label start');
    }
    final lease = await _pool.lease(_connect);
    try {
      final result = await action(lease.sftp);
      if (shouldLog) {
        final elapsedMs = stopwatch.elapsedMilliseconds;
        _logInfo('sftp: $label done durationMs=$elapsedMs');
      } else if (label.startsWith('ensure blob dir:')) {
        final elapsedMs = stopwatch.elapsedMilliseconds;
        if (elapsedMs > 250) {
          final path = label.substring('ensure blob dir:'.length);
          _logInfo('sftp: ensure blob dir done durationMs=$elapsedMs path=$path');
        }
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

  Future<T> _withSftpLogOnly<T>(String label, Future<T> Function() action) async {
    final stopwatch = Stopwatch()..start();
    _logInfo('sftp: $label start');
    try {
      final result = await action();
      _logInfo('sftp: $label done durationMs=${stopwatch.elapsedMilliseconds}');
      return result;
    } catch (error) {
      _logInfo('sftp: $label failed durationMs=${stopwatch.elapsedMilliseconds} error=$error');
      rethrow;
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
        // Ignore: blind create should not stat or fail on existing dirs.
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
  bool _closed = false;
  var _connecting = 0;

  Future<_SftpLease> lease(Future<_SshSftpSession> Function() connect) async {
    if (_closed) {
      throw 'SFTP pool is closed.';
    }
    if (_idle.isNotEmpty) {
      final session = _idle.removeLast();
      return _SftpLease(this, session);
    }
    if (_all.length + _connecting < maxSessions) {
      _connecting += 1;
      try {
        final session = await connect();
        _all.add(session);
        return _SftpLease(this, session);
      } finally {
        _connecting -= 1;
      }
    }
    final waiter = Completer<_SftpLease>();
    _waiters.add(waiter);
    return waiter.future;
  }

  void release(_SshSftpSession session) {
    if (_closed) {
      try {
        session.close();
      } catch (_) {}
      return;
    }
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.isCompleted) {
        waiter.complete(_SftpLease(this, session));
        return;
      }
    }
    _idle.add(session);
  }

  void closeAll() {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.isCompleted) {
        waiter.completeError('SFTP pool closed.');
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
}

class _NativeSftpPool {
  _NativeSftpPool({required this.maxSessions});

  final int maxSessions;
  final List<_NativeSftpSession> _idle = <_NativeSftpSession>[];
  final List<_NativeSftpSession> _all = <_NativeSftpSession>[];
  final List<Completer<_NativeSftpLease>> _waiters = <Completer<_NativeSftpLease>>[];
  bool _closed = false;
  var _connecting = 0;

  Future<_NativeSftpLease> lease(Future<_NativeSftpSession> Function() connect) async {
    if (_closed) {
      throw 'Native SFTP pool is closed.';
    }
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
    final waiter = Completer<_NativeSftpLease>();
    _waiters.add(waiter);
    return waiter.future;
  }

  void release(_NativeSftpSession session) {
    if (_closed) {
      try {
        session.close();
      } catch (_) {}
      return;
    }
    if (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.isCompleted) {
        waiter.complete(_NativeSftpLease(this, session));
        return;
      }
    }
    _idle.add(session);
  }

  void closeAll() {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.isCompleted) {
        waiter.completeError('Native SFTP pool closed.');
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
