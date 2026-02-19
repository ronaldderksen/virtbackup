import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:ffi/ffi.dart';

import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart' show BackupStorage;
import 'package:virtbackup/common/settings.dart';

class SftpBackupDriver implements BackupDriver, RemoteBlobDriver, BlobDirectoryLister {
  SftpBackupDriver({required AppSettings settings, int? poolSessions})
    : _settings = settings,
      _cacheRoot = _cacheRootForSettings(settings),
      _maxConcurrentWrites = poolSessions ?? _resolveUploadConcurrency(settings),
      _pool = _SftpPool(maxSessions: poolSessions ?? _resolveUploadConcurrency(settings)),
      _nativePool = _NativeSftpPool(maxSessions: poolSessions ?? max(_resolveUploadConcurrency(settings), _resolveDownloadConcurrency(settings)));

  final AppSettings _settings;
  final Directory _cacheRoot;
  final _NativeSftpBindings? _nativeSftp = _NativeSftpBindings.tryLoad();

  static const int _defaultConcurrency = 8;
  static const int _nativeTransferChunkSize = 16 * 1024 * 1024;
  static const int _blobCacheReservedSessions = 1;
  static const String _remoteAppFolderName = 'VirtBackup';
  final int _maxConcurrentWrites;
  final _SftpPool _pool;
  final _SftpPool _blobCachePool = _SftpPool(maxSessions: _blobCacheReservedSessions);
  final _NativeSftpPool _nativePool;

  Map<String, dynamic> get _params => _resolveSelectedSftpStorage(_settings).params;
  String get _host => (_params['host'] ?? '').toString().trim();
  int get _port => _parseRequiredPort(_params['port']);
  String get _username => (_params['username'] ?? '').toString().trim();
  String get _password => (_params['password'] ?? '').toString();
  String get _basePath => (_params['basePath'] ?? '').toString().trim();
  int get _blockSizeMB => _settings.blockSizeMB;

  static int _resolveUploadConcurrency(AppSettings settings) {
    final storage = _resolveSelectedSftpStorage(settings);
    return storage.uploadConcurrency ?? _defaultConcurrency;
  }

  static int _resolveDownloadConcurrency(AppSettings settings) {
    final storage = _resolveSelectedSftpStorage(settings);
    return storage.downloadConcurrency ?? _defaultConcurrency;
  }

  static BackupStorage _resolveSelectedSftpStorage(AppSettings settings) {
    final selectedId = settings.backupStorageId?.trim() ?? '';
    if (selectedId.isEmpty) {
      throw StateError('SFTP settings require backupStorageId.');
    }
    for (final storage in settings.storage) {
      if (storage.id == selectedId) {
        if (storage.driverId != 'sftp') {
          throw StateError('Selected storage "$selectedId" is not an SFTP storage.');
        }
        return storage;
      }
    }
    throw StateError('SFTP storage "$selectedId" not found.');
  }

  static int _parseRequiredPort(Object? value) {
    final parsed = value is num ? value.toInt() : int.tryParse((value ?? '').toString().trim());
    if (parsed == null || parsed <= 0 || parsed > 65535) {
      throw StateError('SFTP port is invalid.');
    }
    return parsed;
  }

  static Directory _cacheRootForSettings(AppSettings settings) {
    final basePath = settings.backupPath.trim();
    if (basePath.isEmpty) {
      throw StateError('SFTP cache root requires backupPath in settings.');
    }
    final storageId = _cacheKeyFromSettings(settings);
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}cache${Platform.pathSeparator}$storageId');
  }

  static String _cacheKeyFromSettings(AppSettings settings) {
    final storageId = settings.backupStorageId?.trim() ?? '';
    if (storageId.isEmpty) {
      throw StateError('SFTP cache root requires backupStorageId in settings.');
    }
    return storageId.replaceAll(RegExp(r'[^A-Za-z0-9._-]'), '_');
  }

  @override
  BackupDriverCapabilities get capabilities => BackupDriverCapabilities(
    supportsRangeRead: true,
    supportsBatchDelete: false,
    supportsMultipartUpload: false,
    supportsServerSideCopy: false,
    supportsConditionalWrite: false,
    supportsVersioning: false,
    maxConcurrentWrites: _maxConcurrentWrites,
    params: [],
  );

  @override
  String get storage => _cacheRoot.path;

  @override
  bool get discardWrites => false;

  @override
  int get bufferedBytes => 0;

  @override
  Future<void> ensureReady() async {
    if (_nativeSftp == null) {
      throw StateError('Native SFTP is required for SFTP driver.');
    }
    await _cacheRoot.create(recursive: true);
    await tmpDir().create(recursive: true);
  }

  @override
  Future<void> prepareBackup(String serverId, String vmName) async {
    _validateConfig();
    await _cacheRoot.create(recursive: true);
    await tmpDir().create(recursive: true);

    await _withSftp('prepare remote layout', (sftp) async {
      await _ensureRemoteDir(sftp, _remoteRoot());
      await _ensureRemoteDir(sftp, _remoteBlobsRoot());
      await _ensureRemoteDir(sftp, _remoteTmpRoot());
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
  Directory blobsDir() {
    final basePath = _settings.backupPath.trim();
    if (basePath.isEmpty) {
      return Directory('${_cacheRoot.path}${Platform.pathSeparator}blobs${Platform.pathSeparator}$_blockSizeMB');
    }
    return Directory('$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}blobs${Platform.pathSeparator}$_blockSizeMB');
  }

  @override
  Directory tmpDir() => Directory('${_cacheRoot.path}${Platform.pathSeparator}tmp');

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
      final entries = await _remoteListDir(sftp, _remoteBlobsRoot());
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
      final entries = await _remoteListDir(sftp, _remoteJoin(_remoteBlobsRoot(), shard));
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

  Future<List<SftpName>> _remoteListDir(SftpClient sftp, String remotePath) => sftp.listdir(remotePath);

  Future<SftpFileAttrs> _remoteStat(SftpClient sftp, String remotePath) => sftp.stat(remotePath);

  Future<void> _remoteMkdir(SftpClient sftp, String remotePath) => sftp.mkdir(remotePath);

  Future<void> _remoteRename(SftpClient sftp, String fromPath, String toPath) => sftp.rename(fromPath, toPath);

  Future<SftpFile> _remoteOpen(SftpClient sftp, String remotePath, SftpFileOpenMode mode) => sftp.open(remotePath, mode: mode);

  Future<void> _remoteRemove(SftpClient sftp, String remotePath) => sftp.remove(remotePath);

  Future<void> _remoteWriteAll(SftpFile remoteFile, String remotePath, Uint8List bytes) => remoteFile.writeBytes(bytes);

  Future<void> _remoteCloseFile(SftpFile remoteFile, String remotePath) => remoteFile.close();

  @override
  Future<void> uploadFile({required String relativePath, required File localFile}) async {
    final normalizedPath = _normalizeRelativePath(relativePath);
    await _commitSmallRemoteFile(localFile: localFile, remotePath: _remoteRelativePath(normalizedPath));
  }

  @override
  Future<List<String>> listRelativeFiles(String relativeDir) async {
    final normalizedDir = _normalizeRelativePath(relativeDir);
    if (normalizedDir.isEmpty) {
      return <String>[];
    }
    final remoteRoot = _remoteRelativePath(normalizedDir);
    return _withSftp('list relative files', (sftp) async {
      final result = <String>[];
      await _listRemoteFilesRecursively(sftp: sftp, remoteDir: remoteRoot, relativePrefix: normalizedDir, out: result);
      result.sort();
      return result;
    });
  }

  @override
  Future<List<int>?> readFileBytes(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return null;
    }
    return _readRemoteFileBytes(_remoteRelativePath(normalized));
  }

  @override
  Future<bool> deleteFile(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return false;
    }
    final remotePath = _remoteRelativePath(normalized);
    var deletedRemote = false;
    await _withSftp('delete file', (sftp) async {
      try {
        await _remoteRemove(sftp, remotePath);
        deletedRemote = true;
      } catch (_) {
        if (await _remoteExists(sftp, remotePath)) {
          rethrow;
        }
      }
    });
    final localFile = _relativeCacheFile(normalized);
    if (await localFile.exists()) {
      await localFile.delete();
    }
    return deletedRemote;
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
        await _remoteRename(sftp, current, renamed);
      } catch (error) {
        // If the folder doesn't exist yet, there is nothing to rename.
        try {
          await _remoteStat(sftp, current);
        } catch (_) {
          return;
        }
        rethrow;
      }
    });
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 2) {
      return;
    }
    final remoteDir = _remoteBlobDir(hash);
    await _withSftp('ensure blob dir:$remoteDir', (sftp) async {
      final normalized = _normalizeRemotePath(remoteDir);
      if (normalized == '/' || normalized.isEmpty) {
        return;
      }
      final parts = normalized.split('/').where((part) => part.trim().isNotEmpty).toList();
      var current = normalized.startsWith('/') ? '/' : '';
      for (final part in parts) {
        current = current.isEmpty || current == '/' ? '$current$part' : '$current/$part';
        try {
          await _remoteMkdir(sftp, current);
        } catch (_) {
          // Blind mkdir for blob-cache commanded shard creation.
        }
      }
    });
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2 || bytes.isEmpty) {
      return;
    }
    final remotePath = _remoteBlobPath(hash);
    final remoteTemp = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final data = Uint8List.fromList(bytes);
    return _writeBlobWithTiming(hash: hash, remoteTemp: remoteTemp, remotePath: remotePath, data: data);
  }

  @override
  Future<bool> blobExistsRemote(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    return _withSftp('blob exists', (sftp) async {
      try {
        await _remoteStat(sftp, _remoteBlobPath(hash));
        return true;
      } catch (_) {
        return false;
      }
    });
  }

  @override
  Future<int?> blobLength(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    return _withSftp('blob length', (sftp) async {
      try {
        final attrs = await _remoteStat(sftp, _remoteBlobPath(hash));
        return attrs.size;
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
    final remotePath = _remoteBlobPath(hash);
    try {
      yield* _openBlobStreamNative(remotePath, length: length);
    } on _NativeBlobMissing {
      return;
    }
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    final remotePath = _remoteBlobPath(hash);
    final builder = BytesBuilder(copy: false);
    try {
      await for (final chunk in _openBlobStreamNative(remotePath)) {
        builder.add(chunk);
      }
    } on _NativeBlobMissing {
      return null;
    }
    return builder.takeBytes();
  }

  @override
  String backupCompletedMessage(String outputPath) => 'Backup saved to SFTP';

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
    await _deleteInProgressInDir(blobsDir());
  }

  @override
  Future<void> closeConnections() async {
    _pool.closeAll();
    _blobCachePool.closeAll();
    _nativePool.closeAll();
  }

  Future<_SshSftpSession> _connect() async {
    final opStopwatch = Stopwatch()..start();
    _logDebug('connect start host=$_host port=$_port user=$_username');
    try {
      final socket = await SSHSocket.connect(_host, _port, timeout: const Duration(seconds: 10));
      final client = SSHClient(socket, username: _username, onPasswordRequest: () => _password);
      final sftp = await client.sftp();
      opStopwatch.stop();
      _logDebug('connect success host=$_host port=$_port durationMs=${opStopwatch.elapsedMilliseconds}');
      return _SshSftpSession(socket: socket, client: client, sftp: sftp);
    } catch (error, stackTrace) {
      opStopwatch.stop();
      _logDebug('connect failed host=$_host port=$_port durationMs=${opStopwatch.elapsedMilliseconds} error=$error');
      _logDebug(stackTrace.toString());
      rethrow;
    }
  }

  Future<_NativeSftpSession> _connectNative() async {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final opStopwatch = Stopwatch()..start();
    _logDebug('native connect start host=$_host port=$_port user=$_username');
    try {
      final session = bindings.connect(_host, _port, _username, _password);
      if (session == nullptr) {
        throw 'Native SFTP connect failed.';
      }
      opStopwatch.stop();
      _logDebug('native connect success host=$_host port=$_port durationMs=${opStopwatch.elapsedMilliseconds}');
      return _NativeSftpSession(bindings: bindings, session: session);
    } catch (error, stackTrace) {
      opStopwatch.stop();
      _logDebug('native connect failed host=$_host port=$_port durationMs=${opStopwatch.elapsedMilliseconds} error=$error');
      _logDebug(stackTrace.toString());
      rethrow;
    }
  }

  Future<void> _nativeWriteAll(String remotePath, Uint8List data, {required bool truncate}) async {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw 'Native SFTP is not available.';
    }
    final lease = await _nativePool.lease(_connectNative);
    var invalidateLease = false;
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    var offset = 0;
    try {
      file = bindings.openWrite(lease.session, remotePath, truncate: truncate);
      if (file == nullptr) {
        throw 'Native SFTP openWrite failed: $remotePath';
      }
      final bufferLength = min(_nativeTransferChunkSize, data.length);
      buffer = calloc<Uint8>(bufferLength);
      final nativeView = buffer.asTypedList(bufferLength);
      while (offset < data.length) {
        final chunkLength = min(bufferLength, data.length - offset);
        nativeView.setRange(0, chunkLength, data, offset);
        final wrote = bindings.write(file, buffer, chunkLength);
        if (wrote != chunkLength) {
          throw 'Native SFTP write failed: wrote=$wrote expected=$chunkLength';
        }
        offset += chunkLength;
      }
    } catch (error) {
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
    }
  }

  Stream<List<int>> _openBlobStreamNative(String remotePath, {int? length}) async* {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw StateError('Native SFTP is required for SFTP blob reads.');
    }
    final lease = await _nativePool.lease(_connectNative);
    var invalidateLease = false;
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    var offset = 0;
    try {
      file = bindings.openRead(lease.session, remotePath);
      if (file == nullptr) {
        final exists = await _withSftp('native read open stat', (sftp) async => _remoteExists(sftp, remotePath));
        if (!exists) {
          throw _NativeBlobMissing();
        }
        throw 'Native SFTP openRead failed: $remotePath';
      }
      final chunkLength = length == null ? _nativeTransferChunkSize : min(_nativeTransferChunkSize, length);
      buffer = calloc<Uint8>(chunkLength);
      final emitChunkLength = length == null ? _nativeTransferChunkSize : min(_nativeTransferChunkSize, length);
      var emitBuffer = Uint8List(emitChunkLength);
      var emitOffset = 0;
      while (true) {
        if (length != null && offset >= length) {
          break;
        }
        final toRead = length == null ? chunkLength : min(chunkLength, length - offset);
        if (toRead <= 0) {
          break;
        }
        final read = bindings.read(file, offset, buffer, toRead);
        if (read < 0) {
          throw 'Native SFTP read failed: $remotePath';
        }
        if (read == 0) {
          break;
        }
        offset += read;
        final source = buffer.asTypedList(read);
        var sourceOffset = 0;
        while (sourceOffset < read) {
          final remainingSource = read - sourceOffset;
          final remainingTarget = emitBuffer.length - emitOffset;
          final toCopy = remainingSource < remainingTarget ? remainingSource : remainingTarget;
          emitBuffer.setRange(emitOffset, emitOffset + toCopy, source, sourceOffset);
          emitOffset += toCopy;
          sourceOffset += toCopy;
          if (emitOffset == emitBuffer.length) {
            yield emitBuffer;
            emitBuffer = Uint8List(emitChunkLength);
            emitOffset = 0;
          }
        }
      }
      if (emitOffset > 0) {
        yield Uint8List.fromList(emitBuffer.sublist(0, emitOffset));
      }
    } on _NativeBlobMissing {
      rethrow;
    } catch (error) {
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
    }
  }

  Future<void> _writeBlobWithTiming({required String hash, required String remoteTemp, required String remotePath, required Uint8List data}) async {
    final label = 'write blob $hash';
    return _withRetry(label, () async {
      final pool = _poolForLabel(label);
      final lease = await pool.lease(_connect);
      final opStopwatch = Stopwatch()..start();
      _logDebug('driver=sftp op start label="$label" mode=${_nativeSftp != null ? 'native' : 'dartssh2'} lease={${_formatLeaseMetrics(lease.metrics)}}');
      var released = false;
      try {
        if (_nativeSftp != null) {
          await _nativeWriteAll(remoteTemp, data, truncate: true);
        } else {
          final remoteFile = await _remoteOpen(lease.sftp, remoteTemp, SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
          try {
            await _remoteWriteAll(remoteFile, remoteTemp, data);
          } finally {
            await _remoteCloseFile(remoteFile, remoteTemp);
          }
        }
        try {
          await _remoteRename(lease.sftp, remoteTemp, remotePath);
        } catch (error) {
          if (_isSftpFailureCode(error, 4)) {
            _logDebug('driver=sftp rename code=4 label="$label" remotePath="$remotePath" checking idempotent-success');
            final recovered = await _isRenameIdempotentSuccess(sftp: lease.sftp, hash: hash, remotePath: remotePath, expectedBytes: data.length);
            if (recovered) {
              _logDebug('driver=sftp rename recovered label="$label" remotePath="$remotePath"');
              lease.release();
              released = true;
              opStopwatch.stop();
              _logDebug('driver=sftp op success label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}}');
              return;
            }
            _logDebug('driver=sftp rename recovery failed label="$label" remotePath="$remotePath"');
          }
          await _tryRemoveRemoteFile(lease.sftp, remoteTemp);
          rethrow;
        }
        lease.release();
        released = true;
        opStopwatch.stop();
        _logDebug('driver=sftp op success label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}}');
      } catch (error) {
        opStopwatch.stop();
        _logDebug('driver=sftp op failed label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}} error=$error');
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

  bool _isSftpFailureCode(Object error, int code) {
    return error.toString().contains('Failure(code $code)');
  }

  Future<bool> _isRenameIdempotentSuccess({required SftpClient sftp, required String hash, required String remotePath, required int expectedBytes}) async {
    try {
      final targetAttrs = await _remoteStat(sftp, remotePath);
      final targetSize = targetAttrs.size;
      if (targetSize == null) {
        return false;
      }
      if (targetSize != expectedBytes) {
        return false;
      }
      return true;
    } catch (_) {
      return false;
    }
  }

  Future<T> _withSftp<T>(String label, Future<T> Function(SftpClient sftp) action) async {
    return _withRetry(label, () async {
      final pool = _poolForLabel(label);
      final lease = await pool.lease(_connect);
      final opStopwatch = Stopwatch()..start();
      _logDebug('driver=sftp op start label="$label" mode=dartssh2 lease={${_formatLeaseMetrics(lease.metrics)}}');
      var released = false;
      try {
        final result = await action(lease.sftp);
        opStopwatch.stop();
        _logDebug('driver=sftp op success label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}}');
        lease.release();
        released = true;
        return result;
      } catch (error) {
        opStopwatch.stop();
        _logDebug('driver=sftp op failed label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}} error=$error');
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
      } catch (error, stackTrace) {
        if (attempt >= maxRetries) {
          LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp retry failed label="$label" attempt=${attempt + 1}/${maxRetries + 1} error=$error');
          LogWriter.logAgentSync(level: 'debug', message: stackTrace.toString());
          rethrow;
        }
        _logDebug('driver=sftp retry invalidating pools sftp={${_pool.debugState()}} blob={${_blobCachePool.debugState()}} native={${_nativePool.debugState()}}');
        _pool.invalidateIdle();
        _blobCachePool.invalidateIdle();
        _nativePool.invalidateIdle();
        final nextAttempt = attempt + 1;
        LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp retrying label="$label" attempt=${nextAttempt + 1}/${maxRetries + 1} delay=${delaySeconds}s error=$error');
        LogWriter.logAgentSync(level: 'debug', message: stackTrace.toString());
        attempt += 1;
        await Future.delayed(Duration(seconds: delaySeconds));
        delaySeconds *= 2;
      }
    }
  }

  String _formatLeaseMetrics(_SftpLeaseMetrics metrics) {
    return 'waitMs=${metrics.waitMs} connectMs=${metrics.connectMs} reused=${metrics.reused} queued=${metrics.queued}';
  }

  void _logDebug(String message) {
    LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $message');
  }

  _SftpPool _poolForLabel(String label) {
    if (label == 'list blobs' || label.startsWith('list blobs/') || label.startsWith('ensure blob dir:')) {
      return _blobCachePool;
    }
    return _pool;
  }

  // Keep remote structure scoped under a dedicated folder to avoid cluttering user-provided base paths.
  String _remoteRoot() => _remoteJoin(_normalizeRemotePath(_basePath), _remoteAppFolderName);
  String _remoteBlobsRoot() => _remoteJoin(_remoteRoot(), 'blobs', _blockSizeMB.toString());
  String _remoteTmpRoot() => _remoteJoin(_remoteRoot(), 'tmp');

  String _remoteRelativePath(String relativePath) => _remoteJoin(_remoteRoot(), _normalizeRelativePath(relativePath));

  String _remoteBlobDir(String hash) => _remoteJoin(_remoteBlobsRoot(), hash.substring(0, 2));
  String _remoteBlobPath(String hash) => _remoteJoin(_remoteBlobDir(hash), hash);

  File _relativeCacheFile(String relativePath) {
    final normalized = _normalizeRelativePath(relativePath).replaceAll('/', Platform.pathSeparator);
    return File('${_cacheRoot.path}${Platform.pathSeparator}$normalized');
  }

  Future<void> _commitSmallRemoteFile({required File localFile, required String remotePath}) async {
    if (!await localFile.exists()) {
      return;
    }
    final bytes = await localFile.readAsBytes();
    await _withSftp('upload small file', (sftp) async {
      await _ensureRemoteDir(sftp, _remoteDirName(remotePath));
      final tmpRemote = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
      final remoteFile = await _remoteOpen(sftp, tmpRemote, SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
      try {
        await _remoteWriteAll(remoteFile, tmpRemote, Uint8List.fromList(bytes));
      } finally {
        await _remoteCloseFile(remoteFile, tmpRemote);
      }
      try {
        await _remoteRename(sftp, tmpRemote, remotePath);
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

  Future<void> _listRemoteFilesRecursively({required SftpClient sftp, required String remoteDir, required String relativePrefix, required List<String> out}) async {
    List<SftpName> entries;
    try {
      entries = await _remoteListDir(sftp, remoteDir);
    } catch (_) {
      return;
    }
    for (final entry in entries) {
      final name = entry.filename;
      if (name == '.' || name == '..') {
        continue;
      }
      final remotePath = _remoteJoin(remoteDir, name);
      if (entry.attr.isDirectory) {
        final childPrefix = '$relativePrefix/$name';
        await _listRemoteFilesRecursively(sftp: sftp, remoteDir: remotePath, relativePrefix: childPrefix, out: out);
        continue;
      }
      out.add('$relativePrefix/$name');
    }
  }

  Future<Uint8List?> _readRemoteFileBytes(String remotePath) async {
    final builder = BytesBuilder(copy: false);
    try {
      await for (final chunk in _openBlobStreamNative(remotePath)) {
        builder.add(chunk);
      }
      return builder.takeBytes();
    } on _NativeBlobMissing {
      return null;
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

  String _normalizeRelativePath(String relativePath) {
    final normalized = relativePath.replaceAll('\\', '/');
    final parts = normalized.split('/').where((part) => part.trim().isNotEmpty && part != '.').toList();
    return parts.join('/');
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
        await _remoteMkdir(sftp, current);
      } catch (_) {
        try {
          final attrs = await _remoteStat(sftp, current);
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
      await _remoteStat(sftp, remotePath);
      return true;
    } catch (_) {
      return false;
    }
  }

  Future<void> _tryRemoveRemoteFile(SftpClient sftp, String remotePath) async {
    try {
      await _remoteRemove(sftp, remotePath);
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

  String debugState() {
    return 'max=$maxSessions all=${_all.length} idle=${_idle.length} waiters=${_waiters.length} connecting=$_connecting closed=$_closed';
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

class _NativeBlobMissing implements Exception {}

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
    final opStopwatch = Stopwatch()..start();
    try {
      bindings.disconnect(session);
      opStopwatch.stop();
    } catch (error) {
      opStopwatch.stop();
      rethrow;
    }
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

  String debugState() {
    return 'max=$maxSessions all=${_all.length} idle=${_idle.length} waiters=${_waiters.length} connecting=$_connecting closed=$_closed';
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
