import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

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
      _nativePool = _NativeSftpPool(maxSessions: poolSessions ?? max(_resolveUploadConcurrency(settings), _resolveDownloadConcurrency(settings))),
      _blobCachePool = _NativeSftpPool(maxSessions: _blobCacheReservedSessions);

  final AppSettings _settings;
  final Directory _cacheRoot;
  final _NativeSftpBindings? _nativeSftp = _NativeSftpBindings.tryLoad();

  static const int _defaultConcurrency = 8;
  static const int _nativeTransferChunkSize = 16 * 1024 * 1024;
  static const int _blobCacheReservedSessions = 1;
  static const int _maxRetryAttempts = 4;
  static const Duration _callTimeout = Duration(seconds: 30);
  static const Duration _initialRetryDelay = Duration(seconds: 2);
  static const String _remoteAppFolderName = 'VirtBackup';
  static const int _sftpOk = 0;
  static const int _sftpMissing = 2;
  static const int _sftpFailure = 4;

  final int _maxConcurrentWrites;
  final _NativeSftpPool _nativePool;
  final _NativeSftpPool _blobCachePool;

  Map<String, dynamic> get _params => _resolveSelectedSftpStorage(_settings).params;
  String get _host => (_params['host'] ?? '').toString().trim();
  int get _port => _parseRequiredPort(_params['port']);
  String get _username => (_params['username'] ?? '').toString().trim();
  String get _password => (_params['password'] ?? '').toString();
  String get _basePath => (_params['basePath'] ?? '').toString().trim();
  String get _storageId => _settings.backupStorageId?.trim() ?? '';
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
    await _ensureRemoteDir(_remoteRoot());
    await _ensureRemoteDir(_remoteBlobsRoot());
    await _ensureRemoteDir(_remoteTmpRoot());
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
    final entries = await _remoteListDir(_remoteBlobsRoot(), useBlobCachePool: true);
    final names = <String>{};
    for (final entry in entries) {
      if (entry.name == '.' || entry.name == '..') {
        continue;
      }
      if (!entry.isDirectory) {
        continue;
      }
      names.add(entry.name);
    }
    return names;
  }

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    final entries = await _remoteListDir(_remoteJoin(_remoteBlobsRoot(), shard), useBlobCachePool: true);
    final names = <String>{};
    for (final entry in entries) {
      if (entry.name == '.' || entry.name == '..') {
        continue;
      }
      if (entry.isDirectory) {
        continue;
      }
      if (entry.name.endsWith('.inprogress')) {
        continue;
      }
      names.add(entry.name);
    }
    return names;
  }

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
    final result = <String>[];
    await _listRemoteFilesRecursively(remoteDir: _remoteRelativePath(normalizedDir), relativePrefix: normalizedDir, out: result);
    result.sort();
    return result;
  }

  @override
  Future<List<int>?> readFileBytes(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return null;
    }
    return _readRemoteFileBytesWithRetry(_remoteRelativePath(normalized));
  }

  @override
  Future<bool> deleteFile(String relativePath) async {
    final normalized = _normalizeRelativePath(relativePath);
    if (normalized.isEmpty) {
      return false;
    }
    final remotePath = _remoteRelativePath(normalized);
    var deletedRemote = false;
    try {
      await _remoteRemove(remotePath);
      deletedRemote = true;
    } on _NativeSftpStatusException catch (error) {
      if (!error.isMissing) {
        rethrow;
      }
    }
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
    try {
      await _remoteRename(current, renamed);
    } on _NativeSftpStatusException catch (error) {
      if (error.isMissing) {
        return;
      }
      if (!await _remoteExists(current)) {
        return;
      }
      rethrow;
    }
  }

  @override
  Future<void> ensureBlobDir(String hash) async {
    if (hash.length < 2) {
      return;
    }
    await _ensureRemoteDir(_remoteBlobDir(hash), blindMkdir: true, useBlobCachePool: true);
  }

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    if (hash.length < 2 || bytes.isEmpty) {
      return;
    }
    final remotePath = _remoteBlobPath(hash);
    final remoteTemp = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    final data = bytes is Uint8List ? bytes : Uint8List.fromList(bytes);
    await _nativeWriteAll(remoteTemp, data, truncate: true, label: 'write blob $hash');
    try {
      await _remoteRename(remoteTemp, remotePath, label: 'rename blob $hash');
    } on _NativeSftpStatusException catch (error) {
      if (error.status == _sftpFailure) {
        _logDebug('rename conflict label="write blob $hash" remotePath="$remotePath" checking existing target sha256');
        final recovered = await _verifyExistingTargetForRenameConflict(hash: hash, remotePath: remotePath, expectedBytes: data.length);
        if (recovered) {
          await _tryRemoveRemoteFile(remoteTemp);
          return;
        }
      }
      await _tryRemoveRemoteFile(remoteTemp);
      rethrow;
    } catch (_) {
      await _tryRemoveRemoteFile(remoteTemp);
      rethrow;
    }
  }

  @override
  Future<bool> blobExistsRemote(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    return _remoteExists(_remoteBlobPath(hash));
  }

  @override
  Future<int?> blobLength(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    try {
      final attrs = await _remoteStat(_remoteBlobPath(hash));
      return attrs.size;
    } on _NativeSftpStatusException catch (error) {
      if (error.isMissing) {
        return null;
      }
      rethrow;
    }
  }

  @override
  Stream<List<int>> openBlobStream(String hash, {int? length}) async* {
    if (hash.length < 2) {
      return;
    }
    final remotePath = _remoteBlobPath(hash);
    try {
      await for (final chunk in _openRemoteFileStreamWithRetry(remotePath, length: length, label: 'read blob $remotePath')) {
        yield chunk;
      }
    } on _NativeBlobMissing catch (error) {
      _logDebug(error.toString());
      return;
    }
  }

  @override
  Future<List<int>?> readBlobBytes(String hash) async {
    if (hash.length < 2) {
      return null;
    }
    return _readRemoteFileBytesWithRetry(_remoteBlobPath(hash));
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
    _nativePool.closeAll();
    _blobCachePool.closeAll();
  }

  @override
  void setWriteConcurrencyLimit(int concurrency) {
    if (concurrency <= 0) {
      throw StateError('SFTP write concurrency must be greater than zero.');
    }
    _nativePool.setMaxSessions(concurrency);
  }

  @override
  void setReadConcurrencyLimit(int concurrency) {
    if (concurrency <= 0) {
      throw StateError('SFTP read concurrency must be greater than zero.');
    }
    _nativePool.setMaxSessions(concurrency);
  }

  Future<_NativeSftpSession> _connectNative() async {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw StateError('Native SFTP is not available.');
    }
    final opStopwatch = Stopwatch()..start();
    _logDebug('native connect start host=$_host port=$_port user=$_username');
    try {
      final session = bindings.connect(_host, _port, _username, _password);
      if (session == nullptr) {
        throw _NativeSftpStatusException(label: 'connect', status: -1, path: '$_username@$_host:$_port', detail: 'storage="$_storageId"');
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

  Future<T> _withNativeSession<T>(String label, FutureOr<T> Function(_NativeSftpLease lease, _NativeSftpBindings bindings) action, {_NativeSftpPool? pool}) {
    return _withRetry(label, () async {
      final bindings = _requiredBindings();
      final selectedPool = pool ?? _nativePool;
      final lease = await selectedPool.lease(_connectNative);
      var released = false;
      final opStopwatch = Stopwatch()..start();
      try {
        final result = await Future<T>.sync(() => action(lease, bindings)).timeout(_callTimeout);
        opStopwatch.stop();
        lease.release();
        released = true;
        return result;
      } on TimeoutException catch (error) {
        opStopwatch.stop();
        _logTimeout(label, opStopwatch.elapsed);
        lease.invalidate();
        released = true;
        throw _SftpOperationTimeout(label, opStopwatch.elapsed, cause: error);
      } catch (error) {
        opStopwatch.stop();
        _logDebug('op failed label="$label" durationMs=${opStopwatch.elapsedMilliseconds} lease={${_formatLeaseMetrics(lease.metrics)}} error=$error');
        if (_isRetryable(error)) {
          lease.invalidate();
        } else {
          lease.release();
        }
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
    var attempt = 1;
    var delay = _initialRetryDelay;
    while (true) {
      try {
        return await action();
      } catch (error, stackTrace) {
        if (!_isRetryable(error)) {
          rethrow;
        }
        if (attempt > _maxRetryAttempts) {
          LogWriter.logAgentSync(level: 'error', message: 'driver=sftp failed label="$label" attempts=${_maxRetryAttempts + 1} error=$error');
          LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $stackTrace');
          throw 'sftp $label failed after ${_maxRetryAttempts + 1} attempts: $error';
        }
        LogWriter.logAgentSync(level: 'warn', message: 'driver=sftp retry label="$label" attempt=${attempt + 1}/${_maxRetryAttempts + 1} delayMs=${delay.inMilliseconds} reason=$error');
        LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $stackTrace');
        _nativePool.invalidateIdle();
        _blobCachePool.invalidateIdle();
        await Future<void>.delayed(delay);
        delay *= 2;
        attempt += 1;
      }
    }
  }

  bool _isRetryable(Object error) {
    if (error is _NativeBlobMissing || error is BackupWriteConflictMismatch || error is StateError) {
      return false;
    }
    if (error is _NativeSftpStatusException && error.isMissing) {
      return false;
    }
    return true;
  }

  _NativeSftpBindings _requiredBindings() {
    final bindings = _nativeSftp;
    if (bindings == null) {
      throw StateError('Native SFTP is required for SFTP driver.');
    }
    return bindings;
  }

  void _logDebug(String message) {
    LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $message');
  }

  void _logTimeout(String label, Duration duration) {
    LogWriter.logAgentSync(level: 'warn', message: 'driver=sftp timeout label="$label" durationMs=${duration.inMilliseconds}');
  }

  String _formatLeaseMetrics(_NativeSftpLeaseMetrics metrics) {
    return 'waitMs=${metrics.waitMs} connectMs=${metrics.connectMs} reused=${metrics.reused} queued=${metrics.queued}';
  }

  int _remainingCallTimeoutMs(Stopwatch stopwatch) {
    final remaining = _callTimeout.inMilliseconds - stopwatch.elapsedMilliseconds;
    return remaining <= 0 ? 0 : remaining;
  }

  Future<List<_NativeSftpEntry>> _remoteListDir(String remotePath, {bool useBlobCachePool = false}) {
    final label = 'listdir $remotePath';
    return _withNativeSession(label, (lease, bindings) {
      final listPtr = bindings.listdir(lease.session, remotePath);
      if (listPtr == nullptr) {
        throw _NativeSftpStatusException(label: label, status: bindings.lastError(lease.session), path: remotePath);
      }
      try {
        final list = listPtr.ref;
        final result = <_NativeSftpEntry>[];
        for (var index = 0; index < list.count; index += 1) {
          final entry = (list.entries + index).ref;
          result.add(_NativeSftpEntry(name: entry.name.cast<Utf8>().toDartString(), isDirectory: entry.isDir == 1, size: entry.size));
        }
        return result;
      } finally {
        bindings.freeDirList(listPtr.cast<Void>());
      }
    }, pool: useBlobCachePool ? _blobCachePool : null);
  }

  Future<_NativeSftpFileAttrs> _remoteStat(String remotePath) {
    final label = 'stat $remotePath';
    return _withNativeSession(label, (lease, bindings) {
      final size = calloc<Int64>();
      final isDir = calloc<Int32>();
      try {
        final status = bindings.stat(lease.session, remotePath, size, isDir);
        if (status != _sftpOk) {
          throw _NativeSftpStatusException(label: label, status: status, path: remotePath);
        }
        return _NativeSftpFileAttrs(size: size.value, isDirectory: isDir.value == 1);
      } finally {
        calloc.free(size);
        calloc.free(isDir);
      }
    });
  }

  Future<void> _remoteMkdir(String remotePath, {bool useBlobCachePool = false}) {
    final label = 'mkdir $remotePath';
    return _withNativeSession(label, (lease, bindings) {
      final status = bindings.mkdir(lease.session, remotePath);
      if (status != _sftpOk) {
        final size = calloc<Int64>();
        final isDir = calloc<Int32>();
        try {
          final statStatus = bindings.stat(lease.session, remotePath, size, isDir);
          if (statStatus == _sftpOk && isDir.value == 1) {
            return;
          }
        } finally {
          calloc.free(size);
          calloc.free(isDir);
        }
        throw _NativeSftpStatusException(label: label, status: status, path: remotePath);
      }
    }, pool: useBlobCachePool ? _blobCachePool : null);
  }

  Future<void> _remoteRename(String fromPath, String toPath, {String? label}) {
    final resolvedLabel = label ?? 'rename $fromPath';
    return _withNativeSession(resolvedLabel, (lease, bindings) {
      final status = bindings.rename(lease.session, fromPath, toPath);
      if (status != _sftpOk) {
        throw _NativeSftpStatusException(label: resolvedLabel, status: status, path: fromPath);
      }
    });
  }

  Future<void> _remoteRemove(String remotePath) {
    final label = 'remove $remotePath';
    return _withNativeSession(label, (lease, bindings) {
      final status = bindings.remove(lease.session, remotePath);
      if (status != _sftpOk) {
        throw _NativeSftpStatusException(label: label, status: status, path: remotePath);
      }
    });
  }

  Future<bool> _remoteExists(String remotePath) async {
    try {
      await _remoteStat(remotePath);
      return true;
    } on _NativeSftpStatusException catch (error) {
      if (error.isMissing) {
        return false;
      }
      rethrow;
    }
  }

  Future<void> _nativeWriteAll(String remotePath, Uint8List data, {required bool truncate, required String label}) {
    return _withNativeSession(label, (lease, bindings) {
      Pointer<Void>? file;
      Pointer<Uint8>? buffer;
      var offset = 0;
      final transferStopwatch = Stopwatch()..start();
      try {
        file = bindings.openWrite(lease.session, remotePath, truncate: truncate);
        if (file == nullptr) {
          throw _NativeSftpStatusException(label: '$label openWrite', status: bindings.lastError(lease.session), path: remotePath);
        }
        final bufferLength = min(_nativeTransferChunkSize, data.length);
        buffer = calloc<Uint8>(bufferLength);
        final nativeView = buffer.asTypedList(bufferLength);
        while (offset < data.length) {
          final chunkLength = min(bufferLength, data.length - offset);
          nativeView.setRange(0, chunkLength, data, offset);
          final timeoutMs = _remainingCallTimeoutMs(transferStopwatch);
          if (timeoutMs <= 0) {
            throw _SftpOperationTimeout(label, transferStopwatch.elapsed);
          }
          final wrote = bindings.write(file, buffer, chunkLength, timeoutMs);
          if (wrote != chunkLength) {
            throw _NativeSftpStatusException(label: '$label write', status: -1, path: remotePath, detail: 'wrote=$wrote expected=$chunkLength');
          }
          offset += chunkLength;
        }
      } finally {
        if (file != null && file != nullptr) {
          bindings.closeFile(file);
        }
        if (buffer != null) {
          calloc.free(buffer);
        }
      }
    });
  }

  Stream<List<int>> _openRemoteFileStreamWithRetry(String remotePath, {int? length, required String label}) async* {
    var offset = 0;
    var attempt = 1;
    var delay = _initialRetryDelay;
    while (true) {
      try {
        await for (final chunk in _openRemoteFileStreamAttempt(remotePath, offset: offset, length: length, label: label)) {
          offset += chunk.length;
          yield chunk;
        }
        return;
      } catch (error, stackTrace) {
        if (!_isRetryable(error)) {
          rethrow;
        }
        if (attempt > _maxRetryAttempts) {
          LogWriter.logAgentSync(level: 'error', message: 'driver=sftp failed label="$label" attempts=${_maxRetryAttempts + 1} offset=$offset error=$error');
          LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $stackTrace');
          throw 'sftp $label failed after ${_maxRetryAttempts + 1} attempts at offset $offset: $error';
        }
        LogWriter.logAgentSync(level: 'warn', message: 'driver=sftp retry label="$label" attempt=${attempt + 1}/${_maxRetryAttempts + 1} offset=$offset delayMs=${delay.inMilliseconds} reason=$error');
        LogWriter.logAgentSync(level: 'debug', message: 'driver=sftp $stackTrace');
        _nativePool.invalidateIdle();
        _blobCachePool.invalidateIdle();
        await Future<void>.delayed(delay);
        delay *= 2;
        attempt += 1;
      }
    }
  }

  Stream<List<int>> _openRemoteFileStreamAttempt(String remotePath, {required int offset, int? length, required String label}) async* {
    final bindings = _requiredBindings();
    final lease = await _nativePool.lease(_connectNative);
    var invalidateLease = false;
    Pointer<Void>? file;
    Pointer<Uint8>? buffer;
    var currentOffset = offset;
    final transferStopwatch = Stopwatch()..start();
    try {
      file = bindings.openRead(lease.session, remotePath);
      if (file == nullptr) {
        final status = bindings.lastError(lease.session);
        if (status == _sftpMissing) {
          throw _NativeBlobMissing(remotePath);
        }
        throw _NativeSftpStatusException(label: '$label openRead', status: status, path: remotePath);
      }
      final bufferLength = length == null ? _nativeTransferChunkSize : min(_nativeTransferChunkSize, max(0, length - currentOffset));
      if (bufferLength <= 0) {
        return;
      }
      buffer = calloc<Uint8>(bufferLength);
      while (length == null || currentOffset < length) {
        final toRead = length == null ? bufferLength : min(bufferLength, length - currentOffset);
        if (toRead <= 0) {
          break;
        }
        final timeoutMs = _remainingCallTimeoutMs(transferStopwatch);
        if (timeoutMs <= 0) {
          throw _SftpOperationTimeout(label, transferStopwatch.elapsed);
        }
        final read = bindings.read(file, currentOffset, buffer, toRead, timeoutMs);
        if (read < 0) {
          throw _NativeSftpStatusException(label: '$label read', status: -1, path: remotePath);
        }
        if (read == 0) {
          break;
        }
        currentOffset += read;
        yield Uint8List.fromList(buffer.asTypedList(read));
      }
    } catch (error) {
      invalidateLease = _isRetryable(error);
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

  Future<Uint8List?> _readRemoteFileBytesWithRetry(String remotePath, {int? length, String? label}) {
    final resolvedLabel = label ?? 'read $remotePath';
    return _readRemoteFileBytes(remotePath, length: length, label: resolvedLabel);
  }

  Future<Uint8List?> _readRemoteFileBytes(String remotePath, {int? length, required String label}) {
    return _withNativeSession(label, (lease, bindings) {
      final builder = BytesBuilder(copy: false);
      Pointer<Void>? file;
      Pointer<Uint8>? buffer;
      var offset = 0;
      final transferStopwatch = Stopwatch()..start();
      try {
        file = bindings.openRead(lease.session, remotePath);
        if (file == nullptr) {
          final status = bindings.lastError(lease.session);
          if (status == _sftpMissing) {
            throw _NativeBlobMissing(remotePath);
          }
          throw _NativeSftpStatusException(label: '$label openRead', status: status, path: remotePath);
        }
        final bufferLength = length == null ? _nativeTransferChunkSize : min(_nativeTransferChunkSize, length);
        buffer = calloc<Uint8>(bufferLength);
        while (true) {
          if (length != null && offset >= length) {
            break;
          }
          final toRead = length == null ? bufferLength : min(bufferLength, length - offset);
          if (toRead <= 0) {
            break;
          }
          final timeoutMs = _remainingCallTimeoutMs(transferStopwatch);
          if (timeoutMs <= 0) {
            throw _SftpOperationTimeout(label, transferStopwatch.elapsed);
          }
          final read = bindings.read(file, offset, buffer, toRead, timeoutMs);
          if (read < 0) {
            throw _NativeSftpStatusException(label: '$label read', status: -1, path: remotePath);
          }
          if (read == 0) {
            break;
          }
          builder.add(Uint8List.fromList(buffer.asTypedList(read)));
          offset += read;
        }
      } on _NativeBlobMissing {
        return null;
      } finally {
        if (file != null && file != nullptr) {
          bindings.closeFile(file);
        }
        if (buffer != null) {
          calloc.free(buffer);
        }
      }
      return builder.takeBytes();
    });
  }

  String _nativeSha256Hex(Uint8List bytes) {
    return _requiredBindings().sha256Hex(bytes);
  }

  Future<bool> _verifyExistingTargetForRenameConflict({required String hash, required String remotePath, required int expectedBytes}) async {
    final existingBytes = await _readRemoteFileBytes(remotePath, label: 'rename conflict read $remotePath');
    if (existingBytes == null) {
      final message = 'driver=sftp rename conflict check failed hash=$hash remotePath="$remotePath" existing=missing expectedBytes=$expectedBytes';
      _logConflictCheck('error', message);
      throw BackupWriteConflictMismatch(message);
    }
    final existingSha256 = _nativeSha256Hex(existingBytes);
    final matches = existingBytes.length == expectedBytes && existingSha256 == hash;
    final message =
        'driver=sftp rename conflict check hash=$hash remotePath="$remotePath" expectedBytes=$expectedBytes actualBytes=${existingBytes.length} actualSha256=$existingSha256 result=${matches ? 'match' : 'mismatch'}';
    _logConflictCheck(matches ? 'info' : 'error', message);
    if (!matches) {
      throw BackupWriteConflictMismatch(message);
    }
    return true;
  }

  void _logConflictCheck(String level, String message) {
    LogWriter.logAgentSync(level: level, message: message);
  }

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
    final data = await localFile.readAsBytes();
    await _ensureRemoteDir(_remoteDirName(remotePath));
    final tmpRemote = '$remotePath.inprogress.${DateTime.now().microsecondsSinceEpoch}';
    await _nativeWriteAll(tmpRemote, data, truncate: true, label: 'upload small file $remotePath');
    try {
      await _remoteRename(tmpRemote, remotePath, label: 'rename small file $remotePath');
    } catch (_) {
      if (await _remoteExists(remotePath)) {
        await _tryRemoveRemoteFile(tmpRemote);
        return;
      }
      await _tryRemoveRemoteFile(tmpRemote);
      rethrow;
    }
  }

  Future<void> _listRemoteFilesRecursively({required String remoteDir, required String relativePrefix, required List<String> out}) async {
    List<_NativeSftpEntry> entries;
    try {
      entries = await _remoteListDir(remoteDir);
    } on _NativeSftpStatusException catch (error) {
      if (error.isMissing) {
        return;
      }
      rethrow;
    }
    for (final entry in entries) {
      final name = entry.name;
      if (name == '.' || name == '..') {
        continue;
      }
      final remotePath = _remoteJoin(remoteDir, name);
      if (entry.isDirectory) {
        await _listRemoteFilesRecursively(remoteDir: remotePath, relativePrefix: '$relativePrefix/$name', out: out);
        continue;
      }
      out.add('$relativePrefix/$name');
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
    final parts = <String>[];
    for (final part in normalized.split('/')) {
      final trimmed = part.trim();
      if (trimmed.isEmpty || trimmed == '.') {
        continue;
      }
      if (trimmed == '..') {
        throw StateError('SFTP relative path must not contain "..".');
      }
      parts.add(trimmed);
    }
    return parts.join('/');
  }

  Future<void> _ensureRemoteDir(String remotePath, {bool blindMkdir = false, bool useBlobCachePool = false}) async {
    final normalized = _normalizeRemotePath(remotePath);
    if (normalized == '/' || normalized.isEmpty) {
      return;
    }
    final parts = normalized.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = normalized.startsWith('/') ? '/' : '';
    for (final part in parts) {
      current = current.isEmpty || current == '/' ? '$current$part' : '$current/$part';
      try {
        await _remoteMkdir(current, useBlobCachePool: useBlobCachePool);
      } on _NativeSftpStatusException {
        if (blindMkdir) {
          continue;
        }
        try {
          final attrs = await _remoteStat(current);
          if (attrs.isDirectory) {
            continue;
          }
        } catch (_) {}
        rethrow;
      }
    }
  }

  Future<void> _tryRemoveRemoteFile(String remotePath) async {
    try {
      await _remoteRemove(remotePath);
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
      if (value == null) {
        return;
      }
      final trimmed = value.trim();
      if (trimmed.isEmpty) {
        return;
      }
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

class _NativeSftpEntry {
  const _NativeSftpEntry({required this.name, required this.isDirectory, required this.size});

  final String name;
  final bool isDirectory;
  final int size;
}

class _NativeSftpFileAttrs {
  const _NativeSftpFileAttrs({required this.size, required this.isDirectory});

  final int size;
  final bool isDirectory;
}

class _NativeBlobMissing implements Exception {
  const _NativeBlobMissing(this.remotePath);

  final String remotePath;

  @override
  String toString() => 'Native SFTP blob missing: $remotePath';
}

class _SftpOperationTimeout implements Exception {
  const _SftpOperationTimeout(this.label, this.duration, {this.cause});

  final String label;
  final Duration duration;
  final Object? cause;

  @override
  String toString() => 'SFTP operation timed out label="$label" durationMs=${duration.inMilliseconds}${cause == null ? '' : ' cause=$cause'}';
}

class _NativeSftpStatusException implements Exception {
  const _NativeSftpStatusException({required this.label, required this.status, required this.path, this.detail});

  final String label;
  final int status;
  final String path;
  final String? detail;

  bool get isMissing => status == SftpBackupDriver._sftpMissing;

  @override
  String toString() {
    final suffix = detail == null || detail!.isEmpty ? '' : ' $detail';
    return 'SFTP native call failed label="$label" status=$status path="$path"$suffix';
  }
}

class _NativeSftpBindings {
  _NativeSftpBindings(DynamicLibrary lib)
    : _connect = lib.lookupFunction<_SftpConnectC, _SftpConnectDart>('vb_sftp_connect'),
      _disconnect = lib.lookupFunction<_SftpDisconnectC, _SftpDisconnectDart>('vb_sftp_disconnect'),
      _lastError = lib.lookupFunction<_SftpLastErrorC, _SftpLastErrorDart>('vb_sftp_last_error'),
      _openRead = lib.lookupFunction<_SftpOpenReadC, _SftpOpenReadDart>('vb_sftp_open_read'),
      _openWrite = lib.lookupFunction<_SftpOpenWriteC, _SftpOpenWriteDart>('vb_sftp_open_write'),
      _stat = lib.lookupFunction<_SftpStatC, _SftpStatDart>('vb_sftp_stat'),
      _mkdir = lib.lookupFunction<_SftpMkdirC, _SftpMkdirDart>('vb_sftp_mkdir'),
      _rename = lib.lookupFunction<_SftpRenameC, _SftpRenameDart>('vb_sftp_rename'),
      _remove = lib.lookupFunction<_SftpRemoveC, _SftpRemoveDart>('vb_sftp_remove'),
      _listdir = lib.lookupFunction<_SftpListDirC, _SftpListDirDart>('vb_sftp_listdir'),
      _freeDirList = lib.lookupFunction<_SftpFreeDirListC, _SftpFreeDirListDart>('vb_sftp_free_dir_list'),
      _read = lib.lookupFunction<_SftpReadC, _SftpReadDart>('vb_sftp_read'),
      _write = lib.lookupFunction<_SftpWriteC, _SftpWriteDart>('vb_sftp_write'),
      _closeFile = lib.lookupFunction<_SftpCloseFileC, _SftpCloseFileDart>('vb_sftp_close_file'),
      _sha256Hex = lib.lookupFunction<_Sha256HexC, _Sha256HexDart>('vb_sha256_hex');

  final _SftpConnectDart _connect;
  final _SftpDisconnectDart _disconnect;
  final _SftpLastErrorDart _lastError;
  final _SftpOpenReadDart _openRead;
  final _SftpOpenWriteDart _openWrite;
  final _SftpStatDart _stat;
  final _SftpMkdirDart _mkdir;
  final _SftpRenameDart _rename;
  final _SftpRemoveDart _remove;
  final _SftpListDirDart _listdir;
  final _SftpFreeDirListDart _freeDirList;
  final _SftpReadDart _read;
  final _SftpWriteDart _write;
  final _SftpCloseFileDart _closeFile;
  final _Sha256HexDart _sha256Hex;

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

  int lastError(Pointer<Void> session) {
    return _lastError(session);
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

  int stat(Pointer<Void> session, String path, Pointer<Int64> sizeOut, Pointer<Int32> isDirOut) {
    final pathPtr = path.toNativeUtf8();
    final result = _stat(session, pathPtr, sizeOut, isDirOut);
    calloc.free(pathPtr);
    return result;
  }

  int mkdir(Pointer<Void> session, String path) {
    final pathPtr = path.toNativeUtf8();
    final result = _mkdir(session, pathPtr);
    calloc.free(pathPtr);
    return result;
  }

  int rename(Pointer<Void> session, String fromPath, String toPath) {
    final fromPtr = fromPath.toNativeUtf8();
    final toPtr = toPath.toNativeUtf8();
    final result = _rename(session, fromPtr, toPtr);
    calloc.free(fromPtr);
    calloc.free(toPtr);
    return result;
  }

  int remove(Pointer<Void> session, String path) {
    final pathPtr = path.toNativeUtf8();
    final result = _remove(session, pathPtr);
    calloc.free(pathPtr);
    return result;
  }

  Pointer<_NativeSftpDirList> listdir(Pointer<Void> session, String path) {
    final pathPtr = path.toNativeUtf8();
    final result = _listdir(session, pathPtr);
    calloc.free(pathPtr);
    return result;
  }

  void freeDirList(Pointer<Void> list) {
    _freeDirList(list);
  }

  int read(Pointer<Void> file, int offset, Pointer<Uint8> buffer, int length, int timeoutMs) {
    return _read(file, offset, buffer, length, timeoutMs);
  }

  int write(Pointer<Void> file, Pointer<Uint8> buffer, int length, int timeoutMs) {
    return _write(file, buffer, length, timeoutMs);
  }

  void closeFile(Pointer<Void> file) {
    _closeFile(file);
  }

  String sha256Hex(Uint8List bytes) {
    Pointer<Uint8>? inputPtr;
    if (bytes.isNotEmpty) {
      inputPtr = calloc<Uint8>(bytes.length);
      inputPtr.asTypedList(bytes.length).setAll(0, bytes);
    }
    final outputPtr = calloc<Uint8>(65);
    try {
      final rc = _sha256Hex(inputPtr ?? nullptr.cast<Uint8>(), bytes.length, outputPtr, 65);
      if (rc != 0) {
        throw StateError('Native SHA256 failed.');
      }
      return outputPtr.cast<Utf8>().toDartString();
    } finally {
      if (inputPtr != null) {
        calloc.free(inputPtr);
      }
      calloc.free(outputPtr);
    }
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
  _NativeSftpLease(this._pool, this._session, this.metrics);

  final _NativeSftpPool _pool;
  final _NativeSftpSession _session;
  final _NativeSftpLeaseMetrics metrics;

  Pointer<Void> get session => _session.session;

  void release() {
    _pool.release(_session);
  }

  void invalidate() {
    _pool.invalidate(_session);
  }
}

class _NativeSftpPool {
  _NativeSftpPool({required int maxSessions}) : _maxSessions = maxSessions;

  static const Duration _maxSessionAge = Duration(minutes: 10);

  int _maxSessions;
  int get maxSessions => _maxSessions;
  final List<_NativeSftpSession> _idle = <_NativeSftpSession>[];
  final List<_NativeSftpSession> _all = <_NativeSftpSession>[];
  final List<_NativeSftpLeaseWaiter> _waiters = <_NativeSftpLeaseWaiter>[];
  bool _closed = false;
  var _connecting = 0;

  void setMaxSessions(int value) {
    if (value <= 0) {
      throw StateError('Native SFTP pool maxSessions must be greater than zero.');
    }
    _maxSessions = value;
    _tryStartWaiterConnects();
  }

  Future<_NativeSftpLease> lease(Future<_NativeSftpSession> Function() connect) async {
    final waitStartedAt = DateTime.now();
    if (_closed) {
      throw 'Native SFTP pool is closed.';
    }
    _pruneExpiredIdle();
    if (_idle.isNotEmpty) {
      final session = _idle.removeLast();
      final waitMs = DateTime.now().difference(waitStartedAt).inMilliseconds;
      return _NativeSftpLease(this, session, _NativeSftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: true, queued: false));
    }
    if (_all.length + _connecting < maxSessions) {
      _connecting += 1;
      final connectStartedAt = DateTime.now();
      try {
        final session = await connect();
        _all.add(session);
        final now = DateTime.now();
        return _NativeSftpLease(
          this,
          session,
          _NativeSftpLeaseMetrics(waitMs: now.difference(waitStartedAt).inMilliseconds, connectMs: now.difference(connectStartedAt).inMilliseconds, reused: false, queued: false),
        );
      } finally {
        _connecting -= 1;
      }
    }
    final waiter = _NativeSftpLeaseWaiter(Completer<_NativeSftpLease>(), waitStartedAt, connect);
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
        final waitMs = DateTime.now().difference(waiter.waitStartedAt).inMilliseconds;
        waiter.completer.complete(_NativeSftpLease(this, session, _NativeSftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: true, queued: true)));
        return;
      }
    }
    _idle.add(session);
  }

  void invalidate(_NativeSftpSession session) {
    _idle.remove(session);
    _all.remove(session);
    try {
      session.close();
    } catch (_) {}
    _tryStartWaiterConnects();
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
            final waitMs = DateTime.now().difference(waiter.waitStartedAt).inMilliseconds;
            if (!waiter.completer.isCompleted) {
              waiter.completer.complete(_NativeSftpLease(this, session, _NativeSftpLeaseMetrics(waitMs: waitMs, connectMs: 0, reused: false, queued: true)));
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

  void _tryStartWaiterConnects() {
    if (_closed) {
      return;
    }
    while (_waiters.isNotEmpty && _all.length + _connecting < _maxSessions) {
      _fulfillWaiterWithConnect(_waiters.removeAt(0));
    }
  }
}

class _NativeSftpLeaseWaiter {
  _NativeSftpLeaseWaiter(this.completer, this.waitStartedAt, this.connect);

  final Completer<_NativeSftpLease> completer;
  final DateTime waitStartedAt;
  final Future<_NativeSftpSession> Function() connect;
}

class _NativeSftpLeaseMetrics {
  const _NativeSftpLeaseMetrics({required this.waitMs, required this.connectMs, required this.reused, required this.queued});

  final int waitMs;
  final int connectMs;
  final bool reused;
  final bool queued;
}

final class _NativeSftpDirEntry extends Struct {
  external Pointer<Char> name;

  @Int32()
  external int isDir;

  @Int64()
  external int size;
}

final class _NativeSftpDirList extends Struct {
  @Int32()
  external int count;

  external Pointer<_NativeSftpDirEntry> entries;
}

typedef _SftpConnectC = Pointer<Void> Function(Pointer<Utf8> host, Int32 port, Pointer<Utf8> user, Pointer<Utf8> password);
typedef _SftpConnectDart = Pointer<Void> Function(Pointer<Utf8> host, int port, Pointer<Utf8> user, Pointer<Utf8> password);
typedef _SftpDisconnectC = Void Function(Pointer<Void> session);
typedef _SftpDisconnectDart = void Function(Pointer<Void> session);
typedef _SftpLastErrorC = Int32 Function(Pointer<Void> session);
typedef _SftpLastErrorDart = int Function(Pointer<Void> session);
typedef _SftpOpenReadC = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpOpenReadDart = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpOpenWriteC = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path, Int32 truncate);
typedef _SftpOpenWriteDart = Pointer<Void> Function(Pointer<Void> session, Pointer<Utf8> path, int truncate);
typedef _SftpStatC = Int32 Function(Pointer<Void> session, Pointer<Utf8> path, Pointer<Int64> sizeOut, Pointer<Int32> isDirOut);
typedef _SftpStatDart = int Function(Pointer<Void> session, Pointer<Utf8> path, Pointer<Int64> sizeOut, Pointer<Int32> isDirOut);
typedef _SftpMkdirC = Int32 Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpMkdirDart = int Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpRenameC = Int32 Function(Pointer<Void> session, Pointer<Utf8> fromPath, Pointer<Utf8> toPath);
typedef _SftpRenameDart = int Function(Pointer<Void> session, Pointer<Utf8> fromPath, Pointer<Utf8> toPath);
typedef _SftpRemoveC = Int32 Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpRemoveDart = int Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpListDirC = Pointer<_NativeSftpDirList> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpListDirDart = Pointer<_NativeSftpDirList> Function(Pointer<Void> session, Pointer<Utf8> path);
typedef _SftpFreeDirListC = Void Function(Pointer<Void> list);
typedef _SftpFreeDirListDart = void Function(Pointer<Void> list);
typedef _SftpReadC = Int32 Function(Pointer<Void> file, Int64 offset, Pointer<Uint8> buffer, Int32 length, Int32 timeoutMs);
typedef _SftpReadDart = int Function(Pointer<Void> file, int offset, Pointer<Uint8> buffer, int length, int timeoutMs);
typedef _SftpWriteC = Int32 Function(Pointer<Void> file, Pointer<Uint8> buffer, Int32 length, Int32 timeoutMs);
typedef _SftpWriteDart = int Function(Pointer<Void> file, Pointer<Uint8> buffer, int length, int timeoutMs);
typedef _SftpCloseFileC = Void Function(Pointer<Void> file);
typedef _SftpCloseFileDart = void Function(Pointer<Void> file);
typedef _Sha256HexC = Int32 Function(Pointer<Uint8> data, Int32 length, Pointer<Uint8> outHex, Int32 outLen);
typedef _Sha256HexDart = int Function(Pointer<Uint8> data, int length, Pointer<Uint8> outHex, int outLen);
