import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:ffi/ffi.dart';

import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/agent/logging_config.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';

class BackupAgentHost {
  BackupAgentHost();
  int _sftpRangeBytesSinceLog = 0;
  DateTime _sftpRangeLastLog = DateTime.now();
  int _sftpDownloadBytesSinceLog = 0;
  DateTime _sftpDownloadLastLog = DateTime.now();
  int _sftpStreamBytesSinceLog = 0;
  DateTime _sftpStreamLastLog = DateTime.now();
  int _sftpUploadBytesSinceLog = 0;
  DateTime _sftpUploadLastLog = DateTime.now();
  DateTime? _firstSftpUploadAt;
  DateTime? _lastSftpUploadAt;
  static const int _sftpRangeReadChunkSize = 16 * 1024 * 1024;

  final SSHAlgorithms _sshAlgorithms = const SSHAlgorithms(cipher: [SSHCipherType.aes128ctr, SSHCipherType.aes256ctr]);
  final Map<String, SSHSocket> _eventSockets = {};
  final Map<String, SSHClient> _eventClients = {};
  final Map<String, SSHSession> _eventSessions = {};
  final Map<String, StreamSubscription<String>> _eventStdoutSubs = {};
  final Map<String, StreamSubscription<String>> _eventStderrSubs = {};
  final Set<String> _eventStarting = {};
  final _NativeSftpBindings? _nativeSftp = _NativeSftpBindings.tryLoad();
  bool _nativeSftpLogged = false;
  final Map<String, Pointer<Void>> _nativeSftpSessions = {};
  final Map<String, _NativeSftpReadHandle> _nativeSftpReadHandles = {};

  bool get nativeSftpAvailable => _nativeSftp != null;

  BackupAgentDependencies buildDependencies() {
    return BackupAgentDependencies(
      runSshCommand: _runSshCommandForServer,
      loadVmDiskPaths: _loadVmDiskPaths,
      downloadRemoteFile: _downloadRemoteFileViaSftp,
      streamRemoteFile: _streamRemoteFileViaSftp,
      streamRemoteRange: _streamRemoteRangeViaSftp,
      createVmSnapshot: _createVmSnapshot,
      commitVmSnapshot: _commitVmSnapshot,
      cleanupActiveOverlays: _cleanupActiveOverlays,
      sanitizeFileName: _sanitizeFileName,
      uploadLocalFile: _uploadLocalFileViaSftp,
      ensureHashblocks: _ensureHashblocksOnServer,
      streamHashblocks: _streamHashblocksOutput,
      getRemoteFileSize: _getRemoteFileSize,
      beginLargeTransfer: _beginLargeTransferSession,
      endLargeTransfer: _endLargeTransferSession,
    );
  }

  void _logNativeSftpStatusOnce() {
    if (_nativeSftpLogged) {
      return;
    }
    _nativeSftpLogged = true;
    if (_nativeSftp != null) {
      LogWriter.logAgentSync(level: 'info', message: 'native sftp: loaded');
    }
  }

  Future<void> _beginLargeTransferSession(ServerConfig server) async {
    _logNativeSftpStatusOnce();
    if (_nativeSftp == null) {
      throw StateError('Native SFTP is required but not available.');
    }
    if (_nativeSftpSessions.containsKey(server.id)) {
      return;
    }
    final session = _nativeSftp.connect(server.sshHost.trim(), int.tryParse(server.sshPort.trim()) ?? 22, server.sshUser.trim(), server.sshPassword);
    if (session == nullptr) {
      throw StateError('Native SFTP connect failed for ${server.sshHost}.');
    }
    _nativeSftpSessions[server.id] = session;
  }

  Future<void> _endLargeTransferSession(ServerConfig server) async {
    _closeNativeReadHandlesForServer(server.id);
    final session = _nativeSftpSessions.remove(server.id);
    if (session == null || _nativeSftp == null) {
      return;
    }
    _nativeSftp.disconnect(session);
  }

  Future<void> _reconnectNativeSftpSession(ServerConfig server) async {
    _closeNativeReadHandlesForServer(server.id);
    final session = _nativeSftpSessions.remove(server.id);
    if (session != null && _nativeSftp != null) {
      _nativeSftp.disconnect(session);
    }
    await _beginLargeTransferSession(server);
  }

  String _nativeReadHandleKey(String serverId, String remotePath) {
    return '$serverId|$remotePath';
  }

  void _closeNativeReadHandlesForServer(String serverId) {
    if (_nativeSftp == null) {
      _nativeSftpReadHandles.clear();
      return;
    }
    final keysToClose = _nativeSftpReadHandles.keys.where((key) => key.startsWith('$serverId|')).toList();
    for (final key in keysToClose) {
      final entry = _nativeSftpReadHandles.remove(key);
      if (entry == null) {
        continue;
      }
      if (entry.file != nullptr) {
        _nativeSftp.closeFile(entry.file);
      }
    }
  }

  _NativeSftpReadLease _acquireNativeReadLease(String serverId, String remotePath, Pointer<Void> session) {
    final nativeSftp = _nativeSftp;
    if (nativeSftp == null) {
      throw StateError('Native SFTP bindings are unavailable.');
    }
    final key = _nativeReadHandleKey(serverId, remotePath);
    final cached = _nativeSftpReadHandles[key];
    if (cached != null && !cached.inUse) {
      cached.inUse = true;
      return _NativeSftpReadLease(file: cached.file, cacheKey: key);
    }
    final opened = nativeSftp.openRead(session, remotePath);
    if (opened == nullptr) {
      throw 'native sftp open failed for $remotePath';
    }
    if (cached == null) {
      _nativeSftpReadHandles[key] = _NativeSftpReadHandle(file: opened, inUse: true);
      return _NativeSftpReadLease(file: opened, cacheKey: key);
    }
    return _NativeSftpReadLease(file: opened);
  }

  void _releaseNativeReadLease(_NativeSftpReadLease? lease, {required bool keepOpen}) {
    if (lease == null || _nativeSftp == null) {
      return;
    }
    final key = lease.cacheKey;
    if (key == null) {
      if (lease.file != nullptr) {
        _nativeSftp.closeFile(lease.file);
      }
      return;
    }
    final entry = _nativeSftpReadHandles[key];
    if (entry == null) {
      return;
    }
    if (keepOpen) {
      entry.inUse = false;
      return;
    }
    _nativeSftpReadHandles.remove(key);
    if (entry.file != nullptr) {
      _nativeSftp.closeFile(entry.file);
    }
  }

  void _markSftpUpload(int bytes) {
    if (bytes <= 0) {
      return;
    }
    final now = DateTime.now();
    _firstSftpUploadAt ??= now;
    _lastSftpUploadAt = now;
  }

  void _logSftpUploadWindowIfAny() {
    if (_firstSftpUploadAt == null || _lastSftpUploadAt == null) {
      return;
    }
    final elapsedMs = _lastSftpUploadAt!.difference(_firstSftpUploadAt!).inMilliseconds;
    final elapsedSec = elapsedMs / 1000.0;
    LogWriter.logAgentSync(level: 'info', message: 'SFTP upload window: ${elapsedSec.toStringAsFixed(2)}s');
    _firstSftpUploadAt = null;
    _lastSftpUploadAt = null;
  }

  Future<SshCommandResult> runSshCommand(ServerConfig server, String command) async {
    return _runSshCommandForServer(server, command);
  }

  Future<List<VmEntry>> loadVmInventory(ServerConfig server) async {
    const d = r'$';
    const command = 'for vm in $d(virsh list --all --name); do state=$d(virsh domstate "${d}vm" 2>/dev/null | tr -d "\\r"); echo "${d}vm|${d}state"; done';
    final result = await _runSshCommandForServer(server, command);
    final lines = result.stdout.split('\n').map((line) => line.trim()).where((line) => line.isNotEmpty && line.contains('|'));
    final entries = <VmEntry>[];
    for (final line in lines) {
      final parts = line.split('|');
      if (parts.length < 2) {
        continue;
      }
      final name = parts[0].trim();
      final state = parts[1].trim().toLowerCase();
      final powerState = state.contains('running') ? VmPowerState.running : VmPowerState.stopped;
      entries.add(VmEntry(id: name, name: name, powerState: powerState));
    }
    return entries;
  }

  Future<Map<String, bool>> loadOverlayStatusForVms(ServerConfig server, List<VmEntry> vms) async {
    final statusByName = <String, bool>{};
    for (final vm in vms) {
      statusByName[vm.name] = await _hasActiveOverlay(server, vm);
    }
    return statusByName;
  }

  Future<void> cleanupVmOverlays(ServerConfig server, VmEntry vm) async {
    final activeDisks = await _loadVmDiskPaths(server, vm);
    final inactiveDisks = await _loadVmDiskPaths(server, vm, inactive: true);
    if (activeDisks.isEmpty || inactiveDisks.isEmpty) {
      throw 'No disk files found for ${vm.name}.';
    }
    final domstateResult = await _runSshCommandForServer(server, 'virsh domstate "${vm.name}"');
    final state = domstateResult.stdout.trim().toLowerCase();
    LogWriter.logAgentSync(level: 'info', message: 'Cleanup overlays for ${vm.name}: domstate=${state.isEmpty ? 'unknown' : state}');
    if (state == 'running') {
      await _pivotAllDisks(server, vm, activeDisks);
      return;
    }
    await _cleanupStoppedVmOverlays(server, vm, inactiveDisks);
  }

  Future<void> uploadLocalFile(ServerConfig server, String localPath, String remotePath, {void Function(int bytes)? onBytes}) async {
    await _uploadLocalFileViaSftp(server, localPath, remotePath, onBytes: onBytes);
  }

  Future<void> uploadRemoteStream(ServerConfig server, String remotePath, Stream<List<int>> stream, {void Function(int bytes)? onBytes}) async {
    await _uploadRemoteStreamViaSftp(server, remotePath, stream, onBytes: onBytes);
  }

  Future<void> beginLargeTransferSession(ServerConfig server) async {
    await _beginLargeTransferSession(server);
  }

  Future<void> endLargeTransferSession(ServerConfig server) async {
    await _endLargeTransferSession(server);
  }

  Future<void> startVmEventListener(ServerConfig server, {required void Function(String line) onEvent, void Function()? onStopped, void Function(Object error, StackTrace stackTrace)? onError}) async {
    if (_eventStarting.contains(server.id)) {
      return;
    }
    if (_eventSessions.containsKey(server.id)) {
      return;
    }
    _eventStarting.add(server.id);
    try {
      await stopVmEventListener(server.id);
      final host = server.sshHost.trim();
      final port = int.tryParse(server.sshPort.trim()) ?? 22;
      final user = server.sshUser.trim();
      final password = server.sshPassword;
      const command = 'virsh event --all --loop';
      LogWriter.logAgentSync(level: 'info', message: 'SSH event listener [$user@$host:$port]: $command');
      final socket = await SSHSocket.connect(host, port);
      final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
      final session = await client.execute(command);
      _eventSockets[server.id] = socket;
      _eventClients[server.id] = client;
      _eventSessions[server.id] = session;
      _eventStdoutSubs[server.id] = session.stdout
          .cast<List<int>>()
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(
            (line) {
              final trimmed = line.trimRight();
              if (trimmed.isEmpty) {
                return;
              }
              LogWriter.logAgentSync(level: 'info', message: 'virsh event: $trimmed');
              onEvent(trimmed);
            },
            onError: (Object error, StackTrace stackTrace) {
              onError?.call(error, stackTrace);
            },
          );
      _eventStderrSubs[server.id] = session.stderr
          .cast<List<int>>()
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(
            (line) {
              final trimmed = line.trimRight();
              if (trimmed.isEmpty) {
                return;
              }
              LogWriter.logAgentSync(level: 'info', message: 'virsh event stderr: $trimmed');
            },
            onError: (Object error, StackTrace stackTrace) {
              onError?.call(error, stackTrace);
            },
          );
      session.done
          .then((_) {
            LogWriter.logAgentSync(level: 'info', message: 'VM event listener stopped.');
            stopVmEventListener(server.id);
            onStopped?.call();
          })
          .catchError((Object error, StackTrace stackTrace) {
            LogWriter.logAgentSync(level: 'info', message: 'VM event listener failed. $error');
            LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
            stopVmEventListener(server.id);
            onError?.call(error, stackTrace);
          });
    } catch (error, stackTrace) {
      LogWriter.logAgentSync(level: 'info', message: 'VM event listener start failed. $error');
      LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
      await stopVmEventListener(server.id);
      onError?.call(error, stackTrace);
    } finally {
      _eventStarting.remove(server.id);
    }
  }

  Future<void> stopVmEventListener(String serverId) async {
    await _eventStdoutSubs.remove(serverId)?.cancel();
    await _eventStderrSubs.remove(serverId)?.cancel();
    _eventSessions.remove(serverId)?.close();
    _eventClients.remove(serverId)?.close();
    _eventSockets.remove(serverId)?.close();
    _eventStarting.remove(serverId);
  }

  Future<SshCommandResult> _runSshCommandForServer(ServerConfig server, String command) async {
    final host = server.sshHost.trim();
    final port = int.tryParse(server.sshPort.trim()) ?? 22;
    final user = server.sshUser.trim();
    final password = server.sshPassword;
    LogWriter.logAgentSync(level: 'info', message: 'SSH exec [$user@$host:$port]: $command');
    final socket = await SSHSocket.connect(host, port);
    final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
    try {
      final session = await client.execute(command);
      final stdoutText = await utf8.decodeStream(session.stdout);
      final stderrText = await utf8.decodeStream(session.stderr);
      final exitCode = session.exitCode;
      await session.done;
      LogWriter.logAgentSync(level: 'trace', message: 'SSH exit code: ${exitCode ?? 0}');
      final trimmedStdout = stdoutText.trim();
      final trimmedStderr = stderrText.trim();
      if (trimmedStdout.isNotEmpty) {
        LogWriter.logAgentSync(level: 'trace', message: 'SSH stdout: $trimmedStdout');
      }
      if (trimmedStderr.isNotEmpty) {
        LogWriter.logAgentSync(level: 'info', message: 'SSH stderr: $trimmedStderr');
      }
      return SshCommandResult(stdout: stdoutText, stderr: stderrText, exitCode: exitCode);
    } finally {
      client.close();
      socket.close();
    }
  }

  Future<void> _streamHashblocksOutput(
    ServerConfig server,
    String remoteHashblocksPath,
    String remotePath,
    int blockSize, {
    required Future<void> Function(String line) onLine,
    void Function(HashblocksController controller)? onController,
  }) async {
    final host = server.sshHost.trim();
    final port = int.tryParse(server.sshPort.trim()) ?? 22;
    final user = server.sshUser.trim();
    final password = server.sshPassword;
    final command = '"$remoteHashblocksPath" --read "$remotePath" --block-size $blockSize';
    LogWriter.logAgentSync(level: 'info', message: 'SSH exec [$user@$host:$port]: $command');
    final socket = await SSHSocket.connect(host, port);
    final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
    var brokenPipe = false;
    try {
      final session = await client.execute(command);
      final controller = HashblocksController((cmd) {
        final bytes = utf8.encode('$cmd\n');
        try {
          session.stdin.add(Uint8List.fromList(bytes));
        } catch (error) {
          final message = error.toString();
          if (message.toLowerCase().contains('broken pipe')) {
            brokenPipe = true;
            LogWriter.logAgentSync(level: 'info', message: 'hashblocks control broken pipe, stopping session');
            session.close();
            return;
          }
          rethrow;
        }
      });
      onController?.call(controller);
      final stdoutLines = session.stdout.cast<List<int>>().transform(utf8.decoder).transform(const LineSplitter());
      final stderrFuture = utf8.decodeStream(session.stderr);

      final queue = <String>[];
      Completer<void>? wakeConsumer;
      var doneReading = false;

      Future<void> waitForQueue() async {
        if (queue.isEmpty) {
          wakeConsumer ??= Completer<void>();
          await wakeConsumer!.future;
        }
      }

      final consumer = () async {
        while (!doneReading || queue.isNotEmpty) {
          await waitForQueue();
          while (queue.isNotEmpty) {
            final line = queue.removeAt(0);
            await onLine(line);
          }
        }
      }();

      await for (final line in stdoutLines) {
        final trimmed = line.trimRight();
        if (trimmed.isEmpty) {
          continue;
        }
        queue.add(trimmed);
        if (wakeConsumer != null && !wakeConsumer!.isCompleted) {
          wakeConsumer!.complete();
          wakeConsumer = null;
        }
      }
      doneReading = true;
      if (wakeConsumer != null && !wakeConsumer!.isCompleted) {
        wakeConsumer!.complete();
      }
      await consumer;
      await session.done;
      final exitCode = session.exitCode;
      final stderrText = await stderrFuture;
      if (!brokenPipe && stderrText.toLowerCase().contains('broken pipe')) {
        brokenPipe = true;
      }
      if (brokenPipe) {
        throw 'hashblocks broken pipe';
      }
      if ((exitCode ?? 0) != 0) {
        final trimmed = stderrText.trim();
        throw trimmed.isEmpty ? 'hashblocks failed with exit code ${exitCode ?? 0}' : trimmed;
      }
    } finally {
      client.close();
      socket.close();
    }
  }

  Future<int> _getRemoteFileSize(ServerConfig server, String remotePath) async {
    final result = await _runSshCommandForServer(server, 'stat -c %s "$remotePath"');
    if ((result.exitCode ?? 0) != 0) {
      final stderrText = result.stderr.trim();
      throw stderrText.isEmpty ? 'stat failed for $remotePath' : stderrText;
    }
    final parsed = int.tryParse(result.stdout.trim());
    if (parsed != null && parsed >= 0) {
      return parsed;
    }
    throw 'stat size missing for $remotePath';
  }

  Future<void> _streamRemoteRangeViaSftp(
    ServerConfig server,
    String remotePath,
    int offset,
    int length, {
    required Future<void> Function(List<int> chunk) onChunk,
    void Function(int bytes)? onBytes,
  }) async {
    if (length <= 0) {
      return;
    }
    final nativeSession = _nativeSftpSessions[server.id];
    if (nativeSession == null || _nativeSftp == null) {
      throw StateError('Native SFTP range session is required but unavailable for ${server.sshHost}.');
    }
    var remaining = length;
    var currentOffset = offset;
    const chunkSize = _sftpRangeReadChunkSize;
    final buffer = calloc<Uint8>(chunkSize);
    _NativeSftpReadLease? lease;
    var completed = false;
    try {
      lease = _acquireNativeReadLease(server.id, remotePath, nativeSession);
      while (remaining > 0) {
        final toRead = remaining > chunkSize ? chunkSize : remaining;
        final read = _nativeSftp.read(lease.file, currentOffset, buffer, toRead);
        if (read <= 0) {
          break;
        }
        final data = Uint8List.fromList(buffer.asTypedList(read));
        currentOffset += read;
        remaining -= read;
        _sftpRangeBytesSinceLog += read;
        final now = DateTime.now();
        final elapsedMs = now.difference(_sftpRangeLastLog).inMilliseconds;
        if (elapsedMs >= agentLogInterval.inMilliseconds) {
          final mbPerSec = (_sftpRangeBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
          LogWriter.logAgentSync(level: 'info', message: 'SFTP range speed: ${mbPerSec.toStringAsFixed(1)}MB/s');
          _sftpRangeBytesSinceLog = 0;
          _sftpRangeLastLog = now;
        }
        onBytes?.call(read);
        await onChunk(data);
      }
      if (remaining == 0) {
        completed = true;
        return;
      }
      LogWriter.logAgentSync(level: 'info', message: 'SFTP short read for $remotePath offset=$currentOffset expected=$remaining got=0; retrying once');
      _releaseNativeReadLease(lease, keepOpen: false);
      lease = null;
      await _reconnectNativeSftpSession(server);
      final sessionAfterRetry = _nativeSftpSessions[server.id];
      if (sessionAfterRetry == null) {
        throw 'native sftp reconnect failed for ${server.sshHost}';
      }
      lease = _acquireNativeReadLease(server.id, remotePath, sessionAfterRetry);
      while (remaining > 0) {
        final toRead = remaining > chunkSize ? chunkSize : remaining;
        final read = _nativeSftp.read(lease.file, currentOffset, buffer, toRead);
        if (read <= 0) {
          break;
        }
        final data = Uint8List.fromList(buffer.asTypedList(read));
        currentOffset += read;
        remaining -= read;
        _sftpRangeBytesSinceLog += read;
        final now = DateTime.now();
        final elapsedMs = now.difference(_sftpRangeLastLog).inMilliseconds;
        if (elapsedMs >= agentLogInterval.inMilliseconds) {
          final mbPerSec = (_sftpRangeBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
          LogWriter.logAgentSync(level: 'info', message: 'SFTP range speed: ${mbPerSec.toStringAsFixed(1)}MB/s');
          _sftpRangeBytesSinceLog = 0;
          _sftpRangeLastLog = now;
        }
        onBytes?.call(read);
        await onChunk(data);
      }
      if (remaining == 0) {
        completed = true;
        return;
      }
      throw 'SFTP short read for $remotePath offset=$currentOffset expected=$remaining got=0';
    } finally {
      _releaseNativeReadLease(lease, keepOpen: completed);
      calloc.free(buffer);
    }
  }

  Future<String?> _ensureHashblocksOnServer(ServerConfig server) async {
    final localPath = _findLocalHashblocks();
    if (localPath == null) {
      LogWriter.logAgentSync(level: 'info', message: 'hashblocks not found next to agent; falling back to Dart dedup.');
      return null;
    }
    const remotePath = '/var/tmp/hashblocks';
    try {
      await _runSshCommandForServer(server, 'rm -f "$remotePath"');
    } catch (_) {}
    await _uploadLocalFileViaSftp(server, localPath, remotePath);
    await _runSshCommandForServer(server, 'chmod +x "$remotePath"');
    LogWriter.logAgentSync(level: 'info', message: 'hashblocks uploaded to $remotePath');
    return remotePath;
  }

  String? _findLocalHashblocks() {
    try {
      final exeDir = File(Platform.resolvedExecutable).parent;
      final candidate = File('${exeDir.path}${Platform.pathSeparator}hashblocks');
      if (candidate.existsSync()) {
        return candidate.path;
      }
    } catch (_) {}
    final projectCandidate = File('hashblocks${Platform.pathSeparator}hashblocks');
    if (projectCandidate.existsSync()) {
      return projectCandidate.path;
    }
    final cwdCandidate = File('hashblocks');
    if (cwdCandidate.existsSync()) {
      return cwdCandidate.path;
    }
    return null;
  }

  Future<List<MapEntry<String, String>>> _loadVmDiskPaths(ServerConfig server, VmEntry vm, {bool inactive = false}) async {
    final inactiveFlag = inactive ? ' --inactive' : '';
    final command = 'virsh domblklist --details$inactiveFlag "${vm.name}"';
    final result = await _runSshCommandForServer(server, command);
    if ((result.exitCode ?? 0) != 0) {
      throw result.stderr.isEmpty ? 'domblklist failed' : result.stderr;
    }
    final disks = <MapEntry<String, String>>[];
    final lines = result.stdout.split('\n');
    for (final rawLine in lines) {
      final line = rawLine.trim();
      if (line.isEmpty) {
        continue;
      }
      if (line.startsWith('Type') || line.startsWith('---')) {
        continue;
      }
      final columns = line.split(RegExp(r'\s+'));
      if (columns.length < 4) {
        continue;
      }
      final deviceType = columns[1];
      if (deviceType != 'disk') {
        continue;
      }
      final target = columns[2];
      final source = columns.sublist(3).join(' ');
      if (source.isEmpty) {
        continue;
      }
      disks.add(MapEntry(target, source));
    }
    return disks;
  }

  Future<void> _createVmSnapshot(ServerConfig server, VmEntry vm, String snapshotName) async {
    final command = 'virsh snapshot-create-as --domain "${vm.name}" --name "$snapshotName" --disk-only --atomic --no-metadata';
    await _runSshCommandForServer(server, command);
  }

  Future<void> _cleanupActiveOverlays(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> activeDisks, List<MapEntry<String, String>> inactiveDisks) async {
    final inactiveByTarget = {for (final entry in inactiveDisks) entry.key: entry.value};
    for (final entry in activeDisks) {
      final target = entry.key;
      final activeSource = entry.value;
      final inactiveSource = inactiveByTarget[target];
      if (inactiveSource == null) {
        continue;
      }
      if (activeSource != inactiveSource) {
        final activeIsVirtbackup = _sourcePathLooksLikeOverlay(activeSource);
        final inactiveIsVirtbackup = _sourcePathLooksLikeOverlay(inactiveSource);
        if (!activeIsVirtbackup && !inactiveIsVirtbackup) {
          LogWriter.logAgentSync(level: 'info', message: 'Skipping cleanup for ${vm.name}: non-virtbackup snapshot detected on $target ($activeSource).');
          continue;
        }
        final command = await _blockCommitCommand(server, vm, target, verbose: false, top: activeIsVirtbackup ? activeSource : null, base: activeIsVirtbackup ? inactiveSource : null);
        await _runSshCommandForServer(server, command);
        continue;
      }
      if (_sourcePathLooksLikeOverlay(activeSource)) {
        final command = await _blockCommitCommand(server, vm, target, verbose: false, top: activeSource, base: inactiveSource);
        await _runSshCommandForServer(server, command);
      }
    }
  }

  Future<void> _pivotAllDisks(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> disks) async {
    for (final entry in disks) {
      final target = entry.key;
      final command = await _blockCommitCommand(server, vm, target, verbose: true);
      await _runSshCommandForServer(server, command);
    }
  }

  Future<bool> _hasActiveOverlay(ServerConfig server, VmEntry vm) async {
    try {
      final activeDisks = await _loadVmDiskPaths(server, vm);
      final inactiveDisks = await _loadVmDiskPaths(server, vm, inactive: true);
      if (activeDisks.isEmpty || inactiveDisks.isEmpty) {
        return false;
      }
      final inactiveByTarget = {for (final entry in inactiveDisks) entry.key: entry.value};
      for (final entry in activeDisks) {
        final target = entry.key;
        final activeSource = entry.value;
        final inactiveSource = inactiveByTarget[target];
        if (inactiveSource == null) {
          continue;
        }
        if (activeSource != inactiveSource) {
          return true;
        }
        if (_sourcePathLooksLikeOverlay(activeSource)) {
          return true;
        }
      }
      return false;
    } catch (error, stackTrace) {
      LogWriter.logAgentSync(level: 'info', message: 'Overlay check failed for ${vm.name}. $error');
      LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
      return false;
    }
  }

  bool _sourcePathLooksLikeOverlay(String sourcePath) {
    final lower = sourcePath.toLowerCase();
    return lower.contains('.virtbackup-');
  }

  Future<void> _commitVmSnapshot(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> disks) async {
    final stateResult = await _runSshCommandForServer(server, 'virsh domstate "${vm.name}"');
    final state = stateResult.stdout.trim().toLowerCase();
    if (state != 'running') {
      LogWriter.logAgentSync(level: 'info', message: 'Snapshot commit for ${vm.name}: domstate=$state, using qemu-img commit path.');
      await _cleanupStoppedVmOverlays(server, vm, disks);
      return;
    }
    final activeDisks = await _loadVmDiskPaths(server, vm);
    final inactiveDisks = await _loadVmDiskPaths(server, vm, inactive: true);
    final inactiveByTarget = {for (final entry in inactiveDisks) entry.key: entry.value};
    for (final entry in disks) {
      final target = entry.key;
      final activeSource = activeDisks.firstWhere((disk) => disk.key == target, orElse: () => const MapEntry('', '')).value;
      final inactiveSource = inactiveByTarget[target] ?? '';
      final activeIsVirtbackup = _sourcePathLooksLikeOverlay(activeSource);
      if (activeIsVirtbackup && inactiveSource.isNotEmpty) {
        final backingSource = await _findBackingFile(server, activeSource) ?? '';
        final baseSource = backingSource.isNotEmpty ? backingSource : inactiveSource;
        if (baseSource == activeSource) {
          throw 'Refusing to blockcommit: base equals top for ${vm.name} target $target ($baseSource).';
        }
        final command = await _blockCommitCommand(server, vm, target, verbose: false, top: activeSource, base: baseSource);
        await _runSshCommandForServer(server, command);
        continue;
      }
      if (activeSource.isNotEmpty && !activeIsVirtbackup) {
        LogWriter.logAgentSync(level: 'info', message: 'Skipping commit for ${vm.name}: active disk for $target is not virtbackup ($activeSource).');
        continue;
      }
      final command = await _blockCommitCommand(server, vm, target, verbose: false);
      await _runSshCommandForServer(server, command);
    }
  }

  Future<void> _cleanupStoppedVmOverlays(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> disks) async {
    var xml = (await _runSshCommandForServer(server, 'virsh dumpxml "${vm.name}"')).stdout;
    var updatedXml = xml;
    final overlaysToDelete = <String>[];
    for (final entry in disks) {
      final overlayPath = (await _resolveOverlayPath(server, vm, entry.key, entry.value)).trim();
      if (overlayPath.isEmpty) {
        continue;
      }
      final backingPath = await _findBackingFile(server, overlayPath);
      if (backingPath == null || backingPath.isEmpty) {
        LogWriter.logAgentSync(level: 'info', message: 'Cleanup ${vm.name}: no backing file for $overlayPath');
        continue;
      }
      LogWriter.logAgentSync(level: 'info', message: 'Cleanup ${vm.name}: commit $overlayPath -> $backingPath');
      await _runSshCommandForServer(server, 'qemu-img commit "$overlayPath"');
      updatedXml = updatedXml.replaceAll(overlayPath, backingPath);
      overlaysToDelete.add(overlayPath);
    }
    if (updatedXml != xml) {
      final tempDir = Directory.systemTemp.createTempSync('virtbackup-cleanup-');
      final localXml = File('${tempDir.path}${Platform.pathSeparator}${_sanitizeFileName(vm.name)}.xml');
      await localXml.writeAsString(updatedXml);
      final remoteXml = '/var/tmp/virtbackup/cleanup-${_sanitizeFileName(vm.name)}.xml';
      await _runSshCommandForServer(server, 'mkdir -p "/var/tmp/virtbackup"');
      await _uploadLocalFileViaSftp(server, localXml.path, remoteXml);
      LogWriter.logAgentSync(level: 'info', message: 'Cleanup ${vm.name}: redefining domain using $remoteXml');
      await _runSshCommandForServer(server, 'virsh define "$remoteXml"');
      try {
        await localXml.delete();
      } catch (_) {}
      try {
        tempDir.deleteSync(recursive: true);
      } catch (_) {}
    }
    for (final overlayPath in overlaysToDelete) {
      LogWriter.logAgentSync(level: 'info', message: 'Cleanup ${vm.name}: removing overlay $overlayPath');
      await _runSshCommandForServer(server, 'rm -f "$overlayPath"');
    }
  }

  Future<String?> _findBackingFile(ServerConfig server, String overlayPath) async {
    final result = await _runSshCommandForServer(server, 'qemu-img info --backing-chain --force-share "$overlayPath"');
    final lines = result.stdout.split('\n');
    for (final line in lines) {
      final trimmed = line.trim();
      if (!trimmed.startsWith('backing file:')) {
        continue;
      }
      final value = trimmed.substring('backing file:'.length).trim();
      if (value.isEmpty || value == '(null)') {
        return null;
      }
      return value;
    }
    return null;
  }

  Future<String> _resolveOverlayPath(ServerConfig server, VmEntry vm, String target, String sourcePath) async {
    final trimmed = sourcePath.trim();
    if (trimmed.isEmpty) {
      return trimmed;
    }
    if (_sourcePathLooksLikeOverlay(trimmed)) {
      return trimmed;
    }
    try {
      final activeDisks = await _loadVmDiskPaths(server, vm);
      final inactiveDisks = await _loadVmDiskPaths(server, vm, inactive: true);
      final candidates = <String>[...activeDisks.where((entry) => entry.key == target).map((entry) => entry.value), ...inactiveDisks.where((entry) => entry.key == target).map((entry) => entry.value)];
      for (final candidate in candidates) {
        final candidatePath = candidate.trim();
        if (candidatePath.isEmpty) {
          continue;
        }
        if (_sourcePathLooksLikeOverlay(candidatePath)) {
          LogWriter.logAgentSync(level: 'info', message: 'Cleanup ${vm.name}: resolved overlay for $target -> $candidatePath');
          return candidatePath;
        }
      }
    } catch (error, stackTrace) {
      LogWriter.logAgentSync(level: 'info', message: 'Cleanup overlay resolution failed for ${vm.name}. $error');
      LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
    }
    return trimmed;
  }

  Future<String> _blockCommitCommand(ServerConfig server, VmEntry vm, String target, {required bool verbose, String? top, String? base}) async {
    final commandResult = await _runSshCommandForServer(server, 'virsh domstate "${vm.name}"');
    final state = commandResult.stdout.trim().toLowerCase();
    final args = <String>[];
    if (state == 'running') {
      args.add('--active');
    }
    args.add('--pivot');
    if (top != null && top.isNotEmpty) {
      args.add('--top "$top"');
    }
    if (base != null && base.isNotEmpty) {
      args.add('--base "$base"');
    }
    if (verbose) {
      args.add('--verbose');
    }
    final flags = args.isEmpty ? '' : ' ${args.join(' ')}';
    final topInfo = top == null || top.isEmpty ? '' : ' top=$top';
    final baseInfo = base == null || base.isEmpty ? '' : ' base=$base';
    LogWriter.logAgentSync(level: 'info', message: 'Blockcommit domstate=${state.isEmpty ? 'unknown' : state} target=$target flags=${args.join(' ')}$topInfo$baseInfo');
    return 'virsh blockcommit "${vm.name}" "$target"$flags';
  }

  Future<void> _downloadRemoteFileViaSftp(ServerConfig server, String remotePath, File file, {void Function(int bytes)? onBytes}) async {
    final host = server.sshHost.trim();
    final port = int.tryParse(server.sshPort.trim()) ?? 22;
    final user = server.sshUser.trim();
    final password = server.sshPassword;
    LogWriter.logAgentSync(level: 'info', message: 'SFTP download [$user@$host:$port]: $remotePath');
    final socket = await SSHSocket.connect(host, port);
    final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
    RandomAccessFile? raf;
    Timer? stallTimer;
    Timer? speedTimer;
    var totalDownloaded = 0;
    SftpClient? sftp;
    SftpFile? remoteFile;
    try {
      sftp = await client.sftp();
      final stat = await sftp.stat(remotePath);
      final remoteSize = stat.size;
      if (remoteSize == null || remoteSize <= 0) {
        throw 'Remote file size is unknown for $remotePath';
      }
      LogWriter.logAgentSync(level: 'info', message: 'SFTP size: $remoteSize bytes');
      remoteFile = await sftp.open(remotePath);
      raf = await file.open(mode: FileMode.write);
      var bytesSinceFlush = 0;
      var bytesSinceWatchdog = 0;
      var lastWatchdogTick = DateTime.now();
      var lastDataReceivedAt = DateTime.now();
      stallTimer = Timer.periodic(const Duration(seconds: 15), (_) {
        final secondsWithoutData = DateTime.now().difference(lastDataReceivedAt).inSeconds;
        if (secondsWithoutData >= 15) {
          LogWriter.logAgentSync(level: 'info', message: 'Backup stream stalled: no data for ${secondsWithoutData}s');
        }
      });
      speedTimer = Timer.periodic(agentLogInterval, (_) {});
      const blockSize = 4 * 1024 * 1024;
      const streamTimeout = Duration(seconds: 8);
      var blockBuffer = Uint8List(blockSize);
      var blockOffset = 0;
      final remoteStream = remoteFile
          .read(length: remoteSize, offset: 0)
          .timeout(
            streamTimeout,
            onTimeout: (sink) {
              sink.addError('Backup stream timed out after ${streamTimeout.inSeconds}s');
            },
          );
      try {
        await for (final chunk in remoteStream) {
          lastDataReceivedAt = DateTime.now();
          onBytes?.call(chunk.length);
          _sftpDownloadBytesSinceLog += chunk.length;
          totalDownloaded += chunk.length;
          final now = DateTime.now();
          final elapsedMs = now.difference(_sftpDownloadLastLog).inMilliseconds;
          if (elapsedMs >= 5000) {
            final mbPerSec = (_sftpDownloadBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
            final totalMb = totalDownloaded / (1024 * 1024);
            LogWriter.logAgentSync(level: 'info', message: 'SFTP download speed: ${mbPerSec.toStringAsFixed(1)}MB/s total=${totalMb.toStringAsFixed(1)}MB');
            _sftpDownloadBytesSinceLog = 0;
            _sftpDownloadLastLog = now;
          }
          var chunkOffset = 0;
          while (chunkOffset < chunk.length) {
            final remainingInBlock = blockSize - blockOffset;
            final remainingInChunk = chunk.length - chunkOffset;
            final toCopy = remainingInChunk < remainingInBlock ? remainingInChunk : remainingInBlock;
            blockBuffer.setRange(blockOffset, blockOffset + toCopy, chunk, chunkOffset);
            blockOffset += toCopy;
            chunkOffset += toCopy;
            if (blockOffset == blockSize) {
              await raf.writeFrom(blockBuffer);
              blockBuffer = Uint8List(blockSize);
              blockOffset = 0;
            }
          }
          bytesSinceFlush += chunk.length;
          bytesSinceWatchdog += chunk.length;
          if (bytesSinceFlush >= 16 * 1024 * 1024) {
            bytesSinceFlush = 0;
            await raf.flush();
          }
          final nowWatchdog = DateTime.now();
          final elapsedSinceWatchdog = nowWatchdog.difference(lastWatchdogTick);
          if (elapsedSinceWatchdog.inSeconds >= 15) {
            final mb = bytesSinceWatchdog / (1024 * 1024);
            LogWriter.logAgentSync(level: 'info', message: 'Backup stream progress: ${mb.toStringAsFixed(1)} MB in ${elapsedSinceWatchdog.inSeconds}s');
            lastWatchdogTick = nowWatchdog;
            bytesSinceWatchdog = 0;
          }
        }
      } on StateError catch (error) {
        final message = error.message.toString();
        if (message.contains('Cannot add event after closing')) {
          LogWriter.logAgentSync(level: 'info', message: 'SFTP download stream closed early.');
        } else {
          rethrow;
        }
      }
      if (blockOffset > 0) {
        final tailBlock = Uint8List.sublistView(blockBuffer, 0, blockOffset);
        await raf.writeFrom(tailBlock);
      }
      await raf.flush();
      LogWriter.logAgentSync(level: 'info', message: 'SFTP download completed for $remotePath');
    } finally {
      speedTimer?.cancel();
      stallTimer?.cancel();
      try {
        await remoteFile?.close();
      } catch (_) {}
      try {
        sftp?.close();
      } catch (_) {}
      await raf?.close();
      try {
        client.close();
      } catch (_) {}
      try {
        socket.close();
      } catch (_) {}
    }
  }

  Future<void> _streamRemoteFileViaSftp(ServerConfig server, String remotePath, {required Future<void> Function(List<int> chunk) onChunk, void Function(int bytes)? onBytes}) async {
    final nativeSession = _nativeSftpSessions[server.id];
    if (nativeSession != null && _nativeSftp != null) {
      final file = _nativeSftp.openRead(nativeSession, remotePath);
      if (file == nullptr) {
        throw 'native sftp open failed for $remotePath';
      }
      const chunkSize = 4 * 1024 * 1024;
      final buffer = calloc<Uint8>(chunkSize);
      var total = 0;
      try {
        while (true) {
          final read = _nativeSftp.read(file, total, buffer, chunkSize);
          if (read <= 0) {
            break;
          }
          final data = Uint8List.fromList(buffer.asTypedList(read));
          total += read;
          _sftpStreamBytesSinceLog += read;
          final now = DateTime.now();
          final elapsedMs = now.difference(_sftpStreamLastLog).inMilliseconds;
          if (elapsedMs >= agentLogInterval.inMilliseconds) {
            final mbPerSec = (_sftpStreamBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
            final totalMb = total / (1024 * 1024);
            LogWriter.logAgentSync(level: 'info', message: 'SFTP stream speed: ${mbPerSec.toStringAsFixed(1)}MB/s total=${totalMb.toStringAsFixed(1)}MB');
            _sftpStreamBytesSinceLog = 0;
            _sftpStreamLastLog = now;
          }
          onBytes?.call(read);
          await onChunk(data);
        }
      } finally {
        _nativeSftp.closeFile(file);
        calloc.free(buffer);
      }
      return;
    }
    final host = server.sshHost.trim();
    final port = int.tryParse(server.sshPort.trim()) ?? 22;
    final user = server.sshUser.trim();
    final password = server.sshPassword;
    LogWriter.logAgentSync(level: 'info', message: 'SFTP stream [$user@$host:$port]: $remotePath');
    final socket = await SSHSocket.connect(host, port);
    final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
    Timer? stallTimer;
    Timer? speedTimer;
    var totalDownloaded = 0;
    SftpClient? sftp;
    SftpFile? remoteFile;
    try {
      sftp = await client.sftp();
      final stat = await sftp.stat(remotePath);
      final remoteSize = stat.size;
      if (remoteSize == null || remoteSize <= 0) {
        throw 'Remote file size is unknown for $remotePath';
      }
      LogWriter.logAgentSync(level: 'info', message: 'SFTP size: $remoteSize bytes');
      remoteFile = await sftp.open(remotePath);
      var bytesSinceWatchdog = 0;
      var lastWatchdogTick = DateTime.now();
      var lastDataReceivedAt = DateTime.now();
      stallTimer = Timer.periodic(const Duration(seconds: 15), (_) {
        final secondsWithoutData = DateTime.now().difference(lastDataReceivedAt).inSeconds;
        if (secondsWithoutData >= 15) {
          LogWriter.logAgentSync(level: 'info', message: 'Backup stream stalled: no data for ${secondsWithoutData}s');
        }
      });
      speedTimer = Timer.periodic(agentLogInterval, (_) {});
      const streamTimeout = Duration(seconds: 8);
      final remoteStream = remoteFile
          .read(length: remoteSize, offset: 0)
          .timeout(
            streamTimeout,
            onTimeout: (sink) {
              sink.addError('Backup stream timed out after ${streamTimeout.inSeconds}s');
            },
          );
      try {
        await for (final chunk in remoteStream) {
          lastDataReceivedAt = DateTime.now();
          onBytes?.call(chunk.length);
          await onChunk(chunk);
          _sftpStreamBytesSinceLog += chunk.length;
          totalDownloaded += chunk.length;
          final now = DateTime.now();
          final elapsedMs = now.difference(_sftpStreamLastLog).inMilliseconds;
          if (elapsedMs >= 5000) {
            final mbPerSec = (_sftpStreamBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
            final totalMb = totalDownloaded / (1024 * 1024);
            LogWriter.logAgentSync(level: 'info', message: 'SFTP stream speed: ${mbPerSec.toStringAsFixed(1)}MB/s total=${totalMb.toStringAsFixed(1)}MB');
            _sftpStreamBytesSinceLog = 0;
            _sftpStreamLastLog = now;
          }
          bytesSinceWatchdog += chunk.length;
          final nowWatchdog = DateTime.now();
          final elapsedSinceWatchdog = nowWatchdog.difference(lastWatchdogTick);
          if (elapsedSinceWatchdog.inSeconds >= 15) {
            final mb = bytesSinceWatchdog / (1024 * 1024);
            LogWriter.logAgentSync(level: 'info', message: 'Backup stream progress: ${mb.toStringAsFixed(1)} MB in ${elapsedSinceWatchdog.inSeconds}s');
            lastWatchdogTick = nowWatchdog;
            bytesSinceWatchdog = 0;
          }
        }
      } on StateError catch (error) {
        final message = error.message.toString();
        if (message.contains('Cannot add event after closing')) {
          LogWriter.logAgentSync(level: 'info', message: 'SFTP stream closed early.');
        } else {
          rethrow;
        }
      }
      LogWriter.logAgentSync(level: 'info', message: 'SFTP stream completed for $remotePath');
    } finally {
      speedTimer?.cancel();
      stallTimer?.cancel();
      try {
        await remoteFile?.close();
      } catch (_) {}
      try {
        sftp?.close();
      } catch (_) {}
      client.close();
      socket.destroy();
    }
  }

  Future<void> _uploadLocalFileViaSftp(ServerConfig server, String localPath, String remotePath, {void Function(int bytes)? onBytes}) async {
    final host = server.sshHost.trim();
    final port = int.tryParse(server.sshPort.trim()) ?? 22;
    final user = server.sshUser.trim();
    final password = server.sshPassword;
    final localFile = File(localPath);
    LogWriter.logAgentSync(level: 'info', message: 'SFTP upload [$user@$host:$port]: $localPath -> $remotePath');
    if (!await localFile.exists()) {
      throw 'Local file not found: $localPath';
    }
    final socket = await SSHSocket.connect(host, port);
    final client = SSHClient(socket, username: user, onPasswordRequest: () => password, algorithms: _sshAlgorithms);
    SftpClient? sftp;
    SftpFile? remoteFile;
    Timer? speedTimer;
    var totalUploaded = 0;
    try {
      sftp = await client.sftp();
      remoteFile = await sftp.open(remotePath, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
      speedTimer = Timer.periodic(agentLogInterval, (_) {});
      final stream = localFile.openRead().map((chunk) {
        _sftpUploadBytesSinceLog += chunk.length;
        totalUploaded += chunk.length;
        _markSftpUpload(chunk.length);
        final now = DateTime.now();
        final elapsedMs = now.difference(_sftpUploadLastLog).inMilliseconds;
        if (elapsedMs >= agentLogInterval.inMilliseconds) {
          final mbPerSec = (_sftpUploadBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
          final totalMb = totalUploaded / (1024 * 1024);
          LogWriter.logAgentSync(level: 'info', message: 'SFTP upload speed: ${mbPerSec.toStringAsFixed(1)}MB/s total=${totalMb.toStringAsFixed(1)}MB');
          _sftpUploadBytesSinceLog = 0;
          _sftpUploadLastLog = now;
        }
        onBytes?.call(chunk.length);
        return Uint8List.fromList(chunk);
      });
      await remoteFile.write(stream);
      await remoteFile.close();
      _logSftpUploadWindowIfAny();
    } finally {
      speedTimer?.cancel();
      try {
        await remoteFile?.close();
      } catch (_) {}
      try {
        sftp?.close();
      } catch (_) {}
      try {
        client.close();
      } catch (_) {}
      try {
        socket.close();
      } catch (_) {}
    }
  }

  Future<void> _uploadRemoteStreamViaSftp(ServerConfig server, String remotePath, Stream<List<int>> stream, {void Function(int bytes)? onBytes}) async {
    final nativeSession = _nativeSftpSessions[server.id];
    if (nativeSession == null || _nativeSftp == null) {
      throw StateError('Native SFTP upload session is required but unavailable for ${server.sshHost}.');
    }
    final file = _nativeSftp.openWrite(nativeSession, remotePath, true);
    if (file == nullptr) {
      throw 'native sftp open write failed for $remotePath';
    }
    const targetChunkSize = 16 * 1024 * 1024;
    var totalUploaded = 0;
    Pointer<Uint8>? nativeBuffer;
    var nativeCapacity = 0;
    final pending = Uint8List(targetChunkSize);
    var pendingLength = 0;

    void writeChunk(Uint8List source, int start, int length) {
      if (length <= 0) {
        return;
      }
      if (nativeCapacity < length) {
        if (nativeBuffer != null) {
          calloc.free(nativeBuffer!);
        }
        nativeBuffer = calloc<Uint8>(length);
        nativeCapacity = length;
      }
      nativeBuffer!.asTypedList(length).setAll(0, Uint8List.sublistView(source, start, start + length));
      final wrote = _nativeSftp.write(file, nativeBuffer!, length);
      if (wrote != length) {
        throw 'native sftp write failed for $remotePath';
      }
      _sftpUploadBytesSinceLog += wrote;
      totalUploaded += wrote;
      _markSftpUpload(wrote);
      final now = DateTime.now();
      final elapsedMs = now.difference(_sftpUploadLastLog).inMilliseconds;
      if (elapsedMs >= agentLogInterval.inMilliseconds) {
        final mbPerSec = (_sftpUploadBytesSinceLog / (elapsedMs / 1000)) / (1024 * 1024);
        final totalMb = totalUploaded / (1024 * 1024);
        LogWriter.logAgentSync(level: 'info', message: 'SFTP upload speed: ${mbPerSec.toStringAsFixed(1)}MB/s total=${totalMb.toStringAsFixed(1)}MB');
        _sftpUploadBytesSinceLog = 0;
        _sftpUploadLastLog = now;
      }
      onBytes?.call(wrote);
    }

    try {
      await for (final chunk in stream) {
        final current = chunk is Uint8List ? chunk : Uint8List.fromList(chunk);
        var offset = 0;
        if (pendingLength > 0) {
          final fill = (targetChunkSize - pendingLength) < current.length ? (targetChunkSize - pendingLength) : current.length;
          pending.setRange(pendingLength, pendingLength + fill, current, 0);
          pendingLength += fill;
          offset += fill;
          if (pendingLength == targetChunkSize) {
            writeChunk(pending, 0, targetChunkSize);
            pendingLength = 0;
          }
        }
        while (current.length - offset >= targetChunkSize) {
          writeChunk(current, offset, targetChunkSize);
          offset += targetChunkSize;
        }
        if (offset < current.length) {
          final remaining = current.length - offset;
          pending.setRange(0, remaining, current, offset);
          pendingLength = remaining;
        }
      }
      if (pendingLength > 0) {
        writeChunk(pending, 0, pendingLength);
      }
    } finally {
      if (nativeBuffer != null) {
        calloc.free(nativeBuffer!);
      }
      _nativeSftp.closeFile(file);
      _logSftpUploadWindowIfAny();
    }
  }

  String _sanitizeFileName(String name) {
    return name.trim().replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
  }
}

class _NativeSftpReadHandle {
  _NativeSftpReadHandle({required this.file, required this.inUse});

  final Pointer<Void> file;
  bool inUse;
}

class _NativeSftpReadLease {
  _NativeSftpReadLease({required this.file, this.cacheKey});

  final Pointer<Void> file;
  final String? cacheKey;
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
    final exeDir = File(Platform.resolvedExecutable).parent.path;
    candidates.add('$exeDir/native/linux/libvirtbackup_native.so');
    candidates.add('$exeDir/native/macos/libvirtbackup_native.dylib');

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

  Pointer<Void> openWrite(Pointer<Void> session, String path, bool truncate) {
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
