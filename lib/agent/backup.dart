import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/agent/logging_config.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/log_writer.dart';

part 'backup_models.dart';
part 'backup_types.dart';
part 'workers/blob_cache_worker.dart';
part 'workers/exists_worker.dart';
part 'workers/hashblocks_worker.dart';
part 'workers/sftp_worker.dart';
part 'workers/writer_worker.dart';

class BackupAgentDependencies {
  const BackupAgentDependencies({
    required this.runSshCommand,
    required this.loadVmDiskPaths,
    required this.downloadRemoteFile,
    required this.streamRemoteFile,
    required this.streamRemoteRange,
    required this.createVmSnapshot,
    required this.commitVmSnapshot,
    required this.cleanupActiveOverlays,
    required this.sanitizeFileName,
    required this.uploadLocalFile,
    required this.ensureHashblocks,
    required this.streamHashblocks,
    required this.getRemoteFileSize,
    this.beginLargeTransfer,
    this.endLargeTransfer,
  });

  final SshCommandRunner runSshCommand;
  final DiskPathLoader loadVmDiskPaths;
  final SftpDownloader downloadRemoteFile;
  final SftpStreamer streamRemoteFile;
  final RemoteRangeStreamer streamRemoteRange;
  final SnapshotCreator createVmSnapshot;
  final SnapshotCommitter commitVmSnapshot;
  final OverlayCleaner cleanupActiveOverlays;
  final FileNameSanitizer sanitizeFileName;
  final SftpUploader uploadLocalFile;
  final HashblocksEnsurer ensureHashblocks;
  final HashblocksStreamer streamHashblocks;
  final RemoteFileSizer getRemoteFileSize;
  final LargeTransferHook? beginLargeTransfer;
  final LargeTransferHook? endLargeTransfer;
}

class BackupAgent {
  BackupAgent({
    required BackupAgentDependencies dependencies,
    required BackupProgressListener onProgress,
    required int blockSizeBytes,
    LogInfo? onInfo,
    LogError? onError,
    int? writerBacklogBytesLimit,
    int? hashblocksLimitBufferMb,
    int? writerConcurrencyOverride,
  }) : _dependencies = dependencies,
       _onProgress = onProgress,
       _blockSize = blockSizeBytes,
       _onInfo = onInfo,
       _onError = onError,
       _writerBacklogBytesLimit = writerBacklogBytesLimit ?? _defaultWriterBacklogBytesLimit,
       _writerConcurrencyOverride = writerConcurrencyOverride;

  final BackupAgentDependencies _dependencies;
  final BackupProgressListener _onProgress;
  final LogInfo? _onInfo;
  final LogError? _onError;

  Timer? _speedTimer;
  Timer? _progressLogTimer;
  BackupAgentProgress _progress = BackupAgentProgress.idle;
  DateTime? _lastSpeedUpdate;
  static const Duration _speedWindow = Duration(seconds: 30);
  final List<_SpeedSample> _speedSamples = [];
  bool _isDisposed = false;
  bool _isRunning = false;
  bool _cancelRequested = false;
  HashblocksController? _activeHashblocksController;
  DateTime? _firstSftpReadAt;
  DateTime? _lastSftpReadAt;
  int _physicalBytesTransferred = 0;
  int _logicalBytesTransferred = 0;
  int _writerQueuedBytes = 0;
  int _writerInFlightBytes = 0;
  int _driverBufferedBytes = 0;
  DateTime? _backupStartAt;
  final int _blockSize;
  static const int _sftpPrefetchWindow = 2;
  static const int _defaultWriterBacklogBytesLimit = 4 * 1024 * 1024 * 1024;
  final Set<Future<void>> _inFlightWrites = {};
  final int _writerBacklogBytesLimit;
  final int? _writerConcurrencyOverride;
  _BlobDirectoryCache? _blobDirectoryCache;
  _BlobCacheWorker? _blobCacheWorker;
  Future<void>? _blobCacheWorkerFuture;

  void dispose() {
    _isDisposed = true;
    _speedTimer?.cancel();
    _speedTimer = null;
    _progressLogTimer?.cancel();
    _progressLogTimer = null;
  }

  void cancel() {
    _cancelRequested = true;
    _activeHashblocksController?.stop();
    _setProgress(_progress.copyWith(statusMessage: 'Canceling...'));
  }

  void _markSftpRead(int bytes) {
    if (bytes <= 0) {
      return;
    }
    final now = DateTime.now();
    _firstSftpReadAt ??= now;
    _lastSftpReadAt = now;
  }

  Future<BackupAgentResult> runVmBackup({required ServerConfig server, required VmEntry vm, required BackupDriver driver}) async {
    if (_isRunning) {
      return const BackupAgentResult(success: false, message: 'Backup already running.');
    }
    _isRunning = true;
    _ensureNotCanceled();
    _logInfo('Backup started for ${vm.name}.');
    _setProgress(
      BackupAgentProgress.idle.copyWith(
        isRunning: true,
        statusMessage: 'Preparing backup...',
        sanityBytesTransferred: 0,
        sanitySpeedBytesPerSec: 0,
        totalBytes: 0,
        physicalBytesTransferred: 0,
        physicalSpeedBytesPerSec: 0,
      ),
    );
    _speedSamples.clear();
    _lastSpeedUpdate = DateTime.now();
    _physicalBytesTransferred = 0;
    _logicalBytesTransferred = 0;
    _writerQueuedBytes = 0;
    _writerInFlightBytes = 0;
    _driverBufferedBytes = 0;
    _backupStartAt = DateTime.now();
    _startSpeedTimer();
    _startProgressLogTimer();
    _firstSftpReadAt = null;
    _lastSftpReadAt = null;

    var snapshotCreated = false;
    var disks = <MapEntry<String, String>>[];
    final backupPlans = <_DiskBackupPlan>[];
    Object? runError;
    try {
      await _dependencies.beginLargeTransfer?.call(server);
      if (server.connectionType != ConnectionType.ssh) {
        throw 'Backup via API is not configured.';
      }
      final vmFolderName = _dependencies.sanitizeFileName(vm.name);
      final serverFolderName = _dependencies.sanitizeFileName(server.id);
      await driver.ensureReady();
      final BlobDirectoryLister? blobLister = driver is BlobDirectoryLister ? driver as BlobDirectoryLister : null;
      _blobDirectoryCache = blobLister == null ? null : _BlobDirectoryCache(driver: blobLister, createShard: (hash) => driver.ensureBlobDir(hash));
      _startBlobCacheWorker();
      await driver.prepareBackup(serverFolderName, vmFolderName);
      final manifestsBase = driver.manifestsDir(serverFolderName, vmFolderName);
      final backupTimestamp = _dependencies.sanitizeFileName(DateTime.now().toIso8601String());

      final xmlResult = await _dependencies.runSshCommand(server, 'virsh dumpxml "${vm.name}"');
      if ((xmlResult.exitCode ?? 0) != 0) {
        throw xmlResult.stderr.isEmpty ? 'dumpxml failed' : xmlResult.stderr;
      }
      final xml = xmlResult.stdout.trim();
      if (xml.isEmpty) {
        throw 'dumpxml returned empty output';
      }
      final xmlWrite = driver.startXmlWrite(serverFolderName, vmFolderName, backupTimestamp);
      xmlWrite.sink.write(xml);
      await xmlWrite.sink.flush();
      await xmlWrite.sink.close();
      await xmlWrite.commit();

      final activeDisks = await _dependencies.loadVmDiskPaths(server, vm);
      final inactiveDisks = await _dependencies.loadVmDiskPaths(server, vm, inactive: true);
      if (activeDisks.isEmpty || inactiveDisks.isEmpty) {
        throw 'No disk files found for ${vm.name}.';
      }

      final stateResult = await _dependencies.runSshCommand(server, 'virsh domstate "${vm.name}"');
      final state = stateResult.stdout.trim().toLowerCase();

      if (state == 'running') {
        _logInfo('Checking overlays for ${vm.name}.');
        _setProgress(_progress.copyWith(statusMessage: 'Checking existing overlays...'));
        await _dependencies.cleanupActiveOverlays(server, vm, activeDisks, inactiveDisks);

        _logInfo('Creating snapshot for ${vm.name}.');
        _setProgress(_progress.copyWith(statusMessage: 'Creating snapshot...'));
        final snapshotName = _dependencies.sanitizeFileName('virtbackup-$backupTimestamp');
        await _dependencies.createVmSnapshot(server, vm, snapshotName);
        snapshotCreated = true;
      } else {
        _logInfo('VM ${vm.name} is not running (state=$state). Skipping snapshot creation and cleanup.');
      }

      _logInfo('Refreshing disk chain for ${vm.name} before backup.');
      final activeDisksForBackup = state == 'running' ? await _dependencies.loadVmDiskPaths(server, vm) : inactiveDisks;
      disks = await _dependencies.loadVmDiskPaths(server, vm, inactive: true);
      var totalBytes = 0;
      var totalDisks = 0;
      for (final entry in activeDisksForBackup) {
        final target = _dependencies.sanitizeFileName(entry.key);
        final sourcePath = entry.value;
        final chainPaths = await _loadBackingChain(server, sourcePath);
        while (chainPaths.isNotEmpty && chainPaths.first.toLowerCase().contains('.virtbackup-')) {
          chainPaths.removeAt(0);
        }
        final chainTopSource = chainPaths.isNotEmpty ? chainPaths.first : sourcePath;
        final sourceName = _dependencies.sanitizeFileName(chainTopSource.split(Platform.pathSeparator).last);
        final topDiskId = sourceName.isNotEmpty ? sourceName : (target.isEmpty ? 'disk' : target);
        final chainItems = <_DiskChainItem>[];
        for (final path in chainPaths) {
          final lower = path.toLowerCase();
          if (lower.contains('virtbackup') && !lower.contains('.virtbackup-')) {
            throw 'Refusing to backup: disk path contains "virtbackup": $path';
          }
          final chainName = _dependencies.sanitizeFileName(path.split(Platform.pathSeparator).last);
          final chainDiskId = chainName.isNotEmpty ? chainName : topDiskId;
          chainItems.add(_DiskChainItem(sourcePath: path, diskId: chainDiskId));
          try {
            final size = await _dependencies.getRemoteFileSize(server, path);
            if (size > 0) {
              totalBytes += size;
            }
          } catch (_) {}
        }
        if (chainItems.isEmpty) {
          _logInfo('Backup chain empty for ${vm.name} target ${entry.key}; falling back to $sourcePath');
          chainItems.add(_DiskChainItem(sourcePath: sourcePath, diskId: topDiskId));
        }
        totalDisks += chainItems.length;
        backupPlans.add(_DiskBackupPlan(topDiskId: topDiskId, target: entry.key, chain: chainItems));
      }
      if (totalBytes > 0) {
        _setProgress(_progress.copyWith(totalBytes: totalBytes));
      }

      _setProgress(_progress.copyWith(totalDisks: totalDisks, completedDisks: 0, statusMessage: 'Backing up $totalDisks disk${totalDisks == 1 ? '' : 's'}...'));
      if (state == 'running') {
        final preBackupStateResult = await _dependencies.runSshCommand(server, 'virsh domstate "${vm.name}"');
        final preBackupState = preBackupStateResult.stdout.trim().toLowerCase();
        if (preBackupState != 'running') {
          _logInfo('VM ${vm.name} state changed to $preBackupState just before backup.');
        }
      }

      for (final plan in backupPlans) {
        _ensureNotCanceled();
        if (plan.chain.length > 1) {
          final chainWrite = driver.startChainWrite(serverFolderName, vmFolderName, backupTimestamp, plan.topDiskId);
          chainWrite.sink.write(_formatChainMetadata(plan));
          await chainWrite.sink.flush();
          await chainWrite.sink.close();
          await chainWrite.commit();
        }
        for (final chainItem in plan.chain) {
          _ensureNotCanceled();
          final diskId = chainItem.diskId;
          final sourcePath = chainItem.sourcePath;
          _logInfo('Streaming disk ${_progress.completedDisks + 1}/${_progress.totalDisks} for ${vm.name}: $diskId');
          _setProgress(_progress.copyWith(statusMessage: 'Streaming ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
          _setProgress(_progress.copyWith(statusMessage: 'Backup ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
          final usedHashblocks = await _writeDiskUsingHashblocks(
            driver: driver,
            backupTimestamp: backupTimestamp,
            diskId: diskId,
            serverFolderName: serverFolderName,
            vmFolderName: vmFolderName,
            serverId: server.id,
            vmName: vm.name,
            sourcePath: sourcePath,
            server: server,
          );
          if (!usedHashblocks) {
            _setProgress(_progress.copyWith(statusMessage: 'Backup ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
            await _writeDiskStreamToDedupStore(
              driver: driver,
              backupTimestamp: backupTimestamp,
              diskId: diskId,
              serverFolderName: serverFolderName,
              vmFolderName: vmFolderName,
              serverId: server.id,
              vmName: vm.name,
              sourcePath: sourcePath,
              server: server,
              streamRemoteFile: (onChunk) => _dependencies.streamRemoteFile(server, sourcePath, onChunk: onChunk),
            );
          }
          _setProgress(_progress.copyWith(completedDisks: _progress.completedDisks + 1));
        }
      }

      _logInfo('Committing snapshot for ${vm.name}.');
      _ensureNotCanceled();
      _setProgress(_progress.copyWith(statusMessage: 'Committing snapshot...'));
      if (snapshotCreated) {
        await _dependencies.commitVmSnapshot(server, vm, disks);
      }

      final message = driver.backupCompletedMessage(manifestsBase.path);
      _logInfo(message);
      return BackupAgentResult(success: true, message: message);
    } catch (error, stackTrace) {
      runError = error;
      final isCanceled = error is _BackupCanceled;
      if (!isCanceled) {
        _onError?.call('VM backup failed.', error, stackTrace);
      }
      if (!isCanceled) {
        try {
          await _dependencies.loadVmDiskPaths(server, vm);
        } catch (_) {}
      }
      if (snapshotCreated) {
        try {
          _setProgress(_progress.copyWith(statusMessage: isCanceled ? 'Committing snapshot after cancel...' : 'Committing snapshot after failure...'));
          await _dependencies.commitVmSnapshot(server, vm, disks);
        } catch (commitError, commitStack) {
          _onError?.call('Snapshot commit failed.', commitError, commitStack);
        }
      }
      try {
        await driver.cleanupInProgressFiles();
      } catch (_) {}
      if (isCanceled) {
        return const BackupAgentResult(success: false, message: 'Canceled', canceled: true);
      }
      return BackupAgentResult(success: false, message: error.toString());
    } finally {
      try {
        await _stopBlobCacheWorker();
      } catch (error, stackTrace) {
        if (runError == null) {
          Error.throwWithStackTrace(error, stackTrace);
        }
        _logInfo('blob-cache worker cleanup failed: $error');
      }
      try {
        await driver.closeConnections();
      } catch (error, stackTrace) {
        if (runError == null) {
          Error.throwWithStackTrace(error, stackTrace);
        }
        _logInfo('driver connection cleanup failed: $error');
      }
      await _dependencies.endLargeTransfer?.call(server);
      _stopSpeedTimer();
      _stopProgressLogTimer();
      _blobDirectoryCache = null;
      _setProgress(_progress.copyWith(isRunning: false, statusMessage: '', speedBytesPerSec: 0, physicalSpeedBytesPerSec: 0, sanitySpeedBytesPerSec: 0));
      _isRunning = false;
    }
  }

  void _handleBytes(int bytes) {
    _logicalBytesTransferred += bytes;
    _progress = _progress.copyWith(bytesTransferred: _logicalBytesTransferred);
  }

  void _handlePhysicalBytes(int bytes) {
    if (bytes <= 0) {
      return;
    }
    _physicalBytesTransferred += bytes;
    _progress = _progress.copyWith(physicalBytesTransferred: _physicalBytesTransferred);
  }

  void _handleWriterMetrics(int queuedBytes, int inFlightBytes, int driverBufferedBytes) {
    _writerQueuedBytes = queuedBytes;
    _writerInFlightBytes = inFlightBytes;
    _driverBufferedBytes = driverBufferedBytes;
    _progress = _progress.copyWith(writerQueuedBytes: _writerQueuedBytes, writerInFlightBytes: _writerInFlightBytes, driverBufferedBytes: _driverBufferedBytes);
  }

  void _startBlobCacheWorker() {
    final cache = _blobDirectoryCache;
    if (cache == null) {
      return;
    }
    _blobCacheWorker = _BlobCacheWorker(initialize: cache.initialize, processHash: cache.prefetchHash);
    _blobCacheWorkerFuture = _blobCacheWorker!.run();
  }

  Future<void> _stopBlobCacheWorker() async {
    final worker = _blobCacheWorker;
    if (worker == null) {
      return;
    }
    worker.signalDone();
    final future = _blobCacheWorkerFuture;
    _blobCacheWorker = null;
    _blobCacheWorkerFuture = null;
    if (future != null) {
      await future;
    }
    worker.throwIfError();
  }

  void _enqueueBlobDir(String hash) {
    _blobCacheWorker?.enqueue(hash);
  }

  void _prefetchBlobCache(String hash) {
    _blobCacheWorker?.enqueue(hash);
  }

  void _startSpeedTimer() {
    _speedTimer?.cancel();
    _speedTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      final now = DateTime.now();
      final lastUpdate = _lastSpeedUpdate;
      if (lastUpdate != null) {
        final elapsedMs = now.difference(lastUpdate).inMilliseconds;
        if (elapsedMs > 0) {
          _speedSamples.add(_SpeedSample(at: now, logicalTotal: _logicalBytesTransferred, physicalTotal: _physicalBytesTransferred));
        }
        _lastSpeedUpdate = now;
        _trimSpeedSamples(now);
        final speed = _windowSpeed(now, (sample) => sample.logicalTotal);
        final physicalSpeed = _windowSpeed(now, (sample) => sample.physicalTotal);
        _setProgress(_progress.copyWith(speedBytesPerSec: speed, physicalSpeedBytesPerSec: physicalSpeed));
        return;
      }
      _lastSpeedUpdate = now;
      _trimSpeedSamples(now);
      final speed = _windowSpeed(now, (sample) => sample.logicalTotal);
      final physicalSpeed = _windowSpeed(now, (sample) => sample.physicalTotal);
      _setProgress(_progress.copyWith(speedBytesPerSec: speed, physicalSpeedBytesPerSec: physicalSpeed));
    });
  }

  void _stopSpeedTimer() {
    _speedTimer?.cancel();
    _speedTimer = null;
  }

  void _startProgressLogTimer() {
    _progressLogTimer?.cancel();
    _progressLogTimer = Timer.periodic(agentLogInterval, (_) {
      if (!_progress.isRunning) {
        return;
      }
      final total = _progress.totalBytes;
      final logical = _progress.bytesTransferred;
      final physical = _progress.physicalBytesTransferred;
      final percent = total > 0 ? (logical / total) * 100 : null;
      final progress = percent == null ? _formatBytes(logical) : '${percent.toStringAsFixed(1)}% (${_formatBytes(logical)} / ${_formatBytes(total)})';
      final physicalPercent = _progress.physicalProgressPercent;
      final physicalProgress = _progress.physicalTotalBytes > 0
          ? '${physicalPercent.toStringAsFixed(1)}% (${_formatBytes(physical)} / ${_formatBytes(_progress.physicalTotalBytes)})'
          : _formatBytes(physical);
      final eta = _formatEta(_progress.etaSeconds);
      final remaining = _formatBytes(_progress.physicalRemainingBytes);
      final speedWarmup = _speedSamples.length < 2 || DateTime.now().difference(_speedSamples.first.at) < _speedWindow;
      final flushSpeed = speedWarmup ? 'warming-up' : _formatSpeed(_progress.physicalSpeedBytesPerSec);
      _logInfo('job progress=$progress avg=${_formatSpeed(_progress.averageSpeedBytesPerSec)} eta=$eta | flush $physicalProgress speed=$flushSpeed remaining=$remaining');
    });
  }

  void _stopProgressLogTimer() {
    _progressLogTimer?.cancel();
    _progressLogTimer = null;
  }

  void _setProgress(BackupAgentProgress progress) {
    _progress = _decorateProgress(progress);
    if (_isDisposed) {
      return;
    }
    _onProgress(_progress);
  }

  BackupAgentProgress _decorateProgress(BackupAgentProgress progress) {
    final displayBytes = _computeDisplayBytesTransferred(progress.totalBytes);
    final elapsedSec = _elapsedBackupSeconds();
    final avgLogical = elapsedSec > 0 ? _logicalBytesTransferred / elapsedSec : 0.0;
    final avgPhysical = elapsedSec > 0 ? _physicalBytesTransferred / elapsedSec : 0.0;
    final physicalRemaining = _writerQueuedBytes + _writerInFlightBytes + _driverBufferedBytes;
    final physicalTotal = _physicalBytesTransferred + physicalRemaining;
    final physicalPercent = physicalTotal > 0 ? (_physicalBytesTransferred / physicalTotal) * 100 : 0.0;
    final etaSeconds = _calculateEtaSeconds(avgLogical, avgPhysical, progress.totalBytes, physicalRemaining);
    return progress.copyWith(
      bytesTransferred: displayBytes,
      averageSpeedBytesPerSec: avgLogical,
      averagePhysicalSpeedBytesPerSec: avgPhysical,
      etaSeconds: etaSeconds,
      physicalRemainingBytes: physicalRemaining,
      physicalTotalBytes: physicalTotal,
      physicalProgressPercent: physicalPercent,
    );
  }

  int _computeDisplayBytesTransferred(int totalBytes) {
    if (totalBytes <= 0) {
      return _logicalBytesTransferred;
    }
    if (_logicalBytesTransferred < totalBytes) {
      return _logicalBytesTransferred;
    }
    final remaining = _writerQueuedBytes + _writerInFlightBytes + _driverBufferedBytes;
    if (remaining > 0) {
      return max(0, totalBytes - 1);
    }
    return totalBytes;
  }

  double _elapsedBackupSeconds() {
    final start = _backupStartAt;
    if (start == null) {
      return 0;
    }
    final elapsed = DateTime.now().difference(start).inMilliseconds / 1000.0;
    return elapsed > 0 ? elapsed : 0;
  }

  int? _calculateEtaSeconds(double avgLogical, double avgPhysical, int totalBytes, int physicalRemainingBytes) {
    if (totalBytes <= 0) {
      return null;
    }
    if (_logicalBytesTransferred < totalBytes) {
      if (avgLogical <= 0) {
        return null;
      }
      final remainingLogical = totalBytes - _logicalBytesTransferred;
      return (remainingLogical / avgLogical).ceil();
    }
    if (physicalRemainingBytes > 0) {
      if (avgPhysical <= 0) {
        return null;
      }
      return (physicalRemainingBytes / avgPhysical).ceil();
    }
    return 0;
  }

  void _logInfo(String message) {
    final formatted = _formatLogMessage(message);
    if (formatted != null && formatted.isNotEmpty) {
      _onInfo?.call(formatted);
    }
  }

  void _trimSpeedSamples(DateTime now) {
    final cutoff = now.subtract(_speedWindow);
    while (_speedSamples.length > 2 && _speedSamples[1].at.isBefore(cutoff)) {
      _speedSamples.removeAt(0);
    }
  }

  double _windowSpeed(DateTime now, int Function(_SpeedSample sample) totalForSample) {
    if (_speedSamples.length < 2) {
      return 0;
    }
    final cutoff = now.subtract(_speedWindow);
    _SpeedSample? baseSample;
    for (var i = _speedSamples.length - 1; i >= 0; i -= 1) {
      final sample = _speedSamples[i];
      if (!sample.at.isAfter(cutoff)) {
        baseSample = sample;
        break;
      }
    }
    if (baseSample == null) {
      return 0;
    }
    final last = _speedSamples.last;
    final elapsedSec = last.at.difference(baseSample.at).inMilliseconds / 1000.0;
    if (elapsedSec <= 0) {
      return 0;
    }
    final total = totalForSample(last) - totalForSample(baseSample);
    if (total <= 0) {
      return 0;
    }
    return total / elapsedSec;
  }

  double _smoothSpeed(double current, double instant) {
    const alpha = 0.2;
    if (current <= 0) {
      return instant;
    }
    return (instant * alpha) + (current * (1 - alpha));
  }

  String _formatBytes(int bytes) {
    if (bytes <= 0) {
      return '0 B';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var value = bytes.toDouble();
    var unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    return '${value.toStringAsFixed(value >= 10 ? 1 : 2)} ${units[unitIndex]}';
  }

  String _formatSpeed(double bytesPerSec) {
    if (bytesPerSec <= 0) {
      return '0 B/s';
    }
    return '${_formatBytes(bytesPerSec.round())}/s';
  }

  String _formatEta(int? seconds) {
    if (seconds == null || seconds <= 0) {
      return 'n/a';
    }
    final minutes = seconds ~/ 60;
    final secs = seconds % 60;
    if (minutes <= 0) {
      return '${secs}s';
    }
    final hours = minutes ~/ 60;
    final mins = minutes % 60;
    if (hours <= 0) {
      return '${minutes}m ${secs}s';
    }
    return '${hours}h ${mins}m';
  }

  String? _formatLogMessage(String message) {
    final trimmed = message.trimLeft();
    if (RegExp(r'^[a-zA-Z]+:').hasMatch(trimmed)) {
      return trimmed;
    }
    if (trimmed.startsWith('hashblocks ')) {
      final payload = trimmed.substring(10).trimLeft();
      if (payload.startsWith('limit:') || payload.startsWith('stats:')) {
        return null;
      }
      return 'hashblocks: $payload';
    }
    if (trimmed.startsWith('SFTP range')) {
      return 'missing: $trimmed';
    }
    if (trimmed.startsWith('SFTP ')) {
      return 'sftp: ${trimmed.substring(5)}';
    }
    if (trimmed.startsWith('Blob write failed')) {
      return 'writer: $trimmed';
    }
    if (trimmed.startsWith('job progress')) {
      return 'progress: ${trimmed.substring(4)}';
    }
    return 'backup: $trimmed';
  }

  Future<void> _writeDiskStreamToDedupStore({
    required BackupDriver driver,
    required String backupTimestamp,
    required String diskId,
    required String serverFolderName,
    required String vmFolderName,
    required String serverId,
    required String vmName,
    required String sourcePath,
    required ServerConfig server,
    required Future<void> Function(Future<void> Function(List<int> chunk) onChunk) streamRemoteFile,
  }) async {
    _ensureNotCanceled();
    final manifestWrite = driver.startManifestWrite(serverFolderName, vmFolderName, diskId, backupTimestamp);
    final sink = manifestWrite.sink;
    int? fileSize;
    try {
      fileSize = await _dependencies.getRemoteFileSize(server, sourcePath);
    } catch (_) {}
    sink.writeln('version: 1');
    sink.writeln('block_size: $_blockSize');
    sink.writeln('server_id: $serverId');
    sink.writeln('vm_name: $vmName');
    sink.writeln('disk_id: $diskId');
    sink.writeln('source_path: $sourcePath');
    if (fileSize != null && fileSize > 0) {
      sink.writeln('file_size: $fileSize');
    }
    sink.writeln('timestamp: ${DateTime.now().toUtc().toIso8601String()}');
    sink.writeln('blocks:');

    var index = 0;
    int zeroRunStart = -1;
    int zeroRunEnd = -1;
    final buffer = Uint8List(_blockSize);
    var bufferOffset = 0;
    try {
      await streamRemoteFile((chunk) async {
        _ensureNotCanceled();
        _markSftpRead(chunk.length);
        var offset = 0;
        while (offset < chunk.length) {
          final remaining = _blockSize - bufferOffset;
          final toCopy = (chunk.length - offset) < remaining ? (chunk.length - offset) : remaining;
          buffer.setRange(bufferOffset, bufferOffset + toCopy, chunk, offset);
          bufferOffset += toCopy;
          offset += toCopy;
          if (bufferOffset == _blockSize) {
            if (_isAllZero(buffer, _blockSize)) {
              if (zeroRunStart < 0) {
                zeroRunStart = index;
              }
              zeroRunEnd = index;
            } else {
              if (zeroRunStart >= 0) {
                _writeZeroRun(sink, zeroRunStart, zeroRunEnd);
                zeroRunStart = -1;
                zeroRunEnd = -1;
              }
              final hash = sha256.convert(buffer).toString();
              final exists = await _blobExists(hash);
              if (exists) {
                sink.writeln('$index -> $hash');
                final blockLength = (fileSize != null && fileSize > 0) ? _blockLengthForIndex(index, fileSize) : _blockSize;
                _handleBytes(blockLength);
                index++;
                bufferOffset = 0;
                continue;
              }
              _enqueueBlobDir(hash);
              await _scheduleWriteBlob(hash, buffer, driver);
              _handlePhysicalBytes(buffer.length);
              sink.writeln('$index -> $hash');
            }
            final blockLength = (fileSize != null && fileSize > 0) ? _blockLengthForIndex(index, fileSize) : _blockSize;
            _handleBytes(blockLength);
            index++;
            bufferOffset = 0;
          }
        }
      });
      if (bufferOffset > 0) {
        _ensureNotCanceled();
        final tail = Uint8List.sublistView(buffer, 0, bufferOffset);
        if (_isAllZero(tail, bufferOffset)) {
          if (zeroRunStart < 0) {
            zeroRunStart = index;
          }
          zeroRunEnd = index;
        } else {
          var tailHandled = false;
          if (zeroRunStart >= 0) {
            _writeZeroRun(sink, zeroRunStart, zeroRunEnd);
            zeroRunStart = -1;
            zeroRunEnd = -1;
          }
          final hash = sha256.convert(tail).toString();
          final exists = await _blobExists(hash);
          if (exists) {
            sink.writeln('$index -> $hash');
            _handleBytes(bufferOffset);
            index++;
            tailHandled = true;
          }
          if (!tailHandled) {
            _enqueueBlobDir(hash);
            await _scheduleWriteBlob(hash, tail, driver);
            _handlePhysicalBytes(tail.length);
            sink.writeln('$index -> $hash');
          }
        }
        _handleBytes(bufferOffset);
        index++;
      }
      if (zeroRunStart >= 0) {
        _writeZeroRun(sink, zeroRunStart, zeroRunEnd);
        zeroRunStart = -1;
        zeroRunEnd = -1;
      }
    } finally {
      await _drainPendingWrites();
      await sink.flush();
      await sink.close();
    }

    await driver.finalizeManifest(manifestWrite);
  }

  Future<bool> _writeDiskUsingHashblocks({
    required BackupDriver driver,
    required String backupTimestamp,
    required String diskId,
    required String serverFolderName,
    required String vmFolderName,
    required String serverId,
    required String vmName,
    required String sourcePath,
    required ServerConfig server,
  }) async {
    final remoteHashblocksPath = await _dependencies.ensureHashblocks(server);
    if (remoteHashblocksPath == null) {
      return false;
    }
    _ensureNotCanceled();

    final fileSize = await _dependencies.getRemoteFileSize(server, sourcePath);
    final manifestWrite = driver.startManifestWrite(serverFolderName, vmFolderName, diskId, backupTimestamp);
    final sink = manifestWrite.sink;
    sink.writeln('version: 1');
    sink.writeln('block_size: $_blockSize');
    sink.writeln('server_id: $serverId');
    sink.writeln('vm_name: $vmName');
    sink.writeln('disk_id: $diskId');
    sink.writeln('source_path: $sourcePath');
    sink.writeln('file_size: $fileSize');
    sink.writeln('timestamp: ${DateTime.now().toUtc().toIso8601String()}');
    sink.writeln('blocks:');

    var hashblocksBytes = 0;
    var hashblocksBytesSinceTick = 0;
    var hashblocksSmoothedSpeed = 0.0;
    DateTime? hashblocksLastTick;
    Timer? hashblocksLogTimer;
    var eofSeen = false;

    void handleHashblocksBytes(int bytes) {
      hashblocksBytes += bytes;
      hashblocksBytesSinceTick += bytes;
      final now = DateTime.now();
      if (hashblocksLastTick == null) {
        hashblocksLastTick = now;
        return;
      }
      final elapsedMs = now.difference(hashblocksLastTick!).inMilliseconds;
      if (elapsedMs < 1000) {
        return;
      }
      final instant = hashblocksBytesSinceTick / (elapsedMs / 1000);
      hashblocksSmoothedSpeed = _smoothSpeed(hashblocksSmoothedSpeed, instant);
      hashblocksBytesSinceTick = 0;
      hashblocksLastTick = now;
    }

    Object? missingError;
    StackTrace? missingStack;
    HashblocksController? controller;
    const initialLeadMb = 64;
    const targetQueuedBlocks = 512;
    const limitStepBlocks = 128;
    const limitStepWhenQueueEmptyBlocks = 1024;
    const maxLeadFromExistsCheckBlocks = 1024;
    const limitUpdateInterval = Duration(seconds: 1);
    const unlimitedLimitIndex = 2147483647;
    final maxFileLimitIndex = max(0, ((fileSize + _blockSize - 1) ~/ _blockSize) - 1);
    var lastLimitSent = -1;
    Timer? limitUpdateTimer;
    var hasSeenExistingOrMissing = false;
    var unlimitedSent = false;
    var existsCheckedBlocks = 0;

    late final _WriterWorker writerWorker;
    late final _SftpWorker sftpWorker;
    late final _ExistsWorker existsWorker;
    late final _HashblocksWorker hashblocksWorker;

    String formatLimitBlocks(int blocks) {
      if (blocks <= 0) {
        return '0 B';
      }
      return _formatBytes(blocks * _blockSize);
    }

    void sendLimitCommand(HashblocksController value, int maxIndex, {required String reason, String? detail}) {
      final suffix = detail == null || detail.isEmpty ? '' : ' $detail';
      LogWriter.logAgentSync(level: 'debug', message: _formatLogMessage('hashblocks limit command: LIMIT $maxIndex reason=$reason (${formatLimitBlocks(maxIndex)})$suffix') ?? '');
      value.setLimit(maxIndex);
    }

    bool sendLimit(int maxIndex, {bool force = false, String reason = 'progress', String? detail}) {
      maxIndex = min(maxIndex, maxFileLimitIndex);
      maxIndex = max(maxIndex, lastLimitSent);
      if (maxIndex == lastLimitSent) {
        return false;
      }
      final writerBufferedBytes = _writerQueuedBytes + _writerInFlightBytes + _driverBufferedBytes;
      if (!force && writerBufferedBytes > _writerBacklogBytesLimit && maxIndex > lastLimitSent) {
        return false;
      }
      lastLimitSent = maxIndex;
      if (controller == null) {
        return false;
      }
      sendLimitCommand(controller!, maxIndex, reason: reason, detail: detail);
      return true;
    }

    int leadBlocksForMb(int mb) {
      if (mb <= 0) {
        return 0;
      }
      final bytes = mb * 1024 * 1024;
      final blocks = bytes ~/ _blockSize;
      return max(1, blocks);
    }

    void updateLimitFromProgress({bool force = false, String reason = 'progress'}) {
      if (!unlimitedSent && controller != null && lastLimitSent >= maxFileLimitIndex) {
        sendLimitCommand(controller!, unlimitedLimitIndex, reason: 'eof-unlimited', detail: 'targetQueue=$targetQueuedBlocks queued=0');
        lastLimitSent = unlimitedLimitIndex;
        unlimitedSent = true;
        return;
      }
      final queuedBlocksNow = _writerQueuedBytes <= 0 ? 0 : ((_writerQueuedBytes + _blockSize - 1) ~/ _blockSize);
      if (!hasSeenExistingOrMissing) {
        return;
      }
      if (queuedBlocksNow >= targetQueuedBlocks) {
        return;
      }
      final stepBlocks = queuedBlocksNow == 0 ? limitStepWhenQueueEmptyBlocks : limitStepBlocks;
      final maxLimitFromExistsCheck = max(0, existsCheckedBlocks + maxLeadFromExistsCheckBlocks - 1);
      final nextLimit = min(max(0, lastLimitSent + stepBlocks), maxLimitFromExistsCheck);
      sendLimit(nextLimit, force: force, reason: reason, detail: 'targetQueue=$targetQueuedBlocks queued=$queuedBlocksNow');
    }

    void registerProgressBlocks(int count) {
      if (count <= 0) {
        return;
      }
      existsCheckedBlocks += count;
    }

    void registerWriterCompletedBlocks(int count) {
      if (count <= 0) {
        return;
      }
    }

    final missingQueue = <_MissingRun>[];
    Completer<void>? wakeMissingWorker;
    var missingDone = false;
    Future<void>? writerFutureRef;
    var writerAwaited = false;
    var missingRunLogged = false;
    var sftpStartLogged = false;

    void enqueueMissingRun(int startIndex, List<String> hashes) {
      if (hashes.isEmpty) {
        return;
      }
      if (!missingRunLogged) {
        missingRunLogged = true;
        _logInfo('missing: first run queued blocks=${hashes.length} start=$startIndex');
      }
      missingQueue.add(_MissingRun(startIndex, hashes));
      if (wakeMissingWorker != null && !wakeMissingWorker!.isCompleted) {
        wakeMissingWorker!.complete();
        wakeMissingWorker = null;
      }
    }

    final writerTimeout = const Duration(minutes: 5);
    final cache = _blobDirectoryCache;
    writerWorker = _WriterWorker(
      maxConcurrentWrites: _writerConcurrencyOverride ?? max(1, driver.capabilities.maxConcurrentWrites),
      blockTimeout: writerTimeout,
      logInterval: agentLogInterval,
      backlogLimitBytes: _writerBacklogBytesLimit,
      driverBufferedBytes: () => driver.bufferedBytes,
      onMetrics: _handleWriterMetrics,
      scheduleWrite: (hash, bytes) => _scheduleWriteBlob(hash, bytes, driver),
      handlePhysicalBytes: _handlePhysicalBytes,
      onWriteCompletedBlocks: registerWriterCompletedBlocks,
      isWriteReady: () => cache?.isWriteReady() ?? true,
      waitForWriteReady: cache?.waitForWriteReady,
    );

    sftpWorker = _SftpWorker(
      server: server,
      sourcePath: sourcePath,
      blockSize: _blockSize,
      fileSize: fileSize,
      prefetchWindow: _sftpPrefetchWindow,
      streamRemoteRange: _dependencies.streamRemoteRange,
      handleBytes: _handleBytes,
      markSftpRead: (bytes) {
        _markSftpRead(bytes);
        if (!sftpStartLogged) {
          sftpStartLogged = true;
          _logInfo('missing: first sftp bytes received');
        }
      },
      registerProgressBlocks: registerProgressBlocks,
      enqueueWriteBlock: (hash, bytes) async {
        _blobDirectoryCache?.markHashKnown(hash);
        writerWorker.enqueue(hash, bytes);
      },
      ensureNotCanceled: _ensureNotCanceled,
    );

    const maxMissingRun = 1;

    existsWorker = _ExistsWorker(
      maxMissingRun: maxMissingRun,
      blobExists: _blobExists,
      enqueueMissingRun: enqueueMissingRun,
      handleBytes: _handleBytes,
      registerProgressBlocks: registerProgressBlocks,
      ensureNotCanceled: _ensureNotCanceled,
      onExisting: () {
        hasSeenExistingOrMissing = true;
        hashblocksWorker.markExisting();
      },
      onMissing: () {
        hasSeenExistingOrMissing = true;
        hashblocksWorker.markMissing();
      },
    );

    hashblocksWorker = _HashblocksWorker(
      sink: sink,
      fileSize: fileSize,
      ensureNotCanceled: _ensureNotCanceled,
      writeZeroRun: (start, end) => _writeZeroRun(sink, start, end),
      parseZeroRange: _parseZeroRange,
      parseHashEntry: _parseHashEntry,
      bytesForRange: _bytesForRange,
      blockLengthForIndex: _blockLengthForIndex,
      handleBytes: _handleBytes,
      handleHashblocksBytes: handleHashblocksBytes,
      registerProgressBlocks: registerProgressBlocks,
      prefetchBlob: _prefetchBlobCache,
      enqueueExists: existsWorker.enqueue,
    );

    var manifestClosed = false;

    try {
      final writerFuture = writerWorker.run();
      writerFutureRef = writerFuture;
      final existsWorkerFuture = existsWorker.run();

      final missingWorkerFuture =
          () async {
            while (!missingDone || missingQueue.isNotEmpty) {
              if (missingQueue.isEmpty) {
                wakeMissingWorker ??= Completer<void>();
                await wakeMissingWorker!.future;
              }
              while (missingQueue.isNotEmpty) {
                final run = missingQueue.removeAt(0);
                await sftpWorker.fetchMissingRun(run.startIndex, run.hashes);
              }
            }
          }().catchError((Object error, StackTrace stackTrace) {
            missingError = error;
            missingStack = stackTrace;
          });

      await _dependencies.streamHashblocks(
        server,
        remoteHashblocksPath,
        sourcePath,
        _blockSize,
        onController: (value) {
          controller = value;
          _activeHashblocksController = value;
          if (_cancelRequested) {
            value.stop();
          }
          final initialLimitIndex = max(0, leadBlocksForMb(initialLeadMb) - 1);
          sendLimit(initialLimitIndex, force: true, reason: 'initial', detail: 'targetQueue=$targetQueuedBlocks queued=0');
          limitUpdateTimer?.cancel();
          limitUpdateTimer = Timer.periodic(limitUpdateInterval, (_) {
            updateLimitFromProgress(force: true, reason: 'progress');
          });
        },
        onLine: (line) async {
          try {
            _ensureNotCanceled();
            final trimmed = line.trim();
            if (trimmed.isEmpty || trimmed == 'EOF') {
              if (!eofSeen && trimmed == 'EOF') {
                eofSeen = true;
                hashblocksLogTimer?.cancel();
                hashblocksLogTimer = null;
                hashblocksWorker.logStats(prefix: 'hashblocks stats: EOF');
              }
              return;
            }
            await hashblocksWorker.handleLine(trimmed);
          } on _BackupCanceled {
            return;
          }
        },
      );

      if (_cancelRequested) {
        throw const _BackupCanceled();
      }
      await hashblocksWorker.finishBatch();
      existsWorker.signalDone();
      await existsWorkerFuture;
      existsWorker.throwIfError();
      if (!eofSeen) {
        hashblocksWorker.logStats();
      }
      missingDone = true;
      if (wakeMissingWorker != null && !wakeMissingWorker!.isCompleted) {
        wakeMissingWorker!.complete();
        wakeMissingWorker = null;
      }
      await missingWorkerFuture;
      writerWorker.signalDone();
      await writerFuture;
      writerAwaited = true;
      if (missingError != null) {
        Error.throwWithStackTrace(missingError!, missingStack ?? StackTrace.current);
      }
      writerWorker.throwIfError();

      _setProgress(_progress.copyWith(statusMessage: 'Backup ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
      hashblocksLogTimer?.cancel();
      hashblocksLogTimer = null;
      handleHashblocksBytes(0);
      final doneMbPerSec = hashblocksSmoothedSpeed / (1024 * 1024);
      final doneTotalMb = hashblocksBytes / (1024 * 1024);
      _logInfo(
        'hashblocks done: lines=${hashblocksWorker.totalLines} existing=${hashblocksWorker.existingBlocks} missing=${hashblocksWorker.missingBlocks} zero=${hashblocksWorker.zeroBlocks} speed=${doneMbPerSec.toStringAsFixed(1)}MB/s total=${doneTotalMb.toStringAsFixed(1)}MB',
      );
      await sink.flush();
      await sink.close();
      manifestClosed = true;
    } finally {
      hashblocksLogTimer?.cancel();
      if (!eofSeen) {
        try {
          controller?.stop();
        } catch (_) {}
      }
      if (_activeHashblocksController == controller) {
        _activeHashblocksController = null;
      }
      limitUpdateTimer?.cancel();
      existsWorker.signalDone();
      writerWorker.signalDone();
      if (writerFutureRef != null && !writerAwaited) {
        await writerFutureRef;
      }
      await _drainPendingWrites();
      if (!manifestClosed) {
        await sink.flush();
        await sink.close();
      }
    }

    if (_firstSftpReadAt != null && _lastSftpReadAt != null) {
      final elapsedMs = _lastSftpReadAt!.difference(_firstSftpReadAt!).inMilliseconds;
      final elapsedSec = elapsedMs / 1000.0;
      _logInfo('SFTP read window: ${elapsedSec.toStringAsFixed(2)}s');
    }

    await driver.finalizeManifest(manifestWrite);
    return true;
  }

  Future<void> _writeBlob(String hash, List<int> bytes, BackupDriver driver) async {
    await driver.writeBlob(hash, bytes);
  }

  Future<void> _scheduleWriteBlob(String hash, Uint8List bytes, BackupDriver driver) async {
    _ensureNotCanceled();
    _blobDirectoryCache?.markHashKnown(hash);
    final payload = Uint8List.fromList(bytes);
    final future = _writeBlob(hash, payload, driver);
    final tracking = future.then((_) {});
    _inFlightWrites.add(tracking);
    tracking.whenComplete(() => _inFlightWrites.remove(tracking));
    return future;
  }

  Future<void> _drainPendingWrites() async {
    if (_inFlightWrites.isEmpty) {
      return;
    }
    await Future.wait(_inFlightWrites);
    _inFlightWrites.clear();
  }

  Future<bool> _blobExists(String hash) async {
    final cache = _blobDirectoryCache;
    if (cache == null) {
      return false;
    }
    return cache.blobExists(hash);
  }

  bool _isAllZero(Uint8List data, int length) {
    for (var i = 0; i < length; i += 1) {
      if (data[i] != 0) {
        return false;
      }
    }
    return true;
  }

  void _writeZeroRun(IOSink sink, int start, int end) {
    if (start == end) {
      sink.writeln('$start -> ZERO');
    } else {
      sink.writeln('$start-$end -> ZERO');
    }
  }

  (int, int)? _parseZeroRange(String line) {
    if (!line.endsWith('-> ZERO')) {
      return null;
    }
    final parts = line.split('->');
    if (parts.isEmpty) {
      return null;
    }
    final left = parts.first.trim();
    final rangeParts = left.split('-').map((value) => value.trim()).where((value) => value.isNotEmpty).toList();
    if (rangeParts.isEmpty) {
      return null;
    }
    final start = int.tryParse(rangeParts.first);
    if (start == null) {
      return null;
    }
    if (rangeParts.length == 1) {
      return (start, start);
    }
    final end = int.tryParse(rangeParts.last);
    if (end == null) {
      return null;
    }
    return (start, end);
  }

  (int, String)? _parseHashEntry(String line) {
    final parts = line.split('->');
    if (parts.length < 2) {
      return null;
    }
    final left = parts.first.trim();
    final right = parts.last.trim();
    if (right.isEmpty || right == 'ZERO') {
      return null;
    }
    final index = int.tryParse(left);
    if (index == null) {
      return null;
    }
    return (index, right);
  }

  int _minInt(int a, int b) => a < b ? a : b;

  int _bytesForRange(int start, int end, int totalSize) {
    final startOffset = start * _blockSize;
    final endExclusive = _minInt(totalSize, (end + 1) * _blockSize);
    final length = endExclusive - startOffset;
    return length < 0 ? 0 : length;
  }

  int _blockLengthForIndex(int index, int totalSize) {
    final start = index * _blockSize;
    final end = _minInt(totalSize, start + _blockSize);
    return end - start;
  }

  void _ensureNotCanceled() {
    _blobCacheWorker?.throwIfError();
    if (_cancelRequested) {
      throw const _BackupCanceled();
    }
  }

  Future<List<String>> _loadBackingChain(ServerConfig server, String sourcePath) async {
    try {
      final result = await _dependencies.runSshCommand(server, 'qemu-img info --backing-chain --force-share "$sourcePath"');
      if ((result.exitCode ?? 0) != 0) {
        return [sourcePath];
      }
      final paths = <String>[];
      final backingPaths = <String>[];
      for (final line in result.stdout.split('\n')) {
        final trimmed = line.trim();
        if (!trimmed.startsWith('image:')) {
          if (trimmed.startsWith('backing file:')) {
            final value = trimmed.substring('backing file:'.length).trim();
            if (value.isNotEmpty && value != '(null)') {
              backingPaths.add(value);
            }
          }
          continue;
        }
        final value = trimmed.substring('image:'.length).trim();
        if (value.isEmpty) {
          continue;
        }
        if (!paths.contains(value)) {
          paths.add(value);
        }
      }
      for (final backing in backingPaths) {
        if (!paths.contains(backing)) {
          paths.add(backing);
        }
      }
      if (paths.isEmpty) {
        return [sourcePath];
      }
      if (!paths.contains(sourcePath)) {
        paths.insert(0, sourcePath);
      }
      return paths;
    } catch (_) {
      return [sourcePath];
    }
  }

  String _formatChainMetadata(_DiskBackupPlan plan) {
    final buffer = StringBuffer();
    buffer.writeln('version: 1');
    buffer.writeln('disk_id: ${plan.topDiskId}');
    buffer.writeln('chain:');
    for (final item in plan.chain) {
      buffer.writeln('- path: ${item.sourcePath}');
      buffer.writeln('  disk_id: ${item.diskId}');
    }
    return buffer.toString();
  }
}

class _SpeedSample {
  _SpeedSample({required this.at, required this.logicalTotal, required this.physicalTotal});

  final DateTime at;
  final int logicalTotal;
  final int physicalTotal;
}

class _BlobDirectoryCache {
  _BlobDirectoryCache({required BlobDirectoryLister driver, required this.createShard}) : _driver = driver;

  final BlobDirectoryLister _driver;
  final Future<void> Function(String hash) createShard;

  Set<String>? _shardNames;
  Future<Set<String>>? _shardsInFlight;
  final Map<String, Set<String>> _blobNamesByShardKey = {};
  final Map<String, Future<Set<String>>> _blobNamesInFlight = {};
  final Map<String, Future<void>> _shardCreateInFlight = {};
  final List<Completer<void>> _writeReadyWaiters = <Completer<void>>[];
  final Map<String, DateTime> _lastExistsLogAt = <String, DateTime>{};
  Future<void>? _initializeInFlight;
  bool _writeReady = false;

  Future<void> initialize() async {
    await _ensureInitialized();
  }

  bool isWriteReady() {
    return _writeReady;
  }

  Future<void> waitForWriteReady() async {
    if (_writeReady) {
      return;
    }
    final completer = Completer<void>();
    _writeReadyWaiters.add(completer);
    return completer.future;
  }

  Future<bool> blobExists(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    await _ensureInitialized();
    final shardKey = hash.substring(0, 2);
    final shardNames = _shardNames ?? <String>{};
    if (!shardNames.contains(shardKey)) {
      _maybeLogExistsShard(shardKey, 'absent');
      return false;
    }
    final blobNames = await _loadBlobNames(shardKey);
    final exists = blobNames.contains(hash);
    _maybeLogExistsShard(shardKey, exists ? 'hit' : 'miss');
    return exists;
  }

  Future<void> prefetchHash(String hash) async {
    if (hash.length < 2) {
      return;
    }
    await _ensureInitialized();
    final shardKey = hash.substring(0, 2);
    final shardNames = _shardNames ?? <String>{};
    if (!shardNames.contains(shardKey)) {
      return;
    }
    await _loadBlobNames(shardKey);
  }

  void markHashKnown(String hash) {
    if (hash.length < 2) {
      return;
    }
    final shardKey = hash.substring(0, 2);
    (_shardNames ??= <String>{}).add(shardKey);
    final shardSet = _blobNamesByShardKey.putIfAbsent(shardKey, () => <String>{});
    final added = shardSet.add(hash);
    if (added && (shardSet.length == 1 || shardSet.length % 1024 == 0)) {
      LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: markHashKnown shard=$shardKey total=${shardSet.length}');
    }
  }

  Future<void> _ensureInitialized() async {
    final inFlight = _initializeInFlight;
    if (inFlight != null) {
      return inFlight;
    }
    final future = _initializeCore();
    _initializeInFlight = future;
    return future;
  }

  Future<void> _initializeCore() async {
    LogWriter.logAgentSync(level: 'info', message: 'blob-cache writeReady=false (initializing)');
    LogWriter.logAgentSync(level: 'info', message: 'blob-cache write ready: false');
    LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: writeReady=false initialize-start');
    LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: write ready=false initialize-start');
    _writeReady = false;
    final shardNames = await _loadShards();
    LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: top-level shard scan completed count=${shardNames.length}');
    final missingShards = <String>[];
    for (var i = 0; i < 256; i += 1) {
      final shardKey = i.toRadixString(16).padLeft(2, '0');
      if (!shardNames.contains(shardKey)) {
        missingShards.add(shardKey);
      }
    }
    for (final shardKey in missingShards) {
      await _ensureShardCreated(shardKey: shardKey);
    }
    _writeReady = true;
    LogWriter.logAgentSync(level: 'info', message: 'blob-cache writeReady=true (missingShardsCreated=${missingShards.length})');
    LogWriter.logAgentSync(level: 'info', message: 'blob-cache write ready: true');
    LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: writeReady=true missingShardsCreated=${missingShards.length}');
    LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: write ready=true missingShardsCreated=${missingShards.length}');
    _notifyWriteReady();
  }

  Future<Set<String>> _loadShards() async {
    final cached = _shardNames;
    if (cached != null) {
      return cached;
    }
    final inFlight = _shardsInFlight;
    if (inFlight != null) {
      return inFlight;
    }
    final future = _driver.listBlobShards();
    _shardsInFlight = future;
    try {
      final names = await future;
      _shardNames = names;
      return names;
    } finally {
      _shardsInFlight = null;
    }
  }

  Future<Set<String>> _loadBlobNames(String shardKey) async {
    final cached = _blobNamesByShardKey[shardKey];
    if (cached != null) {
      return cached;
    }
    final inFlight = _blobNamesInFlight[shardKey];
    if (inFlight != null) {
      return inFlight;
    }
    final future = _driver.listBlobNames(shardKey);
    _blobNamesInFlight[shardKey] = future;
    try {
      final names = await future;
      _blobNamesByShardKey[shardKey] = names;
      LogWriter.logAgentSync(level: 'trace', message: 'blob-cache: shard scan completed shard=$shardKey blobCount=${names.length}');
      return names;
    } finally {
      _blobNamesInFlight.remove(shardKey);
    }
  }

  Future<void> _ensureShardCreated({required String shardKey}) async {
    final existing = _shardCreateInFlight[shardKey];
    if (existing != null) {
      await existing;
      return;
    }
    final createFuture = () async {
      if (shardKey == 'ff') {
        LogWriter.logAgentSync(level: 'info', message: 'blob-cache shard create: ff start');
        LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: shard create ff start');
      }
      await createShard(shardKey);
      (_shardNames ??= <String>{}).add(shardKey);
      _blobNamesByShardKey.putIfAbsent(shardKey, () => <String>{});
      if (shardKey == 'ff') {
        LogWriter.logAgentSync(level: 'info', message: 'blob-cache shard create: ff ok');
        LogWriter.logAgentSync(level: 'debug', message: 'blob-cache: shard create ff ok');
      }
    }();
    _shardCreateInFlight[shardKey] = createFuture;
    try {
      await createFuture;
    } finally {
      if (identical(_shardCreateInFlight[shardKey], createFuture)) {
        _shardCreateInFlight.remove(shardKey);
      }
    }
  }

  void _notifyWriteReady() {
    if (_writeReadyWaiters.isEmpty) {
      return;
    }
    for (final waiter in _writeReadyWaiters) {
      if (!waiter.isCompleted) {
        waiter.complete();
      }
    }
    _writeReadyWaiters.clear();
  }

  void _maybeLogExistsShard(String shardKey, String result) {
    final now = DateTime.now();
    final last = _lastExistsLogAt[shardKey];
    if (last != null && now.difference(last) < const Duration(seconds: 5)) {
      return;
    }
    _lastExistsLogAt[shardKey] = now;
    LogWriter.logAgentSync(level: 'trace', message: 'blob-cache: exists shard=$shardKey result=$result writeReady=$_writeReady');
  }
}
