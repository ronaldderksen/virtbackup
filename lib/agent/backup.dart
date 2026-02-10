import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/agent/logging_config.dart';
import 'package:virtbackup/common/models.dart';

part 'backup_models.dart';
part 'backup_types.dart';
part 'workers/blob_cache_worker.dart';
part 'workers/dir_create_worker.dart';
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
    LogInfo? onInfo,
    LogError? onError,
    int? writerBacklogBytesLimit,
    int? hashblocksLimitBufferMb,
  }) : _dependencies = dependencies,
       _onProgress = onProgress,
       _onInfo = onInfo,
       _onError = onError,
       _writerBacklogBytesLimit = writerBacklogBytesLimit ?? _defaultWriterBacklogBytesLimit,
       _writerBacklogClearBytes = _calcBackpressureClearBytes(writerBacklogBytesLimit ?? _defaultWriterBacklogBytesLimit),
       _hashblocksLimitBufferMb = hashblocksLimitBufferMb ?? _defaultHashblocksLimitBufferMb;

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
  static const int _blockSize = 1024 * 1024;
  static const int _maxRemoteReadBytes = 4 * 1024 * 1024;
  static const int _hashblocksRangeSizeBytes = 128 * 1024 * 1024;
  static const int _hashblocksRangeConcurrency = 4;
  static const int _defaultWriterBacklogBytesLimit = 4 * 1024 * 1024 * 1024;
  static const int _defaultHashblocksLimitBufferMb = 1024;
  final Set<Future<void>> _inFlightWrites = {};
  final int _writerBacklogBytesLimit;
  final int _writerBacklogClearBytes;
  final int _hashblocksLimitBufferMb;
  _DirCreateWorker? _dirCreateWorker;
  Future<void>? _dirCreateWorkerFuture;
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
      _blobDirectoryCache = blobLister == null
          ? null
          : _BlobDirectoryCache(
              driver: blobLister,
              logInfo: _logInfo,
              markShardReady: (shardKey) {
                _dirCreateWorker?.markReady(shardKey);
              },
              createShard: (hash) async {
                final worker = _dirCreateWorker;
                if (worker == null) {
                  return;
                }
                await worker.waitForShard(hash);
              },
            );
      _startBlobCacheWorker();
      await driver.prepareBackup(serverFolderName, vmFolderName);
      _startDirCreateWorker(driver);
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
        await _stopDirCreateWorker();
      } catch (error, stackTrace) {
        if (runError == null) {
          Error.throwWithStackTrace(error, stackTrace);
        }
        _logInfo('dir-create worker cleanup failed: $error');
      }
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

  void _startDirCreateWorker(BackupDriver driver) {
    _dirCreateWorker = _DirCreateWorker(ensureBlobDir: driver.ensureBlobDir, logInfo: _logInfo);
    _dirCreateWorkerFuture = _dirCreateWorker!.run();
  }

  void _startBlobCacheWorker() {
    final cache = _blobDirectoryCache;
    if (cache == null) {
      return;
    }
    _blobCacheWorker = _BlobCacheWorker(prefetch: cache.prefetchHash, logInfo: _logInfo, logInterval: agentLogInterval);
    _blobCacheWorkerFuture = _blobCacheWorker!.run();
  }

  Future<void> _stopDirCreateWorker() async {
    final worker = _dirCreateWorker;
    if (worker == null) {
      return;
    }
    worker.signalDone();
    final future = _dirCreateWorkerFuture;
    _dirCreateWorker = null;
    _dirCreateWorkerFuture = null;
    if (future != null) {
      await future;
    }
    worker.throwIfError();
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
    if (_blobDirectoryCache != null) {
      return;
    }
    final cache = _blobDirectoryCache;
    if (cache != null && !cache.shouldEnsureBlobDir(hash)) {
      return;
    }
    _dirCreateWorker?.enqueue(hash);
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

  static int _calcBackpressureClearBytes(int limitBytes) {
    const clearTarget = 2 * 1024 * 1024 * 1024;
    if (limitBytes <= 0) {
      return 0;
    }
    return min(limitBytes, clearTarget);
  }

  static int _calcLimitBufferBlocks(int bufferMb) {
    if (bufferMb <= 0) {
      return 0;
    }
    final bytes = bufferMb * 1024 * 1024;
    final blocks = bytes ~/ _blockSize;
    return max(1, blocks);
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
              _enqueueBlobDir(hash);
              final wrote = await _scheduleWriteBlobIfMissing(hash, buffer, driver);
              if (wrote) {
                _handlePhysicalBytes(buffer.length);
              }
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
          if (zeroRunStart >= 0) {
            _writeZeroRun(sink, zeroRunStart, zeroRunEnd);
            zeroRunStart = -1;
            zeroRunEnd = -1;
          }
          final hash = sha256.convert(tail).toString();
          _enqueueBlobDir(hash);
          final wrote = await _scheduleWriteBlobIfMissing(hash, tail, driver);
          if (wrote) {
            _handlePhysicalBytes(tail.length);
          }
          sink.writeln('$index -> $hash');
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
    final bufferBlocks = _calcLimitBufferBlocks(_hashblocksLimitBufferMb);
    final halfBufferBlocks = max(1, bufferBlocks ~/ 2);
    var sftpProgressBlocks = 0;
    var nextLimitUpdateAt = halfBufferBlocks;
    var lastLimitSent = -1;
    int? pendingLimit;

    late final _WriterWorker writerWorker;
    late final _SftpWorker sftpWorker;
    late final _HashblocksWorker hashblocksWorker;

    String formatLimitBlocks(int blocks) {
      if (blocks <= 0) {
        return '0 B';
      }
      return _formatBytes(blocks * _blockSize);
    }

    void sendLimit(int maxIndex, {bool force = false}) {
      if (!force && maxIndex == lastLimitSent) {
        return;
      }
      final writerBufferedBytes = _writerQueuedBytes + _writerInFlightBytes + _driverBufferedBytes;
      if (writerBufferedBytes > _writerBacklogClearBytes && maxIndex > lastLimitSent) {
        return;
      }
      lastLimitSent = maxIndex;
      if (controller == null) {
        pendingLimit = maxIndex;
        _logInfo('hashblocks limit (pending): $maxIndex (${formatLimitBlocks(maxIndex)})');
        return;
      }
      _logInfo('hashblocks limit: $maxIndex (${formatLimitBlocks(maxIndex)})');
      controller?.setLimit(maxIndex);
    }

    void updateLimitFromProgress({bool force = false}) {
      if (hashblocksWorker.batchHold) {
        return;
      }
      final writerBufferedBytes = _writerQueuedBytes + _writerInFlightBytes + _driverBufferedBytes;
      if (!force && writerBufferedBytes > _writerBacklogClearBytes) {
        return;
      }
      final maxIndex = max(0, sftpProgressBlocks + bufferBlocks - 1);
      sendLimit(maxIndex, force: force);
    }

    void registerProgressBlocks(int count) {
      if (count <= 0) {
        return;
      }
      sftpProgressBlocks += count;
      if (sftpProgressBlocks >= nextLimitUpdateAt) {
        updateLimitFromProgress();
        nextLimitUpdateAt = sftpProgressBlocks + halfBufferBlocks;
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
    final dirWorker = _dirCreateWorker;
    writerWorker = _WriterWorker(
      maxConcurrentWrites: max(1, driver.capabilities.maxConcurrentWrites),
      blockTimeout: writerTimeout,
      logInterval: agentLogInterval,
      backlogLimitBytes: _writerBacklogBytesLimit,
      backlogClearBytes: _writerBacklogClearBytes,
      driverBufferedBytes: () => driver.bufferedBytes,
      onMetrics: _handleWriterMetrics,
      scheduleWrite: (hash, bytes) => _scheduleWriteBlobIfMissing(hash, bytes, driver),
      handlePhysicalBytes: _handlePhysicalBytes,
      logInfo: _logInfo,
      isShardReady: (shardKey) => dirWorker?.isShardReady(shardKey) ?? true,
      waitForAnyShardReady: dirWorker?.waitForAnyReady,
      onBackpressureStart: () {},
      onBackpressureEnd: () => updateLimitFromProgress(force: true),
    );

    sftpWorker = _SftpWorker(
      server: server,
      sourcePath: sourcePath,
      blockSize: _blockSize,
      fileSize: fileSize,
      maxRemoteReadBytes: _maxRemoteReadBytes,
      rangeSizeBytes: _hashblocksRangeSizeBytes,
      rangeConcurrency: _hashblocksRangeConcurrency,
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
      enqueueWriteBlock: (hash, bytes) async => writerWorker.enqueue(hash, bytes),
      waitForBackpressureClear: writerWorker.waitForBackpressureClear,
      ensureNotCanceled: _ensureNotCanceled,
      logInfo: _logInfo,
    );

    final maxBatchBlocks = (1024 * 1024 * 1024) ~/ _blockSize;
    final maxNonZeroBlocks = maxBatchBlocks > 0 ? maxBatchBlocks : 1;
    const maxMissingRun = 2048;

    hashblocksWorker = _HashblocksWorker(
      sink: sink,
      blockSize: _blockSize,
      fileSize: fileSize,
      maxNonZeroBlocks: maxNonZeroBlocks,
      maxMissingRun: maxMissingRun,
      logInfo: _logInfo,
      ensureNotCanceled: _ensureNotCanceled,
      writeZeroRun: (start, end) => _writeZeroRun(sink, start, end),
      parseZeroRange: _parseZeroRange,
      parseHashEntry: _parseHashEntry,
      bytesForRange: _bytesForRange,
      blockLengthForIndex: _blockLengthForIndex,
      handleBytes: _handleBytes,
      handleHashblocksBytes: handleHashblocksBytes,
      registerProgressBlocks: registerProgressBlocks,
      blobExists: (hash) => _blobExists(hash, driver),
      prefetchBlob: _prefetchBlobCache,
      enqueueBlobDir: _enqueueBlobDir,
      enqueueMissingRun: enqueueMissingRun,
      sendLimit: sendLimit,
      updateLimitFromProgress: updateLimitFromProgress,
    );

    var manifestClosed = false;

    try {
      final writerFuture = writerWorker.run();
      writerFutureRef = writerFuture;

      final missingWorkerFuture =
          () async {
            while (!missingDone || missingQueue.isNotEmpty) {
              if (missingQueue.isEmpty) {
                wakeMissingWorker ??= Completer<void>();
                await wakeMissingWorker!.future;
              }
              while (missingQueue.isNotEmpty) {
                final run = missingQueue.removeAt(0);
                await writerWorker.waitForBackpressureClear();
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
          if (pendingLimit != null) {
            _logInfo('hashblocks limit: $pendingLimit (${formatLimitBlocks(pendingLimit!)})');
            value.setLimit(pendingLimit!);
            pendingLimit = null;
          } else {
            updateLimitFromProgress(force: true);
          }
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

  Future<bool> _writeBlobIfMissing(String hash, List<int> bytes, BackupDriver driver) async {
    return driver.writeBlobIfMissing(hash, bytes);
  }

  Future<bool> _scheduleWriteBlobIfMissing(String hash, Uint8List bytes, BackupDriver driver) async {
    final dirWorker = _dirCreateWorker;
    if (dirWorker != null) {
      dirWorker.enqueue(hash);
    }
    _ensureNotCanceled();
    final payload = Uint8List.fromList(bytes);
    final future = _writeBlobIfMissing(hash, payload, driver);
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

  Future<bool> _blobExists(String hash, BackupDriver driver) async {
    final cache = _blobDirectoryCache;
    if (cache != null) {
      return cache.blobExists(hash);
    }
    return driver.blobExists(hash);
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
    _dirCreateWorker?.throwIfError();
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
  _BlobDirectoryCache({required BlobDirectoryLister driver, required this.logInfo, required this.markShardReady, required this.createShard}) : _driver = driver;

  final BlobDirectoryLister _driver;
  final void Function(String message) logInfo;
  final void Function(String shardKey) markShardReady;
  final Future<void> Function(String hash) createShard;

  Set<String>? _shard1Names;
  Future<Set<String>>? _shard1InFlight;
  final Map<String, Set<String>> _shard2NamesByShard1 = {};
  final Map<String, Future<Set<String>>> _shard2InFlight = {};
  final Map<String, Set<String>> _blobNamesByShardKey = {};
  final Map<String, Future<Set<String>>> _blobNamesInFlight = {};

  Future<bool> blobExists(String hash) async {
    if (hash.length < 4) {
      return false;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final shardKey = '$shard1$shard2';
    final shard1Names = _shard1Names;
    if (shard1Names == null) {
      _loadShard1().catchError((_) => <String>{});
      return false;
    }
    if (!shard1Names.contains(shard1)) {
      return false;
    }
    final shard2Names = _shard2NamesByShard1[shard1];
    if (shard2Names == null) {
      _loadShard2(shard1).catchError((_) => <String>{});
      return false;
    }
    if (!shard2Names.contains(shard2)) {
      return false;
    }
    markShardReady(shardKey);
    final blobNames = _blobNamesByShardKey[shardKey];
    if (blobNames == null) {
      _loadBlobNames(shard1, shard2).catchError((_) => <String>{});
      return false;
    }
    return blobNames.contains(hash);
  }

  bool shouldEnsureBlobDir(String hash) {
    if (hash.length < 4) {
      return false;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final shard1Names = _shard1Names;
    if (shard1Names != null && !shard1Names.contains(shard1)) {
      return true;
    }
    final shard2Names = _shard2NamesByShard1[shard1];
    if (shard2Names != null && !shard2Names.contains(shard2)) {
      return true;
    }
    return false;
  }

  Future<void> prefetchHash(String hash) async {
    if (hash.length < 4) {
      return;
    }
    final shard1 = hash.substring(0, 2);
    final shard2 = hash.substring(2, 4);
    final shardKey = '$shard1$shard2';
    final shard1Names = await _loadShard1();
    if (!shard1Names.contains(shard1)) {
      await createShard(hash);
      _shard1Names?.add(shard1);
      _shard2NamesByShard1.putIfAbsent(shard1, () => <String>{}).add(shard2);
      _blobNamesByShardKey.putIfAbsent(shardKey, () => <String>{});
      markShardReady(shardKey);
      return;
    }
    final shard2Names = await _loadShard2(shard1);
    if (!shard2Names.contains(shard2)) {
      await createShard(hash);
      _shard2NamesByShard1.putIfAbsent(shard1, () => <String>{}).add(shard2);
      _blobNamesByShardKey.putIfAbsent(shardKey, () => <String>{});
      markShardReady(shardKey);
      return;
    }
    markShardReady(shardKey);
    await _loadBlobNames(shard1, shard2);
  }

  Future<Set<String>> _loadShard1() async {
    final cached = _shard1Names;
    if (cached != null) {
      return cached;
    }
    final inFlight = _shard1InFlight;
    if (inFlight != null) {
      return inFlight;
    }
    final future = _driver.listBlobShard1();
    _shard1InFlight = future;
    try {
      final names = await future;
      _shard1Names = names;
      return names;
    } finally {
      _shard1InFlight = null;
    }
  }

  Future<Set<String>> _loadShard2(String shard1) async {
    final cached = _shard2NamesByShard1[shard1];
    if (cached != null) {
      return cached;
    }
    final inFlight = _shard2InFlight[shard1];
    if (inFlight != null) {
      return inFlight;
    }
    final future = _driver.listBlobShard2(shard1);
    _shard2InFlight[shard1] = future;
    try {
      final names = await future;
      _shard2NamesByShard1[shard1] = names;
      for (final shard2 in names) {
        final shardKey = '$shard1$shard2';
        markShardReady(shardKey);
      }
      return names;
    } finally {
      _shard2InFlight.remove(shard1);
    }
  }

  Future<Set<String>> _loadBlobNames(String shard1, String shard2) async {
    final key = '$shard1$shard2';
    final cached = _blobNamesByShardKey[key];
    if (cached != null) {
      return cached;
    }
    final inFlight = _blobNamesInFlight[key];
    if (inFlight != null) {
      return inFlight;
    }
    final future = _driver.listBlobNames(shard1, shard2);
    _blobNamesInFlight[key] = future;
    try {
      final names = await future;
      _blobNamesByShardKey[key] = names;
      return names;
    } finally {
      _blobNamesInFlight.remove(key);
    }
  }
}
