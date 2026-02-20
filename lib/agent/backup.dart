import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

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
    int? writerConcurrencyOverride,
  }) : _dependencies = dependencies,
       _onProgress = onProgress,
       _blockSize = blockSizeBytes,
       _onInfo = onInfo,
       _onError = onError,
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
  final Set<Future<void>> _inFlightWrites = {};
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
    _LocalManifestWrite? vmManifestWrite;
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
      final manifestsBase = Directory('${driver.storage}${Platform.pathSeparator}manifests${Platform.pathSeparator}$serverFolderName${Platform.pathSeparator}$vmFolderName');
      final timestampSeconds = DateTime.now().toIso8601String().split('.').first;
      final blockSizeMb = _blockSize ~/ (1024 * 1024);
      final backupTimestamp = '$timestampSeconds-${blockSizeMb}MB';

      final xmlResult = await _dependencies.runSshCommand(server, 'virsh dumpxml "${vm.name}"');
      if ((xmlResult.exitCode ?? 0) != 0) {
        throw xmlResult.stderr.isEmpty ? 'dumpxml failed' : xmlResult.stderr;
      }
      final xml = xmlResult.stdout.trim();
      if (xml.isEmpty) {
        throw 'dumpxml returned empty output';
      }
      vmManifestWrite = _startManifestWrite(driver: driver, serverFolderName: serverFolderName, vmFolderName: vmFolderName, backupTimestamp: backupTimestamp);
      final vmManifestSink = vmManifestWrite.sink;
      _writeManifestHeader(vmManifestSink, serverId: server.id, vmName: vm.name, backupTimestamp: backupTimestamp, domainXml: xml);

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
        for (final chainItem in plan.chain) {
          _ensureNotCanceled();
          final diskId = chainItem.diskId;
          final sourcePath = chainItem.sourcePath;
          _logInfo('Streaming disk ${_progress.completedDisks + 1}/${_progress.totalDisks} for ${vm.name}: $diskId');
          _setProgress(_progress.copyWith(statusMessage: 'Streaming ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
          _setProgress(_progress.copyWith(statusMessage: 'Backup ${_progress.completedDisks + 1} of ${_progress.totalDisks}: $diskId'));
          await _writeDiskUsingHashblocks(driver: driver, manifestSink: vmManifestSink, diskId: diskId, sourcePath: sourcePath, server: server, chain: plan.chain);
          _setProgress(_progress.copyWith(completedDisks: _progress.completedDisks + 1));
        }
      }

      _logInfo('Committing snapshot for ${vm.name}.');
      _ensureNotCanceled();
      _setProgress(_progress.copyWith(statusMessage: 'Committing snapshot...'));
      if (snapshotCreated) {
        await _dependencies.commitVmSnapshot(server, vm, disks);
      }
      vmManifestSink.writeln('EOF');
      await _closeManifestSinkIfOpen(vmManifestSink, manifestWrite: vmManifestWrite);
      await _finalizeManifestWrite(driver: driver, manifestWrite: vmManifestWrite);
      vmManifestWrite = null;

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
      if (vmManifestWrite != null) {
        await _abortManifestWrite(vmManifestWrite);
        vmManifestWrite = null;
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

  Future<void> _writeDiskUsingHashblocks({
    required BackupDriver driver,
    required IOSink manifestSink,
    required String diskId,
    required String sourcePath,
    required ServerConfig server,
    required List<_DiskChainItem> chain,
  }) async {
    final remoteHashblocksPath = await _dependencies.ensureHashblocks(server);
    _ensureNotCanceled();

    final fileSize = await _dependencies.getRemoteFileSize(server, sourcePath);
    final sink = manifestSink;
    sink.writeln('disk_id: $diskId');
    sink.writeln('source_path: $sourcePath');
    sink.writeln('file_size: $fileSize');
    _writeManifestChainMetadata(sink, chain: chain);
    sink.writeln('blocks: $sourcePath');

    var hashblocksBytes = 0;
    var hashblocksBytesSinceTick = 0;
    var hashblocksSmoothedSpeed = 0.0;
    DateTime? hashblocksLastTick;
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
    const limitUpdateInterval = Duration(seconds: 1);
    const limitStepBytes = 1024 * 1024 * 1024;
    final limitStepBlocks = max(1, (limitStepBytes + _blockSize - 1) ~/ _blockSize);
    const sftpReadLeadBytes = 1024 * 1024 * 1024;
    final sftpReadLeadBlocks = max(1, (sftpReadLeadBytes + _blockSize - 1) ~/ _blockSize);
    final maxFileLimitIndex = max(0, ((fileSize + _blockSize - 1) ~/ _blockSize) - 1);
    Timer? limitUpdateTimer;
    var lastLimitSent = -1;
    var sftpReadBytes = 0;
    var resolvedZeroOrExistingBlocks = 0;
    var sftpReachedDataEnd = false;
    var unlimitedLimitSent = false;

    late final _WriterWorker writerWorker;
    late final _SftpWorker sftpWorker;
    late final _ExistsWorker existsWorker;
    late final _HashblocksWorker hashblocksWorker;

    String limitProgressDetail({required bool includeStep}) {
      final stepPart = includeStep ? 'stepBytes=$limitStepBytes ' : '';
      return '${stepPart}readBytes=$sftpReadBytes resolvedBlocks=$resolvedZeroOrExistingBlocks readLeadBytes=$sftpReadLeadBytes';
    }

    void sendLimitCommand(HashblocksController value, int maxIndex, {required String reason, String? detail}) {
      final suffix = detail == null || detail.isEmpty ? '' : ' $detail';
      LogWriter.logAgentSync(level: 'debug', message: _formatLogMessage('hashblocks limit command: LIMIT $maxIndex reason=$reason (${_formatBytes(max(0, maxIndex + 1) * _blockSize)})$suffix') ?? '');
      value.setLimit(maxIndex);
    }

    void sendUnlimitedLimitCommand(HashblocksController value, {required String reason, String? detail}) {
      final suffix = detail == null || detail.isEmpty ? '' : ' $detail';
      LogWriter.logAgentSync(level: 'debug', message: _formatLogMessage('hashblocks limit command: LIMIT ${HashblocksController.unlimitedLimit} reason=$reason (unlimited)$suffix') ?? '');
      value.setUnlimitedLimit();
    }

    void maybeSendUnlimitedLimit({required String reason}) {
      if (unlimitedLimitSent) {
        return;
      }
      sftpReachedDataEnd = true;
      final currentController = controller;
      if (currentController == null) {
        return;
      }
      unlimitedLimitSent = true;
      sendUnlimitedLimitCommand(currentController, reason: reason, detail: limitProgressDetail(includeStep: false));
    }

    void maybeAdvanceLimit({required String reason}) {
      if (unlimitedLimitSent) {
        return;
      }
      final currentController = controller;
      if (currentController == null) {
        return;
      }
      final sftpReadBlocks = sftpReadBytes <= 0 ? 0 : ((sftpReadBytes + _blockSize - 1) ~/ _blockSize);
      final progressedBlocks = sftpReadBlocks + resolvedZeroOrExistingBlocks;
      final maxByProgress = max(0, progressedBlocks + sftpReadLeadBlocks - 1);
      final candidate = min(maxFileLimitIndex, min(lastLimitSent + limitStepBlocks, maxByProgress));
      if (candidate <= lastLimitSent) {
        if (lastLimitSent >= maxFileLimitIndex) {
          maybeSendUnlimitedLimit(reason: '$reason-eof');
        }
        return;
      }
      lastLimitSent = candidate;
      sendLimitCommand(currentController, candidate, reason: reason, detail: limitProgressDetail(includeStep: true));
      if (lastLimitSent >= maxFileLimitIndex) {
        maybeSendUnlimitedLimit(reason: '$reason-eof');
      }
    }

    void registerProgressBlocks(int count) {
      if (count <= 0) {
        return;
      }
      resolvedZeroOrExistingBlocks += count;
    }

    final missingQueue = <_MissingRun>[];
    Completer<void>? wakeMissingWorker;
    Completer<void>? wakeSftpReadResume;
    const sftpReadPauseThresholdBytes = 512 * 1024 * 1024;
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

    final cache = _blobDirectoryCache;
    writerWorker = _WriterWorker(
      maxConcurrentWrites: _writerConcurrencyOverride ?? max(1, driver.capabilities.maxConcurrentWrites),
      logInterval: agentLogInterval,
      driverBufferedBytes: () => driver.bufferedBytes,
      onMetrics: (queuedBytes, inFlightBytes, driverBufferedBytes) {
        _handleWriterMetrics(queuedBytes, inFlightBytes, driverBufferedBytes);
        if (queuedBytes <= sftpReadPauseThresholdBytes) {
          if (wakeSftpReadResume != null && !wakeSftpReadResume!.isCompleted) {
            wakeSftpReadResume!.complete();
            wakeSftpReadResume = null;
          }
        }
      },
      scheduleWrite: (hash, bytes) => _scheduleWriteBlob(hash, bytes, driver),
      handlePhysicalBytes: _handlePhysicalBytes,
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
        sftpReadBytes += bytes;
        if (!sftpStartLogged) {
          sftpStartLogged = true;
          _logInfo('missing: first sftp bytes received');
        }
      },
      enqueueWriteBlock: (hash, bytes) async {
        _blobDirectoryCache?.markHashKnown(hash);
        writerWorker.enqueue(hash, bytes);
      },
      ensureNotCanceled: _ensureNotCanceled,
      shouldPauseRead: () => _writerQueuedBytes > sftpReadPauseThresholdBytes,
      waitForReadResume: () async {
        wakeSftpReadResume ??= Completer<void>();
        await Future.any<void>(<Future<void>>[wakeSftpReadResume!.future, Future<void>.delayed(const Duration(milliseconds: 100))]);
        if (wakeSftpReadResume != null && wakeSftpReadResume!.isCompleted) {
          wakeSftpReadResume = null;
        }
      },
      onDataEnd: () {
        maybeSendUnlimitedLimit(reason: 'sftp-eof');
      },
    );

    const maxMissingRun = 1;

    existsWorker = _ExistsWorker(
      maxMissingRun: maxMissingRun,
      blobExists: _blobExists,
      enqueueMissingRun: enqueueMissingRun,
      handleBytes: _handleBytes,
      ensureNotCanceled: _ensureNotCanceled,
      onExisting: () {
        resolvedZeroOrExistingBlocks += 1;
        hashblocksWorker.markExisting();
      },
      onMissing: () {
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

    var manifestFlushed = false;

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
            return;
          }
          maybeAdvanceLimit(reason: 'initial');
          if (sftpReachedDataEnd) {
            maybeSendUnlimitedLimit(reason: 'controller-ready-after-sftp-eof');
          }
          limitUpdateTimer?.cancel();
          limitUpdateTimer = Timer.periodic(limitUpdateInterval, (_) {
            maybeAdvanceLimit(reason: 'progress');
          });
        },
        onLine: (line) async {
          try {
            _ensureNotCanceled();
            final trimmed = line.trim();
            if (trimmed.isEmpty || trimmed == 'EOF') {
              if (!eofSeen && trimmed == 'EOF') {
                eofSeen = true;
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
      handleHashblocksBytes(0);
      final doneMbPerSec = hashblocksSmoothedSpeed / (1024 * 1024);
      final doneTotalMb = hashblocksBytes / (1024 * 1024);
      _logInfo(
        'hashblocks done: lines=${hashblocksWorker.totalLines} existing=${hashblocksWorker.existingBlocks} missing=${hashblocksWorker.missingBlocks} zero=${hashblocksWorker.zeroBlocks} speed=${doneMbPerSec.toStringAsFixed(1)}MB/s total=${doneTotalMb.toStringAsFixed(1)}MB',
      );
      await sink.flush();
      manifestFlushed = true;
    } finally {
      if (!eofSeen) {
        try {
          controller?.stop();
        } catch (_) {}
      }
      if (_activeHashblocksController == controller) {
        _activeHashblocksController = null;
      }
      limitUpdateTimer?.cancel();
      if (wakeSftpReadResume != null && !wakeSftpReadResume!.isCompleted) {
        wakeSftpReadResume!.complete();
        wakeSftpReadResume = null;
      }
      existsWorker.signalDone();
      writerWorker.signalDone();
      if (writerFutureRef != null && !writerAwaited) {
        await writerFutureRef;
      }
      await _drainPendingWrites();
      if (!manifestFlushed) {
        await sink.flush();
      }
    }

    if (_firstSftpReadAt != null && _lastSftpReadAt != null) {
      final elapsedMs = _lastSftpReadAt!.difference(_firstSftpReadAt!).inMilliseconds;
      final elapsedSec = elapsedMs / 1000.0;
      _logInfo('SFTP read window: ${elapsedSec.toStringAsFixed(2)}s');
    }
  }

  _LocalManifestWrite _startManifestWrite({required BackupDriver driver, required String serverFolderName, required String vmFolderName, required String backupTimestamp}) {
    final baseName = '$backupTimestamp.manifest';
    final relativePath = 'manifests/$serverFolderName/$vmFolderName/$baseName.gz';
    final tempPath = '${driver.tmpDir().path}${Platform.pathSeparator}manifest_${DateTime.now().microsecondsSinceEpoch}.inprogress';
    final localFile = File(tempPath);
    localFile.parent.createSync(recursive: true);
    final sink = localFile.openWrite();
    return _LocalManifestWrite(sink: sink, localFile: localFile, relativePath: relativePath);
  }

  Future<void> _closeManifestSinkIfOpen(IOSink sink, {required _LocalManifestWrite manifestWrite}) async {
    if (manifestWrite.closed) {
      return;
    }
    await sink.flush();
    await sink.close();
    manifestWrite.closed = true;
  }

  Future<void> _finalizeManifestWrite({required BackupDriver driver, required _LocalManifestWrite manifestWrite}) async {
    if (!manifestWrite.closed) {
      await _closeManifestSinkIfOpen(manifestWrite.sink, manifestWrite: manifestWrite);
    }
    final localGzipFile = File('${manifestWrite.localFile.path}.gz');
    try {
      await _gzipFile(source: manifestWrite.localFile, target: localGzipFile);
      await driver.uploadFile(relativePath: manifestWrite.relativePath, localFile: localGzipFile);
    } finally {
      try {
        if (await manifestWrite.localFile.exists()) {
          await manifestWrite.localFile.delete();
        }
      } catch (_) {}
      try {
        if (await localGzipFile.exists()) {
          await localGzipFile.delete();
        }
      } catch (_) {}
    }
  }

  Future<void> _abortManifestWrite(_LocalManifestWrite manifestWrite) async {
    try {
      if (!manifestWrite.closed) {
        await manifestWrite.sink.flush();
        await manifestWrite.sink.close();
        manifestWrite.closed = true;
      }
    } catch (_) {}
    try {
      if (await manifestWrite.localFile.exists()) {
        await manifestWrite.localFile.delete();
      }
    } catch (_) {}
    final gzipFile = File('${manifestWrite.localFile.path}.gz');
    try {
      if (await gzipFile.exists()) {
        await gzipFile.delete();
      }
    } catch (_) {}
  }

  Future<void> _gzipFile({required File source, required File target}) async {
    final input = source.openRead();
    final output = target.openWrite();
    try {
      await output.addStream(input.transform(gzip.encoder));
    } finally {
      await output.close();
    }
  }

  Future<void> _scheduleWriteBlob(String hash, Uint8List bytes, BackupDriver driver) async {
    _ensureNotCanceled();
    _blobDirectoryCache?.markHashKnown(hash);
    final future = driver.writeBlob(hash, bytes);
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

  void _writeManifestHeader(IOSink sink, {required String serverId, required String vmName, required String backupTimestamp, required String domainXml}) {
    sink.writeln('version: 1');
    sink.writeln('block_size: $_blockSize');
    sink.writeln('server_id: $serverId');
    sink.writeln('vm_name: $vmName');
    sink.writeln('timestamp: $backupTimestamp');
    sink.writeln('meta:');
    sink.writeln('  format: inline_v1');
    sink.writeln('  domain_xml_encoding: base64+gzip');
    sink.writeln('  chain_encoding: yaml');
    sink.writeln('domain_xml_b64_gz: |');
    final encodedXml = _encodeGzipBase64(domainXml);
    for (var start = 0; start < encodedXml.length; start += 120) {
      final end = start + 120 < encodedXml.length ? start + 120 : encodedXml.length;
      sink.writeln('  ${encodedXml.substring(start, end)}');
    }
    sink.writeln();
  }

  void _writeManifestChainMetadata(IOSink sink, {required List<_DiskChainItem> chain}) {
    sink.writeln('chain:');
    if (chain.isEmpty) {
      sink.writeln('  []');
      return;
    }
    for (var index = 0; index < chain.length; index += 1) {
      final item = chain[index];
      sink.writeln('  - order: $index');
      sink.writeln('    disk_id: ${item.diskId}');
      sink.writeln('    path: ${item.sourcePath}');
    }
  }

  String _encodeGzipBase64(String value) {
    final bytes = utf8.encode(value);
    final compressed = gzip.encode(bytes);
    return base64.encode(compressed);
  }
}

class _SpeedSample {
  _SpeedSample({required this.at, required this.logicalTotal, required this.physicalTotal});

  final DateTime at;
  final int logicalTotal;
  final int physicalTotal;
}

class _LocalManifestWrite {
  _LocalManifestWrite({required this.sink, required this.localFile, required this.relativePath});

  final IOSink sink;
  final File localFile;
  final String relativePath;
  bool closed = false;
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
