part of '../backup.dart';

class _WriterWorker {
  _WriterWorker({
    required this.maxConcurrentWrites,
    required this.blockTimeout,
    required this.logInterval,
    required this.backlogLimitBytes,
    required this.backlogClearBytes,
    required this.driverBufferedBytes,
    required this.onMetrics,
    required this.scheduleWrite,
    required this.handlePhysicalBytes,
    required this.logInfo,
    required this.isShardReady,
    this.waitForAnyShardReady,
    this.onBackpressureStart,
    this.onBackpressureEnd,
  });

  final int maxConcurrentWrites;
  final Duration blockTimeout;
  final Duration logInterval;
  final int backlogLimitBytes;
  final int backlogClearBytes;
  final int Function() driverBufferedBytes;
  final void Function(int queuedBytes, int inFlightBytes, int driverBufferedBytes) onMetrics;
  final Future<bool> Function(String hash, Uint8List bytes) scheduleWrite;
  final void Function(int bytes) handlePhysicalBytes;
  final void Function(String message) logInfo;
  final bool Function(String shardKey) isShardReady;
  final Future<void> Function()? waitForAnyShardReady;
  final void Function()? onBackpressureStart;
  final void Function()? onBackpressureEnd;

  final Map<String, List<_MissingBlock>> _writeQueues = {};
  final List<String> _shardQueue = [];
  var _queuedBlocks = 0;
  var _queuedBytes = 0;
  var _inFlightBytes = 0;
  var _writtenBlocks = 0;
  DateTime? _lastQueueStatsLogAt;

  bool _backpressureActive = false;
  Completer<void>? _backpressureWaiter;
  Completer<void>? _wakeWriter;
  bool _done = false;
  Object? _writerError;
  StackTrace? _writerStack;

  int backlogBytes() => _queuedBytes + _inFlightBytes + driverBufferedBytes();

  void enqueue(String hash, Uint8List bytes) {
    final shardKey = _shardKeyForHash(hash);
    final bucket = _writeQueues.putIfAbsent(shardKey, () {
      _shardQueue.add(shardKey);
      return <_MissingBlock>[];
    });
    bucket.add(_MissingBlock(hash, bytes));
    _queuedBlocks += 1;
    _queuedBytes += bytes.length;
    _reportMetrics();
    _checkBackpressure('enqueue');
    if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
      _wakeWriter!.complete();
      _wakeWriter = null;
    }
  }

  Future<void> waitForBackpressureClear() async {
    throwIfError();
    if (backlogBytes() > backlogLimitBytes) {
      _applyBackpressure('writer backlog');
    }
    if (!_backpressureActive) {
      return;
    }
    _backpressureWaiter ??= Completer<void>();
    await _backpressureWaiter!.future;
    throwIfError();
  }

  void signalDone() {
    _done = true;
    if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
      _wakeWriter!.complete();
      _wakeWriter = null;
    }
  }

  void throwIfError() {
    if (_writerError != null) {
      logInfo('hashblocks writer abort: $_writerError');
      Error.throwWithStackTrace(_writerError!, _writerStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      const maxBatchBlocks = 128;
      final writeFutures = <Future<bool>>[];
      final writeSizes = <Future<bool>, int>{};

      Future<_WriteResult> awaitAnyWrite() {
        return Future.any(
          writeFutures.map((future) {
            return future.then((wrote) => _WriteResult(future, wrote));
          }),
        );
      }

      Future<void> drainOne() async {
        if (writeFutures.isEmpty) {
          return;
        }
        final result = await awaitAnyWrite();
        writeFutures.remove(result.future);
        final size = writeSizes.remove(result.future) ?? 0;
        if (result.wrote && size > 0) {
          handlePhysicalBytes(size);
        }
        _writtenBlocks += 1;
        _inFlightBytes -= size;
        _reportMetrics();
        _checkBackpressure('writer');
        if (_writtenBlocks % 512 == 0) {
          final now = DateTime.now();
          if (_lastQueueStatsLogAt == null || now.difference(_lastQueueStatsLogAt!) >= logInterval) {
            logInfo('hashblocks queue stats: queued=$_queuedBlocks written=$_writtenBlocks backlogBytes=${backlogBytes()}');
            _lastQueueStatsLogAt = now;
          }
        }
      }

      Future<void> drainCompleted({bool force = false}) async {
        if (writeFutures.isEmpty) {
          return;
        }
        if (!force && writeFutures.length < maxConcurrentWrites) {
          return;
        }
        while (writeFutures.isNotEmpty && (force || writeFutures.length >= maxConcurrentWrites)) {
          await drainOne();
        }
      }

      while (!_done || _queuedBlocks > 0 || writeFutures.isNotEmpty) {
        if (_queuedBlocks == 0 && writeFutures.isEmpty) {
          _wakeWriter ??= Completer<void>();
          await _wakeWriter!.future;
        }
        while (_shardQueue.isNotEmpty) {
          final passSize = _shardQueue.length;
          var wroteThisPass = false;
          for (var i = 0; i < passSize; i += 1) {
            final shardKey = _shardQueue.removeAt(0);
            final bucket = _writeQueues[shardKey];
            if (bucket == null || bucket.isEmpty) {
              _writeQueues.remove(shardKey);
              continue;
            }
            if (!isShardReady(shardKey)) {
              _shardQueue.add(shardKey);
              continue;
            }
            var batchCount = 0;
            while (bucket.isNotEmpty && batchCount < maxBatchBlocks) {
              await drainCompleted();
              final entry = bucket.removeAt(0);
              _queuedBlocks -= 1;
              _queuedBytes -= entry.bytes.length;
              _inFlightBytes += entry.bytes.length;
              _reportMetrics();
              final future = scheduleWrite(entry.hash, entry.bytes).timeout(
                blockTimeout,
                onTimeout: () {
                  throw TimeoutException('hashblocks writer timeout after ${blockTimeout.inSeconds}s');
                },
              );
              writeFutures.add(future);
              writeSizes[future] = entry.bytes.length;
              batchCount += 1;
            }
            wroteThisPass = true;
            if (bucket.isNotEmpty) {
              _shardQueue.add(shardKey);
            } else {
              _writeQueues.remove(shardKey);
            }
          }
          if (!wroteThisPass && _queuedBlocks > 0) {
            if (waitForAnyShardReady != null) {
              await waitForAnyShardReady!();
            } else {
              await Future<void>.delayed(const Duration(milliseconds: 50));
            }
          }
        }
        await drainCompleted(force: true);
      }
    } catch (error, stackTrace) {
      _writerError = error;
      _writerStack = stackTrace;
      logInfo('hashblocks writer error: $error');
      if (_backpressureWaiter != null && !_backpressureWaiter!.isCompleted) {
        _backpressureWaiter!.complete();
      }
      if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
        _wakeWriter!.complete();
      }
    }
  }

  void _applyBackpressure(String reason) {
    if (_backpressureActive) {
      return;
    }
    _backpressureActive = true;
    _backpressureWaiter ??= Completer<void>();
    logInfo('sftp backpressure: writer backlog=${backlogBytes()} limit=$backlogLimitBytes ($reason)');
    onBackpressureStart?.call();
  }

  void _clearBackpressure(String reason) {
    if (!_backpressureActive) {
      return;
    }
    if (backlogBytes() > backlogClearBytes) {
      return;
    }
    _backpressureActive = false;
    if (_backpressureWaiter != null && !_backpressureWaiter!.isCompleted) {
      _backpressureWaiter!.complete();
    }
    _backpressureWaiter = null;
    logInfo('sftp backpressure cleared: writer backlog=${backlogBytes()} threshold=$backlogClearBytes ($reason)');
    onBackpressureEnd?.call();
  }

  void _checkBackpressure(String reason) {
    if (backlogBytes() > backlogLimitBytes) {
      _applyBackpressure(reason);
    } else if (_backpressureActive && backlogBytes() <= backlogClearBytes) {
      _clearBackpressure(reason);
    }
  }

  void _reportMetrics() {
    onMetrics(_queuedBytes, _inFlightBytes, driverBufferedBytes());
  }

  String _shardKeyForHash(String hash) {
    if (hash.length >= 4) {
      return hash.substring(0, 4);
    }
    return hash;
  }
}
