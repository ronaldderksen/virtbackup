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
  final Future<void> Function(String hash, Uint8List bytes) scheduleWrite;
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
  DateTime? _lastLoopDebugLogAt;

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
      final writeFutures = <Future<void>>[];
      final writeSizes = <Future<void>, int>{};

      Future<Future<void>> awaitAnyWrite() {
        return Future.any(
          writeFutures.map((future) {
            return future.then((_) => future);
          }),
        );
      }

      Future<void> drainOne() async {
        if (writeFutures.isEmpty) {
          return;
        }
        final doneFuture = await awaitAnyWrite();
        writeFutures.remove(doneFuture);
        final size = writeSizes.remove(doneFuture) ?? 0;
        if (size > 0) {
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
        if (_queuedBlocks == 0) {
          if (writeFutures.isNotEmpty) {
            await drainOne();
            continue;
          }
          _wakeWriter ??= Completer<void>();
          await _wakeWriter!.future;
          continue;
        }
        var scheduled = false;
        final passSize = _shardQueue.length;
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
          await drainCompleted();
          if (writeFutures.length >= maxConcurrentWrites) {
            _shardQueue.add(shardKey);
            break;
          }
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
          scheduled = true;
          if (bucket.isNotEmpty) {
            _shardQueue.add(shardKey);
          } else {
            _writeQueues.remove(shardKey);
          }
        }
        if (scheduled) {
          _maybeLogLoopDebug('scheduled');
          continue;
        }
        if (writeFutures.isNotEmpty) {
          _maybeLogLoopDebug('drain-inflight');
          await drainOne();
          continue;
        }
        if (_queuedBlocks > 0) {
          _maybeLogLoopDebug('wait-shard-ready');
          if (waitForAnyShardReady != null) {
            // Avoid indefinite stalls when a ready signal is missed due to timing;
            // keep re-checking the queue at a short interval while work is pending.
            await Future.any<void>(<Future<void>>[waitForAnyShardReady!(), Future<void>.delayed(const Duration(milliseconds: 100))]);
          } else {
            await Future<void>.delayed(const Duration(milliseconds: 50));
          }
          continue;
        }
      }
      await drainCompleted(force: true);
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

  void _maybeLogLoopDebug(String reason) {
    final now = DateTime.now();
    if (_lastLoopDebugLogAt != null && now.difference(_lastLoopDebugLogAt!) < logInterval) {
      return;
    }
    _lastLoopDebugLogAt = now;
    var schedulableShards = 0;
    for (final shardKey in _shardQueue) {
      final bucket = _writeQueues[shardKey];
      if (bucket == null || bucket.isEmpty) {
        continue;
      }
      if (isShardReady(shardKey)) {
        schedulableShards += 1;
      }
    }
    logInfo(
      'writer debug: reason=$reason queuedBlocks=$_queuedBlocks queuedBytes=$_queuedBytes inFlightBytes=$_inFlightBytes shards=${_shardQueue.length} schedulableShards=$schedulableShards backpressure=$_backpressureActive',
    );
  }

  String _shardKeyForHash(String hash) {
    if (hash.length >= 4) {
      return hash.substring(0, 4);
    }
    return hash;
  }
}
