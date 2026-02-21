part of '../backup.dart';

class _WriterWorker {
  _WriterWorker({
    required this.maxConcurrentWrites,
    required this.logInterval,
    required this.maxRetryAttempts,
    required this.retryBaseDelay,
    required this.retryMaxDelay,
    required this.retryConcurrencyCooldown,
    required this.driverBufferedBytes,
    required this.onMetrics,
    required this.scheduleWrite,
    required this.handlePhysicalBytes,
    required this.onConcurrencyLimitChanged,
    required this.isWriteReady,
    this.waitForWriteReady,
  });

  final int maxConcurrentWrites;
  final Duration logInterval;
  final int maxRetryAttempts;
  final Duration retryBaseDelay;
  final Duration retryMaxDelay;
  final Duration retryConcurrencyCooldown;
  final int Function() driverBufferedBytes;
  final void Function(int queuedBytes, int inFlightBytes, int driverBufferedBytes) onMetrics;
  final Future<void> Function(String hash, Uint8List bytes) scheduleWrite;
  final void Function(int bytes) handlePhysicalBytes;
  final void Function(int concurrency) onConcurrencyLimitChanged;
  final bool Function() isWriteReady;
  final Future<void> Function()? waitForWriteReady;

  final Map<String, List<_MissingBlock>> _writeQueues = {};
  final List<String> _shardQueue = [];
  var _queuedBlocks = 0;
  var _queuedBytes = 0;
  var _inFlightBytes = 0;
  var _writtenBlocks = 0;
  late int _targetConcurrentWrites = maxConcurrentWrites;
  DateTime? _nextConcurrencyIncreaseAt;
  DateTime? _lastQueueStatsLogAt;
  DateTime? _lastLoopDebugLogAt;

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
    if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
      _wakeWriter!.complete();
      _wakeWriter = null;
    }
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
      LogWriter.logAgentSync(level: 'error', message: 'worker=writer abort: $_writerError');
      Error.throwWithStackTrace(_writerError!, _writerStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      _applyConcurrencyLimit(maxConcurrentWrites, reason: 'start');
      final writeFutures = <_InFlightWrite>[];

      Future<_InFlightWrite> awaitAnyWrite() {
        return Future.any(
          writeFutures.map((entry) {
            return entry.future.then((_) => entry).catchError((Object _) => entry);
          }),
        );
      }

      Future<void> drainOne() async {
        if (writeFutures.isEmpty) {
          return;
        }
        final doneWrite = await awaitAnyWrite();
        writeFutures.remove(doneWrite);
        final size = doneWrite.block.bytes.length;
        try {
          await doneWrite.future;
          if (size > 0) {
            handlePhysicalBytes(size);
          }
          _writtenBlocks += 1;
          if (_writtenBlocks % 512 == 0) {
            final now = DateTime.now();
            if (_lastQueueStatsLogAt == null || now.difference(_lastQueueStatsLogAt!) >= logInterval) {
              LogWriter.logAgentSync(level: 'info', message: 'worker=writer queue stats: queued=$_queuedBlocks written=$_writtenBlocks backlogBytes=${backlogBytes()}');
              _lastQueueStatsLogAt = now;
            }
          }
        } catch (error, stackTrace) {
          final retryAttempt = doneWrite.block.attempt + 1;
          if (retryAttempt > maxRetryAttempts) {
            LogWriter.logAgentSync(level: 'error', message: 'worker=writer write failed after retries hash=${doneWrite.block.hash} attempts=$retryAttempt error=$error');
            Error.throwWithStackTrace(error, stackTrace);
          }
          doneWrite.block.attempt = retryAttempt;
          doneWrite.block.retryAt = DateTime.now().add(_retryDelayForAttempt(doneWrite.block.attempt));
          _requeue(doneWrite.block);
          _registerRetryFailure();
          LogWriter.logAgentSync(
            level: 'warn',
            message:
                'worker=writer write retry scheduled hash=${doneWrite.block.hash} attempt=${doneWrite.block.attempt}/$maxRetryAttempts retryAt=${doneWrite.block.retryAt.toIso8601String()} error=$error',
          );
        }
        _inFlightBytes -= size;
        _reportMetrics();
      }

      Future<void> drainCompleted({bool force = false}) async {
        if (writeFutures.isEmpty) {
          return;
        }
        if (!force && writeFutures.length < _targetConcurrentWrites) {
          return;
        }
        while (writeFutures.isNotEmpty && (force || writeFutures.length >= _targetConcurrentWrites)) {
          await drainOne();
        }
      }

      while (!_done || _queuedBlocks > 0 || writeFutures.isNotEmpty) {
        _maybeRecoverConcurrency();
        if (!isWriteReady()) {
          if (waitForWriteReady != null) {
            await waitForWriteReady!();
          } else {
            await Future<void>.delayed(const Duration(milliseconds: 50));
          }
          continue;
        }
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
          await drainCompleted();
          if (writeFutures.length >= _targetConcurrentWrites) {
            _shardQueue.add(shardKey);
            break;
          }
          final next = bucket.first;
          if (next.retryAt.isAfter(DateTime.now())) {
            _shardQueue.add(shardKey);
            continue;
          }
          final entry = bucket.removeAt(0);
          _queuedBlocks -= 1;
          _queuedBytes -= entry.bytes.length;
          _inFlightBytes += entry.bytes.length;
          _reportMetrics();
          final future = scheduleWrite(entry.hash, entry.bytes);
          writeFutures.add(_InFlightWrite(block: entry, future: future));
          scheduled = true;
          if (bucket.isNotEmpty) {
            _shardQueue.add(shardKey);
          } else {
            _writeQueues.remove(shardKey);
          }
        }
        if (scheduled) {
          _maybeLogLoopDebug('scheduled', writeFutures.length);
          continue;
        }
        if (writeFutures.isNotEmpty) {
          _maybeLogLoopDebug('drain-inflight', writeFutures.length);
          await drainOne();
          continue;
        }
        if (_queuedBlocks > 0) {
          _maybeLogLoopDebug('wait-shard-ready', writeFutures.length);
          final waitDuration = _nextRetryWait();
          if (waitDuration == null) {
            await Future<void>.delayed(const Duration(milliseconds: 50));
          } else {
            await Future<void>.delayed(waitDuration);
          }
          continue;
        }
      }
      await drainCompleted(force: true);
    } catch (error, stackTrace) {
      _writerError = error;
      _writerStack = stackTrace;
      LogWriter.logAgentSync(level: 'error', message: 'worker=writer error: $error');
      if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
        _wakeWriter!.complete();
      }
    }
  }

  void _reportMetrics() {
    onMetrics(_queuedBytes, _inFlightBytes, driverBufferedBytes());
  }

  void _requeue(_MissingBlock block) {
    final shardKey = _shardKeyForHash(block.hash);
    final bucket = _writeQueues.putIfAbsent(shardKey, () {
      _shardQueue.add(shardKey);
      return <_MissingBlock>[];
    });
    bucket.add(block);
    _queuedBlocks += 1;
    _queuedBytes += block.bytes.length;
    if (_wakeWriter != null && !_wakeWriter!.isCompleted) {
      _wakeWriter!.complete();
      _wakeWriter = null;
    }
  }

  Duration _retryDelayForAttempt(int attempt) {
    if (attempt <= 1) {
      return retryBaseDelay;
    }
    var delay = retryBaseDelay;
    for (var i = 1; i < attempt; i += 1) {
      final doubledMs = delay.inMilliseconds * 2;
      if (doubledMs >= retryMaxDelay.inMilliseconds) {
        return retryMaxDelay;
      }
      delay = Duration(milliseconds: doubledMs);
    }
    return delay;
  }

  void _registerRetryFailure() {
    final reduced = max(1, _targetConcurrentWrites - 1);
    if (reduced < _targetConcurrentWrites) {
      _applyConcurrencyLimit(reduced, reason: 'retry-failure');
    }
    _nextConcurrencyIncreaseAt = DateTime.now().add(retryConcurrencyCooldown);
  }

  void _maybeRecoverConcurrency() {
    final next = _nextConcurrencyIncreaseAt;
    if (next == null) {
      return;
    }
    final now = DateTime.now();
    if (now.isBefore(next)) {
      return;
    }
    if (_targetConcurrentWrites < maxConcurrentWrites) {
      final increased = _targetConcurrentWrites + 1;
      _applyConcurrencyLimit(increased, reason: 'retry-recovery');
    }
    if (_targetConcurrentWrites < maxConcurrentWrites) {
      _nextConcurrencyIncreaseAt = now.add(retryConcurrencyCooldown);
      return;
    }
    _nextConcurrencyIncreaseAt = null;
  }

  void _applyConcurrencyLimit(int concurrency, {required String reason}) {
    final next = max(1, min(maxConcurrentWrites, concurrency));
    if (_targetConcurrentWrites == next && reason != 'start') {
      return;
    }
    final previous = _targetConcurrentWrites;
    _targetConcurrentWrites = next;
    onConcurrencyLimitChanged(_targetConcurrentWrites);
    LogWriter.logAgentSync(level: 'debug', message: 'worker=writer concurrency changed reason=$reason previous=$previous current=$_targetConcurrentWrites max=$maxConcurrentWrites');
  }

  Duration? _nextRetryWait() {
    DateTime? earliest;
    for (final shardKey in _shardQueue) {
      final bucket = _writeQueues[shardKey];
      if (bucket == null || bucket.isEmpty) {
        continue;
      }
      final retryAt = bucket.first.retryAt;
      if (earliest == null || retryAt.isBefore(earliest)) {
        earliest = retryAt;
      }
    }
    if (earliest == null) {
      return null;
    }
    final now = DateTime.now();
    if (!earliest.isAfter(now)) {
      return Duration.zero;
    }
    final wait = earliest.difference(now);
    return wait > const Duration(milliseconds: 250) ? const Duration(milliseconds: 250) : wait;
  }

  void _maybeLogLoopDebug(String reason, int inFlightWrites) {
    final now = DateTime.now();
    if (_lastLoopDebugLogAt != null && now.difference(_lastLoopDebugLogAt!) < const Duration(seconds: 1)) {
      return;
    }
    _lastLoopDebugLogAt = now;
    LogWriter.logAgentSync(
      level: 'debug',
      message: 'worker=writer debug: reason=$reason queuedBlocks=$_queuedBlocks queuedBytes=$_queuedBytes inFlightWrites=$inFlightWrites inFlightBytes=$_inFlightBytes queueDepth=$_queuedBlocks',
    );
  }

  String _shardKeyForHash(String hash) {
    if (hash.length >= 2) {
      return hash.substring(0, 2);
    }
    return hash;
  }
}

class _InFlightWrite {
  _InFlightWrite({required this.block, required this.future});

  final _MissingBlock block;
  final Future<void> future;
}
