part of '../backup.dart';

class _WriterWorker {
  _WriterWorker({
    required this.maxConcurrentWrites,
    required this.logInterval,
    required this.driverBufferedBytes,
    required this.onMetrics,
    required this.scheduleWrite,
    required this.handlePhysicalBytes,
    required this.isWriteReady,
    this.waitForWriteReady,
  });

  final int maxConcurrentWrites;
  final Duration logInterval;
  final int Function() driverBufferedBytes;
  final void Function(int queuedBytes, int inFlightBytes, int driverBufferedBytes) onMetrics;
  final Future<void> Function(String hash, Uint8List bytes) scheduleWrite;
  final void Function(int bytes) handlePhysicalBytes;
  final bool Function() isWriteReady;
  final Future<void> Function()? waitForWriteReady;

  final Map<String, List<_MissingBlock>> _writeQueues = {};
  final List<String> _shardQueue = [];
  var _queuedBlocks = 0;
  var _queuedBytes = 0;
  var _inFlightBytes = 0;
  var _writtenBlocks = 0;
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
        if (_writtenBlocks % 512 == 0) {
          final now = DateTime.now();
          if (_lastQueueStatsLogAt == null || now.difference(_lastQueueStatsLogAt!) >= logInterval) {
            LogWriter.logAgentSync(level: 'info', message: 'worker=writer queue stats: queued=$_queuedBlocks written=$_writtenBlocks backlogBytes=${backlogBytes()}');
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
          if (writeFutures.length >= maxConcurrentWrites) {
            _shardQueue.add(shardKey);
            break;
          }
          final entry = bucket.removeAt(0);
          _queuedBlocks -= 1;
          _queuedBytes -= entry.bytes.length;
          _inFlightBytes += entry.bytes.length;
          _reportMetrics();
          final future = scheduleWrite(entry.hash, entry.bytes);
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
          await Future<void>.delayed(const Duration(milliseconds: 50));
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
