part of '../backup.dart';

class _BlobCacheWorker {
  _BlobCacheWorker({required this.prefetch, required this.logInfo, required this.logInterval});

  final Future<void> Function(String hash) prefetch;
  final void Function(String message) logInfo;
  final Duration logInterval;

  final Set<String> _pendingShards = {};
  final List<String> _queue = [];
  Completer<void>? _wakeWorker;
  bool _done = false;
  Object? _error;
  StackTrace? _errorStack;
  DateTime? _lastQueueLogAt;
  var _processed = 0;

  void enqueue(String hash) {
    if (hash.length < 4) {
      return;
    }
    final shardKey = hash.substring(0, 4);
    if (_pendingShards.contains(shardKey)) {
      return;
    }
    _pendingShards.add(shardKey);
    _queue.add(hash);
    _maybeLogQueue();
    if (_wakeWorker != null && !_wakeWorker!.isCompleted) {
      _wakeWorker!.complete();
      _wakeWorker = null;
    }
  }

  void signalDone() {
    _done = true;
    if (_wakeWorker != null && !_wakeWorker!.isCompleted) {
      _wakeWorker!.complete();
      _wakeWorker = null;
    }
  }

  void throwIfError() {
    if (_error != null) {
      logInfo('blob-cache worker abort: $_error');
      Error.throwWithStackTrace(_error!, _errorStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      logInfo('blob-cache worker started');
      const concurrency = 4;
      while (!_done || _queue.isNotEmpty) {
        if (_queue.isEmpty) {
          _wakeWorker ??= Completer<void>();
          await _wakeWorker!.future;
        }
        if (_queue.isEmpty) {
          continue;
        }
        final batch = <String>[];
        while (_queue.isNotEmpty && batch.length < concurrency) {
          batch.add(_queue.removeAt(0));
        }
        final workers = List<Future<void>>.generate(batch.length, (index) async {
          final hash = batch[index];
          final shardKey = hash.substring(0, 4);
          try {
            await prefetch(hash);
          } catch (error, stackTrace) {
            _error = error;
            _errorStack = stackTrace;
            logInfo('blob-cache worker error: $error');
            return;
          } finally {
            _pendingShards.remove(shardKey);
            _processed += 1;
          }
        });
        await Future.wait(workers);
        _maybeLogQueue();
      }
    } catch (error, stackTrace) {
      _error = error;
      _errorStack = stackTrace;
      logInfo('blob-cache worker failed: $error');
    }
  }

  void _maybeLogQueue() {
    final now = DateTime.now();
    if (_lastQueueLogAt != null && now.difference(_lastQueueLogAt!) < logInterval) {
      return;
    }
    _lastQueueLogAt = now;
    logInfo('blob-cache queue: pending=${_pendingShards.length} queued=${_queue.length} processed=$_processed');
  }
}
