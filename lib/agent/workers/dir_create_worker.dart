part of '../backup.dart';

class _DirCreateWorker {
  _DirCreateWorker({required this.ensureBlobDir, required this.logInfo});

  final Future<void> Function(String hash) ensureBlobDir;
  final void Function(String message) logInfo;

  final Set<String> _knownShards = {};
  final Set<String> _pendingShards = {};
  final Map<String, List<Completer<void>>> _waiters = {};
  final List<Completer<void>> _readyWaiters = [];
  final List<String> _queue = [];
  Completer<void>? _wakeWorker;
  bool _done = false;
  Object? _error;
  StackTrace? _errorStack;

  void enqueue(String hash) {
    _queueShard(hash);
  }

  Future<void> waitForShard(String hash) async {
    if (hash.length < 4) {
      return;
    }
    throwIfError();
    final shardKey = hash.substring(0, 4);
    if (_knownShards.contains(shardKey)) {
      return;
    }
    _queueShard(hash);
  }

  bool isShardReady(String shardKey) {
    return _knownShards.contains(shardKey);
  }

  void markReady(String shardKey) {
    if (_knownShards.contains(shardKey)) {
      return;
    }
    _knownShards.add(shardKey);
    _pendingShards.remove(shardKey);
    _queue.removeWhere((hash) => hash.length >= 4 && hash.substring(0, 4) == shardKey);
    if (_readyWaiters.isNotEmpty) {
      for (final waiter in _readyWaiters) {
        if (!waiter.isCompleted) {
          waiter.complete();
        }
      }
      _readyWaiters.clear();
    }
    final waiters = _waiters.remove(shardKey);
    if (waiters != null) {
      for (final waiter in waiters) {
        if (!waiter.isCompleted) {
          waiter.complete();
        }
      }
    }
  }

  Future<void> waitForAnyReady() async {
    throwIfError();
    final completer = Completer<void>();
    _readyWaiters.add(completer);
    return completer.future;
  }

  void _queueShard(String hash) {
    if (hash.length < 4) {
      return;
    }
    final shardKey = hash.substring(0, 4);
    if (_knownShards.contains(shardKey) || _pendingShards.contains(shardKey)) {
      return;
    }
    _pendingShards.add(shardKey);
    _queue.add(hash);
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
      logInfo('dir-create worker abort: $_error');
      Error.throwWithStackTrace(_error!, _errorStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      while (!_done || _queue.isNotEmpty) {
        if (_queue.isEmpty) {
          _wakeWorker ??= Completer<void>();
          await _wakeWorker!.future;
        }
        while (_queue.isNotEmpty) {
          final hash = _queue.removeAt(0);
          final shardKey = hash.substring(0, 4);
          try {
            await ensureBlobDir(hash);
            _knownShards.add(shardKey);
            if (_readyWaiters.isNotEmpty) {
              for (final waiter in _readyWaiters) {
                if (!waiter.isCompleted) {
                  waiter.complete();
                }
              }
              _readyWaiters.clear();
            }
            final waiters = _waiters.remove(shardKey);
            if (waiters != null) {
              for (final waiter in waiters) {
                if (!waiter.isCompleted) {
                  waiter.complete();
                }
              }
            }
          } catch (error, stackTrace) {
            _error = error;
            _errorStack = stackTrace;
            logInfo('dir-create worker error: $error');
            if (_readyWaiters.isNotEmpty) {
              for (final waiter in _readyWaiters) {
                if (!waiter.isCompleted) {
                  waiter.completeError(error, stackTrace);
                }
              }
              _readyWaiters.clear();
            }
            final waiters = _waiters.remove(shardKey);
            if (waiters != null) {
              for (final waiter in waiters) {
                if (!waiter.isCompleted) {
                  waiter.completeError(error, stackTrace);
                }
              }
            }
            return;
          } finally {
            _pendingShards.remove(shardKey);
          }
        }
      }
    } catch (error, stackTrace) {
      _error = error;
      _errorStack = stackTrace;
      logInfo('dir-create worker failed: $error');
    }
  }
}
