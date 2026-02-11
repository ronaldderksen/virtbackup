part of '../backup.dart';

class _BlobCacheWorker {
  _BlobCacheWorker({required this.initialize, required this.processHash, required this.logInfo});

  final Future<void> Function() initialize;
  final Future<void> Function(String hash) processHash;
  final void Function(String message) logInfo;

  final List<String> _queue = [];
  Completer<void>? _wakeWorker;
  bool _done = false;
  Object? _error;
  StackTrace? _errorStack;
  var _inFlightTasks = 0;

  void enqueue(String hash) {
    if (hash.length < 4) {
      return;
    }
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
      logInfo('blob-cache worker abort: $_error');
      Error.throwWithStackTrace(_error!, _errorStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      logInfo('blob-cache worker started');
      await initialize();
      logInfo('blob-cache worker initial scan done');

      while (!_done || _queue.isNotEmpty || _inFlightTasks > 0) {
        while (_queue.isNotEmpty) {
          final hash = _queue.removeAt(0);
          _inFlightTasks += 1;
          unawaited(() async {
            try {
              await processHash(hash);
            } catch (error, stackTrace) {
              _error = error;
              _errorStack = stackTrace;
              logInfo('blob-cache worker error: $error');
            } finally {
              _inFlightTasks -= 1;
            }
          }());
        }
        if (_queue.isEmpty) {
          _wakeWorker ??= Completer<void>();
          await Future.any<void>(<Future<void>>[_wakeWorker!.future, Future<void>.delayed(const Duration(milliseconds: 100))]);
        }
      }
    } catch (error, stackTrace) {
      _error = error;
      _errorStack = stackTrace;
      logInfo('blob-cache worker failed: $error');
    }
  }
}
