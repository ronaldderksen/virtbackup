import 'dart:async';

import 'package:virtbackup/common/log_writer.dart';

class BlobCacheWorker {
  BlobCacheWorker({required this.initialize, required this.processHash});

  final Future<void> Function() initialize;
  final Future<void> Function(String hash) processHash;

  final List<String> _queue = [];
  Completer<void>? _wakeWorker;
  bool _done = false;
  bool _canceled = false;
  Object? _error;
  StackTrace? _errorStack;
  var _inFlightTasks = 0;
  Future<void>? _running;
  final Completer<void> _cancelSignal = Completer<void>();

  String get diagnosticState => 'queued=${_queue.length} inFlight=$_inFlightTasks done=$_done canceled=$_canceled';

  void enqueue(String hash) {
    if (_done || _canceled) {
      return;
    }
    if (hash.length < 2) {
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

  void cancel() {
    _canceled = true;
    _done = true;
    _queue.clear();
    if (!_cancelSignal.isCompleted) _cancelSignal.complete();
    if (_wakeWorker != null && !_wakeWorker!.isCompleted) {
      _wakeWorker!.complete();
      _wakeWorker = null;
    }
  }

  void throwIfError() {
    if (_canceled) {
      return;
    }
    if (_error != null) {
      LogWriter.logAgentSync(level: 'error', message: 'worker=blob_cache abort: $_error');
      Error.throwWithStackTrace(_error!, _errorStack ?? StackTrace.current);
    }
  }

  Future<void> run() => _running ??= _run();

  Future<void> finish({required Duration timeout, required Future<void> Function() abortPendingRequests}) async {
    signalDone();
    try {
      await run().timeout(timeout);
    } on TimeoutException {
      // Only background prefetch work remains here; required existence checks and writes have already finished.
      // Preserve errors observed before shutdown instead of disguising them as cancellation.
      final error = _canceled ? null : _error;
      final stackTrace = _errorStack;
      LogWriter.logAgentSync(level: 'warn', message: 'blob-cache shutdown exceeded ${timeout.inMilliseconds}ms; aborting pending directory requests.');
      cancel();
      Object? abortError;
      StackTrace? abortStack;
      try {
        await abortPendingRequests().timeout(timeout);
      } catch (closeError, closeStack) {
        abortError = closeError;
        abortStack = closeStack;
        LogWriter.logAgentSync(level: 'error', message: 'blob-cache request abort failed: $closeError');
      } finally {
        await run();
      }
      if (error != null) Error.throwWithStackTrace(error, stackTrace!);
      if (abortError != null) Error.throwWithStackTrace(abortError, abortStack!);
    }
    throwIfError();
  }

  Future<void> _run() async {
    try {
      LogWriter.logAgentSync(level: 'info', message: 'worker=blob_cache started');
      await Future.any<void>([initialize(), _cancelSignal.future]);
      if (_canceled) return;
      LogWriter.logAgentSync(level: 'info', message: 'worker=blob_cache initial scan done');

      while (!_canceled && (!_done || _queue.isNotEmpty || _inFlightTasks > 0)) {
        while (_queue.isNotEmpty) {
          if (_canceled) {
            _queue.clear();
            break;
          }
          final hash = _queue.removeAt(0);
          _inFlightTasks += 1;
          unawaited(() async {
            try {
              await processHash(hash);
            } catch (error, stackTrace) {
              if (!_canceled) {
                _error = error;
                _errorStack = stackTrace;
                LogWriter.logAgentSync(level: 'error', message: 'worker=blob_cache error: $error');
              }
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
      if (!_canceled) {
        _error = error;
        _errorStack = stackTrace;
        LogWriter.logAgentSync(level: 'error', message: 'worker=blob_cache failed: $error');
      }
    }
  }
}
