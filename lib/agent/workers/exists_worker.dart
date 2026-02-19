part of '../backup.dart';

class _ExistsWorker {
  _ExistsWorker({
    required this.maxMissingRun,
    required this.blobExists,
    required this.enqueueMissingRun,
    required this.handleBytes,
    required this.registerProgressBlocks,
    required this.ensureNotCanceled,
    required this.onExisting,
    required this.onMissing,
  });

  final int maxMissingRun;
  final Future<bool> Function(String hash) blobExists;
  final void Function(int startIndex, List<String> hashes) enqueueMissingRun;
  final void Function(int bytes) handleBytes;
  final void Function(int blocks) registerProgressBlocks;
  final void Function() ensureNotCanceled;
  final void Function() onExisting;
  final void Function() onMissing;

  final List<_MissingEntry> _queue = <_MissingEntry>[];
  Completer<void>? _wakeWorker;
  bool _done = false;
  Object? _error;
  StackTrace? _errorStack;

  var _pendingMissingStart = -1;
  final List<String> _pendingMissingHashes = <String>[];

  void enqueue(int index, String hash, int blockLength) {
    _queue.add(_MissingEntry(index, hash, blockLength));
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
      LogWriter.logAgentSync(level: 'error', message: 'worker=exists abort: $_error');
      Error.throwWithStackTrace(_error!, _errorStack ?? StackTrace.current);
    }
  }

  Future<void> run() async {
    try {
      while (!_done || _queue.isNotEmpty) {
        while (_queue.isNotEmpty) {
          ensureNotCanceled();
          final entry = _queue.removeAt(0);
          final exists = await blobExists(entry.hash);
          if (exists) {
            onExisting();
            handleBytes(entry.blockLength);
            registerProgressBlocks(1);
            await _flushPendingMissingIfGap(entry.index);
            continue;
          }
          onMissing();
          registerProgressBlocks(1);
          if (_pendingMissingStart < 0) {
            _pendingMissingStart = entry.index;
          } else {
            final expectedNext = _pendingMissingStart + _pendingMissingHashes.length;
            if (entry.index != expectedNext) {
              await _flushPendingMissing();
              _pendingMissingStart = entry.index;
            }
          }
          _pendingMissingHashes.add(entry.hash);
          if (_pendingMissingHashes.length >= maxMissingRun) {
            await _flushPendingMissing();
          }
        }
        if (_queue.isEmpty && !_done) {
          _wakeWorker ??= Completer<void>();
          await _wakeWorker!.future;
        }
      }
      await _flushPendingMissing();
    } catch (error, stackTrace) {
      _error = error;
      _errorStack = stackTrace;
      LogWriter.logAgentSync(level: 'error', message: 'worker=exists failed: $error');
    }
  }

  Future<void> _flushPendingMissingIfGap(int index) async {
    if (_pendingMissingStart < 0 || _pendingMissingHashes.isEmpty) {
      return;
    }
    final expectedNext = _pendingMissingStart + _pendingMissingHashes.length;
    if (index != expectedNext) {
      return;
    }
    await _flushPendingMissing();
  }

  Future<void> _flushPendingMissing() async {
    if (_pendingMissingHashes.isEmpty) {
      return;
    }
    final startIndex = _pendingMissingStart;
    final hashes = List<String>.from(_pendingMissingHashes);
    _pendingMissingHashes.clear();
    _pendingMissingStart = -1;
    enqueueMissingRun(startIndex, hashes);
  }
}
