part of '../backup.dart';

class _HashblocksWorker {
  _HashblocksWorker({
    required this.sink,
    required this.blockSize,
    required this.fileSize,
    required this.maxNonZeroBlocks,
    required this.maxMissingRun,
    required this.logInfo,
    required this.ensureNotCanceled,
    required this.writeZeroRun,
    required this.parseZeroRange,
    required this.parseHashEntry,
    required this.bytesForRange,
    required this.blockLengthForIndex,
    required this.handleBytes,
    required this.handleHashblocksBytes,
    required this.registerProgressBlocks,
    required this.blobExists,
    required this.enqueueBlobDir,
    required this.enqueueMissingRun,
    required this.sendLimit,
    required this.updateLimitFromProgress,
  });

  final IOSink sink;
  final int blockSize;
  final int fileSize;
  final int maxNonZeroBlocks;
  final int maxMissingRun;
  final void Function(String message) logInfo;
  final void Function() ensureNotCanceled;
  final void Function(int start, int end) writeZeroRun;
  final (int, int)? Function(String line) parseZeroRange;
  final (int, String)? Function(String line) parseHashEntry;
  final int Function(int start, int end, int totalSize) bytesForRange;
  final int Function(int index, int totalSize) blockLengthForIndex;
  final void Function(int bytes) handleBytes;
  final void Function(int bytes) handleHashblocksBytes;
  final void Function(int blocks) registerProgressBlocks;
  final Future<bool> Function(String hash) blobExists;
  final void Function(String hash) enqueueBlobDir;
  final void Function(int startIndex, List<String> hashes) enqueueMissingRun;
  final void Function(int maxIndex, {bool force}) sendLimit;
  final void Function({bool force}) updateLimitFromProgress;

  var totalLines = 0;
  var zeroBlocks = 0;
  var existingBlocks = 0;
  var missingBlocks = 0;
  var lastHashblocksIndex = -1;
  var batchHold = false;

  final batchEntries = <_MissingEntry>[];
  var batchNonZeroCount = 0;

  var pendingMissingStart = -1;
  final pendingMissingHashes = <String>[];

  Future<void> handleLine(String line) async {
    ensureNotCanceled();
    totalLines++;
    final zeroRange = parseZeroRange(line);
    if (zeroRange != null) {
      writeZeroRun(zeroRange.$1, zeroRange.$2);
      final zeroCount = zeroRange.$2 - zeroRange.$1 + 1;
      zeroBlocks += zeroCount;
      final zeroBytes = bytesForRange(zeroRange.$1, zeroRange.$2, fileSize);
      handleHashblocksBytes(zeroBytes);
      handleBytes(zeroBytes);
      lastHashblocksIndex = zeroRange.$2;
      registerProgressBlocks(zeroCount);
      return;
    }
    final hashEntry = parseHashEntry(line);
    if (hashEntry == null) {
      return;
    }
    final index = hashEntry.$1;
    final hash = hashEntry.$2;
    sink.writeln('$index -> $hash');
    final blockLength = blockLengthForIndex(index, fileSize);
    handleHashblocksBytes(blockLength);
    batchEntries.add(_MissingEntry(index, hash));
    batchNonZeroCount += 1;
    lastHashblocksIndex = index;
    registerProgressBlocks(1);

    if (batchNonZeroCount >= maxNonZeroBlocks) {
      if (!batchHold) {
        batchHold = true;
        sendLimit(max(0, lastHashblocksIndex), force: true);
      }
      await _bulkCheckBatch();
      logStats();
      if (batchHold) {
        batchHold = false;
        updateLimitFromProgress(force: true);
      }
    }
  }

  Future<void> finishBatch() async {
    await _bulkCheckBatch();
  }

  void logStats({String? prefix}) {
    if (prefix == null || prefix.isEmpty) {
      logInfo('hashblocks stats: lines=$totalLines existing=$existingBlocks missing=$missingBlocks zero=$zeroBlocks');
      return;
    }
    logInfo('$prefix lines=$totalLines existing=$existingBlocks missing=$missingBlocks zero=$zeroBlocks');
  }

  Future<void> _bulkCheckBatch() async {
    if (batchEntries.isEmpty) {
      return;
    }
    for (final entry in batchEntries) {
      ensureNotCanceled();
      final exists = await blobExists(entry.hash);
      if (exists) {
        existingBlocks++;
        handleBytes(blockLengthForIndex(entry.index, fileSize));
        registerProgressBlocks(1);
        continue;
      }
      enqueueBlobDir(entry.hash);
      missingBlocks++;
      if (pendingMissingStart < 0) {
        pendingMissingStart = entry.index;
      } else {
        final expectedNext = pendingMissingStart + pendingMissingHashes.length;
        if (entry.index != expectedNext) {
          await _flushPendingMissing();
          pendingMissingStart = entry.index;
        }
      }
      pendingMissingHashes.add(entry.hash);
      if (pendingMissingHashes.length >= maxMissingRun) {
        await _flushPendingMissing();
      }
    }
    await _flushPendingMissing();
    batchEntries.clear();
    batchNonZeroCount = 0;
  }

  Future<void> _flushPendingMissing() async {
    if (pendingMissingHashes.isEmpty) {
      return;
    }
    final startIndex = pendingMissingStart;
    final hashes = List<String>.from(pendingMissingHashes);
    pendingMissingHashes.clear();
    pendingMissingStart = -1;
    enqueueMissingRun(startIndex, hashes);
  }
}
