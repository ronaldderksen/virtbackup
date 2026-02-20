part of '../backup.dart';

class _HashblocksWorker {
  _HashblocksWorker({
    required this.sink,
    required this.fileSize,
    required this.ensureNotCanceled,
    required this.writeZeroRun,
    required this.parseZeroRange,
    required this.parseHashEntry,
    required this.bytesForRange,
    required this.blockLengthForIndex,
    required this.handleBytes,
    required this.handleHashblocksBytes,
    required this.registerProgressBlocks,
    required this.prefetchBlob,
    required this.enqueueExists,
  });

  final IOSink sink;
  final int fileSize;
  final void Function() ensureNotCanceled;
  final void Function(int start, int end) writeZeroRun;
  final (int, int)? Function(String line) parseZeroRange;
  final (int, String)? Function(String line) parseHashEntry;
  final int Function(int start, int end, int totalSize) bytesForRange;
  final int Function(int index, int totalSize) blockLengthForIndex;
  final void Function(int bytes) handleBytes;
  final void Function(int bytes) handleHashblocksBytes;
  final void Function(int blocks) registerProgressBlocks;
  final void Function(String hash) prefetchBlob;
  final void Function(int index, String hash, int blockLength) enqueueExists;

  var totalLines = 0;
  var zeroBlocks = 0;
  var existingBlocks = 0;
  var missingBlocks = 0;
  var lastHashblocksIndex = -1;

  Future<void> handleLine(String line) async {
    ensureNotCanceled();
    totalLines++;
    final zeroRange = parseZeroRange(line);
    if (zeroRange != null) {
      writeZeroRun(zeroRange.$1, zeroRange.$2);
      final zeroCount = zeroRange.$2 - zeroRange.$1 + 1;
      zeroBlocks += zeroCount;
      registerProgressBlocks(zeroCount);
      final zeroBytes = bytesForRange(zeroRange.$1, zeroRange.$2, fileSize);
      handleHashblocksBytes(zeroBytes);
      handleBytes(zeroBytes);
      lastHashblocksIndex = zeroRange.$2;
      return;
    }
    final hashEntry = parseHashEntry(line);
    if (hashEntry == null) {
      return;
    }
    final index = hashEntry.$1;
    final hash = hashEntry.$2;
    sink.writeln('$index -> $hash');
    prefetchBlob(hash);
    final blockLength = blockLengthForIndex(index, fileSize);
    handleHashblocksBytes(blockLength);
    enqueueExists(index, hash, blockLength);
    lastHashblocksIndex = index;
  }

  void logStats({String? prefix}) {
    if (prefix == null || prefix.isEmpty) {
      LogWriter.logAgentSync(level: 'info', message: 'worker=hashblocks stats: lines=$totalLines existing=$existingBlocks missing=$missingBlocks zero=$zeroBlocks');
      return;
    }
    LogWriter.logAgentSync(level: 'info', message: 'worker=hashblocks $prefix lines=$totalLines existing=$existingBlocks missing=$missingBlocks zero=$zeroBlocks');
  }

  void markExisting() {
    existingBlocks += 1;
  }

  void markMissing() {
    missingBlocks += 1;
  }
}
