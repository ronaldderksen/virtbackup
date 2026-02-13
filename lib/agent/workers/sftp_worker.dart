part of '../backup.dart';

class _SftpWorker {
  _SftpWorker({
    required this.server,
    required this.sourcePath,
    required this.blockSize,
    required this.fileSize,
    required this.maxRemoteReadBytes,
    required this.rangeSizeBytes,
    required this.rangeConcurrency,
    required this.streamRemoteRange,
    required this.handleBytes,
    required this.markSftpRead,
    required this.registerProgressBlocks,
    required this.enqueueWriteBlock,
    required this.ensureNotCanceled,
    required this.logInfo,
  });

  final ServerConfig server;
  final String sourcePath;
  final int blockSize;
  final int fileSize;
  final int maxRemoteReadBytes;
  final int rangeSizeBytes;
  final int rangeConcurrency;
  final RemoteRangeStreamer streamRemoteRange;
  final void Function(int bytes) handleBytes;
  final void Function(int bytes) markSftpRead;
  final void Function(int blocks) registerProgressBlocks;
  final Future<void> Function(String hash, Uint8List bytes) enqueueWriteBlock;
  final void Function() ensureNotCanceled;
  final void Function(String message) logInfo;

  Future<void> fetchMissingRun(int startIndex, List<String> hashes) async {
    if (hashes.isEmpty) {
      return;
    }
    final blocksPerRange = max(1, rangeSizeBytes ~/ blockSize);
    final ranges = <_MissingRange>[];
    var offset = 0;
    while (offset < hashes.length) {
      final remaining = hashes.length - offset;
      final take = remaining < blocksPerRange ? remaining : blocksPerRange;
      ranges.add(_MissingRange(startIndex + offset, hashes.sublist(offset, offset + take)));
      offset += take;
    }
    final workerCount = min(rangeConcurrency, ranges.length);
    var nextIndex = 0;
    Future<void> worker() async {
      while (true) {
        final index = nextIndex;
        if (index >= ranges.length) {
          return;
        }
        nextIndex += 1;
        final range = ranges[index];
        await fetchMissingRange(range.startIndex, range.hashes);
      }
    }

    await Future.wait(List<Future<void>>.generate(workerCount, (_) => worker()));
  }

  Future<void> fetchMissingRange(int startIndex, List<String> hashes) async {
    if (hashes.isEmpty) {
      return;
    }
    final rangeStartOffset = startIndex * blockSize;
    final rangeEndExclusive = min(fileSize, rangeStartOffset + (hashes.length * blockSize));
    final rangeLength = rangeEndExclusive - rangeStartOffset;
    var bytesConsumed = 0;
    var hashIndex = 0;
    final blockBuffer = Uint8List(blockSize);
    var blockOffset = 0;

    Future<void> processChunk(List<int> chunk) async {
      handleBytes(chunk.length);
      var offset = 0;
      while (offset < chunk.length) {
        final remaining = blockSize - blockOffset;
        final toCopy = (chunk.length - offset) < remaining ? (chunk.length - offset) : remaining;
        blockBuffer.setRange(blockOffset, blockOffset + toCopy, chunk, offset);
        blockOffset += toCopy;
        offset += toCopy;
        if (blockOffset == blockSize) {
          final expectedHash = hashes[hashIndex];
          await enqueueWriteBlock(expectedHash, Uint8List.fromList(blockBuffer));
          hashIndex++;
          blockOffset = 0;
          registerProgressBlocks(1);
        }
      }
    }

    while (bytesConsumed < rangeLength) {
      ensureNotCanceled();
      final remaining = rangeLength - bytesConsumed;
      final chunkLength = remaining > maxRemoteReadBytes ? maxRemoteReadBytes : remaining;
      var bytesRead = 0;
      await streamRemoteRange(
        server,
        sourcePath,
        rangeStartOffset + bytesConsumed,
        chunkLength,
        onChunk: (chunk) async {
          bytesRead += chunk.length;
          markSftpRead(chunk.length);
          await processChunk(chunk);
        },
      );
      if (bytesRead <= 0) {
        logInfo('SFTP range returned no data offset=${rangeStartOffset + bytesConsumed} length=$chunkLength');
        break;
      }
      bytesConsumed += bytesRead;
    }

    if (blockOffset > 0 && hashIndex < hashes.length) {
      final tail = Uint8List.sublistView(blockBuffer, 0, blockOffset);
      final expectedHash = hashes[hashIndex];
      await enqueueWriteBlock(expectedHash, Uint8List.fromList(tail));
      hashIndex++;
      registerProgressBlocks(1);
    }
  }
}

class _MissingRange {
  _MissingRange(this.startIndex, this.hashes);

  final int startIndex;
  final List<String> hashes;
}
