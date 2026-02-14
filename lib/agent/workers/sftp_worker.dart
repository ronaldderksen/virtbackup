part of '../backup.dart';

class _SftpWorker {
  _SftpWorker({
    required this.server,
    required this.sourcePath,
    required this.blockSize,
    required this.fileSize,
    required this.prefetchWindow,
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
  final int prefetchWindow;
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
    final window = max(1, prefetchWindow);
    final inFlight = <Future<void>>[];
    for (var offset = 0; offset < hashes.length; offset += 1) {
      ensureNotCanceled();
      inFlight.add(fetchMissingRange(startIndex + offset, <String>[hashes[offset]]));
      if (inFlight.length >= window) {
        await inFlight.removeAt(0);
      }
    }
    while (inFlight.isNotEmpty) {
      await inFlight.removeAt(0);
    }
  }

  Future<void> fetchMissingRange(int startIndex, List<String> hashes) async {
    if (hashes.isEmpty) {
      return;
    }
    final expectedHash = hashes.first;
    final rangeStartOffset = startIndex * blockSize;
    if (rangeStartOffset >= fileSize) {
      return;
    }
    final blockLength = min(blockSize, fileSize - rangeStartOffset);
    final blockBuffer = Uint8List(blockLength);
    var blockOffset = 0;
    Uint8List? directPayload;
    ensureNotCanceled();
    await streamRemoteRange(
      server,
      sourcePath,
      rangeStartOffset,
      blockLength,
      onChunk: (chunk) async {
        if (chunk.isEmpty || blockOffset >= blockLength || directPayload != null) {
          return;
        }
        if (blockOffset == 0 && chunk.length >= blockLength && chunk is Uint8List) {
          directPayload = Uint8List.sublistView(chunk, 0, blockLength);
          blockOffset = blockLength;
          markSftpRead(blockLength);
          handleBytes(blockLength);
          return;
        }
        final remaining = blockLength - blockOffset;
        final toCopy = chunk.length < remaining ? chunk.length : remaining;
        if (toCopy <= 0) {
          return;
        }
        markSftpRead(toCopy);
        handleBytes(toCopy);
        blockBuffer.setRange(blockOffset, blockOffset + toCopy, chunk, 0);
        blockOffset += toCopy;
      },
    );
    if (blockOffset <= 0) {
      logInfo('SFTP range returned no data offset=$rangeStartOffset length=$blockLength');
      return;
    }
    if (blockOffset < blockLength) {
      logInfo('SFTP short read offset=$rangeStartOffset expected=$blockLength got=$blockOffset');
    }
    final payload = directPayload ?? (blockOffset == blockLength ? blockBuffer : Uint8List.sublistView(blockBuffer, 0, blockOffset));
    await enqueueWriteBlock(expectedHash, payload);
    registerProgressBlocks(1);
  }
}
