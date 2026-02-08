import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

void main(List<String> args) async {
  final home = Platform.environment['HOME'];
  if (home == null || home.isEmpty) {
    stderr.writeln('HOME is not set.');
    exitCode = 1;
    return;
  }
  final root = Directory('$home/tmp/blob');
  await root.create(recursive: true);

  const blockSize = 8192;
  const totalBytesTarget = 10 * 1024 * 1024 * 1024;
  final rng = Random.secure();
  final buffer = Uint8List(blockSize);

  int bytesWritten = 0;
  int bytesSinceTick = 0;
  _fillRandom(rng, buffer);
  final stopwatch = Stopwatch()..start();
  Timer? ticker;
  ticker = Timer.periodic(const Duration(seconds: 1), (_) {
    final elapsed = stopwatch.elapsedMilliseconds / 1000.0;
    if (elapsed <= 0) {
      return;
    }
    final instant = bytesSinceTick.toDouble();
    bytesSinceTick = 0;
    final mbPerSec = (instant / (1024 * 1024));
    final totalGb = bytesWritten / (1024 * 1024 * 1024);
    stdout.writeln('written=${totalGb.toStringAsFixed(2)}GB speed=${mbPerSec.toStringAsFixed(1)}MB/s elapsed=${elapsed.toStringAsFixed(1)}s');
  });

  try {
    while (bytesWritten < totalBytesTarget) {
      final name = _randomHex(rng, 32);
      final tempPath = '${root.path}/$name.inprogress.${DateTime.now().microsecondsSinceEpoch}';
      final finalPath = '${root.path}/$name';
      final tempFile = File(tempPath);
      await tempFile.writeAsBytes(buffer, flush: false);
      await tempFile.rename(finalPath);
      bytesWritten += blockSize;
      bytesSinceTick += blockSize;
    }
  } finally {
    ticker.cancel();
  }
}

String _randomHex(Random rng, int length) {
  const chars = '0123456789abcdef';
  final sb = StringBuffer();
  for (var i = 0; i < length; i += 1) {
    sb.write(chars[rng.nextInt(chars.length)]);
  }
  return sb.toString();
}

void _fillRandom(Random rng, Uint8List buffer) {
  var offset = 0;
  while (offset < buffer.length) {
    final value = rng.nextInt(1 << 32);
    buffer[offset] = value & 0xff;
    if (offset + 1 < buffer.length) {
      buffer[offset + 1] = (value >> 8) & 0xff;
    }
    if (offset + 2 < buffer.length) {
      buffer[offset + 2] = (value >> 16) & 0xff;
    }
    if (offset + 3 < buffer.length) {
      buffer[offset + 3] = (value >> 24) & 0xff;
    }
    offset += 4;
  }
}
