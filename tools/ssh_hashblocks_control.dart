import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

Future<void> main(List<String> args) async {
  final host = args.isNotEmpty ? args[0] : 'nuc04';
  final user = args.length > 1 ? args[1] : 'root';
  final home = Platform.environment['HOME'] ?? '';
  final keyPath = args.length > 2 ? args[2] : '$home/.ssh/id_rsa';
  final command = args.length > 3 ? args.sublist(3).join(' ') : '/var/tmp/hashblocks --read /var/lib/libvirt/images/rocky10-5.qcow2 --block-size 8192';

  final sshArgs = ['-i', keyPath, '$user@$host', command];
  final process = await Process.start('ssh', sshArgs);

  final stderrLines = process.stderr.transform(utf8.decoder).transform(const LineSplitter());

  stderrLines.listen((line) {
    if (line.isNotEmpty) {
      stderr.writeln('[stderr] $line');
    }
  });

  var totalBytes = 0;
  var totalLines = 0;
  var bytesSinceLog = 0;
  final start = DateTime.now();
  var lastLog = start;

  int countNewlines(Uint8List data) {
    var count = 0;
    for (final byte in data) {
      if (byte == 10) {
        count++;
      }
    }
    return count;
  }

  void logStats() {
    final now = DateTime.now();
    final windowMs = now.difference(lastLog).inMilliseconds;
    final totalMb = totalBytes / (1024 * 1024);
    if (windowMs <= 0 || bytesSinceLog == 0) {
      stdout.writeln('hashblocks stats: lines=$totalLines total=${totalMb.toStringAsFixed(1)}MB idle');
      return;
    }
    final windowMb = bytesSinceLog / (1024 * 1024);
    final mbPerSec = windowMb / (windowMs / 1000.0);
    stdout.writeln('hashblocks stats: lines=$totalLines total=${totalMb.toStringAsFixed(1)}MB speed=${mbPerSec.toStringAsFixed(1)}MB/s');
    bytesSinceLog = 0;
  }

  final statsTimer = Timer.periodic(const Duration(seconds: 5), (_) {
    logStats();
    lastLog = DateTime.now();
  });

  var bytesSinceControl = 0;
  final manifestFile = File('/var/tmp/manifest.txt');
  final manifestSink = manifestFile.openWrite();
  unawaited(() async {
    await for (final chunk in process.stdout) {
      final data = Uint8List.fromList(chunk);
      totalBytes += data.length;
      totalLines += countNewlines(data);
      bytesSinceLog += data.length;
      bytesSinceControl += data.length;
      manifestSink.add(data);
    }
    await manifestSink.flush();
    await manifestSink.close();
  }());

  void sendControl(String cmd) {
    process.stdin.writeln(cmd);
    stdout.writeln('[control] $cmd');
  }

  const targetBytesPerSec = 1 * 1024 * 1024;
  const controlTick = Duration(milliseconds: 200);
  var paused = false;
  var lastControl = DateTime.now();
  final throttleTimer = Timer.periodic(controlTick, (_) {
    final now = DateTime.now();
    final elapsedMs = now.difference(lastControl).inMilliseconds;
    if (elapsedMs <= 0) {
      return;
    }
    final currentRate = (bytesSinceControl * 1000) / elapsedMs;
    if (!paused && currentRate > targetBytesPerSec) {
      sendControl('PAUSE');
      paused = true;
    } else if (paused && currentRate < targetBytesPerSec * 0.8) {
      sendControl('RESUME');
      paused = false;
    }
    bytesSinceControl = 0;
    lastControl = now;
  });

  final exitCode = await process.exitCode;
  throttleTimer.cancel();
  statsTimer.cancel();
  logStats();
  stdout.writeln('hashblocks exit code: $exitCode');
}
