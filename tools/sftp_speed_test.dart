import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh3/dartssh3.dart';
import 'package:ffi/ffi.dart';

class _Args {
  _Args({
    required this.host,
    required this.port,
    required this.user,
    required this.path,
    required this.interval,
    required this.useSshBinary,
    required this.parallelReads,
    required this.rangeBytes,
    required this.useScpNative,
    this.keyPath,
    this.password,
    this.scpLibPath,
  });

  final String host;
  final int port;
  final String user;
  final String path;
  final Duration interval;
  final bool useSshBinary;
  final int parallelReads;
  final int rangeBytes;
  final bool useScpNative;
  final String? keyPath;
  final String? password;
  final String? scpLibPath;
}

void main(List<String> args) async {
  final parsed = _parseArgs(args);
  if (parsed == null) {
    _printUsage();
    exitCode = 64;
    return;
  }

  SSHSocket? socket;
  SSHClient? client;
  SftpClient? sftp;
  SftpFile? remoteFile;
  Timer? logTimer;
  final totalTimer = Stopwatch()..start();
  var bytesSinceLast = 0;
  var totalBytes = 0;
  var cpuSecondsAtStart = 0.0;
  final cpuEnabled = Platform.isLinux;
  String? clientCipher;
  String? serverCipher;

  void logSpeed() {
    final mb = totalBytes / (1024 * 1024);
    final mbPerSec = (bytesSinceLast / (parsed.interval.inMilliseconds / 1000)) / (1024 * 1024);
    if (!cpuEnabled) {
      stdout.writeln('SFTP read: ${mbPerSec.toStringAsFixed(1)}MB/s total=${mb.toStringAsFixed(1)}MB');
    } else {
      final cpuSeconds = _readCpuSeconds();
      final cpuDelta = cpuSeconds - cpuSecondsAtStart;
      stdout.writeln('SFTP read: ${mbPerSec.toStringAsFixed(1)}MB/s total=${mb.toStringAsFixed(1)}MB cpu=${cpuDelta.toStringAsFixed(2)}s');
    }
    bytesSinceLast = 0;
  }

  logTimer = Timer.periodic(parsed.interval, (_) => logSpeed());

  void logCiphersIfAny() {
    if (clientCipher != null || serverCipher != null) {
      stdout.writeln('SSH cipher: client=$clientCipher server=$serverCipher');
    }
  }

  try {
    if (cpuEnabled) {
      cpuSecondsAtStart = _readCpuSeconds();
    }
    if (parsed.useScpNative) {
      _runScpNative(parsed);
    } else if (parsed.useSshBinary) {
      await _runSshBinaryStream(parsed, (chunk) {
        bytesSinceLast += chunk.length;
        totalBytes += chunk.length;
      });
    } else {
      socket = await SSHSocket.connect(parsed.host, parsed.port);
      final identities = await _loadIdentities(parsed.keyPath);
      client = SSHClient(
        socket,
        username: parsed.user,
        identities: identities,
        onPasswordRequest: parsed.password == null ? null : () => parsed.password,
        printDebug: (line) {
          if (line == null) {
            return;
          }
          if (line.contains('SSHTransport._clientCipherType:')) {
            clientCipher ??= line.split(':').last.trim();
          } else if (line.contains('SSHTransport._serverCipherType:')) {
            serverCipher ??= line.split(':').last.trim();
          }
        },
      );
      sftp = await client.sftp();
      logCiphersIfAny();
      final stat = await sftp.stat(parsed.path);
      final size = stat.size;
      if (size == null || size <= 0) {
        throw StateError('Unable to stat remote file size for ${parsed.path}');
      }
      if (parsed.parallelReads <= 1) {
        remoteFile = await sftp.open(parsed.path);
        final stream = remoteFile.read();
        await for (final chunk in stream) {
          bytesSinceLast += chunk.length;
          totalBytes += chunk.length;
        }
      } else {
        var nextOffset = 0;
        int takeNextOffset() {
          final current = nextOffset;
          nextOffset += parsed.rangeBytes;
          return current;
        }

        final workers = <Future<void>>[];
        for (var i = 0; i < parsed.parallelReads; i++) {
          workers.add(() async {
            final workerFile = await sftp!.open(parsed.path);
            try {
              while (true) {
                final offset = takeNextOffset();
                if (offset >= size) {
                  break;
                }
                final length = (offset + parsed.rangeBytes > size) ? (size - offset) : parsed.rangeBytes;
                final stream = workerFile.read(length: length, offset: offset);
                await for (final chunk in stream) {
                  bytesSinceLast += chunk.length;
                  totalBytes += chunk.length;
                }
              }
            } finally {
              await workerFile.close();
            }
          }());
        }
        await Future.wait(workers);
      }
    }
    logSpeed();
    final elapsedSec = totalTimer.elapsedMilliseconds / 1000;
    if (cpuEnabled) {
      final cpuSeconds = _readCpuSeconds() - cpuSecondsAtStart;
      stdout.writeln('Done. Read ${(totalBytes / (1024 * 1024)).toStringAsFixed(1)}MB in ${elapsedSec.toStringAsFixed(1)}s cpu=${cpuSeconds.toStringAsFixed(2)}s');
    } else {
      stdout.writeln('Done. Read ${(totalBytes / (1024 * 1024)).toStringAsFixed(1)}MB in ${elapsedSec.toStringAsFixed(1)}s');
    }
  } finally {
    logTimer.cancel();
    try {
      await remoteFile?.close();
    } catch (_) {}
    try {
      sftp?.close();
    } catch (_) {}
    try {
      client?.close();
    } catch (_) {}
    try {
      socket?.close();
    } catch (_) {}
  }
}

double _readCpuSeconds() {
  if (!Platform.isLinux) {
    return 0.0;
  }
  try {
    final content = File('/proc/self/stat').readAsStringSync();
    final parts = content.split(' ');
    if (parts.length <= 15) {
      return 0.0;
    }
    final utime = int.parse(parts[13]);
    final stime = int.parse(parts[14]);
    const ticksPerSecond = 100;
    return (utime + stime) / ticksPerSecond;
  } catch (_) {
    return 0.0;
  }
}

Future<List<SSHKeyPair>?> _loadIdentities(String? keyPath) async {
  if (keyPath == null || keyPath.isEmpty) {
    return null;
  }
  final file = File(keyPath);
  final pem = await file.readAsString();
  return SSHKeyPair.fromPem(pem);
}

_Args? _parseArgs(List<String> args) {
  var host = 'nuc04';
  var port = 22;
  var user = 'root';
  var path = '/var/lib/libvirt/images/win10.qcow2';
  var interval = const Duration(seconds: 5);
  var useSshBinary = false;
  var parallelReads = 1;
  var rangeBytes = 4 * 1024 * 1024;
  var useScpNative = false;
  String? scpLibPath;
  String? keyPath = Platform.environment['HOME'] == null ? null : '${Platform.environment['HOME']}/.ssh/id_rsa';
  String? password;

  for (var i = 0; i < args.length; i++) {
    final arg = args[i];
    switch (arg) {
      case '--host':
        host = _readValue(args, ++i, arg);
        break;
      case '--port':
        port = int.parse(_readValue(args, ++i, arg));
        break;
      case '--user':
        user = _readValue(args, ++i, arg);
        break;
      case '--path':
        path = _readValue(args, ++i, arg);
        break;
      case '--interval':
        interval = Duration(seconds: int.parse(_readValue(args, ++i, arg)));
        break;
      case '--mode':
        final mode = _readValue(args, ++i, arg).toLowerCase();
        if (mode == 'ssh') {
          useSshBinary = true;
          useScpNative = false;
        } else if (mode == 'sftp') {
          useSshBinary = false;
          useScpNative = false;
        } else if (mode == 'scp') {
          useSshBinary = false;
          useScpNative = true;
        } else {
          stderr.writeln('Unknown mode: $mode (use sftp, ssh, or scp)');
          return null;
        }
        break;
      case '--parallel':
        parallelReads = int.parse(_readValue(args, ++i, arg));
        if (parallelReads < 1) {
          parallelReads = 1;
        }
        break;
      case '--range-mb':
        rangeBytes = int.parse(_readValue(args, ++i, arg)) * 1024 * 1024;
        if (rangeBytes <= 0) {
          rangeBytes = 4 * 1024 * 1024;
        }
        break;
      case '--key':
        keyPath = _readValue(args, ++i, arg);
        break;
      case '--password':
        password = _readValue(args, ++i, arg);
        break;
      case '--scp-lib':
        scpLibPath = _readValue(args, ++i, arg);
        break;
      case '--help':
      case '-h':
        return null;
      default:
        stderr.writeln('Unknown arg: $arg');
        return null;
    }
  }

  return _Args(
    host: host,
    port: port,
    user: user,
    path: path,
    interval: interval,
    useSshBinary: useSshBinary,
    parallelReads: parallelReads,
    rangeBytes: rangeBytes,
    useScpNative: useScpNative,
    keyPath: keyPath,
    password: password,
    scpLibPath: scpLibPath,
  );
}

String _readValue(List<String> args, int index, String flag) {
  if (index >= args.length) {
    throw ArgumentError('Missing value for $flag');
  }
  return args[index];
}

void _printUsage() {
  stdout.writeln('Usage: dart run tools/sftp_speed_test.dart [options]');
  stdout.writeln('Options:');
  stdout.writeln('  --host <host>       Default nuc04');
  stdout.writeln('  --port <port>       Default 22');
  stdout.writeln('  --user <user>       Default root');
  stdout.writeln('  --path <path>       Default /var/lib/libvirt/images/win10.qcow2');
  stdout.writeln('  --interval <sec>    Default 5');
  stdout.writeln('  --mode <sftp|ssh|scp> Default sftp');
  stdout.writeln('  --parallel <n>      Parallel range reads (sftp mode), default 1');
  stdout.writeln('  --range-mb <n>      Range size in MB (sftp mode), default 4');
  stdout.writeln('  --key <path>        Default ~/.ssh/id_rsa');
  stdout.writeln('  --password <pass>   Optional password');
  stdout.writeln('  --scp-lib <path>    Path to libscp_speed.so (scp mode)');
}

Future<void> _runSshBinaryStream(_Args args, void Function(Uint8List chunk) onChunk) async {
  final sshArgs = <String>[];
  if (args.keyPath != null && args.keyPath!.isNotEmpty) {
    sshArgs.addAll(['-i', args.keyPath!]);
  }
  sshArgs.add('${args.user}@${args.host}');
  sshArgs.add('cat');
  sshArgs.add(args.path);
  final process = await Process.start('ssh', sshArgs);
  await for (final chunk in process.stdout) {
    onChunk(Uint8List.fromList(chunk));
  }
  final exitCode = await process.exitCode;
  if (exitCode != 0) {
    final stderrText = await utf8.decodeStream(process.stderr);
    throw StateError('ssh failed: $exitCode $stderrText');
  }
}

void _runScpNative(_Args args) {
  final libPath = args.scpLibPath ?? Platform.environment['SCP_LIB'] ?? '${Directory.current.path}/native/linux/libvirtbackup_native.so';
  final lib = DynamicLibrary.open(libPath);
  final func = lib.lookupFunction<_ScpSpeedTestC, _ScpSpeedTestDart>('vb_scp_speed_test');

  final host = args.host.toNativeUtf8();
  final user = args.user.toNativeUtf8();
  final key = (args.keyPath ?? '').toNativeUtf8();
  final pass = (args.password ?? '').toNativeUtf8();
  final path = args.path.toNativeUtf8();
  final rc = func(host, args.port, user, key, pass, path, args.interval.inSeconds);
  calloc.free(host);
  calloc.free(user);
  calloc.free(key);
  calloc.free(pass);
  calloc.free(path);
  if (rc != 0) {
    throw StateError('scp_speed_test failed with code $rc');
  }
}

typedef _ScpSpeedTestC = Int32 Function(Pointer<Utf8> host, Int32 port, Pointer<Utf8> user, Pointer<Utf8> keyPath, Pointer<Utf8> password, Pointer<Utf8> remotePath, Int32 intervalSec);

typedef _ScpSpeedTestDart = int Function(Pointer<Utf8> host, int port, Pointer<Utf8> user, Pointer<Utf8> keyPath, Pointer<Utf8> password, Pointer<Utf8> remotePath, int intervalSec);
