import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/instance_lock.dart';

void main() {
  late Directory root;
  late File settings;
  late File file;
  late Directory probeDirectory;
  late String probePath;

  setUpAll(() async {
    probeDirectory = await Directory.systemTemp.createTemp('virtbackup-lock-probe-');
    probePath = '${probeDirectory.path}/probe${Platform.isWindows ? '.exe' : ''}';
    final compiled = await Process.run('dart', ['compile', 'exe', 'test/support/agent_lock_probe.dart', '-o', probePath]);
    expect(compiled.exitCode, 0, reason: '${compiled.stdout}\n${compiled.stderr}');
  });
  tearDownAll(() async => probeDirectory.delete(recursive: true));
  setUp(() async {
    root = await Directory.systemTemp.createTemp('virtbackup-lock-test-');
    settings = File('${root.path}/agent.yaml');
    file = File('${root.path}/agent.lock');
  });
  tearDown(() async => root.delete(recursive: true));

  void writeOwner({required String hostname, required int processId, String token = 'old-owner'}) {
    file.writeAsStringSync(jsonEncode({'version': 1, 'hostname': hostname, 'pid': processId, 'token': token}), flush: true);
  }

  test('acquires beside settings before config is created and releases only its own lock', () {
    final messages = <String>[];
    final lock = AgentInstanceLock.acquire(settingsFile: settings, onMessage: messages.add);
    final data = jsonDecode(file.readAsStringSync());
    expect(data['hostname'], Platform.localHostname);
    expect(data['pid'], pid);
    expect(data['token'], isNotEmpty);
    expect(settings.existsSync(), isFalse);
    expect(messages.single, contains('Acquired agent lockfile'));
    lock.release();
    lock.release();
    expect(file.existsSync(), isFalse);
    expect(File('${file.path}.guard').existsSync(), isTrue);
    expect(root.listSync().whereType<Directory>(), isEmpty);
    expect(messages.last, contains('Removed agent lockfile'));
  });

  test('a running local PID blocks startup and preserves ownership', () {
    final lock = AgentInstanceLock.acquire(settingsFile: settings, onMessage: (_) {});
    final original = file.readAsStringSync();
    try {
      expect(
        () => AgentInstanceLock.acquire(settingsFile: settings, onMessage: (_) {}),
        throwsA(isA<AgentInstanceLockException>().having((error) => error.message, 'message', contains('running PID $pid'))),
      );
      expect(file.readAsStringSync(), original);
    } finally {
      lock.release();
    }
  });

  test('a remote hostname blocks startup even when its PID is absent locally', () {
    writeOwner(hostname: '${Platform.localHostname}-remote', processId: 2147483647);
    final original = file.readAsStringSync();
    expect(
      () => AgentInstanceLock.acquire(settingsFile: settings, onMessage: (_) {}),
      throwsA(isA<AgentInstanceLockException>().having((error) => error.message, 'message', contains('Remote locks are never removed'))),
    );
    expect(file.readAsStringSync(), original);
  });

  test('an abruptly killed local agent leaves a recoverable lock and reports stale removal', () async {
    final child = await _launch(probePath, root.path);
    expect(child.status['status'], 'acquired');
    try {
      child.process.kill(ProcessSignal.sigkill);
      await child.process.exitCode;
      expect(file.existsSync(), isTrue);
      final messages = <String>[];
      final lock = AgentInstanceLock.acquire(settingsFile: settings, onMessage: messages.add);
      try {
        expect(messages.first, contains('Removed stale lockfile'));
        expect(messages.first, contains('PID ${child.process.pid}'));
        expect(messages.first, contains('Continuing agent startup'));
        expect(jsonDecode(file.readAsStringSync())['pid'], pid);
      } finally {
        lock.release();
      }
    } finally {
      child.process.kill();
      await child.process.exitCode;
      await child.stderr;
    }
  }, skip: Platform.isWindows);

  for (final contents in ['', 'not json', '{"version":1,"hostname":"host","pid":0,"token":"x"}', '{"version":1,"hostname":"host","pid":-1,"token":"x"}']) {
    test('unverifiable lock metadata is preserved: $contents', () {
      file.writeAsStringSync(contents);
      expect(
        () => AgentInstanceLock.acquire(settingsFile: settings, onMessage: (_) {}),
        throwsA(isA<AgentInstanceLockException>().having((error) => error.message, 'message', contains('cannot be verified'))),
      );
      expect(file.readAsStringSync(), contents);
    });
  }

  test('release never removes replacement ownership', () {
    final lock = AgentInstanceLock.acquire(settingsFile: settings, onMessage: (_) {});
    writeOwner(hostname: Platform.localHostname, processId: pid, token: 'replacement');
    final replacement = file.readAsStringSync();
    expect(lock.release, throwsA(isA<AgentInstanceLockException>().having((error) => error.message, 'message', contains('ownership changed'))));
    expect(file.readAsStringSync(), replacement);
  });

  for (final stale in [false, true]) {
    test('simultaneous processes have one owner ${stale ? 'when reclaiming a stale lock' : 'on first startup'}', () async {
      if (stale) {
        final finished = await Process.start(probePath, [root.path, 'exit']);
        await finished.exitCode;
        writeOwner(hostname: Platform.localHostname, processId: finished.pid);
      }
      final children = await Future.wait(List.generate(8, (_) => _launch(probePath, root.path)));
      try {
        final owners = children.where((child) => child.status['status'] == 'acquired').toList();
        expect(owners.length, 1, reason: children.map((child) => child.status).join('\n'));
        expect(jsonDecode(file.readAsStringSync())['pid'], owners.single.process.pid);
        for (final child in children.where((child) => child.status['status'] == 'blocked')) {
          expect(await child.process.exitCode, 2);
        }
        owners.single.process.stdin.writeln('stop');
        await owners.single.process.stdin.close();
        expect(await owners.single.process.exitCode, 0);
        expect(file.existsSync(), isFalse);
        if (stale) expect(await owners.single.stderr, contains('Removed stale lockfile'));
      } finally {
        for (final child in children) {
          child.process.kill();
          await child.process.exitCode;
          await child.stderr;
        }
      }
    });
  }

  test('a clean signal shutdown removes the primary lockfile', () async {
    final child = await _launch(probePath, root.path);
    try {
      expect(child.status['status'], 'acquired');
      child.process.kill(ProcessSignal.sigint);
      expect(await child.process.exitCode, 0);
      expect(file.existsSync(), isFalse);
      expect(await child.stderr, contains('Removed agent lockfile'));
    } finally {
      child.process.kill();
      await child.process.exitCode;
      await child.stderr;
    }
  }, skip: Platform.isWindows);
}

Future<({Process process, Map<String, dynamic> status, Future<String> stderr})> _launch(String executable, String directory) async {
  final process = await Process.start(executable, [directory, 'hold']);
  final errors = process.stderr.transform(utf8.decoder).join();
  final line = await process.stdout.transform(utf8.decoder).transform(const LineSplitter()).first;
  return (process: process, status: jsonDecode(line) as Map<String, dynamic>, stderr: errors);
}
