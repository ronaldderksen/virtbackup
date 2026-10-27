import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/restore_worker.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

class _RestoreHost extends BackupAgentHost {
  _RestoreHost({required this.failCommand});
  final String? failCommand;
  final paths = <String>{};
  final commands = <String>[];

  @override
  Future<List<String>> missingRequiredRemoteTools(ServerConfig server) async => [];
  @override
  Future<void> beginLargeTransferSession(ServerConfig server) async {}
  @override
  Future<void> endLargeTransferSession(ServerConfig server) async {}
  @override
  Future<void> uploadLocalFile(ServerConfig server, String localPath, String remotePath, {void Function(int bytes)? onBytes}) async {}
  @override
  String sha256Hex(Uint8List bytes) => sha256.convert(bytes).toString();
  @override
  Future<String> uploadRemoteStream(ServerConfig server, String remotePath, Stream<List<int>> stream, {void Function(int bytes)? onBytes, bool Function()? isCanceled}) async {
    final bytes = <int>[];
    await for (final chunk in stream) {
      bytes.addAll(chunk);
      onBytes?.call(chunk.length);
    }
    paths.add(remotePath);
    return sha256.convert(bytes).toString();
  }

  @override
  Future<SshCommandResult> runSshCommand(ServerConfig server, String command) async {
    commands.add(command);
    final args = RegExp("'([^']*)'").allMatches(command).map((match) => match[1]!).toList();
    if (failCommand != null && command.startsWith(failCommand!)) return SshCommandResult(stdout: '', stderr: 'simulated remote failure', exitCode: 1);
    if (command.startsWith('virsh dominfo')) return SshCommandResult(stdout: '', stderr: 'not found', exitCode: 1);
    if (command.startsWith('test -e')) return SshCommandResult(stdout: '', stderr: '', exitCode: paths.contains(args.single) ? 0 : 1);
    if (command.startsWith('mv --')) {
      paths.remove(args.first);
      paths.add(args.last);
    }
    if (command.startsWith('stat -c')) return SshCommandResult(stdout: '3', stderr: '', exitCode: 0);
    return SshCommandResult(stdout: '', stderr: '', exitCode: 0);
  }
}

void main() {
  late Directory root;
  late AppSettings settings;
  late BackupStorage storage;
  late String xmlPath;
  const timestamp = '2026-10-26T12:00:00-1MB';
  final validHash = sha256.convert([1, 2, 3]).toString();

  Future<void> writeManifest({required String blockHash, required bool chain}) async {
    final directory = await Directory('${root.path}/VirtBackup/manifests/server/vm').create(recursive: true);
    xmlPath = '${directory.path}/${timestamp}__domain.xml';
    final xml = base64.encode(gzip.encode(utf8.encode('<domain><name>vm</name></domain>')));
    String disk(String name, String chainData) => 'disk_id: $name\nsource_path: /images/$name\nfile_size: 3\nchain:\n$chainData\nblocks: /images/$name\n0 -> $blockHash\ndisk_sha256: $validHash\n';
    await File('${directory.path}/$timestamp.manifest').writeAsString(
      'version: 1\nblock_size: 1048576\ndomain_xml_b64_gz: |\n  $xml\n${disk('top.qcow2', chain ? '  - order: 0\n    disk_id: top.qcow2\n    path: /images/top.qcow2\n  - order: 1\n    disk_id: base.qcow2\n    path: /images/base.qcow2' : '  []')}${chain ? disk('base.qcow2', '  []') : ''}',
    );
    final blob = File('${root.path}/VirtBackup/blobs/1/${blockHash.substring(0, 2)}/$blockHash');
    await blob.parent.create(recursive: true);
    await blob.writeAsBytes([1, 2, 3]);
  }

  Future<AgentJobStatus> run(String decision, _RestoreHost host) async {
    final receive = ReceivePort();
    final result = Completer<AgentJobStatus>();
    final finished = Completer<void>();
    receive.listen((message) {
      final payload = message as Map;
      if (payload['type'] == 'ready') {
        (payload['sendPort'] as SendPort).send({
          'type': 'start',
          'jobId': 'restore-test',
          'driverId': 'filesystem',
          'backupPath': root.path,
          'decision': decision,
          'xmlPath': xmlPath,
          'settings': settings.toMap(),
          'storage': storage.toMap(),
          'server': <String, dynamic>{},
        });
      }
      if (payload['type'] == 'result') result.complete(AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map)));
    });
    runRestoreWorker({'sendPort': receive.sendPort}, host: host, onFinished: finished.complete);
    try {
      final status = await result.future.timeout(const Duration(seconds: 5));
      await finished.future.timeout(const Duration(seconds: 5));
      return status;
    } finally {
      receive.close();
    }
  }

  setUp(() async {
    root = await Directory.systemTemp.createTemp('virtbackup-restore-failure-');
    storage = BackupStorage(id: 'filesystem', name: 'Filesystem', driverId: 'filesystem', enabled: true, params: {'path': root.path});
    settings = AppSettings.empty().copyWith(backupPath: root.path, storage: [storage], backupStorageId: 'filesystem');
  });
  tearDown(() async => root.delete(recursive: true));

  test('full check reports corrupt blob as failure', () async {
    await writeManifest(blockHash: 'b' * 64, chain: false);
    final result = await run('full_check', _RestoreHost(failCommand: null));
    expect(result.state, AgentJobState.failure);
    expect(result.message, contains('1 mismatch'));
  });

  test('full check succeeds for correct blob data', () async {
    await writeManifest(blockHash: validHash, chain: false);
    final result = await run('full_check', _RestoreHost(failCommand: null));
    expect(result.state, AgentJobState.success);
    expect(result.message, contains('OK'));
  });

  for (final decision in ['define', 'auto_rename']) {
    test('$decision fails when virsh define fails', () async {
      await writeManifest(blockHash: validHash, chain: false);
      final host = _RestoreHost(failCommand: 'virsh define');
      final result = await run(decision, host);
      expect(result.state, AgentJobState.failure);
      expect(result.message, contains('Cannot define restored VM'));
      expect(host.commands.any((command) => command.startsWith('virsh define')), isTrue);
    });
  }

  test('a failed chain rebase prevents defining the VM', () async {
    await writeManifest(blockHash: validHash, chain: true);
    final host = _RestoreHost(failCommand: 'qemu-img rebase');
    final result = await run('auto_rename', host);
    expect(result.state, AgentJobState.failure);
    expect(result.message, contains('Cannot rebase restored overlay'));
    expect(host.commands.any((command) => command.startsWith('virsh define')), isFalse);
  });
}
