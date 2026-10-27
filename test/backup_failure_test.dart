import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/agent/drv/filesystem_driver.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';

final _server = ServerConfig.fromMap({'id': 'test', 'type': 'ssh'});
final _vm = VmEntry(id: 'test', name: 'test', powerState: VmPowerState.stopped);

class _FailedCommandHost extends BackupAgentHost {
  final commands = <String>[];

  @override
  Future<SshCommandResult> runSshCommand(ServerConfig server, String command) async {
    commands.add(command);
    return SshCommandResult(stdout: '', stderr: 'simulated command failure', exitCode: 1);
  }
}

class _FailedWriter extends FilesystemBackupDriver {
  _FailedWriter(super.root) : super(blockSizeMB: 1);

  @override
  Future<void> writeBlob(String hash, List<int> bytes) async {
    throw const BackupWriteConflictMismatch('simulated unrecoverable write failure');
  }
}

void main() {
  late Directory root;
  setUp(() async {
    root = await Directory.systemTemp.createTemp('virtbackup-failure-test-');
    await LogWriter.configureSourcePath(source: 'agent', path: '${root.path}/agent.log');
    LogWriter.configureSourceLevel(source: 'agent', level: 'error');
  });
  tearDown(() async => root.delete(recursive: true));

  test('a failed remote snapshot is rejected before backup can continue', () async {
    final host = _FailedCommandHost();
    await expectLater(host.buildDependencies().createVmSnapshot(_server, _vm, 'test-snapshot'), throwsStateError);
    expect(host.commands.single, contains('virsh snapshot-create-as'));
  });

  test('a blob rename failure reaches the writer and removes its temporary file', () async {
    final driver = FilesystemBackupDriver(root.path, blockSizeMB: 1);
    final hash = 'a' * 64;
    await driver.ensureBlobDir(hash);
    await Directory(driver.blobFile(hash).path).create();
    await expectLater(driver.writeBlob(hash, [1, 2, 3]), throwsA(isA<FileSystemException>()));
    expect(await driver.blobFile(hash).exists(), isFalse);
    expect(await driver.blobFile(hash).parent.list().where((file) => file is File).toList(), isEmpty);
  });

  for (final failedStage in ['sftp', 'writer']) {
    test('$failedStage failure stops a throttled large backup without cancellation', () async {
      var limit = -1;
      var stopped = false;
      var readAttempted = false;
      final dependencies = BackupAgentDependencies(
        runSshCommand: (_, command) async => SshCommandResult(
          stdout: command.startsWith('virsh dumpxml')
              ? '<domain><name>test</name></domain>'
              : command.startsWith('virsh domstate')
              ? 'shut off'
              : 'image: /disk.qcow2',
          stderr: '',
          exitCode: 0,
        ),
        loadVmDiskPaths: (_, vm, {inactive = false}) async => [const MapEntry('vda', '/disk.qcow2')],
        downloadRemoteFile: (_, path, file, {onBytes}) async => fail('Unexpected full-file download'),
        streamRemoteFile: (_, path, {required onChunk, onBytes}) async => fail('Unexpected full-file stream'),
        streamRemoteRange: (_, path, offset, length, {required onChunk, onBytes}) async {
          readAttempted = true;
          if (failedStage == 'sftp') throw StateError('simulated SFTP failure');
          await onChunk(Uint8List(length));
        },
        createVmSnapshot: (_, vm, name) async => fail('Stopped VM must not be snapshotted'),
        commitVmSnapshot: (_, vm, disks) async => fail('Stopped VM must not be committed'),
        cleanupActiveOverlays: (_, vm, active, inactive) async {},
        sanitizeFileName: (name) => name,
        uploadLocalFile: (_, source, target, {onBytes}) async {},
        ensureHashblocks: (_) async => '/hashblocks',
        streamHashblocks: (_, executable, path, blockSize, {required onLine, onController}) async {
          onController!(
            HashblocksController((command) {
              if (command == 'STOP') stopped = true;
              if (command.startsWith('LIMIT ')) limit = int.parse(command.substring(6));
            }),
          );
          for (var index = 0; index < 2048 && !stopped;) {
            if (index > limit) {
              await Future<void>.delayed(const Duration(milliseconds: 1));
              continue;
            }
            await onLine('$index -> ${index.toRadixString(16).padLeft(64, '0')}');
            index++;
            await Future<void>.delayed(Duration.zero);
          }
          if (!stopped) await onLine('SHA256 ${'c' * 64}');
        },
        getRemoteFileSize: (_, path) async => 2 * 1024 * 1024 * 1024,
      );
      final agent = BackupAgent(dependencies: dependencies, onProgress: (_) {}, blockSizeBytes: 1024 * 1024, requireSimpleDisksForBackup: false);
      try {
        final driver = failedStage == 'writer' ? _FailedWriter(root.path) : FilesystemBackupDriver(root.path, blockSizeMB: 1);
        final result = await agent.runVmBackup(server: _server, vm: _vm, driver: driver).timeout(const Duration(seconds: 3));
        expect(readAttempted, isTrue);
        expect(stopped, isTrue);
        expect(result.success, isFalse);
        expect(result.canceled, isFalse);
        expect(result.message, contains(failedStage == 'writer' ? 'write failure' : 'SFTP failure'));
        final manifests = Directory('${root.path}/VirtBackup/manifests');
        expect(await manifests.exists(), isFalse);
      } finally {
        agent.cancel();
        agent.dispose();
      }
    });
  }
}
