import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/http_server.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';

class _RenameHost extends BackupAgentHost {
  final paths = {'/images/a.qcow2', '/images/b.qcow2'};
  final commands = <String>[];

  @override
  Future<SshCommandResult> runSshCommand(ServerConfig server, String command) async {
    commands.add(command);
    final args = RegExp("'([^']*)'").allMatches(command).map((match) => match[1]!).toList();
    var output = '';
    var code = 0;
    if (command.startsWith('virsh domstate')) output = 'shut off';
    if (command.startsWith('virsh domblklist')) output = 'file disk vda /images/a.qcow2\nfile disk vdb /images/b.qcow2';
    if (command.startsWith('virsh dumpxml')) output = '<domain><name>vm</name><source file="/images/a.qcow2"/><source file="/images/b.qcow2"/></domain>';
    if (command.startsWith('qemu-img info')) output = 'image: ${args.single}';
    if (command.startsWith('test -e')) code = paths.contains(args.single) ? 0 : 1;
    if (command.startsWith('mv --')) {
      if (args.first == '/images/b.qcow2') {
        code = 1;
      } else {
        paths.remove(args.first);
        paths.add(args.last);
      }
    }
    return SshCommandResult(stdout: output, stderr: code == 1 ? 'simulated failure' : '', exitCode: code);
  }
}

class _UefiRenameHost extends BackupAgentHost {
  _UefiRenameHost({required this.failDefine});

  final bool failDefine;
  var nvramExists = true;
  final commands = <String>[];
  final definitions = <String>[];

  @override
  Future<void> uploadLocalFile(ServerConfig server, String localPath, String remotePath, {void Function(int)? onBytes}) async {
    definitions.add(await File(localPath).readAsString());
  }

  @override
  Future<SshCommandResult> runSshCommand(ServerConfig server, String command) async {
    commands.add(command);
    var output = '';
    var code = 0;
    if (command.startsWith('virsh domstate')) output = 'shut off';
    if (command.startsWith('virsh dominfo')) code = 1;
    if (command.startsWith('virsh domblklist')) output = 'file disk vda /images/a.qcow2';
    if (command.startsWith('virsh dumpxml')) {
      output = '<domain><name>vm</name><os><nvram>/vars/vm.fd</nvram></os><devices><disk type="file"><source file="/images/a.qcow2"/></disk></devices></domain>';
    }
    if (command.startsWith('qemu-img info')) output = 'image: /images/a.qcow2';
    if (command.startsWith('virsh undefine') && command.contains('--nvram')) nvramExists = false;
    if (failDefine && command == "virsh define '/var/tmp/virtbackup/rename-renamed.xml'") code = 1;
    return SshCommandResult(stdout: output, stderr: code == 1 ? 'simulated failure' : '', exitCode: code);
  }
}

void main() {
  test('failure moving the second disk rolls back the first disk and preserves the VM definition', () async {
    final directory = await Directory.systemTemp.createTemp('virtbackup-rename-test-');
    try {
      await LogWriter.configureSourcePath(source: 'agent', path: '${directory.path}/agent.log');
      LogWriter.configureSourceLevel(source: 'agent', level: 'error');
      final host = _RenameHost();
      final server = AgentHttpServer(host: host);
      await expectLater(
        server.renameVm(server: ServerConfig.fromMap({'id': 'test'}), vmName: 'vm', newVmName: 'vm', diskFileNamesByTarget: {'vda': 'renamed-a.qcow2', 'vdb': 'renamed-b.qcow2'}),
        throwsA(contains('simulated failure')),
      );
      expect(host.paths, {'/images/a.qcow2', '/images/b.qcow2'});
      expect(host.commands.any((command) => command.startsWith('virsh undefine')), isFalse);
      expect(host.commands, contains("mv -- '/images/renamed-a.qcow2' '/images/a.qcow2'"));
    } finally {
      await directory.delete(recursive: true);
    }
  });
  for (final failDefine in [false, true]) {
    test('UEFI VM rename preserves NVRAM ${failDefine ? 'during rollback' : 'on success'}', () async {
      final directory = await Directory.systemTemp.createTemp('virtbackup-uefi-rename-');
      try {
        await LogWriter.configureSourcePath(source: 'agent', path: '${directory.path}/agent.log');
        LogWriter.configureSourceLevel(source: 'agent', level: 'error');
        final host = _UefiRenameHost(failDefine: failDefine);
        final server = AgentHttpServer(host: host);
        final rename = server.renameVm(server: ServerConfig.fromMap({'id': 'test'}), vmName: 'vm', newVmName: 'renamed', diskFileNamesByTarget: {'vda': 'a.qcow2'});
        if (failDefine) {
          await expectLater(rename, throwsA(contains('simulated failure')));
          expect(host.commands, contains("virsh define '/var/tmp/virtbackup/rename-vm-rollback.xml'"));
        } else {
          await rename;
        }
        expect(host.nvramExists, isTrue);
        expect(host.commands, contains("virsh undefine 'vm' --keep-nvram"));
        expect(host.definitions, everyElement(contains('<nvram>/vars/vm.fd</nvram>')));
      } finally {
        await directory.delete(recursive: true);
      }
    });
  }
}
