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
}
