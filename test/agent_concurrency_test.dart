import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

import 'support/agent_concurrency_probe.dart';

void main() {
  for (final mode in ['jobs', 'guards', 'schedule', 'schedule-failure', 'account-refresh', 'account-changed', 'account-rejected', 'account-disconnected', 'account-retry']) {
    test('agent orchestration regression: $mode', () async {
      final project = Directory.current.path;
      final root = await Directory.systemTemp.createTemp('virtbackup-agent-concurrency-');
      try {
        final source = await File('$project/lib/agent/http_server.dart').readAsString();
        final fixture = await File('${root.path}/probe.dart').writeAsString('$source\n$agentConcurrencyProbe');
        final result = await Process.run('dart', ['--packages=$project/.dart_tool/package_config.json', fixture.path, root.path, mode]);
        expect(result.exitCode, 0, reason: '${result.stdout}\n${result.stderr}');
      } finally {
        await root.delete(recursive: true);
      }
    });
  }
}
