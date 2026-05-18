import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/common/log_writer.dart';

void main() {
  test('agent job logging mirrors scoped lines to a job log file', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_log_writer_test_');
    addTearDown(() async {
      if (await tempDir.exists()) {
        await tempDir.delete(recursive: true);
      }
    });

    final agentLog = File('${tempDir.path}${Platform.pathSeparator}agent.log');
    await LogWriter.configureSourcePath(source: 'agent', path: agentLog.path);
    LogWriter.configureSourceLevel(source: 'agent', level: 'info');

    LogWriter.withJobLogging('job/123', () {
      LogWriter.logAgentSync(level: 'info', message: 'job scoped log line');
    });

    final jobLog = File('${tempDir.path}${Platform.pathSeparator}agent-job-job_123.log');
    expect(await agentLog.readAsString(), contains('job scoped log line'));
    expect(await jobLog.readAsString(), contains('job scoped log line'));
  });

  test('agent json job logs are written as json lines', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_log_writer_json_test_');
    addTearDown(() async {
      if (await tempDir.exists()) {
        await tempDir.delete(recursive: true);
      }
    });

    final agentLog = File('${tempDir.path}${Platform.pathSeparator}agent.log');
    await LogWriter.configureSourcePath(source: 'agent', path: agentLog.path);
    LogWriter.configureSourceLevel(source: 'agent', level: 'info');

    LogWriter.logAgentJsonSync(level: 'info', jobId: 'job-456', fields: <String, Object?>{'event': 'job_result', 'jobId': 'job-456', 'type': 'backup', 'state': 'success'});

    final jobLog = File('${tempDir.path}${Platform.pathSeparator}agent-job-job-456.log');
    final line = (await jobLog.readAsLines()).single;
    final messageMarker = line.indexOf(' message=');
    expect(line, contains(' level=info message='));
    expect(messageMarker, greaterThan(0));
    final decoded = jsonDecode(line.substring(messageMarker + ' message='.length)) as Map<String, dynamic>;
    expect(decoded.containsKey('timestamp'), isFalse);
    expect(decoded.containsKey('level'), isFalse);
    expect(decoded['event'], 'job_result');
    expect(decoded['jobId'], 'job-456');
    expect(decoded['type'], 'backup');
    expect(decoded['state'], 'success');
  });
}
