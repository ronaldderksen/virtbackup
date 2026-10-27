import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

void main() {
  test('a native listing timeout invalidates the session, logs and retries through the Dart driver', () async {
    final project = Directory.current.path;
    final root = await Directory.systemTemp.createTemp('virtbackup-sftp-retry-');
    final platform = Platform.isMacOS ? 'macos' : 'linux';
    final extension = Platform.isMacOS ? 'dylib' : 'so';
    final nativeDir = await Directory('${root.path}/native/$platform').create(recursive: true);
    try {
      final build = await Process.run('make', ['-C', '$project/native', 'test-fixture', 'TEST_FIXTURE_OUT=${nativeDir.path}/libvirtbackup_native.$extension']);
      expect(build.exitCode, 0, reason: '${build.stdout}\n${build.stderr}');
      final probe = await Process.run('dart', ['--packages=$project/.dart_tool/package_config.json', '$project/test/support/sftp_listing_retry_probe.dart', root.path], workingDirectory: root.path);
      expect(probe.exitCode, 0, reason: '${probe.stdout}\n${probe.stderr}');
      expect(probe.stdout, contains('Native SFTP timeout recovered through Dart retry with a new session.'));
    } finally {
      await root.delete(recursive: true);
    }
  }, skip: !Platform.isLinux && !Platform.isMacOS);
}
