import 'dart:ffi';
import 'dart:io';

import 'package:virtbackup/agent/drv/sftp_driver.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

typedef _CountNative = Int32 Function();
typedef _CountDart = int Function();

void _check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final root = args.single;
  final libraryPath = Platform.isMacOS ? 'native/macos/libvirtbackup_native.dylib' : 'native/linux/libvirtbackup_native.so';
  final library = DynamicLibrary.open('${Directory.current.path}/$libraryPath');
  final connections = library.lookupFunction<_CountNative, _CountDart>('vb_test_connection_count');
  final disconnections = library.lookupFunction<_CountNative, _CountDart>('vb_test_disconnection_count');
  final log = File('$root/agent.log');
  await LogWriter.configureSourcePath(source: 'agent', path: log.path);
  LogWriter.configureSourceLevel(source: 'agent', level: 'warn');
  final storage = BackupStorage(
    id: 'sftp',
    name: 'SFTP',
    driverId: 'sftp',
    enabled: true,
    params: {'host': 'test.invalid', 'port': 22, 'username': 'test', 'password': 'test', 'basePath': '/backup'},
    uploadConcurrency: 1,
    downloadConcurrency: 1,
  );
  final settings = AppSettings.empty().copyWith(backupPath: root, logLevel: 'warn', storage: [storage], backupStorageId: storage.id);
  final driver = SftpBackupDriver(settings: settings);
  try {
    await driver.ensureReady();
    final names = await driver.listBlobNames('05');
    _check(names.length == 1 && names.contains('blob'), 'Retry did not return the complete directory listing.');
    _check(connections() == 2 && disconnections() == 1, 'Timed-out session was not discarded before reconnecting.');
    final text = await log.readAsString();
    _check(text.contains('level=error message=driver=sftp timeout label="listdir /backup/VirtBackup/blobs/1/05"'), 'Timeout was not logged at error.');
    _check(text.contains('level=warn message=driver=sftp retry label="listdir /backup/VirtBackup/blobs/1/05" attempt=2/5'), 'Retry was not logged at warn.');
    _check(text.contains('Native SFTP directory listing timed out'), 'Native timeout details were lost.');
  } finally {
    await driver.closeConnections();
  }
  _check(disconnections() == 2, 'Replacement session was not closed.');
  stdout.writeln('Native SFTP timeout recovered through Dart retry with a new session.');
}
