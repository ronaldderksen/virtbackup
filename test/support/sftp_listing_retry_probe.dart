import 'dart:ffi';
import 'dart:io';

import 'package:virtbackup/agent/drv/sftp_driver.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

typedef _CountNative = Int32 Function();
typedef _CountDart = int Function();
typedef _ModeNative = Void Function(Int32);
typedef _ModeDart = void Function(int);

void _check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final root = args[0];
  final mode = args[1];
  final libraryPath = Platform.isMacOS ? 'native/macos/libvirtbackup_native.dylib' : 'native/linux/libvirtbackup_native.so';
  final library = DynamicLibrary.open('${Directory.current.path}/$libraryPath');
  library.lookupFunction<_ModeNative, _ModeDart>('vb_test_transfer_mode')(mode == 'listing' ? 0 : 1);
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
    switch (mode) {
      case 'listing':
        final names = await driver.listBlobNames('05');
        _check(names.length == 1 && names.contains('blob'), 'Retry did not return the complete directory listing.');
      case 'read':
        final bytes = await driver.readFileBytes('manifest');
        _check(bytes != null && String.fromCharCodes(bytes) == 'blob', 'Read retry returned partial or incorrect bytes.');
      case 'stream':
        final bytes = await driver.openBlobStream('05hash', length: 4).fold<List<int>>([], (all, chunk) => all..addAll(chunk));
        _check(String.fromCharCodes(bytes) == 'blob', 'Stream retry returned partial or duplicated bytes.');
      case 'write':
        final source = await File('$root/upload').writeAsString('blob');
        await driver.uploadFile(relativePath: 'write', localFile: source);
      default:
        throw ArgumentError.value(mode, 'mode');
    }
    _check(connections() == 2 && disconnections() == 1, 'Timed-out session was not discarded before reconnecting.');
    final text = await log.readAsString();
    _check(text.contains('level=error message=driver=sftp timeout label='), 'Timeout was not logged at error.');
    _check(text.contains('level=warn message=driver=sftp retry label=') && text.contains('attempt=2/5'), 'Retry was not logged at warn.');
    _check(text.contains(mode == 'listing' ? 'Native SFTP directory listing timed out' : 'Native SFTP ${mode == 'write' ? 'write' : 'read'} timed out'), 'Native timeout details were lost.');
  } finally {
    await driver.closeConnections();
  }
  _check(disconnections() == 2, 'Replacement session was not closed.');
  stdout.writeln('Native SFTP $mode timeout recovered through Dart retry with a new session.');
}
