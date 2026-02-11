import 'dart:async';
import 'dart:io';

import 'package:virtbackup/agent/http_server.dart';
import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/common/debug_log_writer.dart';
import 'package:virtbackup/common/settings.dart';

Future<void> main(List<String> args) async {
  await runZonedGuarded(
    () async {
      final logSshOutput = args.contains('-debug') || args.contains('--debug');
      final host = BackupAgentHost(onInfo: _logInfo, onError: _logError, logSshOutput: logSshOutput);
      late final AppSettingsStore settingsStore;
      try {
        settingsStore = await AppSettingsStore.fromAgentDefaultPath();
      } catch (error, stackTrace) {
        _logInfo('Failed to resolve settings path. $error');
        _logInfo(stackTrace.toString());
        exit(1);
      }
      final loadedSettings = await settingsStore.load();
      _debugLogMirror.configureFromSettings(loadedSettings);
      _logInfo('Settings file: ${settingsStore.file.path}');
      if (!await settingsStore.file.exists()) {
        _logInfo('Settings file not found.');
      }
      final server = AgentHttpServer(host: host, settingsStore: settingsStore);
      await server.start();
      _startEventLoopLagMonitor();

      var shuttingDown = false;
      Future<void> shutdown() async {
        if (shuttingDown) {
          return;
        }
        shuttingDown = true;
        try {
          _logInfo('Shutdown signal received. Stopping server...');
          await server.stop().timeout(const Duration(seconds: 1));
        } on TimeoutException {
          _logInfo('Shutdown timed out; forcing exit.');
        } catch (error) {
          _logInfo('Shutdown error: $error');
        } finally {
          exit(0);
        }
      }

      ProcessSignal.sigint.watch().listen((_) => unawaited(shutdown()));
      ProcessSignal.sigterm.watch().listen((_) => unawaited(shutdown()));

      await Future<void>.delayed(const Duration(days: 3650));
    },
    (error, stackTrace) {
      _logInfo('Unhandled async error: $error');
      _logInfo(stackTrace.toString());
      exit(1);
    },
    zoneSpecification: ZoneSpecification(
      print: (self, parent, zone, line) {
        parent.print(zone, line);
        _debugLogMirror.appendRaw(line);
      },
    ),
  );
}

void _logInfo(String message) {
  stdout.writeln(message);
  _debugLogMirror.append(message, level: 'INFO');
}

void _logError(String message, Object error, StackTrace stackTrace) {
  stderr.writeln('$message $error');
  _debugLogMirror.append('$message $error');
  _debugLogMirror.append(stackTrace.toString(), level: 'ERROR');
}

void _startEventLoopLagMonitor() {
  const interval = Duration(seconds: 1);
  final stopwatch = Stopwatch()..start();
  var lastTickMs = stopwatch.elapsedMilliseconds;
  Timer.periodic(interval, (_) {
    final nowMs = stopwatch.elapsedMilliseconds;
    final elapsed = nowMs - lastTickMs;
    lastTickMs = nowMs;
    final lagMs = elapsed - interval.inMilliseconds;
    if (lagMs > 200) {
      _logInfo('Event loop lag ${lagMs}ms');
    }
  });
}

class _DebugLogMirror {
  String? _logFilePath;

  void configureFromSettings(AppSettings settings) {
    final basePath = settings.backupPath.trim().isEmpty ? '${Platform.pathSeparator}var' : settings.backupPath.trim();
    _logFilePath = '$basePath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}logs${Platform.pathSeparator}debug.log';
  }

  void append(String message, {String level = 'INFO'}) {
    final trimmed = message.trimRight();
    if (trimmed.isEmpty) {
      return;
    }
    appendRaw('${DateTime.now().toIso8601String()} action=console level=$level message=${_sanitize(trimmed)}');
  }

  void appendRaw(String line) {
    final path = _logFilePath;
    if (path == null || path.isEmpty) {
      return;
    }
    unawaited(DebugLogWriter.appendLine(path, line).catchError((_) {}));
  }

  String _sanitize(String value) {
    return value.replaceAll('\n', r'\n').replaceAll('\r', r'\r');
  }
}

final _debugLogMirror = _DebugLogMirror();
