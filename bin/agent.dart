import 'dart:async';
import 'dart:io';

import 'package:virtbackup/agent/http_server.dart';
import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/agent/backup_host.dart';

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
  );
}

void _logInfo(String message) {
  stdout.writeln(message);
}

void _logError(String message, Object error, StackTrace stackTrace) {
  stderr.writeln('$message $error');
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
