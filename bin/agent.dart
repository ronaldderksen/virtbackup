import 'dart:async';
import 'dart:io';

import 'package:virtbackup/agent/http_server.dart';
import 'package:virtbackup/agent/instance_lock.dart';
import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/common/log_writer.dart';

Future<void> main(List<String> _) async {
  AgentInstanceLock? instanceLock;
  var loggingConfigured = false;
  final startupMessages = <String>[];
  void lockMessage(String message) {
    if (loggingConfigured) {
      LogWriter.logAgentSync(level: 'info', message: message);
    } else {
      stderr.writeln(message);
      startupMessages.add(message);
    }
  }

  await runZonedGuarded(
    () async {
      final host = BackupAgentHost();
      late final AppSettingsStore settingsStore;
      try {
        settingsStore = await AppSettingsStore.fromAgentDefaultPath();
      } catch (error, stackTrace) {
        Error.throwWithStackTrace(StateError('Failed to resolve settings path: $error'), stackTrace);
      }
      instanceLock = AgentInstanceLock.acquire(settingsFile: settingsStore.file, onMessage: lockMessage);
      final loadedSettings = await settingsStore.load();
      await LogWriter.configureSourcePath(
        source: 'agent',
        path: LogWriter.defaultPathForSource('agent', basePath: loadedSettings.backupPath.trim()),
      );
      LogWriter.configureSourceLevel(source: 'agent', level: loadedSettings.logLevel);
      await LogWriter.rotateSource('agent');
      loggingConfigured = true;
      for (final message in startupMessages) {
        LogWriter.logAgentSync(level: 'info', message: message);
      }
      startupMessages.clear();
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
          _logInfo('Shutdown already in progress.');
          return;
        }
        shuttingDown = true;
        try {
          _logInfo('Shutdown signal received. Canceling running jobs...');
          await server.cancelRunningJobsAndStop();
        } catch (error) {
          _logInfo('Shutdown error: $error');
        } finally {
          try {
            instanceLock?.release();
          } catch (error) {
            stderr.writeln('Agent lock cleanup failed: $error');
            if (loggingConfigured) LogWriter.logAgentSync(level: 'error', message: 'Agent lock cleanup failed: $error');
            await stderr.flush();
            exit(1);
          }
          await stderr.flush();
          exit(0);
        }
      }

      ProcessSignal.sigint.watch().listen((_) => unawaited(shutdown()));
      ProcessSignal.sigterm.watch().listen((_) => unawaited(shutdown()));

      await Future<void>.delayed(const Duration(days: 3650));
    },
    (error, stackTrace) async {
      stderr.writeln('Agent stopped: $error');
      if (loggingConfigured) {
        LogWriter.logAgentSync(level: 'error', message: 'Unhandled agent error: $error');
        LogWriter.logAgentSync(level: 'debug', message: stackTrace.toString());
      }
      // Keep ownership on abnormal termination until this PID has exited.
      // A subsequent startup on this host can then reclaim the stale lock.
      await stderr.flush();
      exit(1);
    },
    zoneSpecification: ZoneSpecification(
      print: (self, parent, zone, line) {
        unawaited(LogWriter.log(source: 'agent', level: 'info', message: line).catchError((_) {}));
      },
    ),
  );
}

void _logInfo(String message) {
  unawaited(LogWriter.log(source: 'agent', level: 'info', message: message).catchError((_) {}));
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
