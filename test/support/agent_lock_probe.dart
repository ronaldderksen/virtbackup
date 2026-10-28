import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:virtbackup/agent/instance_lock.dart';

Future<void> main(List<String> args) async {
  if (args[1] == 'exit') return;
  StreamSubscription<ProcessSignal>? signal;
  try {
    final lock = AgentInstanceLock.acquire(settingsFile: File('${args[0]}/agent.yaml'), onMessage: stderr.writeln);
    signal = ProcessSignal.sigint.watch().listen((_) async {
      lock.release();
      await stderr.flush();
      exit(0);
    });
    stdout.writeln(jsonEncode({'status': 'acquired', 'pid': pid}));
    await stdout.flush();
    await stdin.first;
    lock.release();
  } catch (error) {
    stdout.writeln(jsonEncode({'status': 'blocked', 'message': error.toString()}));
    await stdout.flush();
    exitCode = 2;
  } finally {
    await signal?.cancel();
  }
}
