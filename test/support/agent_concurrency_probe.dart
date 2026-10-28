// Appended to the production library in a temporary Dart VM fixture so private
// orchestration can be exercised without adding test entry points to the agent.
const agentConcurrencyProbe = r'''
class _ProbeServer extends AgentHttpServer {
  _ProbeServer() : super(host: BackupAgentHost());

  @override
  Future<void> _applyAgentSettings(AppSettings settings, {required String reason, bool forceRestartSshListeners = false, bool runServerRefreshInBackground = false}) async {
    _agentSettings = settings;
  }
}

class _ProbeScheduleServer extends _ProbeServer {
  final gate = Completer<String>();
  var starts = 0;

  @override
  Future<String> _startScheduledJob(ScheduledJob schedule, {bool allowDisabled = false}) {
    starts++;
    return gate.future;
  }
}

void _check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  await LogWriter.configureSourcePath(source: 'agent', path: '${args[0]}/agent.log');
  LogWriter.configureSourceLevel(source: 'agent', level: 'fatal');
  final mode = args[1];
  if (mode == 'jobs') {
    final server = _ProbeServer();
    final ids = List.generate(1000, (_) => server._createJob(AgentJobType.backup));
    _check(ids.toSet().length == 1000 && server._jobs.length == 1000 && server._jobControls.length == 1000, 'Job creation overwrote another job.');
    _check(ids.every((id) => server._jobTypeFromJobId(id) == AgentJobType.backup), 'Job history no longer recognizes the job type.');
    return;
  }
  if (mode == 'guards') {
    final server = _ProbeServer();
    server._agentSettings = AppSettings.empty().copyWith(maxConcurrentBackupRestoreJobs: 8, maxConcurrentJobsPerStorage: 8, maxConcurrentJobsPerVm: 1);
    final first = server._createJob(AgentJobType.backup, serverId: 'nuc04', vmName: 'win11', storageId: 'store');
    String? guard(String serverId, {String vm = 'win11'}) => server._jobStartGuardMessage(type: AgentJobType.restore, serverId: serverId, vmName: vm, storageId: 'store');
    _check(guard('nuc04') != null, 'Same VM was allowed to run twice.');
    _check(guard('nuc02') == null, 'Different servers with equal VM names block each other.');
    final second = server._createJob(AgentJobType.restore, serverId: 'nuc02', vmName: 'win11', storageId: 'store');
    _check(guard('nuc02') != null, 'Restore did not retain its target server identity.');
    server._jobs[first] = server._jobs[first]!.copyWith(state: AgentJobState.success);
    _check(guard('nuc04') == null && guard('nuc02') != null, 'Finished VM job still blocks or releases another server.');
    server._agentSettings = server._agentSettings.copyWith(maxConcurrentJobsPerStorage: 1);
    _check(guard('nuc03', vm: 'other')!.contains('storage'), 'Storage concurrency guard changed.');
    server._agentSettings = server._agentSettings.copyWith(maxConcurrentBackupRestoreJobs: 1);
    _check(guard('nuc03', vm: 'other')!.contains('backup/restore'), 'Global concurrency guard changed.');
    _check(server._jobControls[second]!.serverId == 'nuc02', 'Target identity was not stored.');
    return;
  }
  if (mode == 'schedule' || mode == 'schedule-failure') {
    final server = _ProbeScheduleServer();
    final now = DateTime.now();
    final schedule = ScheduledJob.tryFromMap({
      'id': 'schedule', 'name': 'Probe', 'enabled': true, 'frequency': 'daily',
      'time': '${now.hour.toString().padLeft(2, '0')}:${now.minute.toString().padLeft(2, '0')}',
      'vmName': 'vm', 'type': 'backup', 'serverId': 'server', 'storageId': 'store',
    })!;
    server._agentSettings = AppSettings.empty().copyWith(schedules: [schedule]);
    final first = server._runDueSchedules();
    await server._runDueSchedules();
    _check(server.starts == 1, 'Overlapping scheduler rounds started the same run twice.');
    if (mode == 'schedule-failure') {
      server.gate.completeError(StateError('Simulated startup failure'));
    } else {
      server.gate.complete('job');
    }
    await first;
    _check(!server._schedulePollRunning, 'Scheduler stayed locked after startup.');
    await server._runDueSchedules();
    _check(server.starts == 1, 'A completed schedule was started again.');
    return;
  }
  final backend = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final entered = Completer<void>();
  final release = Completer<void>();
  var requests = 0;
  final subscription = backend.listen((request) async {
    final number = ++requests;
    await request.drain<void>();
    if (!entered.isCompleted) entered.complete();
    await release.future;
    if (mode == 'account-rejected' || number > 1 && mode != 'account-retry') {
      request.response.statusCode = 401;
      request.response.write('{"error":"invalid_refresh_token"}');
    } else if (mode == 'account-retry' && number == 1) {
      request.response.statusCode = 500;
      request.response.write('Simulated backend failure');
    } else {
      final now = DateTime.now().toUtc();
      request.response.write(jsonEncode({
        'email': 'test@example.invalid', 'accessToken': 'refreshed-access', 'refreshToken': 'refreshed-refresh',
        'accessTokenExpiresAt': now.add(const Duration(hours: 1)).toIso8601String(),
        'refreshTokenExpiresAt': now.add(const Duration(days: 30)).toIso8601String(),
      }));
    }
    await request.response.close();
  });
  final server = _ProbeServer();
  final now = DateTime.now().toUtc();
  final old = VirtBackupAccountTokens(
    email: 'test@example.invalid', accountBaseUrl: 'http://127.0.0.1:${backend.port}',
    accessToken: 'old-access', accessTokenExpiresAt: now.subtract(const Duration(minutes: 1)),
    refreshToken: 'old-refresh', refreshTokenExpiresAt: now.add(const Duration(days: 30)),
  );
  final replacement = VirtBackupAccountTokens(
    email: 'new@example.invalid', accountBaseUrl: old.accountBaseUrl,
    accessToken: 'replacement-access', accessTokenExpiresAt: now.add(const Duration(hours: 1)),
    refreshToken: 'replacement-refresh', refreshTokenExpiresAt: now.add(const Duration(days: 30)),
  );
  server._agentSettings = AppSettings.empty().copyWith(virtBackupAccount: old);
  try {
    final first = server._maybeRefreshVirtBackupAccount();
    await entered.future;
    final second = server._maybeRefreshVirtBackupAccount();
    if (mode == 'account-changed' || mode == 'account-rejected') {
      server._agentSettings = server._agentSettings.copyWith(virtBackupAccount: replacement);
    } else if (mode == 'account-disconnected') {
      server._agentSettings = server._agentSettings.copyWith(virtBackupAccount: VirtBackupAccountTokens.empty());
    } else {
      server._agentSettings = server._agentSettings.copyWith(blockSizeMB: 8);
    }
    release.complete();
    await Future.wait([first, second]);
    _check(requests == 1, 'Refresh token was used by parallel HTTP requests.');
    if (mode == 'account-changed' || mode == 'account-rejected') {
      _check(identical(server._agentSettings.virtBackupAccount, replacement), 'Late refresh replaced or cleared new credentials.');
    } else if (mode == 'account-disconnected') {
      _check(!server._agentSettings.virtBackupAccount.isConnected, 'Late refresh reconnected a disconnected account.');
    } else {
      if (mode == 'account-retry') {
        await server._maybeRefreshVirtBackupAccount();
        _check(requests == 2, 'Failed refresh prevented a later attempt.');
      }
      _check(server._agentSettings.virtBackupAccount.accessToken == 'refreshed-access', 'Refreshed tokens were not retained.');
      _check(server._agentSettings.blockSizeMB == 8, 'Refresh overwrote unrelated settings.');
    }
    _check(server._accountRefreshInFlight == null, 'Refresh stayed locked after completion.');
  } finally {
    await server.stop();
    await subscription.cancel();
    await backend.close(force: true);
  }
}
''';
