import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:basic_utils/basic_utils.dart';
import 'package:dartssh2/dartssh2.dart';
import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/backup_worker.dart';
import 'package:virtbackup/agent/restore_worker.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart' as drv;
import 'package:virtbackup/agent/drv/dummy_driver.dart';
import 'package:virtbackup/agent/drv/filesystem_driver.dart';
import 'package:virtbackup/agent/drv/gdrive_driver.dart';
import 'package:virtbackup/agent/drv/sftp_driver.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';
import 'package:virtbackup/agent/settings_store.dart';

class _DriverDescriptor {
  const _DriverDescriptor({required this.id, required this.label, required this.usesPath, required this.capabilities, required this.validateStart, required this.create});

  final String id;
  final String label;
  final bool usesPath;
  final drv.BackupDriverCapabilities capabilities;
  final String? Function() validateStart;
  final drv.BackupDriver Function(Map<String, dynamic> params) create;
}

class _ResolvedStorage {
  const _ResolvedStorage({required this.storage, required this.settings, required this.driverId, required this.backupPath, required this.driverParams});

  final BackupStorage storage;
  final AppSettings settings;
  final String driverId;
  final String backupPath;
  final Map<String, dynamic> driverParams;
}

class AgentHttpServer {
  AgentHttpServer({required BackupAgentHost host, AppSettingsStore? settingsStore}) : _host = host, _agentSettingsStore = settingsStore ?? AppSettingsStore();

  static const String _ntfymeTopic = 'virtbackup-job';
  static final Uri _ntfymeEndpoint = Uri.parse('https://ntfyme.net/msg');

  final BackupAgentHost _host;
  final AppSettingsStore _agentSettingsStore;

  HttpServer? _server;
  AppSettings _agentSettings = AppSettings.empty();
  final Map<String, List<VmStatus>> _vmStatusByServerId = {};
  final Map<String, List<String>> _missingToolsByServerId = {};
  final Map<String, AgentJobStatus> _jobs = {};
  final Map<String, _JobControl> _jobControls = {};
  final Map<HttpResponse, _EventStreamState> _eventStreams = {};
  final Map<String, Timer> _eventRestartTimers = {};
  final Map<String, int> _eventRestartAttempts = {};
  final Map<String, Timer> _refreshDebounceTimers = {};
  final Set<String> _completedScheduleRuns = <String>{};
  final Set<String> _canceledScheduleRuns = <String>{};
  final Map<String, String> _waitingScheduleRuns = <String, String>{};
  late final Map<String, BackupDriverInfo> _driverCatalog = _buildDriverCatalog();
  GdriveBackupDriver? _cachedGdriveDriver;
  Timer? _scheduleTimer;
  Timer? _accountRefreshTimer;
  String _agentToken = '';
  bool _storageWritable = false;
  String _storageWriteError = '';

  Future<void> start() async {
    await _loadAgentSettings();
    await _refreshStorageWriteStatus();
    await _ensureAgentToken();
    await _ensureTlsAssets();
    final bindAddress = InternetAddress.anyIPv4;
    final securityContext = _buildTlsContext();
    _server = await HttpServer.bindSecure(bindAddress, backupAgentPort, securityContext);
    _hostLog('Agent HTTPS listening on ${bindAddress.address}:$backupAgentPort');
    _server?.listen(_handleRequest);
    _restartScheduleTimer();
    _restartAccountRefreshTimer();

    _hostLog('Startup: refreshing ${_agentSettings.servers.length} server(s).');
    await _refreshAllServers();
  }

  Future<void> stop() async {
    for (final serverId in _eventRestartTimers.keys.toList()) {
      await _stopEventListener(serverId);
    }
    for (final timer in _refreshDebounceTimers.values) {
      timer.cancel();
    }
    _scheduleTimer?.cancel();
    _scheduleTimer = null;
    _accountRefreshTimer?.cancel();
    _accountRefreshTimer = null;
    for (final state in _eventStreams.values.toList()) {
      try {
        state.closed = true;
        state.response.close();
      } catch (_) {}
    }
    _eventStreams.clear();
    await _server?.close(force: true);
  }

  Future<void> cancelRunningJobsAndStop() async {
    _scheduleTimer?.cancel();
    _scheduleTimer = null;
    _waitingScheduleRuns.clear();

    final runningJobIds = _jobs.entries.where((entry) => entry.value.state == AgentJobState.running).map((entry) => entry.key).toList();
    if (runningJobIds.isNotEmpty) {
      _hostLog('Shutdown: canceling ${runningJobIds.length} running job(s).');
    }

    for (final jobId in runningJobIds) {
      final job = _jobs[jobId];
      if (job == null || job.state != AgentJobState.running) {
        continue;
      }
      final cancelRequested = _cancelJob(jobId);
      if (cancelRequested) {
        _hostLog('Shutdown: cancel requested for job $jobId.', jobId: jobId);
      } else {
        _hostLog('Shutdown: waiting for job $jobId to finish; cancel is not available in its current state.', jobId: jobId);
      }
    }

    final completions = <Future<AgentJobStatus>>[];
    for (final jobId in runningJobIds) {
      final job = _jobs[jobId];
      final control = _jobControls[jobId];
      if (job?.state == AgentJobState.running && control != null) {
        completions.add(control.completed.future);
      }
    }
    if (completions.isNotEmpty) {
      await Future.wait(completions);
      _hostLog('Shutdown: running jobs finished.');
    }

    await stop();
  }

  void _restartScheduleTimer() {
    _scheduleTimer?.cancel();
    _scheduleTimer = null;
    if (_agentSettings.schedules.where((schedule) => schedule.enabled).isEmpty && _waitingScheduleRuns.isEmpty) {
      return;
    }
    unawaited(_runDueSchedules());
    _scheduleTimer = Timer.periodic(const Duration(seconds: 30), (_) {
      unawaited(_runDueSchedules());
    });
  }

  void _restartAccountRefreshTimer() {
    _accountRefreshTimer?.cancel();
    _accountRefreshTimer = null;
    final refreshAt = _virtBackupAccountRefreshAt(_agentSettings.virtBackupAccount);
    if (refreshAt == null) {
      return;
    }
    final now = DateTime.now().toUtc();
    final delay = refreshAt.isAfter(now) ? refreshAt.difference(now) : Duration.zero;
    _hostLog('Virt Backup account refresh scheduled at ${_formatLocalLogTime(refreshAt)}.');
    _accountRefreshTimer = Timer(delay, () {
      _accountRefreshTimer = null;
      unawaited(_maybeRefreshVirtBackupAccount());
    });
  }

  Future<void> _runDueSchedules() async {
    final now = DateTime.now();
    for (final entry in Map<String, String>.from(_waitingScheduleRuns).entries) {
      final schedule = _scheduleById(entry.key);
      final manualRun = _scheduleRunKeyIsManual(entry.value);
      if (schedule == null || (!manualRun && !schedule.enabled) || !schedule.waitForRunningJobs || !_scheduleRunKeyIsRecent(entry.value, now)) {
        _waitingScheduleRuns.remove(entry.key);
        continue;
      }
      await _tryStartScheduledRun(schedule, entry.value, fromWaitingQueue: true, allowDisabled: manualRun);
    }
    for (final schedule in _agentSettings.schedules) {
      if (!schedule.enabled || !_scheduleIsDue(schedule, now)) {
        continue;
      }
      final runKey = _scheduleRunKey(schedule, now);
      if (_completedScheduleRuns.contains(runKey) || _waitingScheduleRuns[schedule.id] == runKey) {
        continue;
      }
      await _tryStartScheduledRun(schedule, runKey, fromWaitingQueue: false, allowDisabled: false);
    }
    _completedScheduleRuns.removeWhere((key) => !_scheduleRunKeyIsRecent(key, now));
  }

  Future<void> _tryStartScheduledRun(ScheduledJob schedule, String runKey, {required bool fromWaitingQueue, required bool allowDisabled}) async {
    try {
      await _startScheduledJob(schedule, allowDisabled: allowDisabled);
      _completedScheduleRuns.add(runKey);
      _waitingScheduleRuns.remove(schedule.id);
    } on _JobGuardRejected catch (error, stackTrace) {
      if (schedule.waitForRunningJobs) {
        _waitingScheduleRuns[schedule.id] = runKey;
        if (!fromWaitingQueue) {
          _hostLog('Schedule "${schedule.name}" is waiting for running jobs. ${error.message}');
        }
        return;
      }
      _completedScheduleRuns.add(runKey);
      _hostLog('Schedule "${schedule.name}" failed to start. ${error.message}');
      _hostLog(stackTrace.toString());
      _failScheduledJobStart(schedule, error.message);
    } catch (error, stackTrace) {
      _completedScheduleRuns.add(runKey);
      _waitingScheduleRuns.remove(schedule.id);
      _hostLog('Schedule "${schedule.name}" failed to start. $error');
      _hostLog(stackTrace.toString());
    }
  }

  bool _scheduleIsDue(ScheduledJob schedule, DateTime now) {
    final hour = now.hour.toString().padLeft(2, '0');
    final minute = now.minute.toString().padLeft(2, '0');
    if (schedule.frequency == ScheduleFrequency.every5Minutes) {
      final configuredMinute = _scheduleMinute(schedule.time);
      if (configuredMinute == null) {
        return false;
      }
      return now.minute % 5 == configuredMinute % 5;
    }
    if (schedule.frequency == ScheduleFrequency.hourly) {
      final parts = schedule.time.split(':');
      return parts.length == 2 && parts[1] == minute;
    }
    if (schedule.time != '$hour:$minute') {
      return false;
    }
    if (schedule.frequency == ScheduleFrequency.daily) {
      return true;
    }
    return schedule.weekdays.contains(now.weekday);
  }

  int? _scheduleMinute(String time) {
    if (time == '*/5') {
      return 0;
    }
    final parts = time.split(':');
    if (parts.length != 2) {
      return null;
    }
    return int.tryParse(parts[1]);
  }

  String _scheduleRunKey(ScheduledJob schedule, DateTime now) {
    final month = now.month.toString().padLeft(2, '0');
    final day = now.day.toString().padLeft(2, '0');
    final hour = now.hour.toString().padLeft(2, '0');
    final minute = now.minute.toString().padLeft(2, '0');
    return '${schedule.id}:${now.year}-$month-$day $hour:$minute';
  }

  String _manualScheduleRunKey(ScheduledJob schedule) {
    return '${schedule.id}:manual:${DateTime.now().microsecondsSinceEpoch}';
  }

  bool _scheduleRunKeyIsManual(String key) {
    final parts = key.split(':');
    return parts.length == 3 && parts[1] == 'manual';
  }

  DateTime? _scheduleRunQueuedAt(String key) {
    final parts = key.split(':');
    if (parts.length == 3 && parts[1] == 'manual') {
      final micros = int.tryParse(parts[2]);
      return micros == null ? null : DateTime.fromMicrosecondsSinceEpoch(micros);
    }
    if (parts.length < 2) {
      return null;
    }
    return DateTime.tryParse(parts.sublist(1).join(':').replaceFirst(' ', 'T'));
  }

  bool _scheduleRunKeyIsRecent(String key, DateTime now) {
    final parsed = _scheduleRunQueuedAt(key);
    if (parsed == null) {
      return false;
    }
    return now.difference(parsed).inDays < 2;
  }

  Future<String> _startScheduledJob(ScheduledJob schedule, {bool allowDisabled = false}) async {
    final scheduleRunId = _createScheduleRunId(schedule);
    final server = _serverById(schedule.serverId);
    if (server == null) {
      throw StateError('server not found: ${schedule.serverId}');
    }
    final storage = _resolveStorageById(schedule.storageId);
    if (storage == null) {
      throw StateError('storage not found or unavailable: ${schedule.storageId}');
    }
    switch (schedule.type) {
      case ScheduledJobType.backup:
        if (schedule.backupAllVms) {
          return _startScheduledAllVmBackup(schedule, server, storage, scheduleRunId: scheduleRunId, allowDisabled: allowDisabled);
        }
        return _startScheduledBackup(schedule, server, storage, scheduleRunId: scheduleRunId);
      case ScheduledJobType.restore:
        if (schedule.restoreAllLatestVms) {
          return _startScheduledAllLatestVmRestore(schedule, server, storage, scheduleRunId: scheduleRunId, allowDisabled: allowDisabled);
        }
        return _startScheduledRestore(schedule, server, storage, scheduleRunId: scheduleRunId);
    }
  }

  String _createScheduleRunId(ScheduledJob schedule) {
    return '${schedule.id}:${DateTime.now().microsecondsSinceEpoch}';
  }

  ServerConfig? _serverById(String serverId) {
    for (final server in _agentSettings.servers) {
      if (server.id == serverId) {
        return server;
      }
    }
    return null;
  }

  ScheduledJob? _scheduleById(String scheduleId) {
    for (final schedule in _agentSettings.schedules) {
      if (schedule.id == scheduleId) {
        return schedule;
      }
    }
    return null;
  }

  List<ScheduleQueueEntry> _scheduleQueueEntries() {
    final entries = <ScheduleQueueEntry>[];
    var position = 1;
    final runningScheduleIds = <String>{};
    final runningJobs = _jobs.values.where((job) => job.scheduleId.trim().isNotEmpty && job.state == AgentJobState.running).toList()
      ..sort((a, b) {
        final aControl = _jobControls[a.id];
        final bControl = _jobControls[b.id];
        final aStartedAt = aControl?.startedAt;
        final bStartedAt = bControl?.startedAt;
        if (aStartedAt == null && bStartedAt == null) {
          return a.id.compareTo(b.id);
        }
        if (aStartedAt == null) {
          return 1;
        }
        if (bStartedAt == null) {
          return -1;
        }
        return aStartedAt.compareTo(bStartedAt);
      });
    for (final job in runningJobs) {
      final schedule = _scheduleById(job.scheduleId);
      if (schedule == null) {
        continue;
      }
      runningScheduleIds.add(schedule.id);
      final server = _serverById(schedule.serverId);
      final storage = _resolveStorageById(schedule.storageId);
      entries.add(
        ScheduleQueueEntry(
          scheduleId: schedule.id,
          scheduleName: schedule.name,
          type: schedule.type.name,
          serverId: schedule.serverId,
          serverName: server?.name ?? '',
          storageId: schedule.storageId,
          storageName: storage?.storage.name ?? '',
          runKey: _jobControls[job.id]?.scheduleRunId ?? '',
          status: 'running',
          manual: _scheduleRunKeyIsManual(_jobControls[job.id]?.scheduleRunId ?? ''),
          queuedAt: _jobControls[job.id]?.startedAt,
          position: position,
          jobId: job.id,
        ),
      );
      position += 1;
    }
    final waitingEntries = Map<String, String>.from(_waitingScheduleRuns).entries.toList()
      ..sort((a, b) {
        final aQueuedAt = _scheduleRunQueuedAt(a.value);
        final bQueuedAt = _scheduleRunQueuedAt(b.value);
        if (aQueuedAt == null && bQueuedAt == null) {
          return a.key.compareTo(b.key);
        }
        if (aQueuedAt == null) {
          return 1;
        }
        if (bQueuedAt == null) {
          return -1;
        }
        return aQueuedAt.compareTo(bQueuedAt);
      });
    for (final entry in waitingEntries) {
      final schedule = _scheduleById(entry.key);
      if (schedule == null) {
        continue;
      }
      if (runningScheduleIds.contains(schedule.id)) {
        continue;
      }
      final server = _serverById(schedule.serverId);
      final storage = _resolveStorageById(schedule.storageId);
      entries.add(
        ScheduleQueueEntry(
          scheduleId: schedule.id,
          scheduleName: schedule.name,
          type: schedule.type.name,
          serverId: schedule.serverId,
          serverName: server?.name ?? '',
          storageId: schedule.storageId,
          storageName: storage?.storage.name ?? '',
          runKey: entry.value,
          status: 'waiting',
          manual: _scheduleRunKeyIsManual(entry.value),
          queuedAt: _scheduleRunQueuedAt(entry.value),
          position: position,
        ),
      );
      position += 1;
    }
    return entries;
  }

  String _startScheduledBackup(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, {required String scheduleRunId}) {
    if (schedule.vmName.trim().isEmpty) {
      throw StateError('backup schedule requires vmName');
    }
    return _startScheduledBackupForVm(
      schedule,
      server,
      storage,
      VmEntry(id: schedule.vmName, name: schedule.vmName, powerState: VmPowerState.stopped),
      scheduleRunId: scheduleRunId,
    );
  }

  Future<String> _startScheduledAllVmBackup(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, {required String scheduleRunId, required bool allowDisabled}) async {
    final vmSnapshot = await _loadScheduledBackupVmSnapshot(server);
    if (vmSnapshot.isEmpty) {
      throw StateError('backup schedule found no VMs on ${server.name}');
    }
    final firstJobId = _startScheduledBackupForVm(schedule, server, storage, vmSnapshot.first, scheduleRunId: scheduleRunId);
    if (vmSnapshot.length > 1) {
      unawaited(_continueScheduledAllVmBackup(schedule.id, server.id, storage.storage.id, vmSnapshot.skip(1).toList(), firstJobId, scheduleRunId: scheduleRunId, allowDisabled: allowDisabled));
    }
    return firstJobId;
  }

  Future<List<VmEntry>> _loadScheduledBackupVmSnapshot(ServerConfig server) async {
    final missingTools = await _host.missingRequiredRemoteTools(server);
    _missingToolsByServerId[server.id] = missingTools;
    if (missingTools.contains('tr') || missingTools.contains('virsh')) {
      throw StateError('backup schedule failed: server is missing required tools: ${missingTools.join(', ')}');
    }
    final vms = await _host.loadVmInventory(server);
    vms.sort((a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()));
    return vms;
  }

  Future<void> _continueScheduledAllVmBackup(
    String scheduleId,
    String serverId,
    String storageId,
    List<VmEntry> remainingVms,
    String previousJobId, {
    required String scheduleRunId,
    required bool allowDisabled,
  }) async {
    var lastJobId = previousJobId;
    for (final vm in remainingVms) {
      final previousStatus = await _waitForJobToFinish(lastJobId);
      if (previousStatus?.state == AgentJobState.canceled || _scheduleRunIsCanceled(scheduleRunId)) {
        _hostLog('All-VM backup schedule run $scheduleRunId stopped after canceled job $lastJobId.');
        return;
      }
      final schedule = _scheduleById(scheduleId);
      if (schedule == null || (!allowDisabled && !schedule.enabled) || !schedule.backupAllVms) {
        _hostLog('All-VM backup schedule $scheduleId stopped because the schedule changed.');
        return;
      }
      final server = _serverById(serverId);
      if (server == null) {
        _hostLog('All-VM backup schedule "${schedule.name}" stopped because server $serverId no longer exists.');
        return;
      }
      final storage = _resolveStorageById(storageId);
      if (storage == null) {
        _hostLog('All-VM backup schedule "${schedule.name}" stopped because storage $storageId is unavailable.');
        return;
      }
      try {
        lastJobId = _startScheduledBackupForVm(schedule, server, storage, vm, scheduleRunId: scheduleRunId);
      } on _JobGuardRejected catch (error) {
        if (!schedule.waitForRunningJobs) {
          _hostLog('All-VM backup schedule "${schedule.name}" stopped before ${vm.name}. ${error.message}');
          _failScheduledBackupVmStart(schedule, vm.name, error.message);
          return;
        }
        try {
          lastJobId = await _waitAndStartScheduledBackupForVm(schedule, server, storage, vm, scheduleRunId: scheduleRunId, allowDisabled: allowDisabled);
        } on _ScheduleRunCanceled {
          _hostLog('All-VM backup schedule run $scheduleRunId stopped before ${vm.name}.');
          return;
        }
      } on _ScheduleRunCanceled {
        _hostLog('All-VM backup schedule run $scheduleRunId stopped before ${vm.name}.');
        return;
      } catch (error, stackTrace) {
        _hostLog('All-VM backup schedule "${schedule.name}" failed before ${vm.name}. $error');
        _hostLog(stackTrace.toString());
        _failScheduledBackupVmStart(schedule, vm.name, error.toString());
        return;
      }
    }
  }

  Future<String> _waitAndStartScheduledBackupForVm(
    ScheduledJob schedule,
    ServerConfig server,
    _ResolvedStorage storage,
    VmEntry vm, {
    required String scheduleRunId,
    required bool allowDisabled,
  }) async {
    while (true) {
      await Future<void>.delayed(const Duration(seconds: 30));
      if (_scheduleRunIsCanceled(scheduleRunId)) {
        throw const _ScheduleRunCanceled();
      }
      final currentSchedule = _scheduleById(schedule.id);
      if (currentSchedule == null || (!allowDisabled && !currentSchedule.enabled) || !currentSchedule.backupAllVms || !currentSchedule.waitForRunningJobs) {
        throw StateError('schedule changed while waiting for running jobs');
      }
      try {
        return _startScheduledBackupForVm(currentSchedule, server, storage, vm, scheduleRunId: scheduleRunId);
      } on _JobGuardRejected catch (_) {}
    }
  }

  Future<AgentJobStatus?> _waitForJobToFinish(String jobId) async {
    final current = _jobs[jobId];
    if (current == null || current.state != AgentJobState.running) {
      return current;
    }
    final control = _jobControls[jobId];
    if (control == null) {
      return current;
    }
    return control.completed.future;
  }

  String _startScheduledBackupForVm(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, VmEntry vm, {required String scheduleRunId}) {
    if (_scheduleRunIsCanceled(scheduleRunId)) {
      throw const _ScheduleRunCanceled();
    }
    final backupPath = storage.backupPath;
    final registry = _buildDriverRegistry(backupPath: backupPath, settings: storage.settings);
    final descriptor = registry[storage.driverId] ?? registry['filesystem']!;
    if (descriptor.usesPath && backupPath.isEmpty) {
      throw StateError('backup path is empty');
    }
    final validationError = descriptor.validateStart();
    if (validationError != null && validationError.isNotEmpty) {
      throw StateError(validationError);
    }
    final guardMessage = _jobStartGuardMessage(type: AgentJobType.backup, vmName: vm.name, storageId: storage.storage.id);
    if (guardMessage != null) {
      throw _JobGuardRejected(guardMessage);
    }
    final jobId = _createJob(AgentJobType.backup, vmName: vm.name, storageId: storage.storage.id, scheduleId: schedule.id, scheduleRunId: scheduleRunId);
    _hostLog('Schedule "${schedule.name}" starting backup job $jobId for ${vm.name}.');
    _startBackupJob(jobId, server, vm, backupPath, driverIdOverride: storage.driverId, driverParams: storage.driverParams, storage: storage);
    return jobId;
  }

  void _failScheduledBackupVmStart(ScheduledJob schedule, String vmName, String message) {
    final storage = _resolveStorageById(schedule.storageId);
    final jobId = _createJob(AgentJobType.backup, vmName: vmName, storageId: schedule.storageId, scheduleId: schedule.id);
    final server = _serverById(schedule.serverId);
    _setJobContext(jobId, source: server == null ? vmName : _formatJobSource(server, vmName), storageLabel: storage?.storage.name);
    final current = _jobs[jobId];
    if (current == null) {
      return;
    }
    _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
    _notifyJobCompletion(jobId, type: AgentJobType.backup, state: AgentJobState.failure, message: message);
  }

  Future<String> _startScheduledRestore(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, {required String scheduleRunId}) async {
    final missingTools = await _host.missingRequiredRemoteTools(server);
    if (missingTools.isNotEmpty) {
      throw StateError('restore failed: server is missing required tools: ${missingTools.join(', ')}');
    }
    final xmlPath = await _resolveScheduledRestoreXmlPath(schedule, storage);
    if (xmlPath.trim().isEmpty) {
      throw StateError('restore schedule requires restoreXmlPath');
    }
    if (schedule.restoreDecision.trim().isEmpty) {
      throw StateError('restore schedule requires restoreDecision');
    }
    final vmName = _extractVmNameFromXmlPath(xmlPath);
    final guardMessage = _jobStartGuardMessage(type: AgentJobType.restore, vmName: vmName, storageId: storage.storage.id);
    if (guardMessage != null) {
      throw _JobGuardRejected(guardMessage);
    }
    final jobId = _createJob(AgentJobType.restore, vmName: vmName, storageId: storage.storage.id, scheduleId: schedule.id, scheduleRunId: scheduleRunId);
    _hostLog('Schedule "${schedule.name}" starting restore job $jobId.');
    _startRestoreJob(jobId, server, xmlPath, schedule.restoreDecision, storage: storage, driverIdOverride: storage.driverId);
    return jobId;
  }

  Future<String> _startScheduledAllLatestVmRestore(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, {required String scheduleRunId, required bool allowDisabled}) async {
    final entries = await _latestCompleteRestoreEntriesByVm(storage);
    if (entries.isEmpty) {
      throw StateError('restore schedule found no complete latest XML entries');
    }
    final firstJobId = await _startScheduledRestoreForXmlPath(schedule, server, storage, entries.first.xmlPath, scheduleRunId: scheduleRunId);
    if (entries.length > 1) {
      unawaited(
        _continueScheduledAllLatestVmRestore(
          schedule.id,
          server.id,
          storage.storage.id,
          entries.skip(1).map((entry) => entry.xmlPath).toList(),
          firstJobId,
          scheduleRunId: scheduleRunId,
          allowDisabled: allowDisabled,
        ),
      );
    }
    return firstJobId;
  }

  Future<void> _continueScheduledAllLatestVmRestore(
    String scheduleId,
    String serverId,
    String storageId,
    List<String> remainingXmlPaths,
    String previousJobId, {
    required String scheduleRunId,
    required bool allowDisabled,
  }) async {
    var lastJobId = previousJobId;
    for (final xmlPath in remainingXmlPaths) {
      final previousStatus = await _waitForJobToFinish(lastJobId);
      if (previousStatus?.state == AgentJobState.canceled || _scheduleRunIsCanceled(scheduleRunId)) {
        _hostLog('All-latest restore schedule run $scheduleRunId stopped after canceled job $lastJobId.');
        return;
      }
      final schedule = _scheduleById(scheduleId);
      if (schedule == null || (!allowDisabled && !schedule.enabled) || !schedule.restoreAllLatestVms) {
        _hostLog('All-latest restore schedule $scheduleId stopped because the schedule changed.');
        return;
      }
      final server = _serverById(serverId);
      if (server == null) {
        _hostLog('All-latest restore schedule "${schedule.name}" stopped because server $serverId no longer exists.');
        return;
      }
      final storage = _resolveStorageById(storageId);
      if (storage == null) {
        _hostLog('All-latest restore schedule "${schedule.name}" stopped because storage $storageId is unavailable.');
        return;
      }
      try {
        lastJobId = await _startScheduledRestoreForXmlPath(schedule, server, storage, xmlPath, scheduleRunId: scheduleRunId);
      } on _JobGuardRejected catch (error) {
        if (!schedule.waitForRunningJobs) {
          _hostLog('All-latest restore schedule "${schedule.name}" stopped before $xmlPath. ${error.message}');
          _failScheduledRestoreXmlStart(schedule, xmlPath, error.message);
          return;
        }
        try {
          lastJobId = await _waitAndStartScheduledRestoreForXmlPath(schedule, server, storage, xmlPath, scheduleRunId: scheduleRunId, allowDisabled: allowDisabled);
        } on _ScheduleRunCanceled {
          _hostLog('All-latest restore schedule run $scheduleRunId stopped before $xmlPath.');
          return;
        }
      } on _ScheduleRunCanceled {
        _hostLog('All-latest restore schedule run $scheduleRunId stopped before $xmlPath.');
        return;
      } catch (error, stackTrace) {
        _hostLog('All-latest restore schedule "${schedule.name}" failed before $xmlPath. $error');
        _hostLog(stackTrace.toString());
        _failScheduledRestoreXmlStart(schedule, xmlPath, error.toString());
        return;
      }
    }
  }

  Future<String> _waitAndStartScheduledRestoreForXmlPath(
    ScheduledJob schedule,
    ServerConfig server,
    _ResolvedStorage storage,
    String xmlPath, {
    required String scheduleRunId,
    required bool allowDisabled,
  }) async {
    while (true) {
      await Future<void>.delayed(const Duration(seconds: 30));
      if (_scheduleRunIsCanceled(scheduleRunId)) {
        throw const _ScheduleRunCanceled();
      }
      final currentSchedule = _scheduleById(schedule.id);
      if (currentSchedule == null || (!allowDisabled && !currentSchedule.enabled) || !currentSchedule.restoreAllLatestVms || !currentSchedule.waitForRunningJobs) {
        throw StateError('schedule changed while waiting for running jobs');
      }
      try {
        return _startScheduledRestoreForXmlPath(currentSchedule, server, storage, xmlPath, scheduleRunId: scheduleRunId);
      } on _JobGuardRejected catch (_) {}
    }
  }

  Future<String> _startScheduledRestoreForXmlPath(ScheduledJob schedule, ServerConfig server, _ResolvedStorage storage, String xmlPath, {required String scheduleRunId}) async {
    if (_scheduleRunIsCanceled(scheduleRunId)) {
      throw const _ScheduleRunCanceled();
    }
    final missingTools = await _host.missingRequiredRemoteTools(server);
    if (missingTools.isNotEmpty) {
      throw StateError('restore failed: server is missing required tools: ${missingTools.join(', ')}');
    }
    final normalizedXmlPath = xmlPath.trim();
    if (normalizedXmlPath.isEmpty) {
      throw StateError('restore schedule requires restoreXmlPath');
    }
    if (schedule.restoreDecision.trim().isEmpty) {
      throw StateError('restore schedule requires restoreDecision');
    }
    final vmName = _extractVmNameFromXmlPath(normalizedXmlPath);
    final guardMessage = _jobStartGuardMessage(type: AgentJobType.restore, vmName: vmName, storageId: storage.storage.id);
    if (guardMessage != null) {
      throw _JobGuardRejected(guardMessage);
    }
    final jobId = _createJob(AgentJobType.restore, vmName: vmName, storageId: storage.storage.id, scheduleId: schedule.id, scheduleRunId: scheduleRunId);
    _hostLog('Schedule "${schedule.name}" starting restore job $jobId for $normalizedXmlPath.');
    _startRestoreJob(jobId, server, normalizedXmlPath, schedule.restoreDecision, storage: storage, driverIdOverride: storage.driverId);
    return jobId;
  }

  Future<List<RestoreEntry>> _latestCompleteRestoreEntriesByVm(_ResolvedStorage storage) async {
    final entries = await _loadRestoreEntries(storageId: storage.storage.id);
    final latestByVm = <String, RestoreEntry>{};
    for (final entry in entries) {
      if (!entry.hasAllDisks) {
        continue;
      }
      final current = latestByVm[entry.vmName];
      if (current == null || entry.timestamp.compareTo(current.timestamp) > 0) {
        latestByVm[entry.vmName] = entry;
      }
    }
    final result = latestByVm.values.toList();
    result.sort((a, b) => a.vmName.toLowerCase().compareTo(b.vmName.toLowerCase()));
    return result;
  }

  void _failScheduledRestoreXmlStart(ScheduledJob schedule, String xmlPath, String message) {
    final storage = _resolveStorageById(schedule.storageId);
    final jobId = _createJob(AgentJobType.restore, vmName: _extractVmNameFromXmlPath(xmlPath), storageId: schedule.storageId, scheduleId: schedule.id);
    _setJobContext(jobId, source: xmlPath, storageLabel: storage?.storage.name);
    final current = _jobs[jobId];
    if (current == null) {
      return;
    }
    _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
    _notifyJobCompletion(jobId, type: AgentJobType.restore, state: AgentJobState.failure, message: message);
  }

  bool _scheduleRunIsCanceled(String scheduleRunId) {
    return _canceledScheduleRuns.contains(scheduleRunId);
  }

  Future<String> _resolveScheduledRestoreXmlPath(ScheduledJob schedule, _ResolvedStorage storage) async {
    final requested = schedule.restoreXmlPath.trim();
    if (requested != ScheduledJob.latestRestoreXmlPath) {
      return requested;
    }
    final vmName = schedule.vmName.trim();
    if (vmName.isEmpty) {
      throw StateError('latest restore schedule requires vmName');
    }
    final entries = await _loadRestoreEntries(storageId: storage.storage.id);
    for (final entry in entries) {
      if (entry.vmName == vmName && entry.hasAllDisks) {
        return entry.xmlPath;
      }
    }
    throw StateError('no complete restore XML found for VM "$vmName"');
  }

  void _failScheduledJobStart(ScheduledJob schedule, String message) {
    final type = schedule.type == ScheduledJobType.backup ? AgentJobType.backup : AgentJobType.restore;
    final server = _serverById(schedule.serverId);
    final storage = _resolveStorageById(schedule.storageId);
    final vmName = _scheduledJobVmName(schedule);
    final jobId = _createJob(type, vmName: vmName, storageId: schedule.storageId, scheduleId: schedule.id);
    _setJobContext(jobId, source: server == null ? schedule.name : _formatJobSource(server, vmName), storageLabel: storage?.storage.name);
    final current = _jobs[jobId];
    if (current == null) {
      return;
    }
    _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
    _notifyJobCompletion(jobId, type: type, state: AgentJobState.failure, message: message);
  }

  String _scheduledJobVmName(ScheduledJob schedule) {
    final configuredVm = schedule.vmName.trim();
    if (configuredVm.isNotEmpty) {
      return configuredVm;
    }
    if (schedule.restoreXmlPath != ScheduledJob.latestRestoreXmlPath) {
      return _extractVmNameFromXmlPath(schedule.restoreXmlPath);
    }
    return '';
  }

  Future<void> _loadAgentSettings() async {
    _agentSettings = await _agentSettingsStore.load();
    await _maybeRefreshVirtBackupAccount();
  }

  Future<void> _ensureTlsAssets() async {
    final dir = _agentSettingsStore.file.parent;
    await dir.create(recursive: true);
    final certFile = File('${dir.path}${Platform.pathSeparator}agent.crt');
    final keyFile = File('${dir.path}${Platform.pathSeparator}agent.key');
    final certExists = await certFile.exists();
    final keyExists = await keyFile.exists();
    if (certExists && keyExists) {
      return;
    }

    final keyPair = CryptoUtils.generateRSAKeyPair(keySize: 2048);
    final privateKey = keyPair.privateKey as RSAPrivateKey;
    final publicKey = keyPair.publicKey as RSAPublicKey;
    final dn = {'CN': 'virtbackup-agent'};
    final csrPem = X509Utils.generateRsaCsrPem(dn, privateKey, publicKey, san: ['localhost', '127.0.0.1']);
    final certPem = X509Utils.generateSelfSignedCertificate(privateKey, csrPem, 3650);
    final keyPem = CryptoUtils.encodeRSAPrivateKeyToPem(privateKey);

    await certFile.writeAsString(certPem);
    await keyFile.writeAsString(keyPem);
    await AppSettingsStore.setFilePermissions(certFile, ownerOnly: false);
    await AppSettingsStore.setFilePermissions(keyFile, ownerOnly: true);
    _hostLog('Generated TLS certificate and key at ${certFile.path} and ${keyFile.path}.');
  }

  Future<void> _ensureAgentToken() async {
    final existing = await _agentSettingsStore.loadAgentToken();
    if (existing != null && existing.isNotEmpty) {
      _agentToken = existing;
      return;
    }
    _agentToken = await _agentSettingsStore.generateAndStoreAgentToken();
    _hostLog('Generated agent auth token at ${_agentSettingsStore.tokenFile.path}.');
  }

  SecurityContext _buildTlsContext() {
    final dir = _agentSettingsStore.file.parent;
    final certPath = '${dir.path}${Platform.pathSeparator}agent.crt';
    final keyPath = '${dir.path}${Platform.pathSeparator}agent.key';
    final context = SecurityContext();
    context.useCertificateChain(certPath);
    context.usePrivateKey(keyPath);
    return context;
  }

  Future<void> _applyAgentSettings(AppSettings agentSettings, {required String reason, bool forceRestartSshListeners = false, bool runServerRefreshInBackground = false}) async {
    final previousById = {for (final server in _agentSettings.servers) server.id: server};
    final nextById = {for (final server in agentSettings.servers) server.id: server};
    final previousIds = previousById.keys.toSet();
    final nextIds = nextById.keys.toSet();
    final removed = previousIds.difference(nextIds);
    final added = nextIds.difference(previousIds);
    final common = previousIds.intersection(nextIds);
    final changed = <String>{};
    for (final id in common) {
      final previous = previousById[id];
      final next = nextById[id];
      if (previous == null || next == null) {
        continue;
      }
      if (!_isSameServerConfig(previous, next)) {
        changed.add(id);
      }
    }

    _agentSettings = agentSettings;
    _cachedGdriveDriver = null;
    await _agentSettingsStore.save(agentSettings);
    _restartScheduleTimer();
    _restartAccountRefreshTimer();

    Future<void> syncServers() async {
      for (final id in removed) {
        await _safeStopEventListener(id);
        _vmStatusByServerId.remove(id);
        _missingToolsByServerId.remove(id);
      }

      if (forceRestartSshListeners) {
        for (final server in nextById.values) {
          if (server.connectionType == ConnectionType.ssh) {
            await _safeStopEventListener(server.id);
            await _refreshServer(server, reason: 'settings/changed', refreshRequiredTools: true);
            await _safeStartEventListener(server);
          } else {
            _vmStatusByServerId.remove(server.id);
            _missingToolsByServerId.remove(server.id);
          }
        }
        return;
      }

      for (final id in added) {
        final server = nextById[id];
        if (server == null) {
          continue;
        }
        if (server.connectionType == ConnectionType.ssh) {
          await _refreshServer(server, reason: 'settings/added', refreshRequiredTools: true);
          await _safeStartEventListener(server);
        } else {
          _vmStatusByServerId.remove(id);
          _missingToolsByServerId.remove(id);
        }
      }

      for (final id in changed) {
        final server = nextById[id];
        if (server == null) {
          continue;
        }
        await _safeStopEventListener(id);
        if (server.connectionType == ConnectionType.ssh) {
          await _refreshServer(server, reason: 'settings/changed', refreshRequiredTools: true);
          await _safeStartEventListener(server);
        } else {
          _vmStatusByServerId.remove(id);
          _missingToolsByServerId.remove(id);
        }
      }
    }

    if (runServerRefreshInBackground) {
      _hostLog('Settings updated (reason: $reason). Server refresh scheduled in background.');
      unawaited(
        syncServers().catchError((error, stackTrace) {
          _hostLogError('Background server refresh failed.', error, stackTrace is StackTrace ? stackTrace : StackTrace.current);
        }),
      );
      return;
    }

    await syncServers();
    _hostLog('Settings updated (reason: $reason).');
  }

  AppSettings _settingsFromApiConfig(AppSettings agentSettings) {
    return agentSettings.copyWith(virtBackupAccount: _agentSettings.virtBackupAccount);
  }

  Future<void> _ensureBackupBasePathWritable(String backupPath) async {
    final trimmedPath = backupPath.trim();
    if (trimmedPath.isEmpty) {
      throw 'Backup base path is required.';
    }
    final virtBackupDir = Directory('$trimmedPath${Platform.pathSeparator}VirtBackup');
    try {
      await virtBackupDir.create(recursive: true);
      final testFile = File('${virtBackupDir.path}${Platform.pathSeparator}.write-test-${DateTime.now().microsecondsSinceEpoch}-$pid');
      await testFile.writeAsString('virtbackup write test\n', flush: true);
      await testFile.delete();
    } catch (error) {
      throw 'Backup base path is not writable: ${virtBackupDir.path} ($error)';
    }
  }

  Future<void> _refreshStorageWriteStatus() async {
    try {
      await _ensureBackupBasePathWritable(_agentSettings.backupPath);
      _storageWritable = true;
      _storageWriteError = '';
      _hostLog('Storage write check OK: ${_agentSettings.backupPath}${Platform.pathSeparator}VirtBackup');
    } catch (error) {
      _storageWritable = false;
      _storageWriteError = error.toString();
      _hostLog('Storage write check failed. $error');
    }
  }

  AppSettings _settingsFromWorkerUpdate(AppSettings workerSettings) {
    final workerStorageById = {for (final storage in workerSettings.storage) storage.id: storage};
    final mergedStorage = _agentSettings.storage.map((storage) => workerStorageById[storage.id] ?? storage).toList();
    return _agentSettings.copyWith(storage: mergedStorage);
  }

  Future<void> _safeStartEventListener(ServerConfig server) async {
    if ((_missingToolsByServerId[server.id] ?? const <String>[]).contains('virsh')) {
      _hostLog('Skipping VM event listener for ${server.name}: missing required tool virsh.');
      return;
    }
    try {
      await _startEventListener(server);
    } catch (error, stackTrace) {
      _hostLogError('Failed to start event listener for ${server.name}.', error, stackTrace);
    }
  }

  Future<void> _safeStopEventListener(String serverId) async {
    try {
      await _stopEventListener(serverId);
    } catch (error, stackTrace) {
      _hostLogError('Failed to stop event listener for $serverId.', error, stackTrace);
    }
  }

  Future<void> _maybeRefreshVirtBackupAccount() async {
    final account = _agentSettings.virtBackupAccount;
    if (!account.isConnected || account.accessTokenExpiresAt == null || account.refreshTokenExpiresAt == null || account.accountBaseUrl.trim().isEmpty) {
      return;
    }
    final now = DateTime.now().toUtc();
    if (!now.isBefore(account.refreshTokenExpiresAt!.toUtc())) {
      _hostLog('Virt Backup account refresh token expired for ${account.email}; clearing stored account tokens.');
      final updated = _agentSettings.copyWith(virtBackupAccount: VirtBackupAccountTokens.empty());
      await _applyAgentSettings(updated, reason: 'virtbackup-account-expired', forceRestartSshListeners: false);
      return;
    }
    final accessExpiresAt = account.accessTokenExpiresAt!.toUtc();
    final refreshAt = _virtBackupAccountRefreshAt(account);
    if (refreshAt == null) {
      return;
    }
    if (now.isBefore(refreshAt) && now.isBefore(accessExpiresAt)) {
      _restartAccountRefreshTimer();
      return;
    }
    try {
      _hostLog(
        'Refreshing Virt Backup account token for ${account.email}; access expires at ${_formatLocalLogTime(accessExpiresAt)}, refresh token expires at ${_formatLocalLogTime(account.refreshTokenExpiresAt)}.',
      );
      final refreshed = await _refreshVirtBackupAccount(account);
      final updated = _agentSettings.copyWith(virtBackupAccount: refreshed);
      await _applyAgentSettings(updated, reason: 'virtbackup-account-refresh', forceRestartSshListeners: false);
      _hostLog(
        'Virt Backup account token refreshed for ${refreshed.email}; next access expiry ${_formatLocalLogTime(refreshed.accessTokenExpiresAt)}, refresh expiry ${_formatLocalLogTime(refreshed.refreshTokenExpiresAt)}.',
      );
    } on _VirtBackupAccountRefreshRejected catch (error) {
      _hostLog('Virt Backup account refresh rejected: ${error.message}; clearing stored account tokens.');
      final updated = _agentSettings.copyWith(virtBackupAccount: VirtBackupAccountTokens.empty());
      await _applyAgentSettings(updated, reason: 'virtbackup-account-rejected', forceRestartSshListeners: false);
    } catch (error, stackTrace) {
      _hostLogError('Virt Backup account refresh failed.', error, stackTrace);
      _scheduleVirtBackupAccountRefreshRetry();
    }
  }

  DateTime? _virtBackupAccountRefreshAt(VirtBackupAccountTokens account) {
    if (!account.isConnected || account.accessTokenExpiresAt == null || account.refreshTokenExpiresAt == null) {
      return null;
    }
    final issuedAt = account.refreshTokenExpiresAt!.toUtc().subtract(const Duration(days: 30));
    final accessExpiresAt = account.accessTokenExpiresAt!.toUtc();
    return issuedAt.add(Duration(milliseconds: accessExpiresAt.difference(issuedAt).inMilliseconds * 2 ~/ 3));
  }

  void _scheduleVirtBackupAccountRefreshRetry() {
    _accountRefreshTimer?.cancel();
    _accountRefreshTimer = Timer(const Duration(minutes: 5), () {
      _accountRefreshTimer = null;
      unawaited(_maybeRefreshVirtBackupAccount());
    });
  }

  Future<VirtBackupAccountTokens> _refreshVirtBackupAccount(VirtBackupAccountTokens account) async {
    final baseUri = Uri.parse(account.accountBaseUrl);
    final refreshUri = baseUri.replace(path: '/api/auth/refresh', queryParameters: null, fragment: null);
    final client = HttpClient();
    try {
      final request = await client.postUrl(refreshUri);
      request.headers.contentType = ContentType.json;
      request.write(jsonEncode(<String, String>{'refreshToken': account.refreshToken}));
      final response = await request.close();
      final body = await response.transform(utf8.decoder).join();
      if (response.statusCode != 200) {
        if (response.statusCode == 401 && body.contains('invalid_refresh_token')) {
          throw const _VirtBackupAccountRefreshRejected('invalid refresh token');
        }
        throw StateError('Account refresh returned HTTP ${response.statusCode}: $body');
      }
      final decoded = jsonDecode(body);
      if (decoded is! Map) {
        throw StateError('Account refresh returned invalid JSON.');
      }
      final accessToken = (decoded['accessToken'] ?? decoded['sessionToken'] ?? '').toString();
      final refreshToken = (decoded['refreshToken'] ?? '').toString();
      final email = (decoded['email'] ?? account.email).toString();
      if (email.isEmpty || accessToken.isEmpty || refreshToken.isEmpty) {
        throw StateError('Account refresh returned incomplete tokens.');
      }
      return VirtBackupAccountTokens(
        email: email,
        accountBaseUrl: account.accountBaseUrl,
        accessToken: accessToken,
        accessTokenExpiresAt: AppSettings.parseDateTimeOrNull(decoded['accessTokenExpiresAt']),
        refreshToken: refreshToken,
        refreshTokenExpiresAt: AppSettings.parseDateTimeOrNull(decoded['refreshTokenExpiresAt']),
      );
    } finally {
      client.close();
    }
  }

  Future<void> _refreshAllServers() async {
    final servers = _agentSettings.servers.where((server) => server.connectionType == ConnectionType.ssh).toList();
    for (final server in servers) {
      await _refreshServer(server, reason: 'startup/settings', refreshRequiredTools: true);
      await _safeStartEventListener(server);
    }
  }

  Future<void> _refreshServer(ServerConfig server, {required String reason, required bool refreshRequiredTools}) async {
    try {
      _hostLog('Refreshing server ${server.name} (reason: $reason).');
      final missingTools = refreshRequiredTools ? await _host.missingRequiredRemoteTools(server) : _missingToolsByServerId[server.id] ?? const <String>[];
      if (refreshRequiredTools) {
        _missingToolsByServerId[server.id] = missingTools;
        if (missingTools.isNotEmpty) {
          _hostLog('Server ${server.name} missing required tools: ${missingTools.join(', ')}');
        }
      }
      if (missingTools.contains('tr') || missingTools.contains('virsh')) {
        _vmStatusByServerId[server.id] = [];
        return;
      }
      final vms = await _host.loadVmInventory(server);
      final overlay = await _host.loadOverlayStatusForVms(server, vms);
      final status = vms.map((vm) => VmStatus(vm: vm, hasOverlay: overlay[vm.name] == true, missingTools: missingTools)).toList();
      _vmStatusByServerId[server.id] = status;
    } catch (error, stackTrace) {
      _hostLogError('Failed to refresh ${server.name}.', error, stackTrace);
    }
  }

  Future<void> _startEventListener(ServerConfig server) async {
    if (server.connectionType != ConnectionType.ssh) {
      return;
    }
    await _host.startVmEventListener(
      server,
      onEvent: (line) => _handleVmEventLine(server, line),
      onStopped: () => _scheduleEventListenerRestart(server, 'stopped'),
      onError: (error, stackTrace) => _scheduleEventListenerRestart(server, 'error'),
    );
  }

  bool _isSameServerConfig(ServerConfig a, ServerConfig b) {
    return a.id == b.id &&
        a.name == b.name &&
        a.connectionType == b.connectionType &&
        a.sshHost == b.sshHost &&
        a.sshPort == b.sshPort &&
        a.sshUser == b.sshUser &&
        a.sshPassword == b.sshPassword &&
        a.apiBaseUrl == b.apiBaseUrl &&
        a.apiToken == b.apiToken;
  }

  Future<void> _stopEventListener(String serverId) async {
    _eventRestartTimers.remove(serverId)?.cancel();
    _eventRestartAttempts.remove(serverId);
    await _host.stopVmEventListener(serverId);
  }

  void _scheduleEventListenerRestart(ServerConfig server, String reason) {
    if (_eventRestartTimers.containsKey(server.id)) {
      return;
    }
    final attempt = _eventRestartAttempts[server.id] ?? 0;
    final seconds = (2 << attempt).clamp(2, 30);
    _eventRestartAttempts[server.id] = attempt + 1;
    _hostLog('Scheduling event listener restart for ${server.name} in ${seconds}s (reason: $reason).');
    _eventRestartTimers[server.id] = Timer(Duration(seconds: seconds), () async {
      _eventRestartTimers.remove(server.id);
      if (!_agentSettings.servers.any((item) => item.id == server.id)) {
        return;
      }
      await _safeStartEventListener(server);
    });
  }

  void _handleVmEventLine(ServerConfig server, String line) {
    _hostLog('VM event raw: ${line.trim()}');
    final match = RegExp(r"event '([^']+)' for domain '([^']+)'(?:: (.*))?").firstMatch(line);
    if (match == null) {
      return;
    }
    final eventType = match.group(1) ?? '';
    if (eventType != 'lifecycle') {
      return;
    }
    final domain = match.group(2);
    if (domain == null || domain.isEmpty) {
      return;
    }
    final details = match.group(3) ?? '';
    final nextState = _powerStateFromLifecycleDetails(details);
    if (nextState == null) {
      return;
    }
    _publishEvent('vm.lifecycle', {
      'serverId': server.id,
      'serverName': server.name,
      'vmName': domain,
      'state': nextState.name,
      'details': details,
      'timestamp': DateTime.now().toUtc().toIso8601String(),
    });
    final current = _vmStatusByServerId[server.id];
    if (current == null || current.isEmpty) {
      _scheduleRefresh(server);
      return;
    }
    final index = current.indexWhere((entry) => entry.vm.name == domain || entry.vm.id == domain);
    if (index < 0) {
      _scheduleRefresh(server);
      return;
    }
    final existing = current[index];
    if (existing.vm.powerState == nextState) {
      return;
    }
    current[index] = VmStatus(
      vm: VmEntry(id: existing.vm.id, name: existing.vm.name, powerState: nextState),
      hasOverlay: existing.hasOverlay,
      missingTools: existing.missingTools,
    );
  }

  void _scheduleRefresh(ServerConfig server) {
    _refreshDebounceTimers[server.id]?.cancel();
    _refreshDebounceTimers[server.id] = Timer(const Duration(seconds: 2), () async {
      _refreshDebounceTimers.remove(server.id);
      await _refreshServer(server, reason: 'event-sync', refreshRequiredTools: false);
    });
  }

  VmPowerState? _powerStateFromLifecycleDetails(String details) {
    final normalized = details.toLowerCase();
    if (normalized.contains('started') || normalized.contains('booted') || normalized.contains('running')) {
      return VmPowerState.running;
    }
    if (normalized.contains('stopped') ||
        normalized.contains('shutdown') ||
        normalized.contains('destroyed') ||
        normalized.contains('crashed') ||
        normalized.contains('shutoff') ||
        normalized.contains('shut off') ||
        normalized.contains('suspended')) {
      return VmPowerState.stopped;
    }
    return null;
  }

  Future<void> _handleRequest(HttpRequest request) async {
    final startedAt = DateTime.now();
    final stopwatch = Stopwatch()..start();
    try {
      if (!_isAuthorized(request)) {
        _json(request, 401, {'error': 'unauthorized'});
        return;
      }
      final path = request.uri.path;
      if (request.method == 'GET' && path == '/health') {
        _json(request, 200, {
          'ok': true,
          'hostname': Platform.localHostname,
          'nativeSftpAvailable': _host.nativeSftpAvailable,
          'storageWritable': _storageWritable,
          'storageWriteError': _storageWriteError,
        });
        return;
      }
      if (request.method == 'GET' && path == '/drivers') {
        _json(request, 200, _driverCatalog.values.map((driver) => driver.toMap()).toList());
        return;
      }
      if (request.method == 'GET' && path == '/config') {
        await _maybeRefreshVirtBackupAccount();
        _json(request, 200, _agentSettings.toMap());
        return;
      }
      if (request.method == 'GET' && path == '/events') {
        _handleEventStream(request);
        return;
      }
      if (request.method == 'POST' && path == '/ntfyme/test') {
        final body = await _readJson(request);
        final bodyToken = (body['token'] ?? '').toString().trim();
        final token = bodyToken.isNotEmpty ? bodyToken : _agentSettings.ntfymeToken.trim();
        if (token.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'Ntfy me token is not configured.'});
          return;
        }
        const message = 'Test notification from VirtBackup.';
        final payload = <String, dynamic>{'topic': 'virtbackup-test', 'msg': message, 'push_msg': message};
        final result = await _postNtfymeNotification(token, payload);
        if (result.ok) {
          _json(request, 200, {'success': true, 'message': 'Test notification delivered.', 'statusCode': result.statusCode});
        } else {
          _json(request, 502, {'success': false, 'error': result.error ?? 'Ntfy me request failed.', 'statusCode': result.statusCode, 'body': result.body});
        }
        return;
      }
      if (request.method == 'POST' && path == '/notifications/email/test') {
        final body = await _readJson(request);
        final bodyTo = (body['to'] ?? '').toString().trim();
        final to = bodyTo.isNotEmpty ? bodyTo : _agentSettings.notificationEmail.trim();
        if (to.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'Email address is not configured.'});
          return;
        }
        final result = await _postEmailMessage(to: to, subject: 'Virt Backup test email', textBody: _buildTestEmailTextBody(), htmlBody: _buildTestEmailHtmlBody());
        if (result.ok) {
          _json(request, 200, {'success': true, 'message': 'Test email delivered.', 'statusCode': result.statusCode});
        } else {
          _json(request, 502, {'success': false, 'error': result.error ?? 'Email request failed.', 'statusCode': result.statusCode, 'body': result.body});
        }
        return;
      }
      if (request.method == 'POST' && path == '/sftp/test') {
        final body = await _readJson(request);
        final host = (body['host'] ?? '').toString().trim();
        final portValue = body['port'];
        final port = (portValue is num ? portValue.toInt() : int.tryParse((portValue ?? '').toString()));
        final username = (body['username'] ?? '').toString().trim();
        final password = (body['password'] ?? '').toString();
        final basePath = (body['basePath'] ?? '').toString().trim();

        if (host.isEmpty || port == null || username.isEmpty || password.isEmpty || basePath.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'Missing SFTP settings (host/username/password/basePath).'});
          return;
        }
        if (port <= 0 || port > 65535) {
          _json(request, 400, {'success': false, 'error': 'Invalid SFTP port.'});
          return;
        }
        try {
          final message = await _testSftpConnection(host: host, port: port, username: username, password: password, basePath: basePath);
          _json(request, 200, {'success': true, 'message': message});
        } catch (error) {
          _json(request, 200, {'success': false, 'message': error.toString()});
        }
        return;
      }
      if (request.method == 'POST' && path == '/config') {
        final body = await _readJson(request);
        final agentSettings = _settingsFromApiConfig(AppSettings.fromMap(body));
        try {
          await _ensureBackupBasePathWritable(agentSettings.backupPath);
        } catch (error) {
          _storageWritable = false;
          _storageWriteError = error.toString();
          _json(request, 400, {'success': false, 'error': error.toString()});
          return;
        }
        _storageWritable = true;
        _storageWriteError = '';
        await _applyAgentSettings(agentSettings, reason: 'api', forceRestartSshListeners: true, runServerRefreshInBackground: true);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/oauth/google') {
        final body = await _readJson(request);
        final storageId = (body['storageId'] ?? '').toString().trim();
        if (storageId.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'missing storageId'});
          return;
        }
        final storage = _storageById(storageId);
        if (storage == null || storage.driverId != 'gdrive') {
          _json(request, 400, {'success': false, 'error': 'storage is not a Google Drive storage'});
          return;
        }
        final accessToken = (body['accessToken'] ?? '').toString();
        final refreshToken = (body['refreshToken'] ?? '').toString();
        if (refreshToken.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'missing refreshToken'});
          return;
        }
        final accountEmail = (body['accountEmail'] ?? '').toString();
        final scope = (body['scope'] ?? '').toString();
        final expiresAt = _parseExpiresAt(body['expiresAt']);
        final params = Map<String, dynamic>.from(storage.params);
        params['accessToken'] = accessToken;
        params['refreshToken'] = refreshToken;
        params['accountEmail'] = accountEmail;
        if (scope.isEmpty) {
          params.remove('scope');
        } else {
          params['scope'] = scope;
        }
        if (expiresAt == null) {
          params.remove('expiresAt');
        } else {
          params['expiresAt'] = expiresAt.toUtc().toIso8601String();
        }
        final updatedStorages = _replaceStorageParams(storageId: storageId, params: params);
        final updated = _agentSettings.copyWith(storage: updatedStorages);
        await _applyAgentSettings(updated, reason: 'oauth', forceRestartSshListeners: false);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/oauth/google/clear') {
        final body = await _readJson(request);
        final storageId = (body['storageId'] ?? '').toString().trim();
        if (storageId.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'missing storageId'});
          return;
        }
        final storage = _storageById(storageId);
        if (storage == null || storage.driverId != 'gdrive') {
          _json(request, 400, {'success': false, 'error': 'storage is not a Google Drive storage'});
          return;
        }
        final params = Map<String, dynamic>.from(storage.params);
        params['accessToken'] = '';
        params['refreshToken'] = '';
        params['accountEmail'] = '';
        params.remove('expiresAt');
        final updatedStorages = _replaceStorageParams(storageId: storageId, params: params);
        final updated = _agentSettings.copyWith(storage: updatedStorages);
        await _applyAgentSettings(updated, reason: 'oauth', forceRestartSshListeners: false);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/account/virtbackup') {
        final body = await _readJson(request);
        final email = (body['email'] ?? '').toString().trim();
        final accountBaseUrl = (body['accountBaseUrl'] ?? '').toString().trim();
        final accessToken = (body['accessToken'] ?? '').toString();
        final refreshToken = (body['refreshToken'] ?? '').toString();
        if (email.isEmpty || accountBaseUrl.isEmpty || accessToken.isEmpty || refreshToken.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'missing account tokens'});
          return;
        }
        final updated = _agentSettings.copyWith(
          virtBackupAccount: VirtBackupAccountTokens(
            email: email,
            accountBaseUrl: accountBaseUrl,
            accessToken: accessToken,
            accessTokenExpiresAt: _parseExpiresAt(body['accessTokenExpiresAt']),
            refreshToken: refreshToken,
            refreshTokenExpiresAt: _parseExpiresAt(body['refreshTokenExpiresAt']),
          ),
        );
        await _applyAgentSettings(updated, reason: 'virtbackup-account', forceRestartSshListeners: false);
        _hostLog(
          'Virt Backup account login stored for $email; access expires at ${_formatLocalLogTime(updated.virtBackupAccount.accessTokenExpiresAt)}, refresh expires at ${_formatLocalLogTime(updated.virtBackupAccount.refreshTokenExpiresAt)}.',
        );
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/account/virtbackup/clear') {
        final email = _agentSettings.virtBackupAccount.email.trim();
        final updated = _agentSettings.copyWith(virtBackupAccount: VirtBackupAccountTokens.empty());
        await _applyAgentSettings(updated, reason: 'virtbackup-account', forceRestartSshListeners: false);
        _hostLog(email.isEmpty ? 'Virt Backup account tokens cleared.' : 'Virt Backup account tokens cleared for $email.');
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'GET' && path.startsWith('/servers/') && path.endsWith('/vms')) {
        final serverId = path.split('/')[2];
        final data = _vmStatusByServerId[serverId] ?? [];
        _json(request, 200, {'items': data.map((entry) => entry.toMap()).toList(), 'missingTools': _missingToolsByServerId[serverId] ?? const <String>[]});
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/refresh')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'success': false});
          return;
        }
        if (server.connectionType != ConnectionType.ssh) {
          _json(request, 400, {'success': false});
          return;
        }
        await _refreshServer(server, reason: 'manual', refreshRequiredTools: true);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/test')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'success': false});
          return;
        }
        try {
          await _host.runSshCommand(server, 'echo ok');
          _json(request, 200, {'success': true});
        } catch (_) {
          _json(request, 200, {'success': false});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/actions')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'success': false});
          return;
        }
        final body = await _readJson(request);
        final vmName = (body['vmName'] ?? '').toString();
        final action = (body['action'] ?? '').toString();
        if (vmName.isEmpty || action.isEmpty) {
          _json(request, 400, {'success': false});
          return;
        }
        final command = _commandForAction(action, vmName);
        if (command == null) {
          _json(request, 400, {'success': false});
          return;
        }
        try {
          await _host.runSshCommand(server, command);
          _json(request, 200, {'success': true});
        } catch (_) {
          _json(request, 200, {'success': false});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/rename/preview')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'error': 'server not found'});
          return;
        }
        final body = await _readJson(request);
        final vmName = (body['vmName'] ?? '').toString().trim();
        if (vmName.isEmpty) {
          _json(request, 400, {'error': 'missing vmName'});
          return;
        }
        try {
          final preview = await _previewVmRename(server, vmName);
          _json(request, 200, preview);
        } catch (error, stackTrace) {
          if (_isExpectedVmRenamePreviewError(error)) {
            _hostLog('VM rename preview failed for ${server.name}/$vmName. $error');
          } else {
            _hostLogError('VM rename preview failed for ${server.name}/$vmName.', error, stackTrace);
          }
          _json(request, 400, {'error': error.toString()});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/rename/apply')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'success': false, 'error': 'server not found'});
          return;
        }
        final body = await _readJson(request);
        final vmName = (body['vmName'] ?? '').toString().trim();
        final newVmName = (body['newVmName'] ?? '').toString().trim();
        final rawDisks = body['disks'];
        if (vmName.isEmpty || newVmName.isEmpty || rawDisks is! List) {
          _json(request, 400, {'success': false, 'error': 'missing params'});
          return;
        }
        final diskFileNames = <String, String>{};
        for (final item in rawDisks) {
          if (item is! Map) {
            continue;
          }
          final target = (item['target'] ?? '').toString().trim();
          final fileName = (item['fileName'] ?? '').toString().trim();
          if (target.isNotEmpty) {
            diskFileNames[target] = fileName;
          }
        }
        try {
          await _applyVmRename(server: server, vmName: vmName, newVmName: newVmName, diskFileNamesByTarget: diskFileNames);
          await _refreshServer(server, reason: 'rename', refreshRequiredTools: false);
          _json(request, 200, {'success': true});
        } catch (error, stackTrace) {
          _hostLogError('VM rename apply failed for ${server.name}/$vmName -> $newVmName.', error, stackTrace);
          _json(request, 400, {'success': false, 'error': error.toString()});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/cleanup')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'success': false});
          return;
        }
        final body = await _readJson(request);
        final vmName = (body['vmName'] ?? '').toString();
        if (vmName.isEmpty) {
          _json(request, 400, {'success': false});
          return;
        }
        try {
          await _host.cleanupVmOverlays(server, VmEntry(id: vmName, name: vmName, powerState: VmPowerState.stopped));
          await _refreshServer(server, reason: 'cleanup', refreshRequiredTools: false);
          _json(request, 200, {'success': true});
        } catch (_) {
          _json(request, 200, {'success': false});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/backup')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'error': 'server not found'});
          return;
        }
        final body = await _readJson(request);
        final vmName = (body['vmName'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        final driverParams = body['driverParams'] is Map ? Map<String, dynamic>.from(body['driverParams'] as Map) : <String, dynamic>{};
        final blockSizeMBRaw = body['blockSizeMB'];
        final blockSizeMBOverride = _parseBlockSizeMBOverride(blockSizeMBRaw);
        if (blockSizeMBRaw != null && blockSizeMBOverride == null) {
          _json(request, 400, {'error': 'Invalid blockSizeMB. Allowed values: 1, 2, 4, 8.'});
          return;
        }
        final freshRequested = body['fresh'] == true;
        if (vmName.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final resolvedStorage = _resolveStorageById(requestedStorageId);
        if (resolvedStorage == null) {
          _json(request, 400, {'error': 'storage not found or unavailable'});
          return;
        }
        final effectiveFreshRequested = freshRequested && !resolvedStorage.storage.disableFresh;
        if (freshRequested && !effectiveFreshRequested) {
          _hostLog('backup: fresh requested but disabled for storage "${resolvedStorage.storage.name}" (id=${resolvedStorage.storage.id}); continuing without fresh');
        }
        final resolvedDriverId = requestedDriver.isNotEmpty ? requestedDriver : resolvedStorage.driverId;
        final backupPath = resolvedStorage.backupPath;
        final registry = _buildDriverRegistry(backupPath: backupPath, settings: resolvedStorage.settings);
        final descriptor = registry[resolvedDriverId] ?? registry['filesystem']!;
        if (descriptor.usesPath && backupPath.isEmpty) {
          _json(request, 400, {'error': 'missing backup.base_path'});
          return;
        }
        final validationError = descriptor.validateStart();
        if (validationError != null && validationError.isNotEmpty) {
          _json(request, 400, {'error': validationError});
          return;
        }
        final guardMessage = _jobStartGuardMessage(type: AgentJobType.backup, vmName: vmName, storageId: resolvedStorage.storage.id);
        if (guardMessage != null) {
          _json(request, 409, {'error': guardMessage});
          return;
        }
        final jobId = _createJob(AgentJobType.backup, vmName: vmName, storageId: resolvedStorage.storage.id);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startBackupJob(
          jobId,
          server,
          VmEntry(id: vmName, name: vmName, powerState: VmPowerState.stopped),
          backupPath,
          driverIdOverride: resolvedDriverId,
          driverParams: driverParams.isNotEmpty ? driverParams : resolvedStorage.driverParams,
          blockSizeMBOverride: blockSizeMBOverride,
          storage: resolvedStorage,
          freshRequested: effectiveFreshRequested,
        );
        return;
      }
      if (request.method == 'GET' && path == '/schedule-queue') {
        _json(request, 200, _scheduleQueueEntries().map((entry) => entry.toMap()).toList());
        return;
      }
      if (request.method == 'POST' && path.startsWith('/schedule-queue/') && path.endsWith('/remove')) {
        final parts = path.split('/');
        if (parts.length < 4) {
          _json(request, 400, {'error': 'missing schedule id'});
          return;
        }
        final scheduleId = Uri.decodeComponent(parts[2]);
        final removed = _waitingScheduleRuns.remove(scheduleId) != null;
        if (!removed) {
          _json(request, 404, {'error': 'waiting schedule run not found'});
          return;
        }
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path.startsWith('/schedules/') && path.endsWith('/run')) {
        final parts = path.split('/');
        if (parts.length < 4) {
          _json(request, 400, {'error': 'missing schedule id'});
          return;
        }
        final scheduleId = Uri.decodeComponent(parts[2]);
        final schedule = _scheduleById(scheduleId);
        if (schedule == null) {
          _json(request, 404, {'error': 'schedule not found'});
          return;
        }
        try {
          final jobId = await _startScheduledJob(schedule, allowDisabled: true);
          _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        } on _JobGuardRejected catch (error) {
          if (schedule.waitForRunningJobs) {
            final runKey = _manualScheduleRunKey(schedule);
            _waitingScheduleRuns[schedule.id] = runKey;
            _hostLog('Schedule "${schedule.name}" is queued waiting for running jobs. ${error.message}');
            _restartScheduleTimer();
            _json(request, 200, AgentJobStart(jobId: '', queued: true).toMap());
            return;
          }
          _json(request, 409, {'error': error.message});
        } catch (error) {
          _json(request, 400, {'error': error.toString()});
        }
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/restore/precheck')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'error': 'server not found'});
          return;
        }
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        if (xmlPath.isEmpty) {
          _json(request, 400, {'error': 'missing xmlPath'});
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final resolvedStorage = _resolveStorageById(requestedStorageId);
        if (resolvedStorage == null) {
          _json(request, 400, {'error': 'storage not found or unavailable'});
          return;
        }
        final result = await _restorePrecheck(server, xmlPath, storage: resolvedStorage, driverIdOverride: requestedDriver.isEmpty ? null : requestedDriver);
        _json(request, 200, result.toMap());
        return;
      }
      if (request.method == 'POST' && path.startsWith('/servers/') && path.endsWith('/restore/start')) {
        final serverId = path.split('/')[2];
        final server = _agentSettings.servers.firstWhere((item) => item.id == serverId, orElse: () => _missingServer());
        if (server.id == 'missing') {
          _json(request, 404, {'error': 'server not found'});
          return;
        }
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final decision = (body['decision'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        if (xmlPath.isEmpty || decision.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (decision != 'overwrite' && decision != 'define' && decision != 'auto_rename') {
          _json(request, 400, {
            'error': 'unknown restore decision',
            'known': ['overwrite', 'define', 'auto_rename'],
          });
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final resolvedStorage = _resolveStorageById(requestedStorageId);
        if (resolvedStorage == null) {
          _json(request, 400, {'error': 'storage not found or unavailable'});
          return;
        }
        final resolvedDriverId = requestedDriver.isNotEmpty ? requestedDriver : resolvedStorage.driverId;
        final missingTools = await _host.missingRequiredRemoteTools(server);
        if (missingTools.isNotEmpty) {
          _json(request, 409, {'error': 'server is missing required tools: ${missingTools.join(', ')}'});
          return;
        }
        final restoreVmName = _extractVmNameFromXmlPath(xmlPath);
        final guardMessage = _jobStartGuardMessage(type: AgentJobType.restore, vmName: restoreVmName, storageId: resolvedStorage.storage.id);
        if (guardMessage != null) {
          _json(request, 409, {'error': guardMessage});
          return;
        }
        final jobId = _createJob(AgentJobType.restore, vmName: restoreVmName, storageId: resolvedStorage.storage.id);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startRestoreJob(jobId, server, xmlPath, decision, driverIdOverride: resolvedDriverId, storage: resolvedStorage);
        return;
      }
      if (request.method == 'POST' && path == '/restore/sanity') {
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final timestamp = (body['timestamp'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        if (xmlPath.isEmpty || timestamp.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        final resolvedStorage = _resolveStorageById(requestedStorageId);
        if (resolvedStorage == null) {
          _json(request, 400, {'error': 'storage not found or unavailable'});
          return;
        }
        final jobId = _createJob(AgentJobType.sanity, vmName: _extractVmNameFromXmlPath(xmlPath), storageId: resolvedStorage.storage.id);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startSanityCheckJob(jobId, xmlPath, timestamp, storage: resolvedStorage);
        return;
      }
      if (request.method == 'POST' && path == '/restore/quick-check') {
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final timestamp = (body['timestamp'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        if (xmlPath.isEmpty || timestamp.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        final resolvedStorage = _resolveStorageById(requestedStorageId);
        if (resolvedStorage == null) {
          _json(request, 400, {'error': 'storage not found or unavailable'});
          return;
        }
        final jobId = _createJob(AgentJobType.sanity, vmName: _extractVmNameFromXmlPath(xmlPath), storageId: resolvedStorage.storage.id);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startQuickCheckJob(jobId, xmlPath, timestamp, storage: resolvedStorage);
        return;
      }
      if (request.method == 'POST' && path == '/restore/manifests/delete') {
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final timestamp = (body['timestamp'] ?? '').toString();
        final requestedStorageId = (body['storageId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        if (xmlPath.isEmpty || timestamp.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final deletedCount = await _deleteRestoreManifests(xmlPath: xmlPath, timestamp: timestamp, storageId: requestedStorageId, driverIdOverride: requestedDriver.isEmpty ? null : requestedDriver);
        _json(request, 200, {'success': true, 'deletedCount': deletedCount});
        return;
      }
      if (request.method == 'POST' && path.startsWith('/jobs/') && path.endsWith('/cancel')) {
        final parts = path.split('/');
        if (parts.length < 4) {
          _json(request, 400, {'error': 'missing job id'});
          return;
        }
        final jobId = parts[2];
        final canceled = _cancelJob(jobId);
        if (!canceled) {
          _json(request, 404, {'error': 'job not found'});
          return;
        }
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'GET' && path == '/jobs/history') {
        final history = await _loadJobHistoryFromLogs();
        _json(request, 200, history.map((entry) => entry.toMap()).toList());
        return;
      }
      if (request.method == 'GET' && path.startsWith('/jobs/history/') && path.endsWith('/log')) {
        final parts = path.split('/');
        if (parts.length != 5) {
          _json(request, 400, {'error': 'missing job id'});
          return;
        }
        final jobId = Uri.decodeComponent(parts[3]).trim();
        final logFile = await _jobHistoryLogFile(jobId);
        if (logFile == null) {
          _json(request, 404, {'error': 'job log not found'});
          return;
        }
        try {
          _json(request, 200, {'jobId': jobId, 'fileName': _baseName(logFile.path), 'content': await logFile.readAsString()});
        } catch (error) {
          _hostLog('Job history failed to read log ${logFile.path}: $error');
          _json(request, 500, {'error': 'job log unreadable'});
        }
        return;
      }
      if (request.method == 'GET' && path.startsWith('/jobs/')) {
        final jobId = path.split('/')[2];
        final job = _jobs[jobId];
        if (job == null) {
          _json(request, 404, {'error': 'job not found'});
          return;
        }
        _json(request, 200, job.toMap());
        return;
      }
      if (request.method == 'GET' && path == '/jobs') {
        _json(request, 200, _jobs.values.map((job) => job.toMap()).toList());
        return;
      }
      if (request.method == 'GET' && path == '/restore/entries') {
        final storageIdOverride = request.uri.queryParameters['storageId'];
        final requestedStorageId = storageIdOverride?.trim() ?? '';
        if (requestedStorageId.isEmpty) {
          _json(request, 400, {'error': 'missing storageId'});
          return;
        }
        final driverIdOverride = request.uri.queryParameters['driverId'];
        final entries = await _loadRestoreEntries(driverIdOverride: driverIdOverride, storageId: requestedStorageId);
        _json(request, 200, entries.map((entry) => entry.toMap()).toList());
        return;
      }

      _json(request, 404, {'error': 'not found'});
    } catch (error, stackTrace) {
      _hostLogError('Agent request failed.', error, stackTrace);
      _json(request, 500, {'error': error.toString()});
    } finally {
      stopwatch.stop();
      final elapsedMs = stopwatch.elapsedMilliseconds;
      if (elapsedMs >= 200) {
        _hostLog('Slow request ${request.method} ${request.uri.path} ${elapsedMs}ms (started ${startedAt.toIso8601String()})');
      }
    }
  }

  bool _isAuthorized(HttpRequest request) {
    if (_agentToken.isEmpty) {
      return false;
    }
    final authHeader = request.headers.value(HttpHeaders.authorizationHeader);
    if (authHeader != null && authHeader.isNotEmpty) {
      final match = RegExp(r'Bearer\s+(.+)', caseSensitive: false).firstMatch(authHeader);
      final token = match?.group(1)?.trim();
      if (token != null && token.isNotEmpty) {
        return token == _agentToken;
      }
    }
    final altHeader = request.headers.value('x-agent-token');
    if (altHeader != null && altHeader.trim().isNotEmpty) {
      return altHeader.trim() == _agentToken;
    }
    return false;
  }

  void _handleEventStream(HttpRequest request) {
    final response = request.response;
    response.bufferOutput = false;
    response.statusCode = 200;
    response.headers.set(HttpHeaders.contentTypeHeader, 'text/event-stream');
    response.headers.set(HttpHeaders.cacheControlHeader, 'no-cache');
    response.headers.set(HttpHeaders.connectionHeader, 'keep-alive');
    response.headers.set(HttpHeaders.accessControlAllowOriginHeader, '*');
    _hostLog('SSE client connected from ${request.connectionInfo?.remoteAddress.address ?? 'unknown'}');
    response.write('event: ready\n');
    response.write('data: ok\n\n');
    unawaited(response.flush());
    final state = _EventStreamState(response);
    _eventStreams[response] = state;
    response.done
        .then((_) {
          _hostLog('SSE client disconnected.');
        })
        .catchError((error) {
          _hostLog('SSE client error: $error');
        })
        .whenComplete(() {
          state.closed = true;
          _eventStreams.remove(response);
          try {
            response.close();
          } catch (_) {}
        });
  }

  void _publishEvent(String type, Map<String, dynamic> payload) {
    if (_eventStreams.isEmpty) {
      _hostLog('SSE event dropped (no clients): $type');
      return;
    }
    final data = jsonEncode({'type': type, 'payload': payload});
    for (final state in _eventStreams.values.toList()) {
      final response = state.response;
      if (state.closed) {
        _eventStreams.remove(response);
        continue;
      }
      try {
        response.write('event: $type\n');
        response.write('data: $data\n\n');
        unawaited(
          response.flush().catchError((error) {
            _hostLog('SSE event send failed: $error');
            state.closed = true;
            _eventStreams.remove(response);
            try {
              response.close();
            } catch (_) {}
          }),
        );
      } catch (error) {
        _hostLog('SSE event send failed: $error');
        state.closed = true;
        _eventStreams.remove(response);
        try {
          response.close();
        } catch (_) {}
      }
    }
    _hostLog('SSE event sent: $type');
  }

  Future<Map<String, dynamic>> _readJson(HttpRequest request) async {
    final content = await request.cast<List<int>>().transform(utf8.decoder).join();
    if (content.isEmpty) {
      return {};
    }
    final decoded = jsonDecode(content);
    if (decoded is! Map) {
      return {};
    }
    return Map<String, dynamic>.from(decoded);
  }

  DateTime? _parseExpiresAt(Object? value) {
    if (value == null) {
      return null;
    }
    if (value is num) {
      final asInt = value.toInt();
      if (asInt <= 0) {
        return null;
      }
      final ms = asInt < 1000000000000 ? asInt * 1000 : asInt;
      return DateTime.fromMillisecondsSinceEpoch(ms, isUtc: true);
    }
    final text = value.toString().trim();
    if (text.isEmpty) {
      return null;
    }
    return DateTime.tryParse(text);
  }

  Future<List<RestoreEntry>> _loadRestoreEntries({String? driverIdOverride, required String storageId}) async {
    final resolvedStorage = _resolveStorageById(storageId);
    if (resolvedStorage == null) {
      return [];
    }
    final resolvedDriverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : resolvedStorage.driverId;
    final driverInfo = _driverCatalog[resolvedDriverId];
    if (driverInfo == null) {
      throw 'unknown driverId: $resolvedDriverId';
    }
    final backupPath = resolvedStorage.backupPath;
    if (driverInfo.usesPath && backupPath.isEmpty) {
      return [];
    }
    final settings = resolvedStorage.settings;
    final driver = _driverForSettings(driverInfo.id, backupPath, settings: settings);
    try {
      await driver.ensureReady();
      await _cacheRelativeDirFromDriver(driver: driver, relativeDir: 'manifests');
      final manifestsRoot = Directory('${driver.storage}${Platform.pathSeparator}manifests');
      final manifestFiles = await _findManifestFiles(manifestsRoot);
      final grouped = <String, List<File>>{};
      for (final manifest in manifestFiles) {
        final relative = _relativePath(fromDir: manifestsRoot, toPath: manifest.path);
        if (relative.isEmpty) {
          continue;
        }
        final parts = relative.split(RegExp(r'[\\/]')).where((part) => part.isNotEmpty).toList();
        if (parts.length < 3) {
          continue;
        }
        final serverId = parts[0].trim();
        final vmName = parts[1].trim();
        final fileName = _baseName(manifest.path);
        final vmDir = Directory('${manifestsRoot.path}${Platform.pathSeparator}$serverId${Platform.pathSeparator}$vmName');
        final timestamp = _extractTimestampFromManifestFileName(fileName);
        if (serverId.isEmpty || vmName.isEmpty || timestamp.isEmpty) {
          continue;
        }
        final key = '$serverId|$vmName|$timestamp|${vmDir.path}';
        grouped.putIfAbsent(key, () => <File>[]).add(manifest);
      }
      final entries = <RestoreEntry>[];
      for (final group in grouped.entries) {
        final entry = await _buildRestoreEntryFromManifestGroup(group.key, group.value, driver);
        if (entry != null) {
          entries.add(entry);
        }
      }
      entries.sort((a, b) => b.timestamp.compareTo(a.timestamp));
      return entries;
    } finally {
      await driver.closeConnections();
    }
  }

  Future<int> _deleteRestoreManifests({required String xmlPath, required String timestamp, required String storageId, String? driverIdOverride}) async {
    final resolvedStorage = _resolveStorageById(storageId);
    if (resolvedStorage == null) {
      throw 'storage not found or unavailable';
    }
    final resolvedDriverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : resolvedStorage.driverId;
    final driverInfo = _driverCatalog[resolvedDriverId];
    if (driverInfo == null) {
      throw 'unknown driverId: $resolvedDriverId';
    }
    final backupPath = resolvedStorage.backupPath;
    if (driverInfo.usesPath && backupPath.isEmpty) {
      throw 'missing backup.base_path';
    }
    final settings = resolvedStorage.settings;
    final driver = _driverForSettings(driverInfo.id, backupPath, settings: settings);
    try {
      await driver.ensureReady();
      await _cacheRelativeDirFromDriver(driver: driver, relativeDir: 'manifests');
      final manifestsRoot = Directory('${driver.storage}${Platform.pathSeparator}manifests');
      final vmDir = _vmDirFromXmlPath(xmlPath);
      final manifests = await _listManifestFilesForTimestamp(vmDir, timestamp);
      if (manifests.isEmpty) {
        return 0;
      }
      var deleted = 0;
      for (final manifest in manifests) {
        final relativePath = _relativePath(fromDir: manifestsRoot, toPath: manifest.path);
        if (relativePath.isEmpty) {
          continue;
        }
        final deletedFile = await driver.deleteFile('manifests/$relativePath');
        if (deletedFile) {
          deleted += 1;
        }
      }
      return deleted;
    } finally {
      await driver.closeConnections();
    }
  }

  Future<List<File>> _findManifestFiles(Directory manifestsRoot) async {
    if (!await manifestsRoot.exists()) {
      return <File>[];
    }
    final manifests = <File>[];
    await for (final entity in manifestsRoot.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = _baseName(entity.path);
      if (name.endsWith('.manifest') || name.endsWith('.manifest.gz')) {
        manifests.add(entity);
      }
    }
    return manifests;
  }

  String _relativePath({required Directory fromDir, required String toPath}) {
    final from = fromDir.absolute.path;
    final target = File(toPath).absolute.path;
    if (target == from) {
      return '';
    }
    final normalizedFrom = from.endsWith(Platform.pathSeparator) ? from : '$from${Platform.pathSeparator}';
    if (!target.startsWith(normalizedFrom)) {
      return '';
    }
    return target.substring(normalizedFrom.length);
  }

  Future<RestoreEntry?> _buildRestoreEntryFromManifestGroup(String key, List<File> manifests, drv.BackupDriver driver) async {
    try {
      final parts = key.split('|');
      if (parts.length < 4) {
        return null;
      }
      final serverId = parts[0];
      final vmName = parts[1];
      final timestamp = parts[2];
      final vmDirPath = parts.sublist(3).join('|');
      final vmDir = Directory(vmDirPath);
      final diskBasenames = <String>[];
      final requiredDiskIds = <String>{};
      final blockSizeMbValues = <int>{};
      for (final manifest in manifests) {
        final sourcePaths = await _readManifestFields(manifest, 'source_path');
        for (final sourcePath in sourcePaths) {
          if (sourcePath.trim().isEmpty) {
            continue;
          }
          final base = sourcePath.split(RegExp(r'[\\/]')).last.trim();
          if (base.isNotEmpty) {
            diskBasenames.add(base);
          }
        }
        final diskIds = await _readManifestFields(manifest, 'disk_id');
        for (final diskId in diskIds) {
          if (diskId.trim().isNotEmpty) {
            requiredDiskIds.add(diskId.trim());
          }
        }
        final blockSizes = await _readManifestFields(manifest, 'block_size');
        for (final blockSize in blockSizes) {
          final parsed = int.tryParse(blockSize.trim());
          if (parsed == null || parsed <= 0) {
            continue;
          }
          final mb = parsed ~/ (1024 * 1024);
          if (mb > 0) {
            blockSizeMbValues.add(mb);
          }
        }
      }
      final diskNamesForTimestamp = await _collectDiskNamesForTimestamp(vmDir, timestamp);
      final missing = requiredDiskIds.where((diskId) => !_diskExistsForTimestamp(diskId, diskNamesForTimestamp)).toList();
      final serverName = _agentSettings.servers.firstWhere((server) => server.id == serverId, orElse: () => _missingServer()).name;
      return RestoreEntry(
        xmlPath: '${vmDir.path}${Platform.pathSeparator}${timestamp}__domain.xml',
        vmName: vmName,
        timestamp: timestamp,
        diskBasenames: diskBasenames,
        missingDiskBasenames: missing,
        blockSizeMbValues: blockSizeMbValues.toList()..sort(),
        sourceServerId: serverId,
        sourceServerName: serverName.isEmpty ? serverId : serverName,
      );
    } catch (error, stackTrace) {
      _hostLogError('Failed to parse restore entry from manifests: $key', error, stackTrace);
      return null;
    }
  }

  String _extractTimestampFromManifestFileName(String name) {
    var value = name.trim();
    if (name.endsWith('.manifest.gz')) {
      value = name.substring(0, name.length - '.manifest.gz'.length).trim();
    } else if (name.endsWith('.manifest')) {
      value = name.substring(0, name.length - '.manifest'.length).trim();
    }
    if (value.isEmpty) {
      return '';
    }
    final separator = value.indexOf('__');
    if (separator <= 0) {
      return value;
    }
    return value.substring(0, separator).trim();
  }

  Future<List<String>> _collectDiskNamesForTimestamp(Directory vmDir, String timestamp) async {
    final names = <String>[];
    final manifests = await _listManifestFilesForTimestamp(vmDir, timestamp);
    for (final manifest in manifests) {
      final diskIds = await _readManifestFields(manifest, 'disk_id');
      if (diskIds.isNotEmpty) {
        names.addAll(diskIds.where((value) => value.trim().isNotEmpty));
        continue;
      }
      final name = _baseName(manifest.parent.path);
      if (name.isNotEmpty) {
        names.add(name);
      }
    }
    return names;
  }

  bool _diskExistsForTimestamp(String diskBaseName, List<String> diskNamesForTimestamp) {
    final normalizedDisk = _sanitizeFileName(diskBaseName);
    for (final diskId in diskNamesForTimestamp) {
      if (diskId == diskBaseName || diskId == normalizedDisk || diskId.endsWith(diskBaseName) || diskId.endsWith(normalizedDisk)) {
        return true;
      }
    }
    return false;
  }

  String _sanitizeFileName(String name) {
    return name.trim().replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
  }

  static const String _sftpRemoteAppFolderName = 'VirtBackup';

  Future<String> _testSftpConnection({required String host, required int port, required String username, required String password, required String basePath}) async {
    final socket = await SSHSocket.connect(host, port, timeout: const Duration(seconds: 10));
    final client = SSHClient(socket, username: username, onPasswordRequest: () => password);
    final sftp = await client.sftp();
    try {
      final normalizedBase = _normalizeRemotePath(basePath);
      if (normalizedBase.isEmpty) {
        throw 'SFTP base path is empty.';
      }
      await _ensureRemoteDir(sftp, normalizedBase);
      final baseAttrs = await sftp.stat(normalizedBase);
      if (!baseAttrs.isDirectory) {
        throw 'SFTP base path is not a directory: $normalizedBase';
      }

      final normalized = _remoteJoin(normalizedBase, _sftpRemoteAppFolderName);
      await _ensureRemoteDir(sftp, normalized);
      final attrs = await sftp.stat(normalized);
      if (!attrs.isDirectory) {
        throw 'SFTP VirtBackup folder is not a directory: $normalized';
      }

      final testFile = _remoteJoin(normalized, '.virtbackup_test_${DateTime.now().microsecondsSinceEpoch}');
      final file = await sftp.open(testFile, mode: SftpFileOpenMode.create | SftpFileOpenMode.write | SftpFileOpenMode.truncate);
      try {
        await file.writeBytes(Uint8List.fromList(utf8.encode('virtbackup sftp test\n')));
      } finally {
        await file.close();
      }
      try {
        await sftp.remove(testFile);
      } catch (_) {}
      return 'SFTP connection successful (read/write OK).';
    } finally {
      try {
        sftp.close();
      } catch (_) {}
      try {
        client.close();
      } catch (_) {}
      try {
        socket.close();
      } catch (_) {}
    }
  }

  Future<void> _ensureRemoteDir(SftpClient sftp, String remotePath) async {
    final normalized = _normalizeRemotePath(remotePath);
    if (normalized == '/' || normalized.isEmpty) {
      return;
    }
    final parts = normalized.split('/').where((part) => part.trim().isNotEmpty).toList();
    var current = normalized.startsWith('/') ? '/' : '';
    for (final part in parts) {
      current = current.isEmpty || current == '/' ? '$current$part' : '$current/$part';
      try {
        await sftp.mkdir(current);
      } catch (_) {
        try {
          final attrs = await sftp.stat(current);
          if (attrs.isDirectory) {
            continue;
          }
        } catch (_) {}
        rethrow;
      }
    }
  }

  String _normalizeRemotePath(String path) {
    final trimmed = path.trim();
    if (trimmed.isEmpty) {
      return '';
    }
    var value = trimmed.replaceAll('\\', '/');
    while (value.contains('//')) {
      value = value.replaceAll('//', '/');
    }
    if (value.length > 1 && value.endsWith('/')) {
      value = value.substring(0, value.length - 1);
    }
    return value;
  }

  String _remoteJoin(String a, [String? b, String? c, String? d, String? e]) {
    final parts = <String>[];
    void add(String? value) {
      if (value == null) return;
      final trimmed = value.trim();
      if (trimmed.isEmpty) return;
      parts.add(trimmed);
    }

    add(a);
    add(b);
    add(c);
    add(d);
    add(e);

    final raw = parts.join('/');
    return _normalizeRemotePath(raw.startsWith('/') ? '/$raw' : raw);
  }

  void _json(HttpRequest request, int status, Object body) {
    request.response.statusCode = status;
    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode(body));
    request.response.close();
  }

  drv.BackupDriver _driverForSettings(String driverId, String backupPath, {required AppSettings settings}) {
    final registry = _buildDriverRegistry(backupPath: backupPath, settings: settings);
    final descriptor = registry[driverId];
    if (descriptor == null) {
      throw 'unknown driverId: $driverId';
    }
    return descriptor.create(const <String, dynamic>{});
  }

  BackupStorage? _storageById(String storageId) {
    for (final storage in _agentSettings.storage) {
      if (storage.id == storageId) {
        return storage;
      }
    }
    return null;
  }

  List<BackupStorage> _replaceStorageParams({required String storageId, required Map<String, dynamic> params}) {
    return _agentSettings.storage.map((storage) {
      if (storage.id != storageId) {
        return storage;
      }
      return BackupStorage(
        id: storage.id,
        name: storage.name,
        driverId: storage.driverId,
        enabled: storage.enabled,
        params: params,
        disableFresh: storage.disableFresh,
        storeBlobs: storage.storeBlobs,
        useBlobs: storage.useBlobs,
        uploadConcurrency: storage.uploadConcurrency,
        downloadConcurrency: storage.downloadConcurrency,
      );
    }).toList();
  }

  _ResolvedStorage? _resolveStorageById(String requestedStorageId) {
    final requestedId = requestedStorageId.trim();
    if (requestedId.isEmpty) {
      return null;
    }
    final storage = _storageById(requestedId);
    if (storage == null || !storage.enabled) {
      return null;
    }
    final driverId = storage.driverId.trim();
    if (driverId.isEmpty) {
      return null;
    }
    final settings = _settingsForStorage(storage);
    final backupPath = _backupPathForStorage(storage);
    final driverParams = Map<String, dynamic>.from(storage.params);
    return _ResolvedStorage(storage: storage, settings: settings, driverId: driverId, backupPath: backupPath, driverParams: driverParams);
  }

  String _backupPathForStorage(BackupStorage storage) {
    if (storage.driverId == 'filesystem') {
      final path = storage.params['path']?.toString().trim() ?? '';
      return path;
    }
    return _filesystemBackupPath();
  }

  String _filesystemBackupPath() {
    for (final storage in _agentSettings.storage) {
      if (storage.id != AppSettings.filesystemStorageId) {
        continue;
      }
      return storage.params['path']?.toString().trim() ?? '';
    }
    return '';
  }

  AppSettings _settingsForStorage(BackupStorage storage) {
    return _agentSettings.copyWith(backupStorageId: storage.id);
  }

  Map<String, BackupDriverInfo> _buildDriverCatalog() {
    final registry = _buildDriverRegistry(backupPath: _filesystemBackupPath(), settings: _agentSettings);
    return registry.map((key, descriptor) {
      return MapEntry(key, BackupDriverInfo(id: descriptor.id, label: descriptor.label, usesPath: descriptor.usesPath, capabilities: _mapCapabilities(descriptor.capabilities)));
    });
  }

  Map<String, _DriverDescriptor> _buildDriverRegistry({required String backupPath, required AppSettings settings}) {
    final trimmedPath = backupPath.trim();
    final sourceSettings = settings;
    final filesystem = FilesystemBackupDriver(trimmedPath, blockSizeMB: sourceSettings.blockSizeMB);
    final dummy = DummyBackupDriver(trimmedPath, tmpWritesEnabled: sourceSettings.dummyDriverTmpWrites, blockSizeMB: sourceSettings.blockSizeMB);
    final sftpCapabilities = _catalogSftpCapabilities(sourceSettings);

    return {
      'filesystem': _DriverDescriptor(
        id: 'filesystem',
        label: 'Filesystem',
        usesPath: true,
        capabilities: filesystem.capabilities,
        validateStart: () => null,
        create: (_) => FilesystemBackupDriver(trimmedPath, blockSizeMB: sourceSettings.blockSizeMB),
      ),
      'dummy': _DriverDescriptor(
        id: 'dummy',
        label: 'Dummy',
        usesPath: false,
        capabilities: dummy.capabilities,
        validateStart: () => null,
        create: (params) => DummyBackupDriver(trimmedPath, tmpWritesEnabled: sourceSettings.dummyDriverTmpWrites, blockSizeMB: sourceSettings.blockSizeMB, driverParams: params),
      ),
      'gdrive': _DriverDescriptor(
        id: 'gdrive',
        label: 'Google Drive (Preview)',
        usesPath: false,
        capabilities: _gdriveCapabilities(),
        validateStart: () {
          try {
            final params = _requireSelectedStorageParams(settings: sourceSettings, expectedDriverId: 'gdrive');
            final refreshToken = (params['refreshToken'] ?? '').toString().trim();
            return refreshToken.isEmpty ? 'google drive is not connected' : null;
          } catch (_) {
            return 'google drive is not configured';
          }
        },
        create: (_) => identical(sourceSettings, _agentSettings)
            ? (_cachedGdriveDriver ??= GdriveBackupDriver(
                settings: sourceSettings,
                persistSettings: (updated) => _applyAgentSettings(updated, reason: 'gdrive', forceRestartSshListeners: false),
                settingsDir: _agentSettingsStore.file.parent,
                logInfo: _hostLog,
              ))
            : GdriveBackupDriver(
                settings: sourceSettings,
                persistSettings: (updated) => _applyAgentSettings(updated, reason: 'gdrive', forceRestartSshListeners: false),
                settingsDir: _agentSettingsStore.file.parent,
                logInfo: _hostLog,
              ),
      ),
      'sftp': _DriverDescriptor(
        id: 'sftp',
        label: 'SFTP',
        usesPath: false,
        capabilities: sftpCapabilities,
        validateStart: () {
          try {
            final params = _requireSelectedStorageParams(settings: sourceSettings, expectedDriverId: 'sftp');
            final host = (params['host'] ?? '').toString().trim();
            final user = (params['username'] ?? '').toString().trim();
            final password = (params['password'] ?? '').toString();
            final basePath = (params['basePath'] ?? '').toString().trim();
            if (host.isEmpty || user.isEmpty || password.isEmpty || basePath.isEmpty) {
              return 'sftp is not configured';
            }
            final port = params['port'];
            final parsedPort = port is num ? port.toInt() : int.tryParse((port ?? '').toString().trim());
            if (parsedPort == null || parsedPort <= 0 || parsedPort > 65535) {
              return 'sftp port is invalid';
            }
            return null;
          } catch (_) {
            return 'sftp is not configured';
          }
        },
        create: (_) => SftpBackupDriver(settings: sourceSettings),
      ),
    };
  }

  drv.BackupDriverCapabilities _catalogSftpCapabilities(AppSettings settings) {
    var maxConcurrentWrites = 8;
    final selectedId = settings.backupStorageId?.trim() ?? '';
    if (selectedId.isNotEmpty) {
      for (final storage in settings.storage) {
        if (storage.id != selectedId || storage.driverId != 'sftp') {
          continue;
        }
        maxConcurrentWrites = storage.uploadConcurrency ?? 8;
        break;
      }
    }
    return drv.BackupDriverCapabilities(
      supportsRangeRead: true,
      supportsBatchDelete: false,
      supportsMultipartUpload: false,
      supportsServerSideCopy: false,
      supportsConditionalWrite: false,
      supportsVersioning: false,
      maxConcurrentWrites: maxConcurrentWrites,
      maxConcurrentDirectoryListings: 1,
      params: const <drv.DriverParamDefinition>[],
    );
  }

  drv.BackupDriverCapabilities _gdriveCapabilities() {
    return const drv.BackupDriverCapabilities(
      supportsRangeRead: true,
      supportsBatchDelete: true,
      supportsMultipartUpload: false,
      supportsServerSideCopy: false,
      supportsConditionalWrite: false,
      supportsVersioning: false,
      maxConcurrentWrites: 4,
      maxConcurrentDirectoryListings: 8,
    );
  }

  Map<String, dynamic> _requireSelectedStorageParams({required AppSettings settings, required String expectedDriverId}) {
    final selectedId = settings.backupStorageId?.trim() ?? '';
    if (selectedId.isEmpty) {
      throw StateError('backupStorageId is required.');
    }
    for (final storage in settings.storage) {
      if (storage.id != selectedId) {
        continue;
      }
      if (storage.driverId != expectedDriverId) {
        throw StateError('Storage "$selectedId" is not a "$expectedDriverId" storage.');
      }
      return storage.params;
    }
    throw StateError('Storage "$selectedId" not found.');
  }

  BackupDriverCapabilities _mapCapabilities(drv.BackupDriverCapabilities caps) {
    return BackupDriverCapabilities(
      supportsRangeRead: caps.supportsRangeRead,
      supportsBatchDelete: caps.supportsBatchDelete,
      supportsMultipartUpload: caps.supportsMultipartUpload,
      supportsServerSideCopy: caps.supportsServerSideCopy,
      supportsConditionalWrite: caps.supportsConditionalWrite,
      supportsVersioning: caps.supportsVersioning,
      maxConcurrentWrites: caps.maxConcurrentWrites,
      params: caps.params.map(_mapParamDefinition).toList(),
    );
  }

  DriverParamDefinition _mapParamDefinition(drv.DriverParamDefinition def) {
    return DriverParamDefinition(
      key: def.key,
      label: def.label,
      type: DriverParamType.values.firstWhere((value) => value.name == def.type.name, orElse: () => DriverParamType.text),
      defaultValue: def.defaultValue,
      min: def.min,
      max: def.max,
      step: def.step,
      unit: def.unit,
      help: def.help,
    );
  }

  String? _commandForAction(String action, String vmName) {
    return switch (action) {
      'start' => 'virsh start "$vmName"',
      'reboot' => 'virsh reboot "$vmName"',
      'shutdown' => 'virsh shutdown "$vmName"',
      'forceReset' => 'virsh reset "$vmName"',
      'forceOff' => 'virsh destroy "$vmName"',
      _ => null,
    };
  }

  Future<Map<String, dynamic>> _previewVmRename(ServerConfig server, String vmName) async {
    _validateVmRenameName(vmName);
    final state = await _remoteVmState(server, vmName);
    if (state != 'shut off') {
      throw 'VM must be stopped before rename.';
    }
    final disks = await _remoteVmDiskPaths(server, vmName);
    if (disks.isEmpty) {
      throw 'VM has no file disks to rename.';
    }
    final xml = await _remoteVmInactiveXml(server, vmName);
    await _ensureVmRenameHasNoSnapshotsCheckpointsOrBackingChains(server, vmName: vmName, inactiveDisks: disks, inactiveXml: xml);
    return {
      'vmName': vmName,
      'disks': disks.map((disk) => {'target': disk.key, 'path': disk.value, 'directory': _remoteDirName(disk.value), 'fileName': _remoteBaseName(disk.value)}).toList(),
    };
  }

  bool _isExpectedVmRenamePreviewError(Object error) {
    final message = error.toString();
    return message == 'VM must be stopped before rename.' ||
        message == 'VM has no file disks to rename.' ||
        message.startsWith('Rename is blocked:') ||
        message.startsWith('Invalid VM name:') ||
        message.startsWith('Cannot inspect VM snapshots.') ||
        message.startsWith('Cannot inspect VM checkpoints.');
  }

  Future<void> _applyVmRename({required ServerConfig server, required String vmName, required String newVmName, required Map<String, String> diskFileNamesByTarget}) async {
    _validateVmRenameName(vmName);
    _validateVmRenameName(newVmName);
    final state = await _remoteVmState(server, vmName);
    if (state != 'shut off') {
      throw 'VM must be stopped before rename.';
    }
    if (newVmName != vmName && await _remoteVmExists(server, newVmName)) {
      throw 'VM already exists: $newVmName';
    }
    final disks = await _remoteVmDiskPaths(server, vmName);
    if (disks.isEmpty) {
      throw 'VM has no file disks to rename.';
    }
    final oldXml = await _remoteVmInactiveXml(server, vmName);
    await _ensureVmRenameHasNoSnapshotsCheckpointsOrBackingChains(server, vmName: vmName, inactiveDisks: disks, inactiveXml: oldXml);
    final pathByTarget = <String, String>{};
    final targetPaths = <String>{};
    var hasChanges = newVmName != vmName;
    for (final disk in disks) {
      final currentPath = disk.value.trim();
      _validateVmRenameDiskPath(currentPath);
      final newFileName = diskFileNamesByTarget[disk.key]?.trim() ?? '';
      _validateVmRenameFileName(newFileName);
      final dir = _remoteDirName(currentPath);
      if (dir.isEmpty) {
        throw 'Cannot resolve disk directory for $currentPath';
      }
      final newPath = '$dir/$newFileName';
      if (!targetPaths.add(newPath)) {
        throw 'Duplicate target disk path: $newPath';
      }
      if (!await _remotePathExists(server, currentPath)) {
        throw 'Source disk does not exist: $currentPath';
      }
      if (newPath != currentPath && await _remotePathExists(server, newPath)) {
        throw 'Target disk already exists: $newPath';
      }
      if (newPath != currentPath) {
        hasChanges = true;
      }
      pathByTarget[disk.key] = newPath;
    }
    if (!hasChanges) {
      throw 'No rename changes requested.';
    }
    var xml = oldXml;
    xml = _replaceVmRenameDomainName(xml, oldName: vmName, newName: newVmName);
    for (final disk in disks) {
      final newPath = pathByTarget[disk.key]!;
      if (newPath == disk.value) {
        continue;
      }
      xml = _replaceVmRenameXmlPath(xml, oldPath: disk.value, newPath: newPath);
    }
    final moves = <MapEntry<String, String>>[];
    for (final disk in disks) {
      final newPath = pathByTarget[disk.key]!;
      if (newPath != disk.value) {
        moves.add(MapEntry(disk.value, newPath));
      }
    }
    for (final move in moves) {
      await _runChecked(server, 'mv -- ${_shellQuote(move.key)} ${_shellQuote(move.value)}', 'Move failed: ${move.key} -> ${move.value}');
    }
    final tempDir = Directory.systemTemp.createTempSync('virtbackup-vm-rename-');
    final localXml = File('${tempDir.path}${Platform.pathSeparator}${_sanitizeFileName(newVmName)}.xml');
    final localOldXml = File('${tempDir.path}${Platform.pathSeparator}${_sanitizeFileName(vmName)}-rollback.xml');
    final remoteXml = '/var/tmp/virtbackup/rename-${_sanitizeFileName(newVmName)}.xml';
    final remoteOldXml = '/var/tmp/virtbackup/rename-${_sanitizeFileName(vmName)}-rollback.xml';
    final undefineCommand = oldXml.contains(RegExp(r'<nvram(?:\s|>)')) ? 'virsh undefine ${_shellQuote(vmName)} --nvram' : 'virsh undefine ${_shellQuote(vmName)}';
    var oldVmUndefined = false;
    try {
      await localXml.writeAsString(xml);
      await localOldXml.writeAsString(oldXml);
      await _runChecked(server, 'mkdir -p /var/tmp/virtbackup', 'Cannot create remote temp directory.');
      await _host.uploadLocalFile(server, localXml.path, remoteXml);
      await _host.uploadLocalFile(server, localOldXml.path, remoteOldXml);
      await _runChecked(server, undefineCommand, 'Cannot undefine VM $vmName.');
      oldVmUndefined = true;
      await _runChecked(server, 'virsh define ${_shellQuote(remoteXml)}', 'Cannot define renamed VM $newVmName.');
    } catch (error) {
      if (oldVmUndefined && !await _remoteVmExists(server, vmName)) {
        try {
          await _runChecked(server, 'virsh define ${_shellQuote(remoteOldXml)}', 'Rollback define failed for VM $vmName.');
        } catch (rollbackError, rollbackStackTrace) {
          _hostLogError('VM rename rollback define failed for $vmName.', rollbackError, rollbackStackTrace);
        }
      }
      for (final move in moves.reversed) {
        if (await _remotePathExists(server, move.value) && !await _remotePathExists(server, move.key)) {
          try {
            await _runChecked(server, 'mv -- ${_shellQuote(move.value)} ${_shellQuote(move.key)}', 'Rollback move failed: ${move.value} -> ${move.key}');
          } catch (rollbackError, rollbackStackTrace) {
            _hostLogError('VM rename rollback move failed for ${move.value} -> ${move.key}.', rollbackError, rollbackStackTrace);
          }
        }
      }
      rethrow;
    } finally {
      try {
        await localXml.delete();
      } catch (_) {}
      try {
        await localOldXml.delete();
      } catch (_) {}
      try {
        await tempDir.delete();
      } catch (_) {}
      try {
        await _host.runSshCommand(server, 'rm -f -- ${_shellQuote(remoteXml)} ${_shellQuote(remoteOldXml)}');
      } catch (_) {}
    }
  }

  Future<String> _remoteVmState(ServerConfig server, String vmName) async {
    final command = 'virsh domstate ${_shellQuote(vmName)}';
    final result = await _host.runSshCommand(server, command);
    if ((result.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: command, result: result);
      throw result.stderr.trim().isEmpty ? 'Cannot read VM state.' : result.stderr.trim();
    }
    return result.stdout.trim().toLowerCase();
  }

  Future<String> _remoteVmInactiveXml(ServerConfig server, String vmName) async {
    final command = 'virsh dumpxml --inactive ${_shellQuote(vmName)}';
    final result = await _host.runSshCommand(server, command);
    if ((result.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: command, result: result);
      throw result.stderr.trim().isEmpty ? 'Cannot dump VM XML.' : result.stderr.trim();
    }
    return result.stdout;
  }

  Future<bool> _remoteVmExists(ServerConfig server, String vmName) async {
    final result = await _host.runSshCommand(server, 'virsh dominfo ${_shellQuote(vmName)}');
    return (result.exitCode ?? 1) == 0;
  }

  Future<bool> _remotePathExists(ServerConfig server, String path) async {
    final result = await _host.runSshCommand(server, 'test -e ${_shellQuote(path)}');
    return (result.exitCode ?? 1) == 0;
  }

  Future<void> _ensureVmRenameHasNoSnapshotsCheckpointsOrBackingChains(
    ServerConfig server, {
    required String vmName,
    required List<MapEntry<String, String>> inactiveDisks,
    required String inactiveXml,
  }) async {
    if (_xmlHasNonEmptyBackingStore(inactiveXml)) {
      throw 'Rename is blocked: VM XML contains backingStore metadata.';
    }
    final snapshotCommand = 'virsh snapshot-list --name ${_shellQuote(vmName)}';
    final snapshotResult = await _host.runSshCommand(server, snapshotCommand);
    if ((snapshotResult.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: snapshotCommand, result: snapshotResult);
      throw snapshotResult.stderr.trim().isEmpty ? 'Cannot inspect VM snapshots.' : snapshotResult.stderr.trim();
    }
    final snapshots = snapshotResult.stdout.split('\n').map((line) => line.trim()).where((line) => line.isNotEmpty).toList();
    if (snapshots.isNotEmpty) {
      throw 'Rename is blocked: VM has snapshots.';
    }

    final checkpointCommand = 'virsh checkpoint-list --name ${_shellQuote(vmName)}';
    final checkpointResult = await _host.runSshCommand(server, checkpointCommand);
    if ((checkpointResult.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: checkpointCommand, result: checkpointResult);
      throw checkpointResult.stderr.trim().isEmpty ? 'Cannot inspect VM checkpoints.' : checkpointResult.stderr.trim();
    }
    final checkpoints = checkpointResult.stdout.split('\n').map((line) => line.trim()).where((line) => line.isNotEmpty).toList();
    if (checkpoints.isNotEmpty) {
      throw 'Rename is blocked: VM has checkpoints.';
    }

    final activeDisks = await _remoteVmDiskPaths(server, vmName, inactive: false);
    final inactiveByTarget = {for (final disk in inactiveDisks) disk.key: disk.value};
    for (final activeDisk in activeDisks) {
      final inactiveSource = inactiveByTarget[activeDisk.key];
      if (inactiveSource != null && inactiveSource != activeDisk.value) {
        throw 'Rename is blocked: disk ${activeDisk.key} has an active snapshot or overlay.';
      }
    }
    for (final disk in inactiveDisks) {
      final chain = await _remoteVmRenameBackingChain(server, disk.value);
      if (chain.length > 1) {
        throw 'Rename is blocked: disk ${disk.key} has a backing chain.';
      }
    }
  }

  bool _xmlHasNonEmptyBackingStore(String xml) {
    for (final match in RegExp(r'<backingStore\b[^>]*>').allMatches(xml)) {
      final tag = match.group(0)?.trim() ?? '';
      if (!tag.endsWith('/>')) {
        return true;
      }
    }
    return false;
  }

  Future<List<String>> _remoteVmRenameBackingChain(ServerConfig server, String sourcePath) async {
    final command = 'qemu-img info --backing-chain --force-share ${_shellQuote(sourcePath)}';
    final result = await _host.runSshCommand(server, command);
    if ((result.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: command, result: result);
      final detail = result.stderr.trim();
      throw detail.isEmpty ? 'Cannot inspect disk chain for $sourcePath.' : 'Cannot inspect disk chain for $sourcePath: $detail';
    }
    return _parseVmRenameBackingChain(result.stdout, sourcePath: sourcePath);
  }

  List<String> _parseVmRenameBackingChain(String output, {required String sourcePath}) {
    final paths = <String>[];
    final backingPaths = <String>[];
    for (final line in output.split('\n')) {
      final trimmed = line.trim();
      if (!trimmed.startsWith('image:')) {
        if (trimmed.startsWith('backing file:')) {
          final value = trimmed.substring('backing file:'.length).trim();
          if (value.isNotEmpty && value != '(null)') {
            backingPaths.add(value);
          }
        }
        continue;
      }
      final value = trimmed.substring('image:'.length).trim();
      if (value.isNotEmpty && !paths.contains(value)) {
        paths.add(value);
      }
    }
    for (final path in backingPaths) {
      if (!paths.contains(path)) {
        paths.add(path);
      }
    }
    if (paths.isEmpty) {
      return [sourcePath];
    }
    if (!paths.contains(sourcePath)) {
      paths.insert(0, sourcePath);
    }
    return paths;
  }

  Future<List<MapEntry<String, String>>> _remoteVmDiskPaths(ServerConfig server, String vmName, {bool inactive = true}) async {
    final inactiveFlag = inactive ? ' --inactive' : '';
    final command = 'virsh domblklist --details$inactiveFlag ${_shellQuote(vmName)}';
    final result = await _host.runSshCommand(server, command);
    if ((result.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: command, result: result);
      throw result.stderr.trim().isEmpty ? 'Cannot list VM disks.' : result.stderr.trim();
    }
    final disks = <MapEntry<String, String>>[];
    for (final rawLine in result.stdout.split('\n')) {
      final line = rawLine.trim();
      if (line.isEmpty || line.startsWith('Type') || line.startsWith('---')) {
        continue;
      }
      final columns = line.split(RegExp(r'\s+'));
      if (columns.length < 4 || columns[0] != 'file' || columns[1] != 'disk') {
        continue;
      }
      final target = columns[2].trim();
      final source = columns.sublist(3).join(' ').trim();
      if (target.isEmpty || source.isEmpty) {
        continue;
      }
      _validateVmRenameDiskPath(source);
      disks.add(MapEntry(target, source));
    }
    return disks;
  }

  void _validateVmRenameName(String value) {
    final name = value.trim();
    if (name.isEmpty || name.contains('/') || name.contains('\\') || name.contains(RegExp(r'[\r\n\u0000]'))) {
      throw 'Invalid VM name.';
    }
  }

  void _validateVmRenameDiskPath(String path) {
    final value = path.trim();
    if (value.isEmpty || !value.startsWith('/') || value.endsWith('/') || value.contains('://') || value.contains(RegExp(r'[\r\n\u0000]'))) {
      throw 'Unsupported disk path: $path';
    }
  }

  void _validateVmRenameFileName(String value) {
    final fileName = value.trim();
    if (fileName.isEmpty || fileName == '.' || fileName == '..' || fileName.contains('/') || fileName.contains('\\') || fileName.contains(RegExp(r'[\r\n\u0000]'))) {
      throw 'Invalid disk file name: $value';
    }
  }

  String _replaceVmRenameDomainName(String xml, {required String oldName, required String newName}) {
    final pattern = RegExp('(<name>\\s*)${RegExp.escape(oldName)}(\\s*</name>)');
    final matches = pattern.allMatches(xml).toList();
    if (matches.length != 1) {
      throw 'Expected exactly one VM name entry in XML, found ${matches.length}.';
    }
    return xml.replaceFirst(pattern, '<name>$newName</name>');
  }

  String _replaceVmRenameXmlPath(String xml, {required String oldPath, required String newPath}) {
    final doublePattern = RegExp("file=\"${RegExp.escape(oldPath)}\"");
    final singlePattern = RegExp("file='${RegExp.escape(oldPath)}'");
    final doubleMatches = doublePattern.allMatches(xml).length;
    final singleMatches = singlePattern.allMatches(xml).length;
    if (doubleMatches + singleMatches != 1) {
      throw 'Expected exactly one disk path in XML for $oldPath, found ${doubleMatches + singleMatches}.';
    }
    if (doubleMatches == 1) {
      return xml.replaceFirst(doublePattern, 'file="$newPath"');
    }
    return xml.replaceFirst(singlePattern, "file='$newPath'");
  }

  Future<void> _runChecked(ServerConfig server, String command, String message) async {
    final result = await _host.runSshCommand(server, command);
    if ((result.exitCode ?? 1) != 0) {
      _logFailedRemoteCommand(command: command, result: result);
      final detail = result.stderr.trim();
      throw detail.isEmpty ? message : '$message $detail';
    }
  }

  void _logFailedRemoteCommand({required String command, required SshCommandResult result}) {
    _hostLog('Remote command failed: $command');
    _hostLog('Remote command exit code: ${result.exitCode ?? 'unknown'}');
    final output = result.stdout.trim();
    if (output.isNotEmpty) {
      _hostLog('Remote command stdout: $output');
    }
    final errorOutput = result.stderr.trim();
    if (errorOutput.isNotEmpty) {
      _hostLog('Remote command stderr: $errorOutput');
    }
  }

  String _remoteBaseName(String path) {
    final normalized = path.replaceAll('\\', '/');
    final index = normalized.lastIndexOf('/');
    return index < 0 ? normalized : normalized.substring(index + 1);
  }

  String _remoteDirName(String path) {
    final normalized = path.replaceAll('\\', '/');
    final index = normalized.lastIndexOf('/');
    return index <= 0 ? '' : normalized.substring(0, index);
  }

  String _shellQuote(String value) {
    return "'${value.replaceAll("'", "'\"'\"'")}'";
  }

  String _createJob(AgentJobType type, {String? vmName, String? storageId, String? scheduleId, String? scheduleRunId}) {
    final jobId = '${DateTime.now().millisecondsSinceEpoch}-${type.name}';
    final normalizedScheduleRunId = scheduleRunId?.trim() ?? '';
    _jobs[jobId] = AgentJobStatus(
      id: jobId,
      type: type,
      state: AgentJobState.running,
      message: '',
      totalUnits: 0,
      completedUnits: 0,
      bytesTransferred: 0,
      speedBytesPerSec: 0,
      averageSpeedBytesPerSec: 0,
      physicalBytesTransferred: 0,
      physicalSpeedBytesPerSec: 0,
      averagePhysicalSpeedBytesPerSec: 0,
      totalBytes: 0,
      sanityBytesTransferred: 0,
      sanitySpeedBytesPerSec: 0,
      etaSeconds: null,
      physicalRemainingBytes: 0,
      physicalTotalBytes: 0,
      physicalProgressPercent: 0,
      scheduleId: scheduleId?.trim() ?? '',
      vmName: vmName?.trim() ?? '',
      storageId: storageId?.trim() ?? '',
    );
    _jobControls[jobId] = _JobControl(startedAt: DateTime.now(), vmName: vmName, storageId: storageId, scheduleRunId: normalizedScheduleRunId);
    return jobId;
  }

  String? _jobStartGuardMessage({required AgentJobType type, required String vmName, required String storageId}) {
    if (!_usesBackupRestoreGuards(type)) {
      return null;
    }
    final running = _runningBackupRestoreJobs().toList();
    final maxGlobal = _agentSettings.maxConcurrentBackupRestoreJobs;
    if (running.length >= maxGlobal) {
      return 'Maximum concurrent backup/restore jobs reached ($maxGlobal).';
    }

    final normalizedVmName = vmName.trim();
    if (normalizedVmName.isNotEmpty) {
      final activeForVm = running.where((entry) => _jobControls[entry.key]?.vmName == normalizedVmName).length;
      final maxPerVm = _agentSettings.maxConcurrentJobsPerVm;
      if (activeForVm >= maxPerVm) {
        return 'Maximum concurrent jobs for VM "$normalizedVmName" reached ($maxPerVm).';
      }
    }

    final normalizedStorageId = storageId.trim();
    if (normalizedStorageId.isNotEmpty) {
      final activeForStorage = running.where((entry) => _jobControls[entry.key]?.storageId == normalizedStorageId).length;
      final maxPerStorage = _agentSettings.maxConcurrentJobsPerStorage;
      if (activeForStorage >= maxPerStorage) {
        return 'Maximum concurrent jobs for storage "$normalizedStorageId" reached ($maxPerStorage).';
      }
    }
    return null;
  }

  Iterable<MapEntry<String, AgentJobStatus>> _runningBackupRestoreJobs() {
    return _jobs.entries.where((entry) => entry.value.state == AgentJobState.running && _usesBackupRestoreGuards(entry.value.type));
  }

  bool _usesBackupRestoreGuards(AgentJobType type) {
    return type == AgentJobType.backup || type == AgentJobType.restore;
  }

  void _logJobResult(AgentJobStatus status) {
    final fields = <String, Object?>{'event': 'job_result', 'jobId': status.id, 'type': status.type.name, 'state': status.state.name};
    final message = status.message.trim();
    if (message.isNotEmpty) {
      fields['message'] = message;
    }
    final sizeBytes = status.totalBytes > 0 ? status.totalBytes : null;
    final notification = _buildJobCompletionNotification(status.id, type: status.type, state: status.state, message: status.message, sizeBytes: sizeBytes);
    fields['notificationStatus'] = notification.status;
    fields['title'] = notification.title;
    final vmName = status.vmName.trim();
    if (vmName.isNotEmpty) {
      fields['vmName'] = vmName;
    }
    final storageLabel = _jobControls[status.id]?.storageLabel?.trim() ?? '';
    if (storageLabel.isNotEmpty) {
      fields['storage'] = storageLabel;
    }
    if (notification.source != null && notification.source!.trim().isNotEmpty) {
      fields['source'] = notification.source;
    }
    if (notification.target != null && notification.target!.trim().isNotEmpty) {
      fields['target'] = notification.target;
    }
    if (notification.durationSeconds != null) {
      fields['durationSeconds'] = notification.durationSeconds;
    }
    if (notification.sizeBytes != null) {
      fields['size'] = _formatBytes(notification.sizeBytes!);
    }
    if (notification.error != null && notification.error!.trim().isNotEmpty) {
      fields['error'] = notification.error;
    }
    if (notification.warning != null && notification.warning!.trim().isNotEmpty) {
      fields['warning'] = notification.warning;
    }
    if (status.scheduleId.trim().isNotEmpty) {
      fields['scheduleId'] = status.scheduleId.trim();
    }
    fields['transferred'] = _formatBytes(status.bytesTransferred);
    fields['averageSpeed'] = _formatSpeed(status.averageSpeedBytesPerSec);
    if (status.type != AgentJobType.restore) {
      fields['physicalTransferred'] = _formatBytes(status.physicalBytesTransferred);
      fields['averagePhysicalSpeed'] = _formatSpeed(status.averagePhysicalSpeedBytesPerSec);
      fields['total'] = _formatBytes(status.totalBytes);
      fields['physicalTotal'] = _formatBytes(status.physicalTotalBytes);
    }
    LogWriter.logAgentJsonSync(level: 'info', fields: fields, jobId: status.id);
  }

  Future<List<AgentJobHistoryEntry>> _loadJobHistoryFromLogs() async {
    final agentLogPath = LogWriter.defaultPathForSource('agent', basePath: _agentSettings.backupPath.trim());
    final separatorIndex = agentLogPath.lastIndexOf(Platform.pathSeparator);
    final logsDir = Directory(separatorIndex < 0 ? '.' : agentLogPath.substring(0, separatorIndex));
    if (!await logsDir.exists()) {
      return <AgentJobHistoryEntry>[];
    }

    final files = <File>[];
    await for (final entity in logsDir.list(followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = _baseName(entity.path);
      if (name.startsWith('agent-job-') && name.endsWith('.log')) {
        files.add(entity);
      }
    }

    final byJobId = <String, AgentJobHistoryEntry>{};
    for (final file in files) {
      List<String> lines;
      try {
        lines = await file.readAsLines();
      } catch (error) {
        _hostLog('Job history skipped unreadable log ${file.path}: $error');
        continue;
      }
      for (final line in lines) {
        final entry = _parseJobHistoryLogLine(line);
        if (entry == null) {
          continue;
        }
        final previous = byJobId[entry.jobId];
        if (previous == null || entry.timestamp.isAfter(previous.timestamp)) {
          byJobId[entry.jobId] = entry;
        }
      }
      final jobId = _jobIdFromJobLogPath(file.path);
      if (jobId.isNotEmpty && !byJobId.containsKey(jobId)) {
        byJobId[jobId] = await _unknownJobHistoryEntry(file, lines, jobId);
      }
    }

    final history = byJobId.values.toList()..sort((a, b) => b.timestamp.compareTo(a.timestamp));
    return history;
  }

  Future<File?> _jobHistoryLogFile(String jobId) async {
    if (jobId.isEmpty) {
      return null;
    }
    final agentLogPath = LogWriter.defaultPathForSource('agent', basePath: _agentSettings.backupPath.trim());
    final separatorIndex = agentLogPath.lastIndexOf(Platform.pathSeparator);
    final logsDir = Directory(separatorIndex < 0 ? '.' : agentLogPath.substring(0, separatorIndex));
    if (!await logsDir.exists()) {
      return null;
    }
    await for (final entity in logsDir.list(followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = _baseName(entity.path);
      if (!name.startsWith('agent-job-') || !name.endsWith('.log')) {
        continue;
      }
      if (_jobIdFromJobLogPath(entity.path) == jobId) {
        return entity;
      }
    }
    return null;
  }

  Future<AgentJobHistoryEntry> _unknownJobHistoryEntry(File file, List<String> lines, String jobId) async {
    final timestamp = _lastLogTimestamp(lines) ?? (await file.stat()).modified;
    final timestampText = timestamp.toIso8601String();
    const message = 'Job log has no valid job result JSON.';
    final type = _jobTypeFromJobId(jobId);
    final fields = <String, dynamic>{
      'timestamp': timestampText,
      'level': 'warn',
      'jobId': jobId,
      'type': type.name,
      'state': AgentJobState.unknown.name,
      'message': message,
      'logFile': _baseName(file.path),
    };
    return AgentJobHistoryEntry(
      timestamp: timestamp,
      jobId: jobId,
      type: type,
      state: AgentJobState.unknown,
      message: message,
      vmName: '',
      storageId: '',
      storage: '',
      notificationStatus: '',
      title: '',
      source: '',
      target: '',
      durationSeconds: null,
      sizeBytes: null,
      error: '',
      warning: message,
      scheduleId: '',
      bytesTransferred: null,
      averageSpeedBytesPerSec: null,
      physicalBytesTransferred: null,
      averagePhysicalSpeedBytesPerSec: null,
      totalBytes: null,
      physicalTotalBytes: null,
      fields: fields,
    );
  }

  DateTime? _lastLogTimestamp(List<String> lines) {
    for (final line in lines.reversed) {
      final trimmed = line.trim();
      final firstSpace = trimmed.indexOf(' ');
      if (firstSpace <= 0) {
        continue;
      }
      final parsed = DateTime.tryParse(trimmed.substring(0, firstSpace).trim());
      if (parsed != null) {
        return parsed;
      }
    }
    return null;
  }

  String _jobIdFromJobLogPath(String path) {
    final name = _baseName(path);
    const prefix = 'agent-job-';
    const suffix = '.log';
    if (!name.startsWith(prefix) || !name.endsWith(suffix) || name.length <= prefix.length + suffix.length) {
      return '';
    }
    return name.substring(prefix.length, name.length - suffix.length).trim();
  }

  AgentJobType _jobTypeFromJobId(String jobId) {
    final suffix = jobId.split('-').last.trim();
    for (final type in AgentJobType.values) {
      if (type.name == suffix) {
        return type;
      }
    }
    return AgentJobType.unknown;
  }

  AgentJobHistoryEntry? _parseJobHistoryLogLine(String line) {
    final trimmed = line.trim();
    final firstSpace = trimmed.indexOf(' ');
    final messageMarker = trimmed.indexOf(' message=');
    if (firstSpace <= 0 || messageMarker <= firstSpace) {
      return null;
    }
    final timestampText = trimmed.substring(0, firstSpace).trim();
    final levelTextRaw = trimmed.substring(firstSpace + 1, messageMarker).trim();
    if (!levelTextRaw.startsWith('level=')) {
      return null;
    }
    final levelText = levelTextRaw.substring('level='.length).trim();
    final jsonText = trimmed.substring(messageMarker + ' message='.length).trim();
    Object? decoded;
    try {
      decoded = jsonDecode(jsonText);
    } catch (_) {
      return null;
    }
    if (decoded is! Map) {
      return null;
    }
    final data = Map<String, dynamic>.from(decoded);
    if (data['event'] != 'job_result') {
      return null;
    }
    final jobId = data['jobId']?.toString().trim() ?? '';
    final typeText = data['type']?.toString().trim() ?? '';
    final stateText = data['state']?.toString().trim() ?? '';
    if (timestampText.isEmpty || jobId.isEmpty || typeText.isEmpty || stateText.isEmpty) {
      return null;
    }
    final timestamp = DateTime.tryParse(timestampText);
    if (timestamp == null) {
      return null;
    }
    AgentJobType type;
    AgentJobState state;
    try {
      type = AgentJobType.values.firstWhere((value) => value.name == typeText);
      state = AgentJobState.values.firstWhere((value) => value.name == stateText);
    } catch (_) {
      return null;
    }
    final fields = Map<String, dynamic>.from(data)
      ..remove('event')
      ..remove('notificationStatus');
    fields['timestamp'] = timestampText;
    if (levelText.isNotEmpty) {
      fields['level'] = levelText;
    }
    return AgentJobHistoryEntry(
      timestamp: timestamp,
      jobId: jobId,
      type: type,
      state: state,
      message: data['message']?.toString() ?? '',
      vmName: data['vmName']?.toString() ?? '',
      storageId: data['storageId']?.toString() ?? '',
      storage: data['storage']?.toString() ?? '',
      notificationStatus: data['notificationStatus']?.toString() ?? '',
      title: data['title']?.toString() ?? '',
      source: data['source']?.toString() ?? '',
      target: data['target']?.toString() ?? '',
      durationSeconds: (data['durationSeconds'] as num?)?.toInt(),
      sizeBytes: (data['sizeBytes'] as num?)?.toInt(),
      error: data['error']?.toString() ?? '',
      warning: data['warning']?.toString() ?? '',
      scheduleId: data['scheduleId']?.toString() ?? '',
      bytesTransferred: (data['bytesTransferred'] as num?)?.toInt(),
      averageSpeedBytesPerSec: (data['averageSpeedBytesPerSec'] as num?)?.toDouble(),
      physicalBytesTransferred: (data['physicalBytesTransferred'] as num?)?.toInt(),
      averagePhysicalSpeedBytesPerSec: (data['averagePhysicalSpeedBytesPerSec'] as num?)?.toDouble(),
      totalBytes: (data['totalBytes'] as num?)?.toInt(),
      physicalTotalBytes: (data['physicalTotalBytes'] as num?)?.toInt(),
      fields: fields,
    );
  }

  void _updateJob(String jobId, AgentJobStatus status) {
    final previous = _jobs[jobId];
    final control = _jobControls[jobId];
    final isCancelingStatus = status.message.toLowerCase().startsWith('canceling');
    if (control?.canceled == true && previous != null && previous.state != AgentJobState.running) {
      return;
    }
    if (control?.canceled == true && previous?.state == AgentJobState.running && status.state == AgentJobState.running && !isCancelingStatus) {
      return;
    }
    final effectiveStatus = control?.protectedCancelRequested == true && previous?.state == AgentJobState.running && status.state == AgentJobState.running && !isCancelingStatus
        ? status.copyWith(message: previous?.message)
        : status;
    final next = previous == null
        ? effectiveStatus
        : effectiveStatus.copyWith(
            scheduleId: effectiveStatus.scheduleId.isEmpty ? previous.scheduleId : effectiveStatus.scheduleId,
            vmName: effectiveStatus.vmName.isEmpty ? previous.vmName : effectiveStatus.vmName,
            storageId: effectiveStatus.storageId.isEmpty ? previous.storageId : effectiveStatus.storageId,
          );
    _jobs[jobId] = next;
    if (next.state != AgentJobState.running) {
      if (previous?.state != next.state) {
        _logJobResult(next);
      }
      if (control != null && !control.completed.isCompleted) {
        control.completed.complete(next);
      }
    }
    if (status.state == AgentJobState.failure && previous?.state != AgentJobState.failure) {
      _publishEvent('agent.job_failure', {'jobId': status.id, 'type': status.type.name, 'message': status.message});
    }
  }

  bool _cancelJob(String jobId) {
    final job = _jobs[jobId];
    if (job == null) {
      return false;
    }
    if (job.state != AgentJobState.running) {
      return false;
    }
    if (_jobCancelDisabled(job)) {
      return false;
    }
    final control = _jobControls[jobId];
    if (control?.canceled == true || control?.protectedCancelRequested == true) {
      return false;
    }
    final scheduleRunId = control?.scheduleRunId ?? '';
    if (job.scheduleId.isNotEmpty && scheduleRunId.isNotEmpty) {
      _canceledScheduleRuns.add(scheduleRunId);
      _cancelRunningScheduleJobs(scheduleRunId);
    } else {
      _requestJobCancel(jobId, job, control);
    }
    return true;
  }

  void _cancelRunningScheduleJobs(String scheduleRunId) {
    for (final entry in _jobs.entries) {
      final control = _jobControls[entry.key];
      if (entry.value.state != AgentJobState.running || control?.scheduleRunId != scheduleRunId || control?.canceled == true) {
        continue;
      }
      _requestJobCancel(entry.key, entry.value, control);
    }
  }

  bool _jobCancelDisabled(AgentJobStatus job) {
    final message = job.message.toLowerCase();
    if (job.type == AgentJobType.backup) {
      return message.contains('committing snapshot');
    }
    if (job.type == AgentJobType.restore) {
      return message.contains('finalizing disk files') || message.contains('rebasing restored overlays') || message.contains('uploading domain xml') || message.contains('defining vm');
    }
    return false;
  }

  void _requestJobCancel(String jobId, AgentJobStatus job, _JobControl? control) {
    if (job.type == AgentJobType.backup) {
      control?.canceled = true;
      control?.backupAgent?.cancel();
      control?.workerSendPort?.send({'type': 'cancel'});
      final message = control?.backupVmCleanupRequired == true ? 'Canceling. Finishing VM snapshot cleanup...' : 'Canceling. Checking VM cleanup...';
      _updateJob(jobId, job.copyWith(message: message, speedBytesPerSec: 0, physicalSpeedBytesPerSec: 0, sanitySpeedBytesPerSec: 0));
      return;
    }
    if (job.type == AgentJobType.restore && control?.restoreFinalizing == true) {
      control?.protectedCancelRequested = true;
      _updateJob(jobId, job.copyWith(message: 'Canceling. Finishing restore safely...', speedBytesPerSec: 0, physicalSpeedBytesPerSec: 0, sanitySpeedBytesPerSec: 0));
      return;
    }
    control?.canceled = true;
    control?.backupAgent?.cancel();
    control?.workerSendPort?.send({'type': 'cancel'});
    for (final driver in control?.checkDrivers ?? const <drv.BackupDriver>[]) {
      unawaited(driver.closeConnections());
    }
    if (job.type == AgentJobType.backup || job.type == AgentJobType.restore || job.type == AgentJobType.sanity) {
      control?.workerReceivePort?.close();
      control?.workerReceivePort = null;
      control?.workerSendPort = null;
      control?.workerIsolate?.kill(priority: Isolate.immediate);
      control?.workerIsolate = null;
      control?.resultHandled = true;
      control?.checkDrivers.clear();
    }
    _updateJob(jobId, job.copyWith(state: AgentJobState.canceled, message: 'Canceled', speedBytesPerSec: 0, physicalSpeedBytesPerSec: 0, sanitySpeedBytesPerSec: 0));
    final sizeBytes = job.totalBytes > 0 ? job.totalBytes : null;
    if (job.type == AgentJobType.backup || job.type == AgentJobType.restore) {
      _notifyJobCompletion(jobId, type: job.type, state: AgentJobState.canceled, message: 'Canceled', sizeBytes: sizeBytes);
    } else if (job.type == AgentJobType.sanity) {
      _notifyJobCompletion(jobId, type: AgentJobType.sanity, state: AgentJobState.canceled, message: 'Canceled');
    }
  }

  bool _isJobCanceled(String jobId) {
    return _jobControls[jobId]?.canceled == true;
  }

  void _ensureJobNotCanceled(String jobId) {
    if (_isJobCanceled(jobId)) {
      throw const _JobCanceled();
    }
  }

  void _startBackupJob(
    String jobId,
    ServerConfig server,
    VmEntry vm,
    String backupPath, {
    String? driverIdOverride,
    Map<String, dynamic>? driverParams,
    int? blockSizeMBOverride,
    _ResolvedStorage? storage,
    bool freshRequested = false,
  }) {
    final defaultDriverId = storage?.driverId ?? 'filesystem';
    final driverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : defaultDriverId;
    final driverInfo = _driverCatalog[driverId] ?? _driverCatalog['filesystem']!;
    _setJobContext(
      jobId,
      source: _formatJobSource(server, vm.name),
      target: _formatBackupTarget(driverInfo, backupPath, driverId, storageName: storage?.storage.name),
      storageLabel: _formatBackupTarget(driverInfo, backupPath, driverId, storageName: storage?.storage.name),
    );
    _hostLog('Backup job $jobId using driver: $driverId', jobId: jobId);
    final control = _jobControls[jobId];
    if (control == null) {
      return;
    }
    final workerReceive = ReceivePort();
    control.workerReceivePort = workerReceive;
    unawaited(
      Isolate.spawn(backupWorkerMain, {'sendPort': workerReceive.sendPort}).then((isolate) {
        if (control.resultHandled) {
          isolate.kill(priority: Isolate.immediate);
          return;
        }
        control.workerIsolate = isolate;
      }),
    );
    workerReceive.listen((message) async {
      final payload = Map<String, dynamic>.from(message as Map);
      final type = payload['type']?.toString();
      if (type == 'ready') {
        control.workerSendPort = payload['sendPort'] as SendPort?;
        if (control.canceled) {
          control.resultHandled = true;
          workerReceive.close();
          control.workerReceivePort = null;
          control.workerSendPort = null;
          control.workerIsolate?.kill(priority: Isolate.immediate);
          control.workerIsolate = null;
          _updateJob(jobId, _jobs[jobId]!.copyWith(state: AgentJobState.canceled, message: 'Canceled', speedBytesPerSec: 0, physicalSpeedBytesPerSec: 0, sanitySpeedBytesPerSec: 0));
          _notifyJobCompletion(jobId, type: AgentJobType.backup, state: AgentJobState.canceled, message: 'Canceled');
          return;
        }
        control.workerSendPort?.send({
          'type': 'start',
          'jobId': jobId,
          'driverId': driverId,
          'backupPath': backupPath,
          'driverParams': driverParams ?? const <String, dynamic>{},
          'blockSizeMB': blockSizeMBOverride,
          'fresh': freshRequested,
          'settings': (storage?.settings ?? _agentSettings).toMap(),
          'storage': storage?.storage.toMap(),
          'server': server.toMap(),
          'vm': vm.toMap(),
        });
        return;
      }
      if (type == 'progress') {
        final progress = BackupAgentProgress.fromMap(Map<String, dynamic>.from(payload['progress'] as Map));
        control.backupVmCleanupRequired = progress.vmCleanupRequired;
        final current = _jobs[jobId];
        if (current == null) {
          return;
        }
        if (control.canceled) {
          return;
        }
        _updateJob(
          jobId,
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.backup,
            state: AgentJobState.running,
            message: progress.statusMessage,
            totalUnits: progress.totalDisks,
            completedUnits: progress.completedDisks,
            bytesTransferred: progress.bytesTransferred,
            speedBytesPerSec: progress.speedBytesPerSec,
            averageSpeedBytesPerSec: progress.averageSpeedBytesPerSec,
            physicalBytesTransferred: progress.physicalBytesTransferred,
            physicalSpeedBytesPerSec: progress.physicalSpeedBytesPerSec,
            averagePhysicalSpeedBytesPerSec: progress.averagePhysicalSpeedBytesPerSec,
            totalBytes: progress.totalBytes,
            sanityBytesTransferred: progress.sanityBytesTransferred,
            sanitySpeedBytesPerSec: progress.sanitySpeedBytesPerSec,
            etaSeconds: progress.etaSeconds,
            physicalRemainingBytes: progress.physicalRemainingBytes,
            physicalTotalBytes: progress.physicalTotalBytes,
            physicalProgressPercent: progress.physicalProgressPercent,
            writerQueuedBytes: progress.writerQueuedBytes,
            writerInFlightBytes: progress.writerInFlightBytes,
            driverBufferedBytes: progress.driverBufferedBytes,
          ),
        );
        return;
      }
      if (type == 'settings') {
        final updated = AppSettings.fromMap(Map<String, dynamic>.from(payload['settings'] as Map));
        unawaited(_applyAgentSettings(_settingsFromWorkerUpdate(updated), reason: 'worker', forceRestartSshListeners: false));
        return;
      }
      if (type == 'result') {
        if (control.resultHandled) {
          return;
        }
        control.resultHandled = true;
        final result = BackupAgentResult.fromMap(Map<String, dynamic>.from(payload['result'] as Map));
        final state = control.canceled || result.canceled ? AgentJobState.canceled : (result.success ? AgentJobState.success : AgentJobState.failure);
        final message = state == AgentJobState.canceled ? 'Canceled' : result.message ?? '';
        final current = _jobs[jobId];
        final sizeBytes = current != null && current.totalBytes > 0 ? current.totalBytes : null;
        if (state == AgentJobState.failure && control.canceled != true) {
          try {
            await _refreshServer(server, reason: 'backup-failed', refreshRequiredTools: false);
          } catch (_) {}
        }
        _updateJob(
          jobId,
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.backup,
            state: state,
            message: message,
            totalUnits: current?.totalUnits ?? 0,
            completedUnits: current?.completedUnits ?? 0,
            bytesTransferred: current?.bytesTransferred ?? 0,
            speedBytesPerSec: 0,
            averageSpeedBytesPerSec: current?.averageSpeedBytesPerSec ?? 0,
            physicalBytesTransferred: current?.physicalBytesTransferred ?? 0,
            physicalSpeedBytesPerSec: 0,
            averagePhysicalSpeedBytesPerSec: current?.averagePhysicalSpeedBytesPerSec ?? 0,
            totalBytes: current?.totalBytes ?? 0,
            sanityBytesTransferred: current?.sanityBytesTransferred ?? 0,
            sanitySpeedBytesPerSec: 0,
            etaSeconds: current?.etaSeconds,
            physicalRemainingBytes: current?.physicalRemainingBytes ?? 0,
            physicalTotalBytes: current?.physicalTotalBytes ?? 0,
            physicalProgressPercent: current?.physicalProgressPercent ?? 0,
            writerQueuedBytes: current?.writerQueuedBytes ?? 0,
            writerInFlightBytes: current?.writerInFlightBytes ?? 0,
            driverBufferedBytes: current?.driverBufferedBytes ?? 0,
          ),
        );
        _notifyJobCompletion(jobId, type: AgentJobType.backup, state: state, message: message, sizeBytes: sizeBytes);
        workerReceive.close();
        control.workerReceivePort = null;
        control.workerSendPort = null;
        control.workerIsolate?.kill(priority: Isolate.immediate);
        control.workerIsolate = null;
      }
    });
  }

  int? _parseBlockSizeMBOverride(Object? value) {
    if (value == null) {
      return null;
    }
    final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
    if (parsed == null || (parsed != 1 && parsed != 2 && parsed != 4 && parsed != 8)) {
      return null;
    }
    return parsed;
  }

  void _startSanityCheckJob(String jobId, String xmlPath, String timestamp, {required _ResolvedStorage storage}) {
    final driverInfo = _driverCatalog[storage.driverId];
    if (driverInfo == null) {
      final current = _jobs[jobId];
      if (current != null) {
        final message = 'unknown driverId: ${storage.driverId}';
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
        _notifyJobCompletion(jobId, type: AgentJobType.sanity, state: AgentJobState.failure, message: message);
      }
      return;
    }
    final backupPath = storage.backupPath;
    if (driverInfo.usesPath && backupPath.isEmpty) {
      final current = _jobs[jobId];
      if (current != null) {
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: 'Backup path is empty.'));
      }
      return;
    }
    _setJobContext(jobId, source: xmlPath, storageLabel: storage.storage.name);
    final control = _jobControls[jobId];
    if (control == null) {
      return;
    }
    final workerReceive = ReceivePort();
    control.workerReceivePort = workerReceive;
    unawaited(
      Isolate.spawn(restoreWorkerMain, {'sendPort': workerReceive.sendPort}).then((isolate) {
        control.workerIsolate = isolate;
      }),
    );
    workerReceive.listen((message) async {
      final payload = Map<String, dynamic>.from(message as Map);
      final type = payload['type']?.toString();
      if (type == 'ready') {
        control.workerSendPort = payload['sendPort'] as SendPort?;
        control.workerSendPort?.send({
          'type': 'start',
          'jobId': jobId,
          'driverId': storage.driverId,
          'backupPath': backupPath,
          'decision': 'full_check',
          'xmlPath': xmlPath,
          'timestamp': timestamp,
          'settings': storage.settings.toMap(),
          'storage': storage.storage.toMap(),
          'server': _missingServer().toMap(),
        });
        return;
      }
      if (type == 'status') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        _updateJob(jobId, _toSanityJobStatus(status));
        return;
      }
      if (type == 'context') {
        _setJobContext(jobId, source: payload['source']?.toString(), target: payload['target']?.toString());
        return;
      }
      if (type == 'settings') {
        final updated = AppSettings.fromMap(Map<String, dynamic>.from(payload['settings'] as Map));
        unawaited(_applyAgentSettings(_settingsFromWorkerUpdate(updated), reason: 'worker', forceRestartSshListeners: false));
        return;
      }
      if (type == 'result') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        final sanityStatus = _toSanityJobStatus(status);
        _updateJob(jobId, sanityStatus);
        _notifyJobCompletion(jobId, type: AgentJobType.sanity, state: sanityStatus.state, message: sanityStatus.message);
        workerReceive.close();
        control.workerReceivePort = null;
        control.workerSendPort = null;
        control.workerIsolate?.kill(priority: Isolate.immediate);
        control.workerIsolate = null;
      }
    });
  }

  void _startQuickCheckJob(String jobId, String xmlPath, String timestamp, {required _ResolvedStorage storage}) {
    _startRestoreCheckJob(jobId, xmlPath, timestamp, storage: storage, mode: _RestoreCheckMode.quick);
  }

  AgentJobStatus _toSanityJobStatus(AgentJobStatus status) {
    return AgentJobStatus(
      id: status.id,
      type: AgentJobType.sanity,
      state: status.state,
      message: status.message,
      totalUnits: status.totalUnits,
      completedUnits: status.completedUnits,
      bytesTransferred: status.bytesTransferred,
      speedBytesPerSec: status.speedBytesPerSec,
      averageSpeedBytesPerSec: status.averageSpeedBytesPerSec,
      physicalBytesTransferred: status.physicalBytesTransferred,
      physicalSpeedBytesPerSec: status.physicalSpeedBytesPerSec,
      averagePhysicalSpeedBytesPerSec: status.averagePhysicalSpeedBytesPerSec,
      totalBytes: status.totalBytes,
      sanityBytesTransferred: status.sanityBytesTransferred,
      sanitySpeedBytesPerSec: status.sanitySpeedBytesPerSec,
      etaSeconds: status.etaSeconds,
      physicalRemainingBytes: status.physicalRemainingBytes,
      physicalTotalBytes: status.physicalTotalBytes,
      physicalProgressPercent: status.physicalProgressPercent,
      writerQueuedBytes: status.writerQueuedBytes,
      writerInFlightBytes: status.writerInFlightBytes,
      driverBufferedBytes: status.driverBufferedBytes,
      presentUnits: status.presentUnits,
      missingUnits: status.missingUnits,
    );
  }

  void _startRestoreCheckJob(String jobId, String xmlPath, String timestamp, {required _ResolvedStorage storage, required _RestoreCheckMode mode}) {
    unawaited(() async {
      await LogWriter.withJobLogging(jobId, () async {
        final checkDriversByBlockSizeMB = <int, drv.BackupDriver>{};
        final quickCachesByBlockSizeMB = <int, BlobDirectoryCache>{};
        final checkLabel = mode == _RestoreCheckMode.quick ? 'Quick check' : 'Sanity check';
        final control = _jobControls[jobId];
        try {
          final driverInfo = _driverCatalog[storage.driverId];
          if (driverInfo == null) {
            throw 'unknown driverId: ${storage.driverId}';
          }
          final backupPath = storage.backupPath;
          if (driverInfo.usesPath && backupPath.isEmpty) {
            throw 'Backup path is not configured';
          }
          final vmDir = _vmDirFromXmlPath(xmlPath);
          if (!await vmDir.exists()) {
            throw 'Cannot resolve restore location for $xmlPath';
          }
          final manifests = await _listManifestFilesForTimestamp(vmDir, timestamp);
          if (manifests.isEmpty) {
            throw 'No manifests found for $timestamp';
          }

          var totalBytes = 0;
          var totalBlocks = 0;
          for (final manifest in manifests) {
            final lines = await _readManifestLines(manifest);
            var blockSize = 1024 * 1024;
            int? currentFileSize;
            var currentMaxIndex = -1;
            var inBlocks = false;
            void addCurrentDiskBytes() {
              if (currentFileSize != null && currentFileSize! > 0) {
                totalBytes += currentFileSize!;
              } else if (currentMaxIndex >= 0) {
                totalBytes += (currentMaxIndex + 1) * blockSize;
              }
              currentFileSize = null;
              currentMaxIndex = -1;
            }

            for (final line in lines) {
              final trimmed = line.trim();
              if (trimmed.isEmpty) {
                continue;
              }
              if (trimmed.startsWith('disk_id:')) {
                addCurrentDiskBytes();
                inBlocks = false;
                continue;
              }
              if (!inBlocks) {
                if (trimmed.startsWith('block_size:')) {
                  final value = trimmed.substring('block_size:'.length).trim();
                  final parsed = int.tryParse(value);
                  if (parsed != null && parsed > 0) {
                    blockSize = parsed;
                  }
                } else if (trimmed.startsWith('file_size:')) {
                  final value = trimmed.substring('file_size:'.length).trim();
                  currentFileSize = int.tryParse(value);
                } else if (trimmed == 'blocks:' || trimmed.startsWith('blocks:')) {
                  inBlocks = true;
                }
                continue;
              }
              if (trimmed.endsWith('-> ZERO')) {
                final range = _parseZeroRange(trimmed);
                if (range != null && range.$2 > currentMaxIndex) {
                  currentMaxIndex = range.$2;
                }
                continue;
              }
              final parts = trimmed.split('->');
              if (parts.length < 2) {
                continue;
              }
              final index = int.tryParse(parts.first.trim());
              if (index == null) {
                continue;
              }
              final hash = parts.last.trim();
              if (hash.isNotEmpty && hash != 'ZERO') {
                totalBlocks += 1;
              }
              if (index > currentMaxIndex) {
                currentMaxIndex = index;
              }
            }
            _blockSizeMbFromManifestBytes(blockSize, manifest.path);
            addCurrentDiskBytes();
          }

          _updateJob(jobId, _jobs[jobId]!.copyWith(totalUnits: totalBlocks, completedUnits: 0, bytesTransferred: 0, speedBytesPerSec: 0, totalBytes: totalBytes, message: '$checkLabel...'));

          var bytesChecked = 0;
          var bytesSinceTick = 0;
          var smoothedSpeed = 0.0;
          var speedTrackingStarted = false;
          var lastSpeedUpdate = DateTime.now();
          var lastProgressUpdate = DateTime.now();
          var mismatches = 0;
          var checked = 0;
          var presentBlocks = 0;
          var missingBlocks = 0;
          final maxConcurrentDownloads = storage.driverId == 'filesystem' ? 1 : storage.storage.downloadConcurrency;
          if (maxConcurrentDownloads == null) {
            throw '$checkLabel requires downloadConcurrency for storage ${storage.storage.id}.';
          }
          final blocksByBlockSizeMB = <int, List<_CheckBlockRef>>{};

          void handleBytes(int bytes, {String? message}) {
            if (bytes <= 0) {
              return;
            }
            _ensureJobNotCanceled(jobId);
            bytesChecked += bytes;
            if (speedTrackingStarted) {
              bytesSinceTick += bytes;
            }
            final now = DateTime.now();
            if (speedTrackingStarted) {
              final elapsedMs = now.difference(lastSpeedUpdate).inMilliseconds;
              if (elapsedMs >= 1000) {
                final instant = bytesSinceTick / (elapsedMs / 1000);
                smoothedSpeed = _smoothSpeed(smoothedSpeed, instant);
                bytesSinceTick = 0;
                lastSpeedUpdate = now;
              }
            }
            if (now.difference(lastProgressUpdate).inMilliseconds >= 500) {
              lastProgressUpdate = now;
              _updateJob(
                jobId,
                _jobs[jobId]!.copyWith(
                  totalUnits: totalBlocks,
                  completedUnits: checked,
                  bytesTransferred: bytesChecked,
                  speedBytesPerSec: smoothedSpeed,
                  message: message ?? _jobs[jobId]!.message,
                  presentUnits: presentBlocks,
                  missingUnits: missingBlocks,
                ),
              );
            }
          }

          drv.BackupDriver checkDriverForBlockSize(int blockSizeMB) {
            return checkDriversByBlockSizeMB.putIfAbsent(blockSizeMB, () {
              final settingsForBlockSize = storage.settings.copyWith(blockSizeMB: blockSizeMB);
              final driver = _driverForSettings(driverInfo.id, backupPath, settings: settingsForBlockSize);
              control?.checkDrivers.add(driver);
              return driver;
            });
          }

          for (final manifest in manifests) {
            _ensureJobNotCanceled(jobId);
            final lines = await _readManifestLines(manifest);
            var blockSize = 1024 * 1024;
            var blockSizeMB = 1;
            int? fileSize;
            String? diskId;
            var inBlocks = false;
            for (final line in lines) {
              final trimmed = line.trim();
              if (trimmed.isEmpty) {
                continue;
              }
              if (trimmed.startsWith('disk_id:')) {
                inBlocks = false;
                fileSize = null;
                diskId = trimmed.substring('disk_id:'.length).trim();
                continue;
              }
              if (!inBlocks) {
                if (trimmed.startsWith('block_size:')) {
                  final value = trimmed.substring('block_size:'.length).trim();
                  final parsed = int.tryParse(value);
                  if (parsed != null && parsed > 0) {
                    blockSize = parsed;
                    blockSizeMB = _blockSizeMbFromManifestBytes(blockSize, manifest.path);
                  }
                } else if (trimmed.startsWith('file_size:')) {
                  final value = trimmed.substring('file_size:'.length).trim();
                  fileSize = int.tryParse(value);
                } else if (trimmed == 'blocks:' || trimmed.startsWith('blocks:')) {
                  inBlocks = true;
                }
                continue;
              }
              _ensureJobNotCanceled(jobId);
              final message = diskId == null || diskId.isEmpty ? '$checkLabel...' : '$checkLabel: $diskId';
              if (trimmed.endsWith('-> ZERO')) {
                final range = _parseZeroRange(trimmed);
                if (range != null) {
                  final bytes = _bytesForRange(range.$1, range.$2, fileSize, blockSize);
                  handleBytes(bytes, message: message);
                }
                continue;
              }
              final parts = trimmed.split('->');
              if (parts.length < 2) {
                continue;
              }
              final index = int.tryParse(parts.first.trim());
              if (index == null) {
                continue;
              }
              final hash = parts.last.trim();
              if (hash.isEmpty || hash == 'ZERO') {
                continue;
              }
              final expectedLength = _blockLengthForIndex(index, fileSize, blockSize);
              if (mode == _RestoreCheckMode.quick) {
                final driverForBlockSize = checkDriverForBlockSize(blockSizeMB);
                final cache = quickCachesByBlockSizeMB.putIfAbsent(
                  blockSizeMB,
                  () => BlobDirectoryCache(driver: driverForBlockSize, createShard: (_) async {}, ensureNotCanceled: () => _ensureJobNotCanceled(jobId), ensureMissingShards: false),
                );
                await cache.prefillAllBlobNames(
                  concurrency: maxConcurrentDownloads,
                  onProgress: (loaded, total) {
                    if (total <= 0) {
                      return;
                    }
                    _updateJob(
                      jobId,
                      _jobs[jobId]!.copyWith(
                        totalUnits: total,
                        completedUnits: loaded,
                        bytesTransferred: bytesChecked,
                        speedBytesPerSec: 0,
                        totalBytes: totalBytes,
                        message: '$checkLabel: loading blob cache $loaded/$total shards',
                        presentUnits: presentBlocks,
                        missingUnits: missingBlocks,
                      ),
                    );
                  },
                );
                _updateJob(
                  jobId,
                  _jobs[jobId]!.copyWith(
                    totalUnits: totalBlocks,
                    completedUnits: checked,
                    bytesTransferred: bytesChecked,
                    speedBytesPerSec: smoothedSpeed,
                    totalBytes: totalBytes,
                    message: message,
                    presentUnits: presentBlocks,
                    missingUnits: missingBlocks,
                  ),
                );
                final exists = await cache.blobExists(hash);
                checked += 1;
                if (!exists) {
                  mismatches += 1;
                  missingBlocks += 1;
                } else {
                  presentBlocks += 1;
                }
                handleBytes(expectedLength, message: message);
                continue;
              }
              final refs = blocksByBlockSizeMB.putIfAbsent(blockSizeMB, () => <_CheckBlockRef>[]);
              refs.add(_CheckBlockRef(hash: hash, expectedLength: expectedLength, index: index, message: message));
            }
          }

          if (mode == _RestoreCheckMode.full) {
            for (final blockGroup in blocksByBlockSizeMB.entries) {
              _ensureJobNotCanceled(jobId);
              final blockSizeMB = blockGroup.key;
              final blocks = blockGroup.value;
              if (blocks.isEmpty) {
                continue;
              }
              final driverForBlockSize = checkDriverForBlockSize(blockSizeMB);
              final remote = driverForBlockSize is drv.RemoteBlobDriver ? driverForBlockSize as drv.RemoteBlobDriver : null;
              if (driverInfo.id != 'filesystem' && remote == null) {
                throw '$checkLabel requires remote blob reads for driver ${driverInfo.id}.';
              }

              Future<_CheckBlockFetchResult> startFetch(int position) async {
                final block = blocks[position];
                if (_isJobCanceled(jobId)) {
                  return _CheckBlockFetchResult(position: position, block: block, bytes: const <int>[], missing: false, canceled: true);
                }
                try {
                  if (driverInfo.id == 'filesystem') {
                    final blobFile = driverForBlockSize.blobFile(block.hash);
                    try {
                      final bytes = await blobFile.readAsBytes();
                      return _CheckBlockFetchResult(position: position, block: block, bytes: bytes, missing: false, canceled: false);
                    } on FileSystemException {
                      return _CheckBlockFetchResult(position: position, block: block, bytes: const <int>[], missing: true, canceled: false);
                    }
                  }
                  final builder = BytesBuilder(copy: false);
                  await for (final chunk in remote!.openBlobStream(block.hash, length: block.expectedLength)) {
                    _ensureJobNotCanceled(jobId);
                    builder.add(chunk);
                  }
                  final bytes = builder.takeBytes();
                  return _CheckBlockFetchResult(position: position, block: block, bytes: bytes, missing: bytes.isEmpty, canceled: false);
                } on _JobCanceled {
                  return _CheckBlockFetchResult(position: position, block: block, bytes: const <int>[], missing: false, canceled: true);
                }
              }

              var nextFetch = 0;
              var nextConsume = 0;
              final inFlight = <int, Future<_CheckBlockFetchResult>>{};

              void scheduleFetches() {
                while (inFlight.length < maxConcurrentDownloads && nextFetch < blocks.length) {
                  if (_isJobCanceled(jobId)) {
                    break;
                  }
                  final position = nextFetch;
                  final future = startFetch(position);
                  inFlight[position] = future;
                  nextFetch += 1;
                }
              }

              try {
                scheduleFetches();
                while (nextConsume < blocks.length) {
                  if (_isJobCanceled(jobId)) {
                    throw const _JobCanceled();
                  }
                  scheduleFetches();
                  final future = inFlight[nextConsume];
                  if (future == null) {
                    if (_isJobCanceled(jobId)) {
                      throw const _JobCanceled();
                    }
                    await Future<void>.delayed(const Duration(milliseconds: 1));
                    continue;
                  }
                  final fetched = await future;
                  _ensureJobNotCanceled(jobId);
                  inFlight.remove(nextConsume);
                  nextConsume += 1;
                  if (fetched.canceled) {
                    throw const _JobCanceled();
                  }
                  checked += 1;

                  if (fetched.missing || fetched.bytes.isEmpty) {
                    mismatches += 1;
                    _hostLog('$checkLabel missing blob index=${fetched.block.index} hash=${fetched.block.hash}');
                    handleBytes(fetched.block.expectedLength, message: fetched.block.message);
                    continue;
                  }
                  if (!speedTrackingStarted) {
                    speedTrackingStarted = true;
                    bytesSinceTick = 0;
                    smoothedSpeed = 0;
                    lastSpeedUpdate = DateTime.now();
                  }

                  final hashInput = fetched.bytes is Uint8List ? fetched.bytes as Uint8List : Uint8List.fromList(fetched.bytes);
                  final actual = _host.sha256Hex(hashInput);
                  handleBytes(hashInput.length, message: fetched.block.message);
                  if (actual != fetched.block.hash) {
                    mismatches += 1;
                    _hostLog('$checkLabel hash mismatch index=${fetched.block.index} expected=${fetched.block.hash} got=$actual');
                  }
                  if (fetched.bytes.length < fetched.block.expectedLength) {
                    handleBytes(fetched.block.expectedLength - fetched.bytes.length, message: fetched.block.message);
                  }
                }
              } finally {
                final pending = inFlight.values.toList();
                if (pending.isNotEmpty) {
                  if (_isJobCanceled(jobId)) {
                    for (final future in pending) {
                      unawaited(() async {
                        try {
                          await future;
                        } catch (_) {}
                      }());
                    }
                  } else {
                    await Future.wait(
                      pending.map((future) async {
                        try {
                          await future;
                        } catch (_) {}
                      }),
                    );
                  }
                }
              }
            }
          }

          final resultMessage = mismatches == 0 ? '$checkLabel OK ($checked blocks checked)' : '$checkLabel: $mismatches mismatch(es) out of $checked blocks';
          _updateJob(
            jobId,
            _jobs[jobId]!.copyWith(
              state: AgentJobState.success,
              message: resultMessage,
              totalUnits: totalBlocks,
              completedUnits: checked,
              bytesTransferred: bytesChecked,
              speedBytesPerSec: 0,
              presentUnits: presentBlocks,
              missingUnits: missingBlocks,
            ),
          );
        } catch (error, stackTrace) {
          final isCanceled = error is _JobCanceled || _isJobCanceled(jobId);
          if (isCanceled) {
            _hostLog('$checkLabel canceled.');
          } else {
            _hostLogError('$checkLabel failed.', error, stackTrace);
          }
          _updateJob(jobId, _jobs[jobId]!.copyWith(state: isCanceled ? AgentJobState.canceled : AgentJobState.failure, message: isCanceled ? 'Canceled' : error.toString(), speedBytesPerSec: 0));
        } finally {
          for (final driver in checkDriversByBlockSizeMB.values) {
            try {
              await driver.closeConnections();
            } catch (_) {}
          }
          control?.checkDrivers.clear();
        }
      });
    }());
  }

  Future<RestorePrecheckResult> _restorePrecheck(ServerConfig server, String xmlPath, {required _ResolvedStorage storage, String? driverIdOverride}) async {
    final resolvedStorage = storage;
    final requestedDriverId = driverIdOverride?.trim() ?? '';
    final effectiveDriverId = requestedDriverId.isEmpty ? resolvedStorage.driverId : requestedDriverId;
    final driverInfo = _driverCatalog[effectiveDriverId];
    if (driverInfo == null) {
      throw 'unknown driverId: $effectiveDriverId';
    }
    final backupPath = resolvedStorage.backupPath;
    if (driverInfo.usesPath && backupPath.isEmpty) {
      return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
    }
    final driver = _driverForSettings(driverInfo.id, backupPath, settings: resolvedStorage.settings);
    try {
      await driver.ensureReady();
      await _cacheRelativeDirFromDriver(driver: driver, relativeDir: 'manifests');
      final vmDir = _vmDirFromXmlPath(xmlPath);
      if (!await vmDir.exists()) {
        return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
      }
      final timestamp = _extractTimestampFromManifestXmlPath(xmlPath);
      if (timestamp.isEmpty) {
        return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
      }
      final manifests = await _listManifestFilesForTimestamp(vmDir, timestamp);
      if (manifests.isEmpty) {
        return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
      }
      final diskSourcePaths = <String>[];
      for (final manifest in manifests) {
        final sourcePaths = await _readManifestFields(manifest, 'source_path');
        for (final sourcePath in sourcePaths) {
          final trimmed = sourcePath.trim();
          if (trimmed.isEmpty) {
            continue;
          }
          diskSourcePaths.add(trimmed);
        }
      }
      final vmName = _extractVmNameFromXmlPath(xmlPath);
      if (vmName.isEmpty) {
        return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
      }
      final vmExistsResult = await _host.runSshCommand(server, 'virsh dominfo "$vmName"');
      final vmExists = (vmExistsResult.exitCode ?? 1) == 0;
      if (!vmExists) {
        return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
      }
      var canDefineOnly = true;
      for (final sourcePath in diskSourcePaths) {
        final existsResult = await _host.runSshCommand(server, 'test -f "$sourcePath"');
        if ((existsResult.exitCode ?? 1) != 0) {
          canDefineOnly = false;
          break;
        }
      }
      return RestorePrecheckResult(vmExists: true, canDefineOnly: canDefineOnly);
    } finally {
      await driver.closeConnections();
    }
  }

  Future<void> _cacheRelativeDirFromDriver({required drv.BackupDriver driver, required String relativeDir}) async {
    if (driver is FilesystemBackupDriver) {
      return;
    }
    final normalizedDir = relativeDir.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/');
    if (normalizedDir.isEmpty) {
      return;
    }
    final files = await driver.listRelativeFiles(normalizedDir);
    final remoteFiles = files
        .map((relativePath) => relativePath.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/'))
        .where((relativePath) => relativePath.isNotEmpty)
        .toSet();
    await _pruneCachedRelativeDir(driver: driver, normalizedDir: normalizedDir, remoteFiles: remoteFiles);
    for (final relativePath in files) {
      final normalizedPath = relativePath.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/');
      if (normalizedPath.isEmpty) {
        continue;
      }
      final localFilePath = '${driver.storage}${Platform.pathSeparator}${normalizedPath.replaceAll('/', Platform.pathSeparator)}';
      final localFile = File(localFilePath);
      if (await localFile.exists()) {
        continue;
      }
      final bytes = await driver.readFileBytes(relativePath);
      if (bytes == null) {
        continue;
      }
      final tempFile = File('${localFile.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}');
      await tempFile.parent.create(recursive: true);
      await tempFile.writeAsBytes(bytes);
      await tempFile.rename(localFile.path);
    }
  }

  Future<void> _pruneCachedRelativeDir({required drv.BackupDriver driver, required String normalizedDir, required Set<String> remoteFiles}) async {
    final localRoot = Directory('${driver.storage}${Platform.pathSeparator}${normalizedDir.replaceAll('/', Platform.pathSeparator)}');
    if (!await localRoot.exists()) {
      return;
    }
    await for (final entity in localRoot.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = _baseName(entity.path);
      if (name.contains('.inprogress.')) {
        continue;
      }
      final relative = _relativePath(fromDir: Directory(driver.storage), toPath: entity.path).replaceAll('\\', '/');
      if (relative.isEmpty || remoteFiles.contains(relative)) {
        continue;
      }
      await entity.delete();
    }
  }

  void _startRestoreJob(String jobId, ServerConfig server, String xmlPath, String decision, {_ResolvedStorage? storage, String? driverIdOverride}) {
    if (storage == null) {
      final current = _jobs[jobId];
      if (current != null) {
        const message = 'Restore storage is required.';
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
        _notifyJobCompletion(jobId, type: AgentJobType.restore, state: AgentJobState.failure, message: message);
      }
      return;
    }
    final driverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : storage.driverId;
    final driverInfo = _driverCatalog[driverId];
    if (driverInfo == null) {
      final current = _jobs[jobId];
      if (current != null) {
        final message = 'unknown driverId: $driverId';
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: message));
        _notifyJobCompletion(jobId, type: AgentJobType.restore, state: AgentJobState.failure, message: message);
      }
      return;
    }
    _setJobContext(jobId, source: xmlPath, storageLabel: storage.storage.name);
    final backupPath = storage.backupPath;
    if (driverInfo.usesPath && backupPath.isEmpty) {
      final current = _jobs[jobId];
      if (current != null) {
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: 'Backup path is empty.'));
        _notifyJobCompletion(jobId, type: AgentJobType.restore, state: AgentJobState.failure, message: 'Backup path is empty.');
      }
      return;
    }
    _hostLog('Restore job $jobId using driver: $driverId', jobId: jobId);
    final control = _jobControls[jobId];
    if (control == null) {
      return;
    }
    control.restoreDecision = decision.trim();
    final workerReceive = ReceivePort();
    control.workerReceivePort = workerReceive;
    unawaited(
      Isolate.spawn(restoreWorkerMain, {'sendPort': workerReceive.sendPort}).then((isolate) {
        control.workerIsolate = isolate;
      }),
    );
    workerReceive.listen((message) async {
      final payload = Map<String, dynamic>.from(message as Map);
      final type = payload['type']?.toString();
      if (type == 'ready') {
        control.workerSendPort = payload['sendPort'] as SendPort?;
        control.workerSendPort?.send({
          'type': 'start',
          'jobId': jobId,
          'driverId': driverId,
          'backupPath': backupPath,
          'decision': decision,
          'xmlPath': xmlPath,
          'settings': storage.settings.toMap(),
          'storage': storage.storage.toMap(),
          'server': server.toMap(),
        });
        return;
      }
      if (type == 'status') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        if (_isRestoreFinalizingStatus(status)) {
          control.restoreFinalizing = true;
        }
        if (control.canceled) {
          return;
        }
        _updateJob(jobId, status);
        return;
      }
      if (type == 'context') {
        _setJobContext(jobId, source: payload['source']?.toString(), target: payload['target']?.toString());
        return;
      }
      if (type == 'settings') {
        final updated = AppSettings.fromMap(Map<String, dynamic>.from(payload['settings'] as Map));
        unawaited(_applyAgentSettings(_settingsFromWorkerUpdate(updated), reason: 'worker', forceRestartSshListeners: false));
        return;
      }
      if (type == 'result') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        final current = _jobs[jobId];
        final next = control.canceled || control.protectedCancelRequested ? status.copyWith(state: AgentJobState.canceled, message: 'Canceled', speedBytesPerSec: 0) : status;
        _updateJob(jobId, next);
        final sizeBytes = next.totalBytes > 0 ? next.totalBytes : (current != null && current.totalBytes > 0 ? current.totalBytes : null);
        _notifyJobCompletion(jobId, type: AgentJobType.restore, state: next.state, message: next.message, sizeBytes: sizeBytes);
        workerReceive.close();
        control.workerReceivePort = null;
        control.workerSendPort = null;
        control.workerIsolate?.kill(priority: Isolate.immediate);
        control.workerIsolate = null;
      }
    });
  }

  bool _isRestoreFinalizingStatus(AgentJobStatus status) {
    if (status.type != AgentJobType.restore || status.state != AgentJobState.running) {
      return false;
    }
    final message = status.message.toLowerCase();
    return message.contains('finalizing disk files') || message.contains('rebasing restored overlays') || message.contains('uploading domain xml') || message.contains('defining vm');
  }

  String _extractTimestampFromManifestXmlPath(String xmlPath) {
    final base = _baseName(xmlPath).trim();
    final separator = base.indexOf('__');
    if (separator <= 0) {
      return '';
    }
    return base.substring(0, separator).trim();
  }

  String _extractVmNameFromXmlPath(String xmlPath) {
    final normalized = xmlPath.replaceAll('\\', '/');
    final segments = normalized.split('/').where((segment) => segment.isNotEmpty).toList();
    if (segments.length < 2) {
      return '';
    }
    return segments[segments.length - 2].trim();
  }

  Directory _vmDirFromXmlPath(String xmlPath) {
    return File(xmlPath).parent;
  }

  void _setJobContext(String jobId, {String? source, String? target, String? storageLabel}) {
    final control = _jobControls[jobId];
    if (control == null) {
      return;
    }
    if (source != null && source.trim().isNotEmpty) {
      control.source = source.trim();
    }
    if (target != null && target.trim().isNotEmpty) {
      control.target = target.trim();
    }
    if (storageLabel != null && storageLabel.trim().isNotEmpty) {
      control.storageLabel = storageLabel.trim();
    }
  }

  String _formatJobSource(ServerConfig server, String vmName) {
    final serverName = server.name.trim();
    final vm = vmName.trim();
    if (serverName.isEmpty) {
      return vm.isEmpty ? 'Unknown VM' : vm;
    }
    if (vm.isEmpty) {
      return serverName;
    }
    return '$serverName:$vm';
  }

  String _formatBackupTarget(BackupDriverInfo driverInfo, String backupPath, String driverId, {String? storageName}) {
    final storageLabel = storageName?.trim() ?? '';
    if (storageLabel.isNotEmpty) {
      return storageLabel;
    }
    if (driverInfo.usesPath) {
      final trimmedPath = backupPath.trim();
      if (trimmedPath.isNotEmpty) {
        return trimmedPath;
      }
    }
    return driverId.trim().isEmpty ? driverInfo.id : driverId.trim();
  }

  void _notifyJobCompletion(String jobId, {required AgentJobType type, required AgentJobState state, required String message, int? sizeBytes}) {
    if (state != AgentJobState.success && state != AgentJobState.failure) {
      return;
    }
    LogWriter.withJobLogging(jobId, () {
      final control = _jobControls[jobId];
      final notification = _buildJobCompletionNotification(jobId, type: type, state: state, message: message, sizeBytes: sizeBytes);
      final dedupeKey = '${type.name}:${notification.status}';
      if (control != null && control.lastNtfyCompletionKey == dedupeKey) {
        return;
      }
      if (control != null) {
        control.lastNtfyCompletionKey = dedupeKey;
      }
      _sendNtfymeNotification(notification);
      _sendEmailNotification(notification);
    });
  }

  _JobCompletionNotification _buildJobCompletionNotification(String jobId, {required AgentJobType type, required AgentJobState state, required String message, int? sizeBytes}) {
    final control = _jobControls[jobId];
    final jobStatus = _jobs[jobId];
    final notificationStatus = _notificationStatusForJob(type: type, state: state, message: message);
    final duration = control?.startedAt == null ? null : DateTime.now().difference(control!.startedAt).inSeconds;
    final storageLabel = _resolveStorageLabel(storageLabel: control?.storageLabel);
    final sourceLabel = _notificationSourceForJob(type: type, source: control?.source);
    final targetLabel = type == AgentJobType.backup ? storageLabel : control?.target;
    final title = _buildNotificationTitle(type, notificationStatus);
    final detailMessage = message.trim();
    final error = state == AgentJobState.failure ? (detailMessage.isEmpty ? title : detailMessage) : null;
    final warning = notificationStatus == 'warning' ? (detailMessage.isEmpty ? title : detailMessage) : null;
    return _JobCompletionNotification(
      jobId: jobId,
      type: type,
      status: notificationStatus,
      title: title,
      message: detailMessage,
      source: sourceLabel,
      target: targetLabel,
      storage: storageLabel,
      durationSeconds: duration,
      sizeBytes: sizeBytes,
      error: error,
      warning: warning,
      details: _buildJobNotificationDetails(
        jobId: jobId,
        status: notificationStatus,
        jobStatus: jobStatus,
        message: detailMessage,
        error: error,
        warning: warning,
        source: sourceLabel,
        target: targetLabel,
        storage: storageLabel,
        durationSeconds: duration,
        sizeBytes: sizeBytes,
      ),
    );
  }

  void _sendNtfymeNotification(_JobCompletionNotification notification) {
    final token = _agentSettings.ntfymeToken.trim();
    if (token.isEmpty) {
      _hostLog('Ntfy me notification skipped (no token configured).');
      return;
    }
    final pushMessage = _buildNtfymePushMessage(message: notification.title, source: notification.source, target: notification.target);
    final payload = <String, dynamic>{
      'topic': _ntfymeTopic,
      'msg': notification.title,
      'push_msg': pushMessage,
      'type': notification.type.name,
      'status': notification.status,
      'storage': notification.storage,
    };
    if (notification.durationSeconds != null) {
      payload['duration_sec'] = notification.durationSeconds;
    }
    if (notification.source != null && notification.source!.trim().isNotEmpty) {
      payload['source'] = notification.source;
    }
    if (notification.type != AgentJobType.backup && notification.target != null && notification.target!.trim().isNotEmpty) {
      payload['target'] = notification.target;
    }
    if (notification.type == AgentJobType.backup && notification.sizeBytes != null) {
      payload['size'] = _formatBytes(notification.sizeBytes!);
    }
    if (notification.error != null && notification.error!.trim().isNotEmpty) {
      payload['error'] = notification.error;
    } else if (notification.warning != null && notification.warning!.trim().isNotEmpty) {
      payload['warning'] = notification.warning;
    }
    _hostLog('Ntfy me notification queued: ${jsonEncode(payload)}');
    unawaited(_postNtfymeNotification(token, payload));
  }

  void _sendEmailNotification(_JobCompletionNotification notification) {
    final to = _agentSettings.notificationEmail.trim();
    if (to.isEmpty) {
      _hostLog('Email notification skipped (no email address configured).');
      return;
    }
    unawaited(() async {
      final result = await _postEmailMessage(to: to, subject: _buildEmailSubject(notification), textBody: _buildEmailTextBody(notification), htmlBody: _buildEmailHtmlBody(notification));
      if (result.ok) {
        _hostLog('Email notification delivered (${result.statusCode}).');
      } else {
        _hostLog('Email notification failed${result.statusCode == null ? '' : ' (${result.statusCode})'}: ${result.body ?? result.error ?? 'unknown error'}');
      }
    }());
  }

  Future<_NotificationEmailResult> _postEmailMessage({required String to, required String subject, required String textBody, required String htmlBody}) async {
    try {
      await _maybeRefreshVirtBackupAccount();
      final account = _agentSettings.virtBackupAccount;
      if (!account.isConnected || account.accountBaseUrl.trim().isEmpty || account.accessToken.trim().isEmpty) {
        return _NotificationEmailResult.failure('Virt Backup account is not signed in.');
      }
      final endpoint = Uri.parse(account.accountBaseUrl).replace(path: '/api/notifications/email', queryParameters: null, fragment: null);
      final client = HttpClient();
      try {
        final request = await client.postUrl(endpoint);
        request.headers.set(HttpHeaders.authorizationHeader, 'Bearer ${account.accessToken}');
        request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
        request.add(utf8.encode(jsonEncode(<String, String>{'to': to, 'subject': _emailSubjectWithPrefix(subject), 'textBody': textBody, 'htmlBody': htmlBody})));
        final response = await request.close();
        final responseBody = await response.transform(utf8.decoder).join();
        if (response.statusCode < 200 || response.statusCode >= 300) {
          return _NotificationEmailResult.failure('HTTP ${response.statusCode}', statusCode: response.statusCode, body: responseBody);
        }
        return _NotificationEmailResult.success(response.statusCode, responseBody);
      } finally {
        client.close(force: true);
      }
    } catch (error, stackTrace) {
      _hostLogError('Email notification failed.', error, stackTrace);
      return _NotificationEmailResult.failure(error.toString());
    }
  }

  String _emailSubjectWithPrefix(String subject) {
    final trimmed = subject.trim();
    if (trimmed.startsWith('[VirtBackup]')) {
      return trimmed;
    }
    return trimmed.isEmpty ? '[VirtBackup]' : '[VirtBackup] $trimmed';
  }

  String _notificationStatusForJob({required AgentJobType type, required AgentJobState state, required String message}) {
    if (state == AgentJobState.failure) {
      return 'failed';
    }
    if (type == AgentJobType.restore && message.toLowerCase().contains('warning')) {
      return 'warning';
    }
    return 'success';
  }

  String _buildNotificationTitle(AgentJobType type, String status) {
    final label = switch (type) {
      AgentJobType.backup => 'Backup',
      AgentJobType.restore => 'Restore',
      AgentJobType.sanity => 'Check',
      AgentJobType.unknown => 'Job',
    };
    final statusText = switch (status) {
      'success' => 'succeeded',
      'warning' => 'warning',
      _ => 'failed',
    };
    return '$label $statusText';
  }

  String _buildEmailSubject(_JobCompletionNotification notification) {
    final source = notification.source?.trim() ?? '';
    final target = notification.target?.trim() ?? '';
    if (source.isNotEmpty && target.isNotEmpty) {
      return '${notification.title}: $source -> $target';
    }
    return source.isEmpty ? notification.title : '${notification.title}: $source';
  }

  List<_JobNotificationDetail> _buildJobNotificationDetails({
    required String jobId,
    required String status,
    required AgentJobStatus? jobStatus,
    required String message,
    required String? error,
    required String? warning,
    required String? source,
    required String? target,
    required String storage,
    required int? durationSeconds,
    required int? sizeBytes,
  }) {
    final details = <_JobNotificationDetail>[_JobNotificationDetail('Job ID', jobId), _JobNotificationDetail('Type', jobStatus?.type.name ?? ''), _JobNotificationDetail('Status', status)];
    if (error != null && error.trim().isNotEmpty) {
      details.add(_JobNotificationDetail('Error', error.trim()));
    }
    if (warning != null && warning.trim().isNotEmpty) {
      details.add(_JobNotificationDetail('Warning', warning.trim()));
    }
    if (message.isNotEmpty && message != error && message != warning) {
      details.add(_JobNotificationDetail('Message', message));
    }
    final jobMessage = jobStatus?.message.trim() ?? '';
    if (jobMessage.isNotEmpty && jobMessage != message && jobMessage != error && jobMessage != warning) {
      details.add(_JobNotificationDetail('Job message', jobMessage));
    }
    details.add(_JobNotificationDetail('Storage', storage));
    final sourceValue = source?.trim() ?? '';
    if (sourceValue.isNotEmpty) {
      details.add(_JobNotificationDetail('Source', sourceValue));
    }
    final targetValue = target?.trim() ?? '';
    if (targetValue.isNotEmpty) {
      details.add(_JobNotificationDetail('Target', targetValue));
    }
    if (durationSeconds != null) {
      details.add(_JobNotificationDetail('Duration', '${durationSeconds}s'));
    }
    final isRestore = jobStatus?.type == AgentJobType.restore;
    if (sizeBytes != null) {
      details.add(_JobNotificationDetail('Size', isRestore ? '${_formatBytes(sizeBytes)} ($sizeBytes bytes)' : _formatBytes(sizeBytes)));
      if (!isRestore) {
        details.add(_JobNotificationDetail('Size bytes', sizeBytes.toString()));
      }
    }
    if (jobStatus == null) {
      return details.where((detail) => detail.value.trim().isNotEmpty).toList();
    }
    if (jobStatus.scheduleId.trim().isNotEmpty) {
      details.add(_JobNotificationDetail('Schedule ID', jobStatus.scheduleId.trim()));
    }
    if (isRestore) {
      details.addAll(<_JobNotificationDetail>[
        _JobNotificationDetail('Bytes transferred', _formatBytes(jobStatus.bytesTransferred)),
        _JobNotificationDetail('Average speed', _formatSpeed(jobStatus.averageSpeedBytesPerSec)),
      ]);
    } else {
      details.addAll(<_JobNotificationDetail>[
        _JobNotificationDetail('Bytes transferred', '${_formatBytes(jobStatus.bytesTransferred)} (${jobStatus.bytesTransferred})'),
        _JobNotificationDetail('Average speed', _formatSpeed(jobStatus.averageSpeedBytesPerSec)),
        _JobNotificationDetail('Physical bytes transferred', '${_formatBytes(jobStatus.physicalBytesTransferred)} (${jobStatus.physicalBytesTransferred})'),
        _JobNotificationDetail('Average physical speed', _formatSpeed(jobStatus.averagePhysicalSpeedBytesPerSec)),
        _JobNotificationDetail('Total bytes', '${_formatBytes(jobStatus.totalBytes)} (${jobStatus.totalBytes})'),
        _JobNotificationDetail('Physical total bytes', '${_formatBytes(jobStatus.physicalTotalBytes)} (${jobStatus.physicalTotalBytes})'),
      ]);
    }
    return details.where((detail) => detail.value.trim().isNotEmpty).toList();
  }

  String _buildEmailTextBody(_JobCompletionNotification notification) {
    final lines = <String>[notification.title, ''];
    for (final detail in notification.details) {
      lines.add('${detail.label}: ${detail.value}');
    }
    return lines.join('\n');
  }

  String _buildEmailHtmlBody(_JobCompletionNotification notification) {
    final rows = notification.details.map((detail) => _buildEmailMetaRow(detail.label, detail.value)).toList();
    return '''
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111827;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6;padding:32px 16px;">
      <tr>
        <td align="center">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
            <tr>
              <td style="padding:28px 32px;background:${_emailAccentColor(notification.status)};">
                <div style="font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,.78);">Virt Backup</div>
                <h1 style="margin:10px 0 0 0;font-size:28px;line-height:1.2;color:#ffffff;">${_escapeEmailHtml(notification.title)}</h1>
              </td>
            </tr>
            <tr>
              <td style="padding:28px 32px;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                  ${rows.join('\n')}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
''';
  }

  String _buildTestEmailTextBody() {
    return 'Test email delivered\n\nThis test email confirms that Virt Backup can send notification emails through your backend account.';
  }

  String _buildTestEmailHtmlBody() {
    return '''
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111827;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6;padding:32px 16px;">
      <tr>
        <td align="center">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
            <tr>
              <td style="padding:28px 32px;background:#2563eb;">
                <div style="font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,.78);">Virt Backup</div>
                <h1 style="margin:10px 0 0 0;font-size:28px;line-height:1.2;color:#ffffff;">Test email delivered</h1>
              </td>
            </tr>
            <tr>
              <td style="padding:28px 32px;">
                <p style="margin:0;font-size:15px;line-height:1.6;color:#111827;">This test email confirms that Virt Backup can send notification emails through your backend account.</p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
''';
  }

  String _buildEmailMetaRow(String label, String value) {
    return '''
      <tr>
        <td style="width:160px;padding:10px 0;border-bottom:1px solid #e5e7eb;font-size:13px;font-weight:700;color:#64748b;">${_escapeEmailHtml(label)}</td>
        <td style="padding:10px 0;border-bottom:1px solid #e5e7eb;font-size:14px;color:#111827;">${_escapeEmailHtml(value)}</td>
      </tr>
''';
  }

  String _emailAccentColor(String status) {
    return switch (status) {
      'success' => '#047857',
      'warning' => '#b45309',
      _ => '#b91c1c',
    };
  }

  String _escapeEmailHtml(String value) {
    return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  }

  String _buildNtfymePushMessage({required String message, String? source, String? target}) {
    final sourceLabel = source?.trim() ?? '';
    final targetLabel = target?.trim() ?? '';
    if (sourceLabel.isEmpty || targetLabel.isEmpty) {
      return message;
    }
    return '$message: $sourceLabel -> $targetLabel';
  }

  String _resolveStorageLabel({String? storageLabel}) {
    final fallback = storageLabel?.trim() ?? '';
    return fallback.isEmpty ? 'Unknown storage' : fallback;
  }

  String? _notificationSourceForJob({required AgentJobType type, String? source}) {
    final value = source?.trim() ?? '';
    if (value.isEmpty || type != AgentJobType.restore) {
      return value.isEmpty ? null : value;
    }
    final vmName = _extractVmNameFromXmlPath(value);
    final timestamp = _extractTimestampFromManifestXmlPath(value);
    if (vmName.isEmpty || timestamp.isEmpty) {
      return value;
    }
    return '$vmName / $timestamp';
  }

  String _formatBytes(int bytes) {
    if (bytes <= 0) {
      return '0 B';
    }
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    var value = bytes.toDouble();
    var unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    final precision = value >= 100 ? 0 : (value >= 10 ? 1 : 2);
    final formatted = value.toStringAsFixed(precision);
    return '$formatted ${units[unitIndex]}';
  }

  String _formatSpeed(double bytesPerSec) {
    if (bytesPerSec <= 0) {
      return '0 B/s';
    }
    return '${_formatBytes(bytesPerSec.round())}/s';
  }

  Future<_NtfymeResult> _postNtfymeNotification(String token, Map<String, dynamic> payload) async {
    final client = HttpClient();
    try {
      final request = await client.postUrl(_ntfymeEndpoint);
      request.headers.set(HttpHeaders.authorizationHeader, 'Bearer $token');
      request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
      request.add(utf8.encode(jsonEncode(payload)));
      final response = await request.close();
      final body = await response.transform(utf8.decoder).join();
      if (response.statusCode < 200 || response.statusCode >= 300) {
        _hostLog('Ntfy me notification failed (${response.statusCode}): $body');
        return _NtfymeResult.failure('HTTP ${response.statusCode}', statusCode: response.statusCode, body: body);
      }
      _hostLog('Ntfy me notification delivered (${response.statusCode}).');
      return _NtfymeResult.success(response.statusCode, body);
    } catch (error, stackTrace) {
      _hostLogError('Ntfy me notification failed.', error, stackTrace);
      return _NtfymeResult.failure(error.toString());
    } finally {
      client.close(force: true);
    }
  }

  Future<List<String>> _readManifestFields(File manifest, String field) async {
    final values = <String>[];
    try {
      final lines = await _readManifestLines(manifest);
      var inBlocks = false;
      for (final line in lines) {
        final trimmed = line.trim();
        if (trimmed.startsWith('disk_id:')) {
          inBlocks = false;
        }
        if (inBlocks && trimmed.contains('->')) {
          continue;
        }
        if (trimmed.startsWith('$field:')) {
          final value = trimmed.substring(field.length + 1).trim();
          if (value.isNotEmpty) {
            values.add(value);
          }
          continue;
        }
        if (trimmed.startsWith('blocks:')) {
          inBlocks = true;
        }
      }
    } catch (_) {}
    return values;
  }

  Future<List<String>> _readManifestLines(File manifest) async {
    if (manifest.path.endsWith('.gz')) {
      final bytes = await manifest.readAsBytes();
      final decoded = gzip.decode(bytes);
      final content = utf8.decode(decoded);
      return const LineSplitter().convert(content);
    }
    return manifest.readAsLines();
  }

  Future<List<File>> _listManifestFilesForTimestamp(Directory vmDir, String timestamp) async {
    final manifests = <File>[];
    if (!await vmDir.exists()) {
      return manifests;
    }
    await for (final entity in vmDir.list(recursive: true, followLinks: false)) {
      if (entity is! File) {
        continue;
      }
      final name = _baseName(entity.path).trim();
      final isManifest = name.endsWith('.manifest') || name.endsWith('.manifest.gz');
      if (!isManifest) {
        continue;
      }
      if (!_manifestMatchesTimestamp(name, timestamp)) {
        continue;
      }
      manifests.add(entity);
    }
    manifests.sort((a, b) => a.path.compareTo(b.path));
    return manifests;
  }

  bool _manifestMatchesTimestamp(String fileName, String timestamp) {
    var value = fileName.trim();
    if (value.endsWith('.manifest.gz')) {
      value = value.substring(0, value.length - '.manifest.gz'.length).trim();
    } else if (value.endsWith('.manifest')) {
      value = value.substring(0, value.length - '.manifest'.length).trim();
    } else {
      return false;
    }
    if (value == timestamp) {
      return true;
    }
    return value.startsWith('${timestamp}__');
  }

  double _smoothSpeed(double current, double instant) {
    const alpha = 0.2;
    if (current <= 0) {
      return instant;
    }
    return (instant * alpha) + (current * (1 - alpha));
  }

  int _blockLengthForIndex(int index, int? totalSize, int blockSize) {
    if (totalSize == null || totalSize <= 0) {
      return blockSize;
    }
    final start = index * blockSize;
    final end = start + blockSize;
    return end > totalSize ? (totalSize - start) : blockSize;
  }

  int _bytesForRange(int start, int end, int? totalSize, int blockSize) {
    if (totalSize == null || totalSize <= 0) {
      return (end - start + 1) * blockSize;
    }
    final startOffset = start * blockSize;
    final endExclusive = ((end + 1) * blockSize);
    final length = endExclusive > totalSize ? (totalSize - startOffset) : (endExclusive - startOffset);
    return length < 0 ? 0 : length;
  }

  int _blockSizeMbFromManifestBytes(int blockSizeBytes, String manifestPath) {
    const bytesPerMb = 1024 * 1024;
    if (blockSizeBytes <= 0) {
      throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (must be > 0 bytes)';
    }
    if (blockSizeBytes % bytesPerMb != 0) {
      throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (must be divisible by $bytesPerMb)';
    }
    final blockSizeMB = blockSizeBytes ~/ bytesPerMb;
    if (blockSizeMB != 1 && blockSizeMB != 2 && blockSizeMB != 4 && blockSizeMB != 8) {
      throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (allowed: 1048576, 2097152, 4194304, 8388608)';
    }
    return blockSizeMB;
  }

  (int, int)? _parseZeroRange(String line) {
    if (!line.endsWith('-> ZERO')) {
      return null;
    }
    final parts = line.split('->');
    if (parts.isEmpty) {
      return null;
    }
    final left = parts.first.trim();
    final rangeParts = left.split('-').map((value) => value.trim()).where((value) => value.isNotEmpty).toList();
    if (rangeParts.isEmpty) {
      return null;
    }
    final start = int.tryParse(rangeParts.first);
    if (start == null) {
      return null;
    }
    if (rangeParts.length == 1) {
      return (start, start);
    }
    final end = int.tryParse(rangeParts.last);
    if (end == null) {
      return null;
    }
    return (start, end);
  }

  String _baseName(String path) {
    final parts = path.split(RegExp(r'[\\/]')).where((part) => part.isNotEmpty).toList();
    return parts.isEmpty ? path : parts.last;
  }

  ServerConfig _missingServer() {
    return ServerConfig(id: 'missing', name: 'missing', connectionType: ConnectionType.ssh, sshHost: '', sshPort: '22', sshUser: '', sshPassword: '', apiBaseUrl: '', apiToken: '');
  }

  void _hostLog(String message, {String? jobId}) {
    LogWriter.logAgentSync(level: 'info', message: message, jobId: jobId);
  }

  void _hostLogError(String message, Object error, StackTrace stackTrace, {String? jobId}) {
    LogWriter.logAgentSync(level: 'error', message: '$message $error', jobId: jobId);
    LogWriter.logAgentSync(level: 'info', message: stackTrace.toString(), jobId: jobId);
  }

  String _formatLocalLogTime(DateTime? value) {
    if (value == null) {
      return 'unknown';
    }
    return value.toLocal().toIso8601String();
  }
}

class _JobControl {
  _JobControl({required this.startedAt, String? vmName, String? storageId, required this.scheduleRunId}) : vmName = vmName?.trim(), storageId = storageId?.trim();

  final DateTime startedAt;
  final String? vmName;
  final String? storageId;
  final String scheduleRunId;
  final Completer<AgentJobStatus> completed = Completer<AgentJobStatus>();
  bool canceled = false;
  BackupAgent? backupAgent;
  Isolate? workerIsolate;
  ReceivePort? workerReceivePort;
  SendPort? workerSendPort;
  String? source;
  String? target;
  String? storageLabel;
  bool resultHandled = false;
  String restoreDecision = '';
  bool protectedCancelRequested = false;
  bool restoreFinalizing = false;
  bool backupVmCleanupRequired = false;
  String? lastNtfyCompletionKey;
  final List<drv.BackupDriver> checkDrivers = <drv.BackupDriver>[];
}

class _JobGuardRejected implements Exception {
  const _JobGuardRejected(this.message);

  final String message;

  @override
  String toString() => message;
}

class _VirtBackupAccountRefreshRejected implements Exception {
  const _VirtBackupAccountRefreshRejected(this.message);

  final String message;
}

enum _RestoreCheckMode { full, quick }

class _CheckBlockRef {
  const _CheckBlockRef({required this.hash, required this.expectedLength, required this.index, required this.message});

  final String hash;
  final int expectedLength;
  final int index;
  final String message;
}

class _CheckBlockFetchResult {
  const _CheckBlockFetchResult({required this.position, required this.block, required this.bytes, required this.missing, required this.canceled});

  final int position;
  final _CheckBlockRef block;
  final List<int> bytes;
  final bool missing;
  final bool canceled;
}

class _JobCanceled implements Exception {
  const _JobCanceled();

  @override
  String toString() => 'Canceled';
}

class _ScheduleRunCanceled implements Exception {
  const _ScheduleRunCanceled();

  @override
  String toString() => 'Schedule run canceled';
}

class _EventStreamState {
  _EventStreamState(this.response);

  final HttpResponse response;
  bool closed = false;
}

class _JobCompletionNotification {
  const _JobCompletionNotification({
    required this.jobId,
    required this.type,
    required this.status,
    required this.title,
    required this.message,
    required this.source,
    required this.target,
    required this.storage,
    required this.durationSeconds,
    required this.sizeBytes,
    required this.error,
    required this.warning,
    required this.details,
  });

  final String jobId;
  final AgentJobType type;
  final String status;
  final String title;
  final String message;
  final String? source;
  final String? target;
  final String storage;
  final int? durationSeconds;
  final int? sizeBytes;
  final String? error;
  final String? warning;
  final List<_JobNotificationDetail> details;
}

class _JobNotificationDetail {
  const _JobNotificationDetail(this.label, this.value);

  final String label;
  final String value;
}

class _NotificationEmailResult {
  const _NotificationEmailResult._(this.ok, {this.statusCode, this.body, this.error});

  final bool ok;
  final int? statusCode;
  final String? body;
  final String? error;

  factory _NotificationEmailResult.success(int statusCode, String body) => _NotificationEmailResult._(true, statusCode: statusCode, body: body);
  factory _NotificationEmailResult.failure(String error, {int? statusCode, String? body}) => _NotificationEmailResult._(false, statusCode: statusCode, body: body, error: error);
}

class _NtfymeResult {
  const _NtfymeResult._(this.ok, {this.statusCode, this.body, this.error});

  final bool ok;
  final int? statusCode;
  final String? body;
  final String? error;

  factory _NtfymeResult.success(int statusCode, String body) => _NtfymeResult._(true, statusCode: statusCode, body: body);
  factory _NtfymeResult.failure(String error, {int? statusCode, String? body}) => _NtfymeResult._(false, statusCode: statusCode, body: body, error: error);
}
