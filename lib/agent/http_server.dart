import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:basic_utils/basic_utils.dart';
import 'package:crypto/crypto.dart';
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

class _ResolvedDestination {
  const _ResolvedDestination({required this.destination, required this.settings, required this.driverId, required this.backupPath, required this.driverParams});

  final BackupDestination destination;
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
  final Map<String, AgentJobStatus> _jobs = {};
  final Map<String, _JobControl> _jobControls = {};
  final Map<HttpResponse, _EventStreamState> _eventStreams = {};
  final Map<String, Timer> _eventRestartTimers = {};
  final Map<String, int> _eventRestartAttempts = {};
  final Map<String, Timer> _refreshDebounceTimers = {};
  late final Map<String, BackupDriverInfo> _driverCatalog = _buildDriverCatalog();
  GdriveBackupDriver? _cachedGdriveDriver;
  String _agentToken = '';

  Future<void> start() async {
    await _loadAgentSettings();
    await _ensureAgentToken();
    await _ensureTlsAssets();
    final bindAddress = _agentSettings.listenAll ? InternetAddress.anyIPv4 : InternetAddress.loopbackIPv4;
    final securityContext = _buildTlsContext();
    _server = await HttpServer.bindSecure(bindAddress, backupAgentPort, securityContext);
    _hostLog('Agent HTTPS listening on ${bindAddress.address}:$backupAgentPort');
    _server?.listen(_handleRequest);

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
    for (final state in _eventStreams.values.toList()) {
      try {
        state.closed = true;
        state.response.close();
      } catch (_) {}
    }
    _eventStreams.clear();
    await _server?.close(force: true);
  }

  Future<void> _loadAgentSettings() async {
    _agentSettings = await _agentSettingsStore.load();
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

    final resolvedDriverId = _driverCatalog.containsKey(agentSettings.backupDriverId.trim()) ? agentSettings.backupDriverId.trim() : 'filesystem';
    final normalized = agentSettings.copyWith(backupDriverId: resolvedDriverId);
    _agentSettings = normalized;
    _cachedGdriveDriver = null;
    await _agentSettingsStore.save(normalized);

    Future<void> syncServers() async {
      for (final id in removed) {
        await _safeStopEventListener(id);
        _vmStatusByServerId.remove(id);
      }

      if (forceRestartSshListeners) {
        for (final server in nextById.values) {
          if (server.connectionType == ConnectionType.ssh) {
            await _safeStopEventListener(server.id);
            await _refreshServer(server, reason: 'settings/changed');
            await _safeStartEventListener(server);
          } else {
            _vmStatusByServerId.remove(server.id);
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
          await _refreshServer(server, reason: 'settings/added');
          await _safeStartEventListener(server);
        } else {
          _vmStatusByServerId.remove(id);
        }
      }

      for (final id in changed) {
        final server = nextById[id];
        if (server == null) {
          continue;
        }
        await _safeStopEventListener(id);
        if (server.connectionType == ConnectionType.ssh) {
          await _refreshServer(server, reason: 'settings/changed');
          await _safeStartEventListener(server);
        } else {
          _vmStatusByServerId.remove(id);
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

  Future<void> _safeStartEventListener(ServerConfig server) async {
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

  Future<void> _refreshAllServers() async {
    final servers = _agentSettings.servers.where((server) => server.connectionType == ConnectionType.ssh).toList();
    for (final server in servers) {
      await _refreshServer(server, reason: 'startup/settings');
      await _startEventListener(server);
    }
  }

  Future<void> _refreshServer(ServerConfig server, {required String reason}) async {
    try {
      _hostLog('Refreshing server ${server.name} (reason: $reason).');
      final vms = await _host.loadVmInventory(server);
      final overlay = await _host.loadOverlayStatusForVms(server, vms);
      final status = vms.map((vm) => VmStatus(vm: vm, hasOverlay: overlay[vm.name] == true)).toList();
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
      await _startEventListener(server);
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
    );
  }

  void _scheduleRefresh(ServerConfig server) {
    _refreshDebounceTimers[server.id]?.cancel();
    _refreshDebounceTimers[server.id] = Timer(const Duration(seconds: 2), () async {
      _refreshDebounceTimers.remove(server.id);
      await _refreshServer(server, reason: 'event-sync');
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
        _json(request, 200, {'ok': true, 'nativeSftpAvailable': _host.nativeSftpAvailable});
        return;
      }
      if (request.method == 'GET' && path == '/drivers') {
        _json(request, 200, _driverCatalog.values.map((driver) => driver.toMap()).toList());
        return;
      }
      if (request.method == 'GET' && path == '/config') {
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
      if (request.method == 'POST' && path == '/sftp/test') {
        final body = await _readJson(request);
        final host = (body['host'] ?? _agentSettings.sftpHost).toString().trim();
        final portValue = body['port'];
        final port = (portValue is num ? portValue.toInt() : int.tryParse((portValue ?? '').toString())) ?? _agentSettings.sftpPort;
        final username = (body['username'] ?? _agentSettings.sftpUsername).toString().trim();
        final password = (body['password'] ?? _agentSettings.sftpPassword).toString();
        final basePath = (body['basePath'] ?? _agentSettings.sftpBasePath).toString().trim();

        if (host.isEmpty || username.isEmpty || password.isEmpty || basePath.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'Missing SFTP settings (host/username/password/basePath).'});
          return;
        }
        final effectivePort = port <= 0 ? 22 : port;
        try {
          final message = await _testSftpConnection(host: host, port: effectivePort, username: username, password: password, basePath: basePath);
          _json(request, 200, {'success': true, 'message': message});
        } catch (error) {
          _json(request, 200, {'success': false, 'message': error.toString()});
        }
        return;
      }
      if (request.method == 'POST' && path == '/config') {
        final body = await _readJson(request);
        final agentSettings = AppSettings.fromMap(body);
        await _applyAgentSettings(agentSettings, reason: 'api', forceRestartSshListeners: true, runServerRefreshInBackground: true);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/oauth/google') {
        final body = await _readJson(request);
        final accessToken = (body['accessToken'] ?? '').toString();
        final refreshToken = (body['refreshToken'] ?? '').toString();
        if (refreshToken.isEmpty) {
          _json(request, 400, {'success': false, 'error': 'missing refreshToken'});
          return;
        }
        final accountEmail = (body['accountEmail'] ?? '').toString();
        final scope = (body['scope'] ?? '').toString();
        final expiresAt = _parseExpiresAt(body['expiresAt']);
        final updated = _agentSettings.copyWith(
          gdriveAccessToken: accessToken,
          gdriveRefreshToken: refreshToken,
          gdriveAccountEmail: accountEmail,
          gdriveScope: scope.isEmpty ? _agentSettings.gdriveScope : scope,
          gdriveExpiresAt: expiresAt,
        );
        await _applyAgentSettings(updated, reason: 'oauth', forceRestartSshListeners: false);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'POST' && path == '/oauth/google/clear') {
        final updated = _agentSettings.copyWith(gdriveAccessToken: '', gdriveRefreshToken: '', gdriveAccountEmail: '', gdriveExpiresAt: null);
        await _applyAgentSettings(updated, reason: 'oauth', forceRestartSshListeners: false);
        _json(request, 200, {'success': true});
        return;
      }
      if (request.method == 'GET' && path.startsWith('/servers/') && path.endsWith('/vms')) {
        final serverId = path.split('/')[2];
        final data = _vmStatusByServerId[serverId] ?? [];
        _json(request, 200, data.map((entry) => entry.toMap()).toList());
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
        await _refreshServer(server, reason: 'manual');
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
          await _refreshServer(server, reason: 'cleanup');
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
        final requestedDestinationId = (body['destinationId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        final driverParams = body['driverParams'] is Map ? Map<String, dynamic>.from(body['driverParams'] as Map) : <String, dynamic>{};
        final freshRequested = body['fresh'] == true;
        if (vmName.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final resolvedDestination = _resolveBackupDestination(requestedDestinationId);
        if (resolvedDestination == null) {
          _json(request, 400, {'error': 'destination not found or unavailable'});
          return;
        }
        final effectiveFreshRequested = freshRequested && !resolvedDestination.destination.disableFresh;
        if (freshRequested && !effectiveFreshRequested) {
          _hostLog('backup: fresh requested but disabled for destination "${resolvedDestination.destination.name}" (id=${resolvedDestination.destination.id}); continuing without fresh');
        }
        final resolvedDriverId = requestedDriver.isNotEmpty ? requestedDriver : resolvedDestination.driverId;
        final backupPath = resolvedDestination.backupPath;
        final registry = _buildDriverRegistry(backupPath: backupPath, settings: resolvedDestination.settings);
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
        final jobId = _createJob(AgentJobType.backup);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startBackupJob(
          jobId,
          server,
          VmEntry(id: vmName, name: vmName, powerState: VmPowerState.stopped),
          backupPath,
          driverIdOverride: resolvedDriverId,
          driverParams: driverParams.isNotEmpty ? driverParams : resolvedDestination.driverParams,
          destination: resolvedDestination,
          freshRequested: effectiveFreshRequested,
        );
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
        if (xmlPath.isEmpty) {
          _json(request, 400, {'error': 'missing xmlPath'});
          return;
        }
        final result = await _restorePrecheck(server, xmlPath);
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
        final requestedDestinationId = (body['destinationId'] ?? '').toString().trim();
        final requestedDriver = (body['driverId'] ?? '').toString().trim();
        if (xmlPath.isEmpty || decision.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        if (requestedDriver.isNotEmpty && !_driverCatalog.containsKey(requestedDriver)) {
          _json(request, 400, {'error': 'unknown driverId', 'known': _driverCatalog.keys.toList()});
          return;
        }
        final resolvedDestination = _resolveBackupDestination(requestedDestinationId);
        if (requestedDestinationId.isNotEmpty && resolvedDestination == null) {
          _json(request, 400, {'error': 'destination not found or unavailable'});
          return;
        }
        final resolvedDriverId = requestedDriver.isNotEmpty
            ? requestedDriver
            : (resolvedDestination?.driverId ?? (_agentSettings.backupDriverId.trim().isEmpty ? 'filesystem' : _agentSettings.backupDriverId.trim()));
        final jobId = _createJob(AgentJobType.restore);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startRestoreJob(jobId, server, xmlPath, decision, driverIdOverride: resolvedDriverId, destination: resolvedDestination);
        return;
      }
      if (request.method == 'POST' && path == '/restore/sanity') {
        final body = await _readJson(request);
        final xmlPath = (body['xmlPath'] ?? '').toString();
        final timestamp = (body['timestamp'] ?? '').toString();
        if (xmlPath.isEmpty || timestamp.isEmpty) {
          _json(request, 400, {'error': 'missing params'});
          return;
        }
        final jobId = _createJob(AgentJobType.sanity);
        _json(request, 200, AgentJobStart(jobId: jobId).toMap());
        _startSanityJob(jobId, xmlPath, timestamp);
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
        final destinationIdOverride = request.uri.queryParameters['destinationId'];
        final driverIdOverride = request.uri.queryParameters['driverId'];
        final entries = await _loadRestoreEntries(driverIdOverride: driverIdOverride, destinationIdOverride: destinationIdOverride);
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

  Future<List<RestoreEntry>> _loadRestoreEntries({String? driverIdOverride, String? destinationIdOverride}) async {
    _ResolvedDestination? resolvedDestination;
    final requestedDestinationId = destinationIdOverride?.trim() ?? '';
    if (requestedDestinationId.isNotEmpty) {
      resolvedDestination = _resolveBackupDestination(requestedDestinationId);
      if (resolvedDestination == null) {
        return [];
      }
    } else if (driverIdOverride == null || driverIdOverride.trim().isEmpty) {
      resolvedDestination = _resolveBackupDestination(null);
    }
    final resolvedDriverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : (resolvedDestination?.driverId ?? _agentSettings.backupDriverId.trim());
    final driverInfo = _driverCatalog[resolvedDriverId] ?? _driverCatalog['filesystem']!;
    final backupPath = resolvedDestination?.backupPath ?? _agentSettings.backupPath.trim();
    if (driverInfo.usesPath && backupPath.isEmpty) {
      return [];
    }
    final settings = resolvedDestination?.settings ?? _agentSettings;
    final driver = _driverForSettings(driverInfo.id, backupPath, settings: settings);
    final xmlFiles = await _findXmlFiles(driver);
    final entries = <RestoreEntry>[];
    for (final xmlFile in xmlFiles) {
      final entry = await _buildRestoreEntryFromXmlFile(xmlFile, driver);
      if (entry != null) {
        entries.add(entry);
      }
    }
    entries.sort((a, b) => b.timestamp.compareTo(a.timestamp));
    return entries;
  }

  Future<List<File>> _findXmlFiles(drv.BackupDriver driver) async {
    final xmlFiles = <File>[];
    await for (final file in driver.listXmlFiles()) {
      xmlFiles.add(file);
    }
    return xmlFiles;
  }

  Future<RestoreEntry?> _buildRestoreEntryFromXmlFile(File xmlFile, drv.BackupDriver driver) async {
    try {
      final xmlContent = await xmlFile.readAsString();
      final vmName = _extractVmNameFromXml(xmlContent);
      final diskBasenames = _extractDiskBasenamesFromXml(xmlContent);
      final fileName = _baseName(xmlFile.path);
      final timestamp = _extractTimestampFromFileName(fileName);
      if (timestamp.isEmpty) {
        return null;
      }
      final location = driver.restoreLocationFromXml(xmlFile);
      if (location == null) {
        return null;
      }
      final vmDir = location.vmDir;
      final serverId = location.serverId;
      final diskNamesForTimestamp = await _collectDiskNamesForTimestamp(driver, vmDir, timestamp);
      final requiredDiskIds = <String>{};
      for (final diskBase in diskBasenames) {
        final diskDir = await driver.findDiskDirForTimestamp(vmDir, timestamp, diskBase);
        File? chainFile = driver.findChainFileForTimestamp(vmDir, diskDir, timestamp, diskBase);
        if (chainFile == null && driver is GdriveBackupDriver) {
          chainFile = await driver.ensureChainFile(vmDir, timestamp, diskBase);
        }
        if (chainFile != null) {
          final chainEntries = await _readChainEntries(chainFile);
          if (chainEntries.isNotEmpty) {
            for (final entry in chainEntries) {
              requiredDiskIds.add(entry.diskId);
            }
            continue;
          }
        }
        requiredDiskIds.add(diskBase);
      }
      final missing = <String>[];
      for (final diskId in requiredDiskIds) {
        if (!_diskExistsForTimestamp(driver, diskId, diskNamesForTimestamp)) {
          missing.add(diskId);
        }
      }
      final serverName = _agentSettings.servers.firstWhere((server) => server.id == serverId, orElse: () => _missingServer()).name;
      return RestoreEntry(
        xmlPath: xmlFile.path,
        vmName: vmName,
        timestamp: timestamp,
        diskBasenames: diskBasenames,
        missingDiskBasenames: missing,
        sourceServerId: serverId,
        sourceServerName: serverName.isEmpty ? serverId : serverName,
      );
    } catch (error, stackTrace) {
      _hostLogError('Failed to parse restore XML: ${xmlFile.path}', error, stackTrace);
      return null;
    }
  }

  List<String> _extractDiskBasenamesFromXml(String xml) {
    final basenames = <String>[];
    final diskBlocks = RegExp("<disk[^>]*device=['\\\"]disk['\\\"][^>]*>(.*?)</disk>", dotAll: true).allMatches(xml);
    for (final diskBlock in diskBlocks) {
      final blockContent = diskBlock.group(1) ?? '';
      final sourceMatch = RegExp("<source[^>]*file=['\\\"]([^'\\\"]+)['\\\"]").firstMatch(blockContent);
      if (sourceMatch == null) {
        continue;
      }
      final sourcePath = sourceMatch.group(1) ?? '';
      if (sourcePath.isEmpty) {
        continue;
      }
      final baseName = sourcePath.split(RegExp(r'[\\/]')).last.trim();
      if (baseName.isEmpty) {
        continue;
      }
      basenames.add(baseName);
    }
    return basenames;
  }

  Future<List<String>> _collectDiskNamesForTimestamp(drv.BackupDriver driver, Directory vmDir, String timestamp) async {
    final names = <String>[];
    final manifests = await driver.listManifestsForTimestamp(vmDir, timestamp);
    for (final manifest in manifests) {
      final diskId = await _readManifestField(manifest, 'disk_id');
      if (diskId != null && diskId.isNotEmpty) {
        names.add(diskId);
        continue;
      }
      final name = driver.baseName(manifest.parent.path);
      if (name.isNotEmpty) {
        names.add(name);
      }
    }
    return names;
  }

  bool _diskExistsForTimestamp(drv.BackupDriver driver, String diskBaseName, List<String> diskNamesForTimestamp) {
    final normalizedDisk = driver.sanitizeFileName(diskBaseName);
    for (final diskId in diskNamesForTimestamp) {
      if (diskId == diskBaseName || diskId == normalizedDisk || diskId.endsWith(diskBaseName) || diskId.endsWith(normalizedDisk)) {
        return true;
      }
    }
    return false;
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

  drv.BackupDriver _driverForBackupPath(String backupPath) {
    return _driverForSettings(_agentSettings.backupDriverId, backupPath, settings: _agentSettings);
  }

  drv.BackupDriver _driverForSettings(String driverId, String backupPath, {required AppSettings settings}) {
    final registry = _buildDriverRegistry(backupPath: backupPath, settings: settings);
    final descriptor = registry[driverId] ?? registry['filesystem']!;
    return descriptor.create(const <String, dynamic>{});
  }

  _ResolvedDestination? _resolveBackupDestination(String? requestedDestinationId) {
    final candidates = _agentSettings.destinations.where((destination) => destination.enabled).toList();
    if (candidates.isEmpty) {
      return null;
    }
    final requestedId = requestedDestinationId?.trim() ?? '';
    BackupDestination? destination;
    if (requestedId.isNotEmpty) {
      for (final entry in candidates) {
        if (entry.id == requestedId) {
          destination = entry;
          break;
        }
      }
      if (destination == null) {
        return null;
      }
    }
    destination ??= _resolveDefaultBackupDestination(candidates);
    final driverId = destination.driverId.trim();
    if (driverId.isEmpty) {
      return null;
    }
    final settings = _settingsForDestination(destination);
    final backupPath = _backupPathForDestination(destination);
    final driverParams = Map<String, dynamic>.from(destination.params);
    return _ResolvedDestination(destination: destination, settings: settings, driverId: driverId, backupPath: backupPath, driverParams: driverParams);
  }

  BackupDestination _resolveDefaultBackupDestination(List<BackupDestination> candidates) {
    final preferredId = _agentSettings.backupDestinationId?.trim() ?? '';
    if (preferredId.isNotEmpty) {
      for (final destination in candidates) {
        if (destination.id == preferredId) {
          return destination;
        }
      }
    }
    return candidates.first;
  }

  String _backupPathForDestination(BackupDestination destination) {
    if (destination.driverId == 'filesystem') {
      final path = destination.params['path']?.toString().trim() ?? '';
      return path;
    }
    return _filesystemBackupPath();
  }

  String _filesystemBackupPath() {
    for (final destination in _agentSettings.destinations) {
      if (destination.id != AppSettings.filesystemDestinationId) {
        continue;
      }
      return destination.params['path']?.toString().trim() ?? '';
    }
    return '';
  }

  AppSettings _settingsForDestination(BackupDestination destination) {
    final params = destination.params;
    final driverId = destination.driverId.trim();
    if (driverId == 'sftp') {
      return _agentSettings.copyWith(
        backupDestinationId: destination.id,
        backupDriverId: 'sftp',
        sftpHost: (params['host'] ?? '').toString(),
        sftpPort: params['port'] is num ? (params['port'] as num).toInt() : int.tryParse((params['port'] ?? '').toString()) ?? 22,
        sftpUsername: (params['username'] ?? '').toString(),
        sftpPassword: (params['password'] ?? '').toString(),
        sftpBasePath: (params['basePath'] ?? '').toString(),
      );
    }
    if (driverId == 'gdrive') {
      return _agentSettings.copyWith(
        backupDestinationId: destination.id,
        backupDriverId: 'gdrive',
        gdriveScope: (params['scope'] ?? '').toString(),
        gdriveRootPath: (params['rootPath'] ?? '').toString(),
        gdriveAccessToken: (params['accessToken'] ?? '').toString(),
        gdriveRefreshToken: (params['refreshToken'] ?? '').toString(),
        gdriveAccountEmail: (params['accountEmail'] ?? '').toString(),
        gdriveExpiresAt: AppSettings.parseDateTimeOrNull(params['expiresAt']),
      );
    }
    if (driverId == 'filesystem') {
      return _agentSettings.copyWith(backupDestinationId: destination.id, backupDriverId: 'filesystem');
    }
    if (driverId == 'dummy') {
      return _agentSettings.copyWith(backupDestinationId: destination.id, backupDriverId: 'dummy');
    }
    return _agentSettings.copyWith(backupDestinationId: destination.id, backupDriverId: driverId);
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
    final filesystem = FilesystemBackupDriver(trimmedPath);
    final dummy = DummyBackupDriver(trimmedPath, tmpWritesEnabled: sourceSettings.dummyDriverTmpWrites);
    final gdrive = identical(sourceSettings, _agentSettings)
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
          );
    final sftp = SftpBackupDriver(settings: sourceSettings);

    return {
      'filesystem': _DriverDescriptor(
        id: 'filesystem',
        label: 'Filesystem',
        usesPath: true,
        capabilities: filesystem.capabilities,
        validateStart: () => null,
        create: (_) => FilesystemBackupDriver(trimmedPath),
      ),
      'dummy': _DriverDescriptor(
        id: 'dummy',
        label: 'Dummy',
        usesPath: false,
        capabilities: dummy.capabilities,
        validateStart: () => null,
        create: (params) => DummyBackupDriver(trimmedPath, tmpWritesEnabled: sourceSettings.dummyDriverTmpWrites, driverParams: params),
      ),
      'gdrive': _DriverDescriptor(
        id: 'gdrive',
        label: 'Google Drive (Preview)',
        usesPath: false,
        capabilities: gdrive.capabilities,
        validateStart: () => sourceSettings.gdriveRefreshToken.trim().isEmpty ? 'google drive is not connected' : null,
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
        capabilities: sftp.capabilities,
        validateStart: () {
          final host = sourceSettings.sftpHost.trim();
          final user = sourceSettings.sftpUsername.trim();
          final password = sourceSettings.sftpPassword;
          final basePath = sourceSettings.sftpBasePath.trim();
          if (host.isEmpty || user.isEmpty || password.isEmpty || basePath.isEmpty) {
            return 'sftp is not configured';
          }
          return null;
        },
        create: (_) => SftpBackupDriver(settings: sourceSettings),
      ),
    };
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

  String _createJob(AgentJobType type) {
    final jobId = '${DateTime.now().millisecondsSinceEpoch}-${type.name}';
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
    );
    _jobControls[jobId] = _JobControl(startedAt: DateTime.now());
    return jobId;
  }

  void _updateJob(String jobId, AgentJobStatus status) {
    _jobs[jobId] = status;
    if (status.state == AgentJobState.failure) {
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
    final control = _jobControls[jobId];
    if (control?.canceled == true) {
      return false;
    }
    control?.canceled = true;
    control?.backupAgent?.cancel();
    control?.workerSendPort?.send({'type': 'cancel'});
    _updateJob(jobId, job.copyWith(message: 'Canceling...'));
    return true;
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
    _ResolvedDestination? destination,
    bool freshRequested = false,
  }) {
    final defaultDriverId = destination?.driverId ?? (_agentSettings.backupDriverId.trim().isEmpty ? 'filesystem' : _agentSettings.backupDriverId.trim());
    final driverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : defaultDriverId;
    final driverInfo = _driverCatalog[driverId] ?? _driverCatalog['filesystem']!;
    _setJobContext(
      jobId,
      source: _formatJobSource(server, vm.name),
      target: _formatBackupTarget(driverInfo, backupPath, driverId, destinationName: destination?.destination.name),
      driverLabel: driverInfo.label,
    );
    _hostLog('Backup job $jobId using driver: $driverId');
    final control = _jobControls[jobId];
    if (control == null) {
      return;
    }
    final workerReceive = ReceivePort();
    control.workerReceivePort = workerReceive;
    unawaited(
      Isolate.spawn(backupWorkerMain, {'sendPort': workerReceive.sendPort}).then((isolate) {
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
          'driverParams': driverParams ?? const <String, dynamic>{},
          'fresh': freshRequested,
          'settings': (destination?.settings ?? _agentSettings).toMap(),
          'destination': destination?.destination.toMap(),
          'server': server.toMap(),
          'vm': vm.toMap(),
        });
        return;
      }
      if (type == 'progress') {
        final progress = BackupAgentProgress.fromMap(Map<String, dynamic>.from(payload['progress'] as Map));
        final current = _jobs[jobId];
        if (current == null) {
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
        unawaited(_applyAgentSettings(updated, reason: 'worker', forceRestartSshListeners: false));
        return;
      }
      if (type == 'result') {
        final result = BackupAgentResult.fromMap(Map<String, dynamic>.from(payload['result'] as Map));
        final state = result.canceled ? AgentJobState.canceled : (result.success ? AgentJobState.success : AgentJobState.failure);
        final current = _jobs[jobId];
        final sizeBytes = current != null && current.totalBytes > 0 ? current.totalBytes : null;
        if (state == AgentJobState.failure) {
          try {
            await _refreshServer(server, reason: 'backup-failed');
          } catch (_) {}
        }
        _updateJob(
          jobId,
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.backup,
            state: state,
            message: result.message ?? '',
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
        _notifyNtfymeJobCompletion(jobId, type: AgentJobType.backup, state: state, message: result.message ?? '', sizeBytes: sizeBytes);
        workerReceive.close();
        control.workerReceivePort = null;
        control.workerSendPort = null;
        control.workerIsolate?.kill(priority: Isolate.immediate);
        control.workerIsolate = null;
      }
    });
  }

  void _startSanityJob(String jobId, String xmlPath, String timestamp) {
    unawaited(() async {
      try {
        final xmlFile = File(xmlPath);
        if (!await xmlFile.exists()) {
          throw 'XML not found: $xmlPath';
        }
        final driverInfo = _driverCatalog[_agentSettings.backupDriverId] ?? _driverCatalog['filesystem']!;
        final backupPath = _agentSettings.backupPath.trim();
        if (driverInfo.usesPath && backupPath.isEmpty) {
          throw 'Backup path is not configured';
        }
        final driver = _driverForBackupPath(backupPath);
        final location = driver.restoreLocationFromXml(xmlFile);
        if (location == null) {
          throw 'Cannot resolve restore location for $xmlPath';
        }
        if (driver is! drv.RemoteBlobDriver) {
          final blobsDir = driver.blobsDir();
          if (!await blobsDir.exists()) {
            throw 'Blobs directory not found: ${blobsDir.path}';
          }
        }
        final vmDir = location.vmDir;
        final manifests = await driver.listManifestsForTimestamp(vmDir, timestamp);
        if (manifests.isEmpty) {
          throw 'No manifests found for $timestamp';
        }

        var totalBytes = 0;
        for (final manifest in manifests) {
          final lines = await _readManifestLines(manifest);
          var blockSize = 1024 * 1024;
          int? fileSize;
          var maxIndex = -1;
          var inBlocks = false;
          for (final line in lines) {
            final trimmed = line.trim();
            if (trimmed.isEmpty) {
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
                fileSize = int.tryParse(value);
              } else if (trimmed == 'blocks:' || trimmed.startsWith('blocks:')) {
                inBlocks = true;
              }
              continue;
            }
            if (trimmed.endsWith('-> ZERO')) {
              final range = _parseZeroRange(trimmed);
              if (range != null) {
                if (range.$2 > maxIndex) {
                  maxIndex = range.$2;
                }
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
            if (index > maxIndex) {
              maxIndex = index;
            }
          }
          if (fileSize != null && fileSize > 0) {
            totalBytes += fileSize;
          } else if (maxIndex >= 0) {
            totalBytes += (maxIndex + 1) * blockSize;
          }
        }

        _updateJob(jobId, _jobs[jobId]!.copyWith(totalUnits: totalBytes, completedUnits: 0, bytesTransferred: 0, speedBytesPerSec: 0, totalBytes: totalBytes, message: 'Sanity check...'));

        var bytesChecked = 0;
        var bytesSinceTick = 0;
        var smoothedSpeed = 0.0;
        var lastSpeedUpdate = DateTime.now();
        var lastProgressUpdate = DateTime.now();
        var mismatches = 0;
        var checked = 0;

        void handleBytes(int bytes, {String? message}) {
          if (bytes <= 0) {
            return;
          }
          bytesChecked += bytes;
          bytesSinceTick += bytes;
          final now = DateTime.now();
          final elapsedMs = now.difference(lastSpeedUpdate).inMilliseconds;
          if (elapsedMs >= 1000) {
            final instant = bytesSinceTick / (elapsedMs / 1000);
            smoothedSpeed = _smoothSpeed(smoothedSpeed, instant);
            bytesSinceTick = 0;
            lastSpeedUpdate = now;
          }
          if (now.difference(lastProgressUpdate).inMilliseconds >= 500) {
            lastProgressUpdate = now;
            _updateJob(jobId, _jobs[jobId]!.copyWith(bytesTransferred: bytesChecked, speedBytesPerSec: smoothedSpeed, message: message ?? _jobs[jobId]!.message));
          }
        }

        for (final manifest in manifests) {
          _ensureJobNotCanceled(jobId);
          final lines = await _readManifestLines(manifest);
          var blockSize = 1024 * 1024;
          int? fileSize;
          String? diskId;
          var inBlocks = false;
          for (final line in lines) {
            final trimmed = line.trim();
            if (trimmed.isEmpty) {
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
                fileSize = int.tryParse(value);
              } else if (trimmed.startsWith('disk_id:')) {
                diskId = trimmed.substring('disk_id:'.length).trim();
              } else if (trimmed == 'blocks:' || trimmed.startsWith('blocks:')) {
                inBlocks = true;
              }
              continue;
            }
            _ensureJobNotCanceled(jobId);
            final message = diskId == null || diskId.isEmpty ? 'Sanity check...' : 'Sanity check: $diskId';
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
            final remote = driver is drv.RemoteBlobDriver ? driver as drv.RemoteBlobDriver : null;
            if (remote != null) {
              final bytes = await remote.readBlobBytes(hash);
              if (bytes == null) {
                mismatches++;
                _hostLog('Sanity check missing blob index=$index hash=$hash');
              } else {
                final actual = sha256.convert(bytes).toString();
                if (actual != hash) {
                  mismatches++;
                  _hostLog('Sanity check hash mismatch index=$index expected=$hash got=$actual');
                }
              }
            } else {
              final blobFile = driver.blobFile(hash);
              if (!await blobFile.exists()) {
                mismatches++;
                _hostLog('Sanity check missing blob index=$index hash=$hash');
              } else {
                final bytes = await blobFile.readAsBytes();
                final actual = sha256.convert(bytes).toString();
                if (actual != hash) {
                  mismatches++;
                  _hostLog('Sanity check hash mismatch index=$index expected=$hash got=$actual');
                }
              }
            }
            checked++;
            handleBytes(_blockLengthForIndex(index, fileSize, blockSize), message: message);
          }
        }

        final resultMessage = mismatches == 0 ? 'Sanity check OK ($checked blocks checked)' : 'Sanity check: $mismatches mismatch(es) out of $checked blocks';
        _updateJob(jobId, _jobs[jobId]!.copyWith(state: AgentJobState.success, message: resultMessage, bytesTransferred: bytesChecked, speedBytesPerSec: 0));
      } catch (error, stackTrace) {
        final isCanceled = error is _JobCanceled;
        if (!isCanceled) {
          _hostLogError('Sanity check failed.', error, stackTrace);
        }
        _updateJob(jobId, _jobs[jobId]!.copyWith(state: isCanceled ? AgentJobState.canceled : AgentJobState.failure, message: isCanceled ? 'Canceled' : error.toString(), speedBytesPerSec: 0));
      }
    }());
  }

  Future<RestorePrecheckResult> _restorePrecheck(ServerConfig server, String xmlPath) async {
    final xmlFile = File(xmlPath);
    if (!await xmlFile.exists()) {
      return RestorePrecheckResult(vmExists: false, canDefineOnly: false);
    }
    final xmlContent = await xmlFile.readAsString();
    final vmName = _extractVmNameFromXml(xmlContent);
    final diskSourcePaths = _extractDiskSourcePathsFromXml(xmlContent);
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
  }

  void _startRestoreJob(String jobId, ServerConfig server, String xmlPath, String decision, {_ResolvedDestination? destination, String? driverIdOverride}) {
    final defaultDriverId = destination?.driverId ?? (_agentSettings.backupDriverId.trim().isEmpty ? 'filesystem' : _agentSettings.backupDriverId.trim());
    final driverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : defaultDriverId;
    final driverInfo = _driverCatalog[driverId] ?? _driverCatalog['filesystem']!;
    _setJobContext(jobId, source: xmlPath, driverLabel: driverInfo.label);
    final backupPath = destination?.backupPath ?? _agentSettings.backupPath.trim();
    if (driverInfo.usesPath && backupPath.isEmpty) {
      final current = _jobs[jobId];
      if (current != null) {
        _updateJob(jobId, current.copyWith(state: AgentJobState.failure, message: 'Backup path is empty.'));
        _notifyNtfymeJobCompletion(jobId, type: AgentJobType.restore, state: AgentJobState.failure, message: 'Backup path is empty.');
      }
      return;
    }
    _hostLog('Restore job $jobId using driver: $driverId');
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
          'driverId': driverId,
          'backupPath': backupPath,
          'decision': decision,
          'xmlPath': xmlPath,
          'settings': (destination?.settings ?? _agentSettings).toMap(),
          'destination': destination?.destination.toMap(),
          'server': server.toMap(),
        });
        return;
      }
      if (type == 'status') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        _updateJob(jobId, status);
        return;
      }
      if (type == 'context') {
        _setJobContext(jobId, source: payload['source']?.toString(), target: payload['target']?.toString());
        return;
      }
      if (type == 'settings') {
        final updated = AppSettings.fromMap(Map<String, dynamic>.from(payload['settings'] as Map));
        unawaited(_applyAgentSettings(updated, reason: 'worker', forceRestartSshListeners: false));
        return;
      }
      if (type == 'result') {
        final status = AgentJobStatus.fromMap(Map<String, dynamic>.from(payload['status'] as Map));
        _updateJob(jobId, status);
        final sizeBytes = status.totalBytes > 0 ? status.totalBytes : null;
        _notifyNtfymeJobCompletion(jobId, type: AgentJobType.restore, state: status.state, message: status.message, sizeBytes: sizeBytes);
        workerReceive.close();
        control.workerReceivePort = null;
        control.workerSendPort = null;
        control.workerIsolate?.kill(priority: Isolate.immediate);
        control.workerIsolate = null;
      }
    });
  }

  String _extractTimestampFromFileName(String fileName) {
    final parts = fileName.split('__');
    if (parts.isEmpty) {
      return '';
    }
    return parts.first.trim();
  }

  String _extractVmNameFromXml(String xml) {
    final match = RegExp(r'<name>([^<]+)</name>').firstMatch(xml);
    if (match == null) {
      return 'Unknown VM';
    }
    final name = match.group(1)?.trim() ?? '';
    return name.isEmpty ? 'Unknown VM' : name;
  }

  void _setJobContext(String jobId, {String? source, String? target, String? driverLabel}) {
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
    if (driverLabel != null && driverLabel.trim().isNotEmpty) {
      control.driverLabel = driverLabel.trim();
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

  String _formatBackupTarget(BackupDriverInfo driverInfo, String backupPath, String driverId, {String? destinationName}) {
    final destinationLabel = destinationName?.trim() ?? '';
    if (destinationLabel.isNotEmpty) {
      return destinationLabel;
    }
    if (driverInfo.usesPath) {
      final trimmedPath = backupPath.trim();
      if (trimmedPath.isNotEmpty) {
        return trimmedPath;
      }
    }
    return driverId.trim().isEmpty ? driverInfo.id : driverId.trim();
  }

  void _notifyNtfymeJobCompletion(String jobId, {required AgentJobType type, required AgentJobState state, required String message, int? sizeBytes}) {
    if (state != AgentJobState.success && state != AgentJobState.failure) {
      return;
    }
    final token = _agentSettings.ntfymeToken.trim();
    if (token.isEmpty) {
      _hostLog('Ntfy me notification skipped (no token configured).');
      return;
    }
    final control = _jobControls[jobId];
    final duration = control?.startedAt == null ? null : DateTime.now().difference(control!.startedAt).inSeconds;
    final status = state == AgentJobState.success ? 'success' : 'failed';
    final destinationLabel = _resolveDestinationLabel(target: control?.target, driverLabel: control?.driverLabel);
    final messageText = _buildNtfymeMessage(type, status, control?.source, control?.target);
    final pushMessage = _buildNtfymePushMessage(messageText, destinationLabel);
    final payload = <String, dynamic>{'topic': _ntfymeTopic, 'msg': messageText, 'push_msg': pushMessage, 'type': type.name, 'status': status, 'destination': destinationLabel};
    if (duration != null) {
      payload['duration_sec'] = duration;
    }
    if (control?.source != null && control!.source!.trim().isNotEmpty) {
      payload['source'] = control.source;
    }
    if (control?.target != null && control!.target!.trim().isNotEmpty) {
      payload['target'] = control.target;
    }
    if (type == AgentJobType.backup && sizeBytes != null) {
      payload['size'] = _formatBytes(sizeBytes);
    }
    if (state == AgentJobState.failure) {
      payload['error'] = messageText;
    }
    _hostLog('Ntfy me notification queued: ${jsonEncode(payload)}');
    unawaited(_postNtfymeNotification(token, payload));
  }

  String _buildNtfymeMessage(AgentJobType type, String status, String? source, String? target) {
    final label = type == AgentJobType.backup ? 'Backup' : 'Restore';
    final statusText = status == 'success' ? 'succeeded' : 'failed';
    return '$label $statusText';
  }

  String _buildNtfymePushMessage(String message, String? destinationLabel) {
    final label = destinationLabel?.trim() ?? '';
    if (label.isEmpty) {
      return message;
    }
    return '$message on $label';
  }

  String _resolveDestinationLabel({String? target, String? driverLabel}) {
    final targetLabel = target?.trim() ?? '';
    if (targetLabel.isNotEmpty) {
      return targetLabel;
    }
    final fallback = driverLabel?.trim() ?? '';
    return fallback.isEmpty ? 'Unknown destination' : fallback;
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

  List<String> _extractDiskSourcePathsFromXml(String xml) {
    final sourcePaths = <String>[];
    final diskBlocks = RegExp("<disk[^>]*device=['\\\"]disk['\\\"][^>]*>(.*?)</disk>", dotAll: true).allMatches(xml);
    for (final diskBlock in diskBlocks) {
      final blockContent = diskBlock.group(1) ?? '';
      final sourceMatch = RegExp("<source[^>]*file=['\\\"]([^'\\\"]+)['\\\"]").firstMatch(blockContent);
      if (sourceMatch == null) {
        continue;
      }
      final sourcePath = sourceMatch.group(1)?.trim() ?? '';
      if (sourcePath.isEmpty) {
        continue;
      }
      sourcePaths.add(sourcePath);
    }
    return sourcePaths;
  }

  Future<String?> _readManifestField(File manifest, String field) async {
    try {
      final lines = await _readManifestLines(manifest);
      for (final line in lines) {
        final trimmed = line.trim();
        if (trimmed.startsWith('$field:')) {
          return trimmed.substring(field.length + 1).trim();
        }
        if (trimmed == 'blocks:') {
          break;
        }
      }
    } catch (_) {}
    return null;
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

  Future<List<_ChainEntry>> _readChainEntries(File chainFile) async {
    if (!await chainFile.exists()) {
      return const <_ChainEntry>[];
    }
    final lines = await chainFile.readAsLines();
    final entries = <_ChainEntry>[];
    String? currentPath;
    String? currentDiskId;
    for (final line in lines) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) {
        continue;
      }
      if (trimmed.startsWith('- path:')) {
        currentPath = trimmed.substring('- path:'.length).trim();
      } else if (trimmed.startsWith('path:')) {
        currentPath = trimmed.substring('path:'.length).trim();
      } else if (trimmed.startsWith('disk_id:')) {
        currentDiskId = trimmed.substring('disk_id:'.length).trim();
      }
      if (currentPath != null && currentDiskId != null) {
        entries.add(_ChainEntry(path: currentPath, diskId: currentDiskId));
        currentPath = null;
        currentDiskId = null;
      }
    }
    return entries;
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

  void _hostLog(String message) {
    LogWriter.logAgentSync(level: 'info', message: message);
  }

  void _hostLogError(String message, Object error, StackTrace stackTrace) {
    LogWriter.logAgentSync(level: 'info', message: '$message $error');
    LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
  }
}

class _JobControl {
  _JobControl({required this.startedAt});

  final DateTime startedAt;
  bool canceled = false;
  BackupAgent? backupAgent;
  Isolate? workerIsolate;
  ReceivePort? workerReceivePort;
  SendPort? workerSendPort;
  String? source;
  String? target;
  String? driverLabel;
}

class _JobCanceled implements Exception {
  const _JobCanceled();

  @override
  String toString() => 'Canceled';
}

class _EventStreamState {
  _EventStreamState(this.response);

  final HttpResponse response;
  bool closed = false;
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

class _ChainEntry {
  const _ChainEntry({required this.path, required this.diskId});

  final String path;
  final String diskId;
}
