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
    final bindAddress = InternetAddress.anyIPv4;
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

    _agentSettings = agentSettings;
    _cachedGdriveDriver = null;
    await _agentSettingsStore.save(agentSettings);

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
        final agentSettings = AppSettings.fromMap(body);
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
        final jobId = _createJob(AgentJobType.backup);
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
        final jobId = _createJob(AgentJobType.restore);
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
        final jobId = _createJob(AgentJobType.sanity);
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
        final jobId = _createJob(AgentJobType.sanity);
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
    final driverInfo = _driverCatalog[resolvedDriverId] ?? _driverCatalog['filesystem']!;
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
    final driverInfo = _driverCatalog[resolvedDriverId] ?? _driverCatalog['filesystem']!;
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
    final descriptor = registry[driverId] ?? registry['filesystem']!;
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
        capabilities: gdrive.capabilities,
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
      params: const <drv.DriverParamDefinition>[],
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
    final previous = _jobs[jobId];
    _jobs[jobId] = status;
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
        if (control.resultHandled) {
          return;
        }
        control.resultHandled = true;
        final result = BackupAgentResult.fromMap(Map<String, dynamic>.from(payload['result'] as Map));
        final state = result.canceled ? AgentJobState.canceled : (result.success ? AgentJobState.success : AgentJobState.failure);
        final current = _jobs[jobId];
        final sizeBytes = current != null && current.totalBytes > 0 ? current.totalBytes : null;
        if (state == AgentJobState.failure && control.canceled != true) {
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
    _startRestoreCheckJob(jobId, xmlPath, timestamp, storage: storage, mode: _RestoreCheckMode.full);
  }

  void _startQuickCheckJob(String jobId, String xmlPath, String timestamp, {required _ResolvedStorage storage}) {
    _startRestoreCheckJob(jobId, xmlPath, timestamp, storage: storage, mode: _RestoreCheckMode.quick);
  }

  void _startRestoreCheckJob(String jobId, String xmlPath, String timestamp, {required _ResolvedStorage storage, required _RestoreCheckMode mode}) {
    unawaited(() async {
      final checkDriversByBlockSizeMB = <int, drv.BackupDriver>{};
      final quickCachesByBlockSizeMB = <int, _BlobDirectoryLookupCache>{};
      final checkLabel = mode == _RestoreCheckMode.quick ? 'Quick check' : 'Sanity check';
      try {
        final driverInfo = _driverCatalog[storage.driverId] ?? _driverCatalog['filesystem']!;
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
              if (range != null && range.$2 > maxIndex) {
                maxIndex = range.$2;
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
            if (index > maxIndex) {
              maxIndex = index;
            }
          }
          _blockSizeMbFromManifestBytes(blockSize, manifest.path);
          if (fileSize != null && fileSize > 0) {
            totalBytes += fileSize;
          } else if (maxIndex >= 0) {
            totalBytes += (maxIndex + 1) * blockSize;
          }
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
        final maxConcurrentDownloads = storage.storage.downloadConcurrency ?? 8;
        if (maxConcurrentDownloads <= 0) {
          throw '$checkLabel requires downloadConcurrency > 0.';
        }
        final blocksByBlockSizeMB = <int, List<_CheckBlockRef>>{};

        void handleBytes(int bytes, {String? message}) {
          if (bytes <= 0) {
            return;
          }
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
              _jobs[jobId]!.copyWith(totalUnits: totalBlocks, completedUnits: checked, bytesTransferred: bytesChecked, speedBytesPerSec: smoothedSpeed, message: message ?? _jobs[jobId]!.message),
            );
          }
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
              } else if (trimmed.startsWith('disk_id:')) {
                diskId = trimmed.substring('disk_id:'.length).trim();
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
              final driverForBlockSize = checkDriversByBlockSizeMB.putIfAbsent(blockSizeMB, () {
                final settingsForBlockSize = storage.settings.copyWith(blockSizeMB: blockSizeMB);
                return _driverForSettings(driverInfo.id, backupPath, settings: settingsForBlockSize);
              });
              final cache = quickCachesByBlockSizeMB.putIfAbsent(blockSizeMB, () {
                final lister = driverForBlockSize is drv.BlobDirectoryLister ? driverForBlockSize as drv.BlobDirectoryLister : null;
                if (lister == null) {
                  throw 'Quick check requires BlobDirectoryLister support.';
                }
                return _BlobDirectoryLookupCache(lister);
              });
              final exists = await cache.blobExists(hash);
              checked += 1;
              if (!exists) {
                mismatches += 1;
                _hostLog('$checkLabel missing blob index=$index hash=$hash');
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
            final driverForBlockSize = checkDriversByBlockSizeMB.putIfAbsent(blockSizeMB, () {
              final settingsForBlockSize = storage.settings.copyWith(blockSizeMB: blockSizeMB);
              return _driverForSettings(driverInfo.id, backupPath, settings: settingsForBlockSize);
            });
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
          _jobs[jobId]!.copyWith(state: AgentJobState.success, message: resultMessage, totalUnits: totalBlocks, completedUnits: checked, bytesTransferred: bytesChecked, speedBytesPerSec: 0),
        );
      } catch (error, stackTrace) {
        final isCanceled = error is _JobCanceled;
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
      }
    }());
  }

  Future<RestorePrecheckResult> _restorePrecheck(ServerConfig server, String xmlPath, {required _ResolvedStorage storage, String? driverIdOverride}) async {
    final resolvedStorage = storage;
    final requestedDriverId = driverIdOverride?.trim() ?? '';
    final effectiveDriverId = requestedDriverId.isEmpty ? resolvedStorage.driverId : requestedDriverId;
    final driverInfo = _driverCatalog[effectiveDriverId] ?? _driverCatalog['filesystem']!;
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
    final normalizedDir = relativeDir.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/');
    if (normalizedDir.isEmpty) {
      return;
    }
    final files = await driver.listRelativeFiles(normalizedDir);
    for (final relativePath in files) {
      final bytes = await driver.readFileBytes(relativePath);
      if (bytes == null) {
        continue;
      }
      final normalizedPath = relativePath.replaceAll('\\', '/').split('/').where((part) => part.trim().isNotEmpty && part != '.').join('/');
      if (normalizedPath.isEmpty) {
        continue;
      }
      final localFilePath = '${driver.storage}${Platform.pathSeparator}${normalizedPath.replaceAll('/', Platform.pathSeparator)}';
      final localFile = File(localFilePath);
      final tempFile = File('${localFile.path}.inprogress.${DateTime.now().microsecondsSinceEpoch}');
      await tempFile.parent.create(recursive: true);
      await tempFile.writeAsBytes(bytes);
      if (await localFile.exists()) {
        await localFile.delete();
      }
      await tempFile.rename(localFile.path);
    }
  }

  void _startRestoreJob(String jobId, ServerConfig server, String xmlPath, String decision, {_ResolvedStorage? storage, String? driverIdOverride}) {
    final defaultDriverId = storage?.driverId ?? 'filesystem';
    final driverId = (driverIdOverride != null && driverIdOverride.trim().isNotEmpty) ? driverIdOverride.trim() : defaultDriverId;
    final driverInfo = _driverCatalog[driverId] ?? _driverCatalog['filesystem']!;
    _setJobContext(jobId, source: xmlPath, driverLabel: driverInfo.label);
    final backupPath = storage?.backupPath ?? _filesystemBackupPath();
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
          'settings': (storage?.settings ?? _agentSettings).toMap(),
          'storage': storage?.storage.toMap(),
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
    final dedupeKey = '${type.name}:${state.name}';
    if (control != null && control.lastNtfyCompletionKey == dedupeKey) {
      return;
    }
    if (control != null) {
      control.lastNtfyCompletionKey = dedupeKey;
    }
    final duration = control?.startedAt == null ? null : DateTime.now().difference(control!.startedAt).inSeconds;
    final status = state == AgentJobState.success ? 'success' : 'failed';
    final storageLabel = _resolveStorageLabel(target: control?.target, driverLabel: control?.driverLabel);
    final messageText = _buildNtfymeMessage(type, status, control?.source, control?.target);
    final pushMessage = _buildNtfymePushMessage(messageText, storageLabel);
    final payload = <String, dynamic>{'topic': _ntfymeTopic, 'msg': messageText, 'push_msg': pushMessage, 'type': type.name, 'status': status, 'storage': storageLabel};
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

  String _buildNtfymePushMessage(String message, String? storageLabel) {
    final label = storageLabel?.trim() ?? '';
    if (label.isEmpty) {
      return message;
    }
    return '$message on $label';
  }

  String _resolveStorageLabel({String? target, String? driverLabel}) {
    final targetLabel = target?.trim() ?? '';
    if (targetLabel.isNotEmpty) {
      return targetLabel;
    }
    final fallback = driverLabel?.trim() ?? '';
    return fallback.isEmpty ? 'Unknown storage' : fallback;
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

  void _hostLog(String message) {
    LogWriter.logAgentSync(level: 'info', message: message);
  }

  void _hostLogError(String message, Object error, StackTrace stackTrace) {
    LogWriter.logAgentSync(level: 'error', message: '$message $error');
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
  bool resultHandled = false;
  String? lastNtfyCompletionKey;
}

enum _RestoreCheckMode { full, quick }

class _BlobDirectoryLookupCache {
  _BlobDirectoryLookupCache(this._driver);

  final drv.BlobDirectoryLister _driver;
  Set<String>? _shards;
  final Map<String, Set<String>> _blobNamesByShard = <String, Set<String>>{};

  Future<bool> blobExists(String hash) async {
    if (hash.length < 2) {
      return false;
    }
    final shard = hash.substring(0, 2);
    final shards = await _loadShards();
    if (!shards.contains(shard)) {
      return false;
    }
    final names = await _loadBlobNames(shard);
    return names.contains(hash);
  }

  Future<Set<String>> _loadShards() async {
    final cached = _shards;
    if (cached != null) {
      return cached;
    }
    final names = await _driver.listBlobShards();
    _shards = names;
    return names;
  }

  Future<Set<String>> _loadBlobNames(String shard) async {
    final cached = _blobNamesByShard[shard];
    if (cached != null) {
      return cached;
    }
    final names = await _driver.listBlobNames(shard);
    _blobNamesByShard[shard] = names;
    return names;
  }
}

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
