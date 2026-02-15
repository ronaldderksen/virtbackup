import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:file_selector/file_selector.dart';
import 'package:crypto/crypto.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import 'package:url_launcher/url_launcher.dart';

import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/common/google_oauth_client.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:virtbackup/gui/agent_api_client.dart';

part 'settings_tab.dart';
part 'manage_tab.dart';
part 'backup_tab.dart';
part 'restore_tab.dart';
part 'ssh_service.dart';
part 'service.dart';

class _AgentEndpoint {
  _AgentEndpoint({required this.id, required this.host, required this.port, required this.token, required this.useLocalToken});

  final String id;
  final String host;
  final int port;
  final String token;
  final bool useLocalToken;

  String get label => '$host:$port';

  Map<String, dynamic> toMap() {
    return {'id': id, 'host': host, 'port': port, 'token': token, 'useLocalToken': useLocalToken};
  }

  factory _AgentEndpoint.fromMap(Map<String, dynamic> json) {
    return _AgentEndpoint(
      id: json['id']?.toString() ?? DateTime.now().millisecondsSinceEpoch.toString(),
      host: json['host']?.toString() ?? '127.0.0.1',
      port: (json['port'] as num?)?.toInt() ?? 33551,
      token: json['token']?.toString() ?? '',
      useLocalToken: json['useLocalToken'] == true,
    );
  }
}

class _OAuthCallbackResult {
  _OAuthCallbackResult({required this.code, required this.state, required this.error});

  final String? code;
  final String? state;
  final String? error;
}

class BackupServerSetupScreen extends StatefulWidget {
  const BackupServerSetupScreen({super.key});

  @override
  State<BackupServerSetupScreen> createState() => _BackupServerSetupScreenState();
}

class _BackupServerSetupScreenState extends State<BackupServerSetupScreen> {
  static const String _guiLogLevelPrefKey = 'log_level';
  static const EdgeInsets _contentPadding = EdgeInsets.only(left: 24, right: 24, bottom: 32);
  static const double _contentTitleSpacing = 8;
  static const double _contentSectionSpacing = 32;
  static const String _gdriveScopeFile = 'https://www.googleapis.com/auth/drive.file';
  final GlobalKey<FormState> _connectionFormKey = GlobalKey<FormState>();
  final GlobalKey<FormState> _localFormKey = GlobalKey<FormState>();
  final TextEditingController _serverNameController = TextEditingController();
  final TextEditingController _backupPathController = TextEditingController();
  final TextEditingController _ntfymeTokenController = TextEditingController();
  final TextEditingController _sshHostController = TextEditingController();
  final TextEditingController _sshPortController = TextEditingController(text: '22');
  final TextEditingController _sshUserController = TextEditingController();
  final TextEditingController _sshPasswordController = TextEditingController();
  final TextEditingController _apiBaseUrlController = TextEditingController();
  final TextEditingController _apiTokenController = TextEditingController();
  final TextEditingController _agentTokenController = TextEditingController();

  final List<ServerConfig> _servers = [];
  final List<BackupDriverInfo> _backupDrivers = [];
  final List<_AgentEndpoint> _agentEndpoints = [];
  String? _selectedAgentId;
  String? _editingServerId;
  String _savedBackupPath = '';
  String _savedNtfymeToken = '';
  String? _selectedBackupDestinationId;
  final Map<String, List<VmEntry>> _vmCacheByServerId = {};
  bool _connectionVerified = false;
  int _selectedMenuIndex = 2;
  bool _isTesting = false;
  bool _allowEmptyServerNameForTest = false;
  bool _isSavingAll = false;
  bool _isVmActionRunning = false;
  bool _isBackupRunning = false;
  bool _isRefreshingServer = false;
  bool _isLoadingRestoreEntries = false;
  bool _isRestoring = false;
  bool _isSanityChecking = false;
  bool _isSendingNtfymeTest = false;
  bool _isGdriveConnecting = false;
  final Map<String, bool> _vmHasOverlayByName = {};
  final Map<String, Map<String, bool>> _overlayByServerId = {};
  final Map<String, DateTime> _lastRefreshByServerId = {};
  final List<RestoreEntry> _restoreEntries = [];
  String? _selectedRestoreVmName;
  String? _selectedRestoreTimestamp;
  String? _selectedRestoreSourceServerId;
  String? _restoreServerId;
  String _restoreStatusMessage = '';
  int _restoreTotalBytes = 0;
  int _restoreBytesTransferred = 0;
  double _restoreSpeedBytesPerSec = 0;
  Timer? _restoreUiTimer;
  Timer? _sanityJobTimer;
  Timer? _backupJobTimer;
  String? _backupJobId;
  String? _restoreJobId;
  String? _sanityJobId;
  String _sanityStatusMessage = '';
  int _sanityBytesTransferred = 0;
  double _sanitySpeedBytesPerSec = 0;
  int _sanityTotalBytes = 0;
  String _backupStatusMessage = '';
  int _backupCompletedDisks = 0;
  int _backupTotalDisks = 0;
  int _backupBytesTransferred = 0;
  double _backupAverageSpeedBytesPerSec = 0;
  double _backupPhysicalSpeedBytesPerSec = 0;
  int _backupPhysicalBytesTransferred = 0;
  int? _backupEtaSeconds;
  int _backupPhysicalRemainingBytes = 0;
  int _backupPhysicalTotalBytes = 0;
  double _backupPhysicalProgressPercent = 0;
  int _backupTotalBytes = 0;
  int _backupSanityBytesTransferred = 0;
  double _backupSanitySpeedBytesPerSec = 0;
  final AgentApiClient _agentApiClient = AgentApiClient();
  AppSettings _agentSettings = AppSettings.empty();
  bool _agentReachable = true;
  bool _agentErrorNotified = false;
  bool _agentAuthFailed = false;
  bool _agentTokenMissing = false;
  String? _currentAgentToken;
  bool? _nativeSftpAvailable;
  String? _trustedAgentCertFingerprint;
  bool _certDialogOpen = false;
  StreamSubscription<AgentEvent>? _eventSubscription;
  Timer? _eventReconnectTimer;
  bool _eventConnecting = false;
  Timer? _agentReconnectTimer;
  bool _isLoadingAgentSettings = false;
  Timer? _jobSyncTimer;
  bool _jobSyncInProgress = false;
  bool _guiLogRotated = false;

  @override
  void initState() {
    super.initState();
    _attachFieldListeners();
    unawaited(_configureGuiLogWriter());
    _loadAgentEndpointsAndSettings();
  }

  @override
  void dispose() {
    _restoreUiTimer?.cancel();
    _sanityJobTimer?.cancel();
    _backupJobTimer?.cancel();
    _jobSyncTimer?.cancel();
    _stopEventStream();
    _agentReconnectTimer?.cancel();
    _serverNameController.dispose();
    _backupPathController.dispose();
    _ntfymeTokenController.dispose();
    _sshHostController.dispose();
    _sshPortController.dispose();
    _sshUserController.dispose();
    _sshPasswordController.dispose();
    _apiBaseUrlController.dispose();
    _apiTokenController.dispose();
    _agentTokenController.dispose();
    super.dispose();
  }

  Future<void> _loadAgentEndpointsAndSettings() async {
    await _loadAgentEndpoints();
    await _applySelectedAgent();
    await _loadAgentSettings();
  }

  Future<GoogleOAuthInstalledClient> _resolveGdriveOAuthClient() async {
    final store = await AppSettingsStore.fromAgentDefaultPath();
    final settingsDir = store.file.parent;
    final sep = Platform.pathSeparator;
    final overrideFile = File('${settingsDir.path}${sep}etc${sep}google_oauth_client.json');
    final locator = GoogleOAuthClientLocator(overrideFile: overrideFile);
    try {
      return await locator.load(requireSecret: false);
    } catch (error) {
      final candidates = locator.candidateFiles().map((file) => file.path).toList();
      _logInfo('Google Drive OAuth client config load failed: $error');
      _logInfo('Google Drive OAuth candidates: ${candidates.join(' | ')}');
      _logInfo('Google Drive OAuth preferred path: ${overrideFile.path}');
      rethrow;
    }
  }

  Future<void> _loadAgentEndpoints() async {
    final prefs = await SharedPreferences.getInstance();
    final encoded = prefs.getString('agent_endpoints');
    final selectedId = prefs.getString('agent_selected_id');
    if (encoded != null && encoded.isNotEmpty) {
      try {
        final decoded = jsonDecode(encoded);
        if (decoded is List) {
          _agentEndpoints
            ..clear()
            ..addAll(decoded.whereType<Map>().map((entry) => _AgentEndpoint.fromMap(Map<String, dynamic>.from(entry))));
        }
      } catch (_) {}
    }
    if (_agentEndpoints.isEmpty) {
      _agentEndpoints.add(_AgentEndpoint(id: DateTime.now().millisecondsSinceEpoch.toString(), host: '127.0.0.1', port: 33551, token: '', useLocalToken: true));
    } else {
      for (var i = 0; i < _agentEndpoints.length; i++) {
        final entry = _agentEndpoints[i];
        if (_isLocalAgentHost(entry.host) && !entry.useLocalToken) {
          _agentEndpoints[i] = _AgentEndpoint(id: entry.id, host: entry.host, port: entry.port, token: entry.token, useLocalToken: true);
        }
      }
    }
    if (selectedId != null && _agentEndpoints.any((item) => item.id == selectedId)) {
      _selectedAgentId = selectedId;
    } else {
      _selectedAgentId = _agentEndpoints.first.id;
    }
    await _persistAgentEndpoints();
  }

  Future<void> _persistAgentEndpoints() async {
    final prefs = await SharedPreferences.getInstance();
    final data = _agentEndpoints.map((entry) => entry.toMap()).toList();
    await prefs.setString('agent_endpoints', jsonEncode(data));
    if (_selectedAgentId != null) {
      await prefs.setString('agent_selected_id', _selectedAgentId!);
    }
  }

  _AgentEndpoint? _currentAgent() {
    if (_selectedAgentId == null) {
      return _agentEndpoints.isNotEmpty ? _agentEndpoints.first : null;
    }
    if (_agentEndpoints.isEmpty) {
      return null;
    }
    return _agentEndpoints.firstWhere((item) => item.id == _selectedAgentId, orElse: () => _agentEndpoints.first);
  }

  bool _isLocalAgentHost(String host) {
    final normalized = host.trim().toLowerCase();
    return normalized == '127.0.0.1';
  }

  Future<void> _applySelectedAgent() async {
    final selected = _currentAgent();
    if (selected == null) {
      return;
    }
    final uri = Uri.parse('https://${selected.host}:${selected.port}');
    _agentApiClient.setBaseUri(uri);
    if (_isLocalAgentHost(selected.host) || selected.useLocalToken) {
      await _loadAgentAuthToken();
      _agentTokenController.text = '';
      _agentTokenMissing = _currentAgentToken == null || _currentAgentToken!.isEmpty;
    } else {
      final token = selected.token.trim();
      _agentApiClient.setAuthToken(token.isEmpty ? null : token);
      _currentAgentToken = token.isEmpty ? null : token;
      _agentTokenController.text = selected.token;
      _agentTokenMissing = token.isEmpty;
    }
    if (mounted) {
      setState(() {});
    }
    await _loadTrustedAgentCertificate();
  }

  Future<void> _loadAgentSettings() async {
    if (_isLoadingAgentSettings) {
      return;
    }
    _isLoadingAgentSettings = true;
    try {
      _backupDrivers
        ..clear()
        ..addAll(await _loadBackupDrivers());
      _agentSettings = await _agentApiClient.fetchConfig();
      _setAgentReachable(true);
      await _loadAgentHealth();
      _agentReconnectTimer?.cancel();
      _agentReconnectTimer = null;
      _startEventStream();
      await _maybeHandleAgentCertificate();
    } catch (error, stackTrace) {
      _logError('Failed to load settings from agent.', error, stackTrace);
      if (_isAuthError(error)) {
        _agentAuthFailed = true;
        if (_selectedMenuIndex != 0 && mounted) {
          setState(() {
            _selectedMenuIndex = 0;
          });
        }
      }
      _setAgentReachable(false);
      _nativeSftpAvailable = null;
      _notifyAgentErrorOnce('Unable to load settings from agent: $error');
      _scheduleAgentReconnect();
    } finally {
      _isLoadingAgentSettings = false;
    }
    _selectedBackupDestinationId = _agentSettings.backupDestinationId;
    _ensureBackupDestinationSelection();
    _backupPathController.text = _agentSettings.backupPath;
    _savedBackupPath = _backupPathController.text.trim();
    unawaited(_configureGuiLogWriter(backupPath: _savedBackupPath, rotateOnStartup: true));
    _ntfymeTokenController.text = _agentSettings.ntfymeToken;
    _savedNtfymeToken = _ntfymeTokenController.text.trim();
    _servers
      ..clear()
      ..addAll(_agentSettings.servers);
    _connectionVerified = _agentSettings.connectionVerified;

    final storedId = _agentSettings.selectedServerId;
    if (storedId != null && _servers.any((server) => server.id == storedId)) {
      _editingServerId = storedId;
    } else if (_servers.isNotEmpty) {
      _editingServerId = _servers.first.id;
    } else {
      _editingServerId = null;
    }

    if (_editingServerId != null) {
      _applyServerToForm(_servers.firstWhere((server) => server.id == _editingServerId));
    } else {
      _resetServerForm();
    }

    _restoreServerId = _editingServerId;

    if (mounted) {
      setState(() {});
    }
    await _syncRunningJobs();
    if (_selectedMenuIndex == 3) {
      await _loadRestoreEntries();
    }
    if (_requiresVmInventory(_selectedMenuIndex)) {
      final server = _getSelectedServer();
      if (server != null) {
        await _loadVmInventory(server);
      }
    }
    if (_requiresVerifiedIndex(_selectedMenuIndex) && !_connectionVerified) {
      setState(() {
        _selectedMenuIndex = 0;
      });
    }
  }

  Future<void> _loadAgentAuthToken() async {
    try {
      final store = await AppSettingsStore.fromAgentDefaultPath();
      final token = await store.loadAgentToken();
      _agentApiClient.setAuthToken(token);
      _currentAgentToken = token;
    } catch (_) {
      _agentApiClient.setAuthToken(null);
      _currentAgentToken = null;
    }
  }

  Future<void> _switchAgent(String? agentId) async {
    if (agentId == null || agentId.isEmpty || agentId == _selectedAgentId) {
      return;
    }
    _selectedAgentId = agentId;
    if (mounted) {
      setState(() {});
    }
    await _persistAgentEndpoints();
    _stopEventStream();
    _agentReconnectTimer?.cancel();
    _agentReconnectTimer = null;
    _agentErrorNotified = false;
    _agentReachable = true;
    await _applySelectedAgent();
    await _loadAgentSettings();
  }

  Future<void> _updateSelectedAgentToken(String value) async {
    final selected = _currentAgent();
    if (selected == null || _isLocalAgentHost(selected.host)) {
      return;
    }
    final index = _agentEndpoints.indexWhere((item) => item.id == selected.id);
    if (index < 0) {
      return;
    }
    _agentTokenMissing = value.trim().isEmpty;
    _agentEndpoints[index] = _AgentEndpoint(id: selected.id, host: selected.host, port: selected.port, token: value, useLocalToken: selected.useLocalToken);
    _agentApiClient.setAuthToken(value.trim().isEmpty ? null : value.trim());
    _currentAgentToken = value.trim().isEmpty ? null : value.trim();
    await _persistAgentEndpoints();
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _toggleUseLocalToken(bool value) async {
    final selected = _currentAgent();
    if (selected == null || _isLocalAgentHost(selected.host)) {
      return;
    }
    final index = _agentEndpoints.indexWhere((item) => item.id == selected.id);
    if (index < 0) {
      return;
    }
    _agentEndpoints[index] = _AgentEndpoint(id: selected.id, host: selected.host, port: selected.port, token: selected.token, useLocalToken: value);
    if (value) {
      await _loadAgentAuthToken();
      _agentTokenController.text = '';
      _agentTokenMissing = _currentAgentToken == null || _currentAgentToken!.isEmpty;
    } else {
      final token = selected.token.trim();
      _agentApiClient.setAuthToken(token.isEmpty ? null : token);
      _currentAgentToken = token.isEmpty ? null : token;
      _agentTokenMissing = token.isEmpty;
    }
    await _persistAgentEndpoints();
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _removeSelectedAgent() async {
    final selected = _currentAgent();
    if (selected == null || _agentEndpoints.length <= 1) {
      return;
    }
    _agentEndpoints.removeWhere((item) => item.id == selected.id);
    _selectedAgentId = _agentEndpoints.first.id;
    await _persistAgentEndpoints();
    await _applySelectedAgent();
    await _loadAgentSettings();
  }

  Future<void> _showAddAgentDialog() async {
    final hostController = TextEditingController();
    final portController = TextEditingController(text: '33551');
    final tokenController = TextEditingController();
    try {
      final created = await showDialog<_AgentEndpoint>(
        context: context,
        builder: (dialogContext) {
          var useLocalToken = false;
          return StatefulBuilder(
            builder: (context, setStateDialog) {
              final hostValue = hostController.text.trim();
              final isLocal = _isLocalAgentHost(hostValue);
              final needsToken = !isLocal && !useLocalToken;
              return AlertDialog(
                title: const Text('Add agent'),
                content: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    TextField(
                      controller: hostController,
                      decoration: const InputDecoration(labelText: 'Host', hintText: '127.0.0.1', border: OutlineInputBorder()),
                      textInputAction: TextInputAction.next,
                      onChanged: (_) => setStateDialog(() {}),
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: portController,
                      decoration: const InputDecoration(labelText: 'Port', hintText: '33551', border: OutlineInputBorder()),
                      keyboardType: TextInputType.number,
                      textInputAction: TextInputAction.next,
                    ),
                    const SizedBox(height: 12),
                    if (!isLocal)
                      CheckboxListTile(
                        contentPadding: EdgeInsets.zero,
                        title: const Text('Use local token'),
                        value: useLocalToken,
                        onChanged: (value) {
                          setStateDialog(() {
                            useLocalToken = value ?? false;
                          });
                        },
                      ),
                    if (needsToken) ...[
                      const SizedBox(height: 12),
                      TextField(
                        controller: tokenController,
                        decoration: const InputDecoration(labelText: 'Token', border: OutlineInputBorder()),
                        textInputAction: TextInputAction.done,
                      ),
                    ],
                  ],
                ),
                actions: [
                  TextButton(onPressed: () => Navigator.of(dialogContext).pop(), child: const Text('Cancel')),
                  FilledButton(
                    onPressed: () {
                      final host = hostController.text.trim();
                      final port = int.tryParse(portController.text.trim()) ?? 33551;
                      if (host.isEmpty) {
                        return;
                      }
                      final token = needsToken ? tokenController.text.trim() : '';
                      if (needsToken && token.isEmpty) {
                        return;
                      }
                      Navigator.of(
                        dialogContext,
                      ).pop(_AgentEndpoint(id: DateTime.now().millisecondsSinceEpoch.toString(), host: host, port: port, token: token, useLocalToken: useLocalToken || isLocal));
                    },
                    child: const Text('Add'),
                  ),
                ],
              );
            },
          );
        },
      );
      if (created == null) {
        return;
      }
      _agentEndpoints.add(created);
      _selectedAgentId = created.id;
      await _persistAgentEndpoints();
      await _applySelectedAgent();
      await _loadAgentSettings();
      if (mounted) {
        setState(() {});
      }
    } finally {
      hostController.dispose();
      portController.dispose();
      tokenController.dispose();
    }
  }

  Future<void> _loadTrustedAgentCertificate() async {
    final uri = _agentApiClient.baseUri;
    if (uri.scheme != 'https') {
      _agentApiClient.setAllowUntrustedCerts(true);
      _agentApiClient.setTrustedCertFingerprint(null);
      return;
    }
    final prefs = await SharedPreferences.getInstance();
    final key = _agentCertPrefsKey();
    final stored = prefs.getString(key);
    _trustedAgentCertFingerprint = stored;
    _agentApiClient.setTrustedCertFingerprint(stored);
    _agentApiClient.setAllowUntrustedCerts(true);
  }

  Future<void> _loadAgentHealth() async {
    try {
      final available = await _agentApiClient.fetchNativeSftpAvailable();
      if (mounted) {
        setState(() {
          _nativeSftpAvailable = available;
        });
      }
    } catch (_) {
      if (mounted) {
        setState(() {
          _nativeSftpAvailable = null;
        });
      }
    }
  }

  String _agentCertPrefsKey() {
    final uri = _agentApiClient.baseUri;
    final host = uri.host.isEmpty ? 'agent' : uri.host;
    final port = uri.hasPort ? uri.port : (uri.scheme == 'https' ? 443 : 80);
    return 'agent_cert_fp_${host}_$port';
  }

  Future<void> _maybeHandleAgentCertificate() async {
    final uri = _agentApiClient.baseUri;
    if (uri.scheme != 'https') {
      return;
    }
    final fingerprint = _agentApiClient.lastSeenCertFingerprint;
    if (fingerprint == null || fingerprint.isEmpty) {
      return;
    }
    final prefs = await SharedPreferences.getInstance();
    final key = _agentCertPrefsKey();
    final stored = prefs.getString(key);
    _trustedAgentCertFingerprint ??= stored;
    if (_trustedAgentCertFingerprint == fingerprint) {
      _agentApiClient.setTrustedCertFingerprint(fingerprint);
      _agentApiClient.setAllowUntrustedCerts(false);
      return;
    }
    if (_certDialogOpen || !mounted) {
      return;
    }
    _certDialogOpen = true;
    try {
      final isFirst = _trustedAgentCertFingerprint == null;
      final title = isFirst ? 'Trust agent certificate?' : 'Agent certificate changed';
      final actionLabel = isFirst ? 'Trust' : 'Trust new';
      final message = isFirst
          ? 'The agent uses a self-signed certificate. Do you want to trust and save it for future connections?\n\nFingerprint: $fingerprint'
          : 'The agent certificate fingerprint has changed.\n\nOld: $_trustedAgentCertFingerprint\nNew: $fingerprint\n\nDo you want to trust and save the new certificate?';
      final approved = await showDialog<bool>(
        context: context,
        builder: (dialogContext) {
          return AlertDialog(
            title: Text(title),
            content: Text(message),
            actions: [
              TextButton(onPressed: () => Navigator.of(dialogContext).pop(false), child: const Text('Cancel')),
              FilledButton(onPressed: () => Navigator.of(dialogContext).pop(true), child: Text(actionLabel)),
            ],
          );
        },
      );
      if (approved == true) {
        await prefs.setString(key, fingerprint);
        _trustedAgentCertFingerprint = fingerprint;
        _agentApiClient.setTrustedCertFingerprint(fingerprint);
        _agentApiClient.setAllowUntrustedCerts(false);
        if (mounted) {
          _showSnackBarInfo('Agent certificate trusted.');
        }
      } else {
        _agentApiClient.setAllowUntrustedCerts(false);
        if (mounted) {
          _notifyAgentErrorOnce('Agent certificate not trusted. Connection blocked.');
          _showSnackBarError('Agent certificate not trusted. Connection blocked.');
        }
      }
    } finally {
      _certDialogOpen = false;
    }
  }

  Future<List<BackupDriverInfo>> _loadBackupDrivers() async {
    try {
      final drivers = await _agentApiClient.fetchDrivers();
      if (drivers.isNotEmpty) {
        return drivers;
      }
    } catch (_) {}
    return _fallbackBackupDrivers();
  }

  List<BackupDriverInfo> _fallbackBackupDrivers() {
    return [
      BackupDriverInfo(
        id: 'filesystem',
        label: 'Filesystem',
        usesPath: true,
        capabilities: BackupDriverCapabilities(
          supportsRangeRead: true,
          supportsBatchDelete: true,
          supportsMultipartUpload: false,
          supportsServerSideCopy: false,
          supportsConditionalWrite: false,
          supportsVersioning: false,
          maxConcurrentWrites: 16,
        ),
      ),
      BackupDriverInfo(
        id: 'dummy',
        label: 'Dummy',
        usesPath: false,
        capabilities: BackupDriverCapabilities(
          supportsRangeRead: false,
          supportsBatchDelete: false,
          supportsMultipartUpload: false,
          supportsServerSideCopy: false,
          supportsConditionalWrite: false,
          supportsVersioning: false,
          maxConcurrentWrites: 1,
          params: [DriverParamDefinition(key: 'throttleMbps', label: 'Dummy throttle', type: DriverParamType.number, min: 0, unit: 'MB/s', help: 'Leave empty or 0 for unlimited.')],
        ),
      ),
    ];
  }

  List<BackupDestination> _enabledDestinations() {
    final result = <BackupDestination>[];
    final seenIds = <String>{};
    for (final destination in _agentSettings.destinations) {
      if (!destination.enabled) {
        continue;
      }
      final id = destination.id.trim();
      if (id.isEmpty || seenIds.contains(id)) {
        continue;
      }
      seenIds.add(id);
      result.add(destination);
    }
    return result;
  }

  void _ensureBackupDestinationSelection() {
    final enabled = _enabledDestinations();
    if (enabled.isEmpty) {
      _selectedBackupDestinationId = null;
      return;
    }
    final currentId = _selectedBackupDestinationId?.trim() ?? '';
    if (currentId.isNotEmpty) {
      for (final destination in enabled) {
        if (destination.id == currentId) {
          return;
        }
      }
    }
    final preferredId = _agentSettings.backupDestinationId?.trim() ?? '';
    if (preferredId.isNotEmpty) {
      for (final destination in enabled) {
        if (destination.id == preferredId) {
          _selectedBackupDestinationId = destination.id;
          return;
        }
      }
    }
    _selectedBackupDestinationId = enabled.first.id;
  }

  void _scheduleAgentReconnect() {
    if (!mounted || _agentReachable) {
      return;
    }
    if (_agentReconnectTimer != null) {
      return;
    }
    _logInfo('Agent reconnect scheduled');
    _agentReconnectTimer = Timer(const Duration(seconds: 3), () {
      _agentReconnectTimer = null;
      _logInfo('Agent reconnecting');
      _loadAgentSettings();
    });
  }

  void _setAgentReachable(bool reachable) {
    if (!mounted) {
      _agentReachable = reachable;
      return;
    }
    if (_agentReachable == reachable) {
      return;
    }
    setState(() {
      _agentReachable = reachable;
      if (reachable) {
        _agentErrorNotified = false;
        _agentAuthFailed = false;
      } else if (_selectedMenuIndex == 0) {
        if (!_agentAuthFailed) {
          _selectedMenuIndex = 1;
        }
      }
    });
    if (reachable) {
      _startEventStream();
      _startJobSyncTimer();
    } else {
      _stopEventStream();
      _stopJobSyncTimer();
    }
  }

  void _startJobSyncTimer() {
    _jobSyncTimer?.cancel();
    _jobSyncTimer = Timer.periodic(const Duration(seconds: 5), (_) async {
      if (_jobSyncInProgress) {
        return;
      }
      _jobSyncInProgress = true;
      try {
        await _syncRunningJobs();
      } finally {
        _jobSyncInProgress = false;
      }
    });
    unawaited(_syncRunningJobs());
  }

  void _stopJobSyncTimer() {
    _jobSyncTimer?.cancel();
    _jobSyncTimer = null;
    _jobSyncInProgress = false;
  }

  bool _isAuthError(Object error) {
    final message = error.toString();
    return message.contains('401');
  }

  void _startEventStream() {
    if (_eventSubscription != null || _eventConnecting) {
      return;
    }
    _eventConnecting = true;
    _eventReconnectTimer?.cancel();
    _eventReconnectTimer = null;
    _logInfo('SSE connect: starting');
    _startJobSyncTimer();
    _eventSubscription = _agentApiClient.eventStream().listen(
      _handleAgentEvent,
      onError: (error) {
        _logError('SSE error', error, StackTrace.current);
        _scheduleEventReconnect();
      },
      onDone: () {
        _logInfo('SSE done');
        _scheduleEventReconnect();
      },
      cancelOnError: true,
    );
    _eventConnecting = false;
  }

  void _stopEventStream() {
    _eventReconnectTimer?.cancel();
    _eventReconnectTimer = null;
    _eventSubscription?.cancel();
    _eventSubscription = null;
    _eventConnecting = false;
    _stopJobSyncTimer();
  }

  void _scheduleEventReconnect() {
    if (!mounted) {
      return;
    }
    _eventSubscription = null;
    if (_eventReconnectTimer != null) {
      return;
    }
    _logInfo('SSE reconnect scheduled');
    _eventReconnectTimer = Timer(const Duration(seconds: 3), () {
      _eventReconnectTimer = null;
      _logInfo('SSE reconnecting');
      _startEventStream();
    });
  }

  void _handleAgentEvent(AgentEvent event) {
    _logInfo('SSE event: ${event.type} ${event.payload}');
    if (event.type != 'vm.lifecycle') {
      return;
    }
    final serverId = (event.payload['serverId'] ?? '').toString();
    final vmName = (event.payload['vmName'] ?? '').toString();
    final state = (event.payload['state'] ?? '').toString();
    if (serverId.isEmpty || vmName.isEmpty || state.isEmpty) {
      return;
    }
    final cache = _vmCacheByServerId[serverId];
    if (cache == null || cache.isEmpty) {
      return;
    }
    final index = cache.indexWhere((vm) => vm.name == vmName || vm.id == vmName);
    if (index < 0) {
      return;
    }
    final powerState = state == 'running' ? VmPowerState.running : VmPowerState.stopped;
    final current = cache[index];
    if (current.powerState == powerState) {
      return;
    }
    cache[index] = VmEntry(id: current.id, name: current.name, powerState: powerState);
    _vmCacheByServerId[serverId] = List<VmEntry>.from(cache);
    _lastRefreshByServerId[serverId] = DateTime.now();
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _pushAgentSettings() async {
    try {
      await _agentApiClient.updateConfig(_agentSettings);
      _setAgentReachable(true);
    } catch (error, stackTrace) {
      _logError('Failed to update settings via agent.', error, stackTrace);
      _setAgentReachable(false);
      _notifyAgentErrorOnce('Unable to update agent config: $error');
    }
  }

  Future<void> _syncRunningJobs() async {
    try {
      final jobs = await _agentApiClient.fetchJobs();
      if (jobs.isEmpty) {
        return;
      }
      final runningBackup = jobs.where((job) => job.type == AgentJobType.backup && job.state == AgentJobState.running).toList();
      if (runningBackup.isNotEmpty) {
        _startBackupJobPolling(runningBackup.first.id);
      }
      final runningRestore = jobs.where((job) => job.type == AgentJobType.restore && job.state == AgentJobState.running).toList();
      if (runningRestore.isNotEmpty) {
        _startRestoreJobPolling(runningRestore.first.id);
      }
      final runningSanity = jobs.where((job) => job.type == AgentJobType.sanity && job.state == AgentJobState.running).toList();
      if (runningSanity.isNotEmpty) {
        _startSanityJobPolling(runningSanity.first.id);
      }
    } catch (error, stackTrace) {
      _logError('Failed to sync running jobs.', error, stackTrace);
      _notifyAgentErrorOnce('Unable to load jobs from agent: $error');
    }
  }

  Future<void> _persistServers({String? selectedId}) async {
    _agentSettings = _agentSettings.copyWith(servers: List<ServerConfig>.from(_servers), selectedServerId: selectedId ?? _agentSettings.selectedServerId);
    await _pushAgentSettings();
  }

  bool _isPersistedServer(String id) {
    return _agentSettings.servers.any((server) => server.id == id);
  }

  void _discardDraftServers() {
    final persistedIds = _agentSettings.servers.map((server) => server.id).toSet();
    final removed = _servers.where((server) => !persistedIds.contains(server.id)).map((server) => server.id).toSet();
    if (removed.isEmpty) {
      return;
    }
    _servers.removeWhere((server) => removed.contains(server.id));
    if (_editingServerId != null && removed.contains(_editingServerId)) {
      _editingServerId = _servers.isEmpty ? null : _servers.first.id;
      if (_editingServerId != null) {
        _applyServerToForm(_servers.firstWhere((server) => server.id == _editingServerId));
      } else {
        _resetServerForm();
      }
    }
  }

  Future<void> _setConnectionVerified(bool verified) async {
    _connectionVerified = verified;
    _agentSettings = _agentSettings.copyWith(connectionVerified: verified);
    await _pushAgentSettings();
    if (mounted) {
      if (!verified && _requiresVerifiedIndex(_selectedMenuIndex)) {
        _selectedMenuIndex = 0;
      }
      setState(() {});
    }
  }

  Future<void> _clearConnectionVerified() async {
    _connectionVerified = false;
    _agentSettings = _agentSettings.copyWith(connectionVerified: false);
    await _pushAgentSettings();
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _selectMenuIndex(int index) async {
    final leavingSettings = _selectedMenuIndex == 0 && index != 0;
    setState(() {
      _selectedMenuIndex = index;
      if (leavingSettings) {
        _discardDraftServers();
      }
    });
    if (index == 2 || index == 3) {
      await _loadAgentHealth();
    }
    if (_requiresVmInventory(index)) {
      final server = _getSelectedServer();
      if (server != null) {
        await _loadVmInventory(server);
      }
    }
    if (index == 3) {
      await _loadRestoreEntries();
    }
  }

  String _menuTitle(int index) {
    switch (index) {
      case 0:
        return 'Settings';
      case 1:
        return 'Manage';
      case 2:
        return 'Backup';
      case 3:
        return 'Restore';
      default:
        return 'Settings';
    }
  }

  String _menuSubtitle(int index) {
    switch (index) {
      case 0:
        return 'Configure servers and storage for backups.';
      case 1:
        return 'Select a server to view its virtual machines.';
      case 2:
        return 'Prepare and schedule VM backups.';
      case 3:
        return 'Restore a VM from a backup.';
      default:
        return 'Configure servers and storage for backups.';
    }
  }

  String _formatLastRefresh(ServerConfig? server) {
    if (server == null) {
      return 'No server selected.';
    }
    final timestamp = _lastRefreshByServerId[server.id];
    if (timestamp == null) {
      return 'Not refreshed yet.';
    }
    final local = timestamp.toLocal();
    final hh = local.hour.toString().padLeft(2, '0');
    final mm = local.minute.toString().padLeft(2, '0');
    final ss = local.second.toString().padLeft(2, '0');
    return 'Last refreshed: ${local.year}-${local.month.toString().padLeft(2, '0')}-${local.day.toString().padLeft(2, '0')} $hh:$mm:$ss';
  }

  String _formatSpeed(double bytesPerSecond) {
    if (bytesPerSecond <= 0) {
      return '0 MB/s';
    }
    final mbPerSec = bytesPerSecond / (1024 * 1024);
    return '${mbPerSec.toStringAsFixed(1)} MB/s';
  }

  String _formatTotalSize(int bytes) {
    if (bytes <= 0) {
      return '0.0 GB';
    }
    final gb = bytes / (1024 * 1024 * 1024);
    return '${gb.toStringAsFixed(2)} GB';
  }

  String _formatTotalSizeWithTotal(int bytes, int totalBytes) {
    final current = _formatTotalSize(bytes);
    if (totalBytes <= 0) {
      return current;
    }
    final total = _formatTotalSize(totalBytes);
    return '$current/$total';
  }

  String _formatEta(int? seconds) {
    if (seconds == null || seconds <= 0) {
      return 'n/a';
    }
    final minutes = seconds ~/ 60;
    final secs = seconds % 60;
    if (minutes <= 0) {
      return '${secs}s';
    }
    final hours = minutes ~/ 60;
    final mins = minutes % 60;
    if (hours <= 0) {
      return '${minutes}m ${secs}s';
    }
    return '${hours}h ${mins}m';
  }

  void _applyBackupJobStatus(AgentJobStatus status) {
    if (!mounted) {
      return;
    }
    setState(() {
      _isBackupRunning = status.state == AgentJobState.running;
      _backupStatusMessage = status.message;
      _backupCompletedDisks = status.completedUnits;
      _backupTotalDisks = status.totalUnits;
      _backupBytesTransferred = status.bytesTransferred;
      _backupAverageSpeedBytesPerSec = status.averageSpeedBytesPerSec;
      _backupPhysicalSpeedBytesPerSec = status.physicalSpeedBytesPerSec;
      _backupPhysicalBytesTransferred = status.physicalBytesTransferred;
      _backupEtaSeconds = status.etaSeconds;
      _backupPhysicalRemainingBytes = status.physicalRemainingBytes;
      _backupPhysicalTotalBytes = status.physicalTotalBytes;
      _backupPhysicalProgressPercent = status.physicalProgressPercent;
      _backupTotalBytes = status.totalBytes;
      _backupSanityBytesTransferred = status.sanityBytesTransferred;
      _backupSanitySpeedBytesPerSec = status.sanitySpeedBytesPerSec;
    });
  }

  void _startBackupJobPolling(String jobId) {
    _backupJobTimer?.cancel();
    _backupJobId = jobId;
    var consecutiveErrors = 0;
    _applyBackupJobStatus(
      AgentJobStatus(
        id: jobId,
        type: AgentJobType.backup,
        state: AgentJobState.running,
        message: 'Preparing backup...',
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
      ),
    );
    _backupJobTimer = Timer.periodic(const Duration(seconds: 1), (_) async {
      final currentJobId = _backupJobId;
      if (currentJobId == null) {
        return;
      }
      try {
        final status = await _agentApiClient.fetchJob(currentJobId);
        if (consecutiveErrors > 0) {
          _logInfo('Backup polling recovered after $consecutiveErrors error(s).');
          consecutiveErrors = 0;
        }
        _applyBackupJobStatus(status);
        if (status.state != AgentJobState.running) {
          _stopBackupJobPolling();
          if (mounted) {
            final message = status.message.isEmpty ? (status.state == AgentJobState.success ? 'Backup completed.' : 'Backup failed.') : status.message;
            if (status.state == AgentJobState.success) {
              _showSnackBarInfo(message);
            } else {
              _showSnackBarError(message);
            }
          }
        }
      } catch (error) {
        consecutiveErrors += 1;
        _logError('Backup polling error (attempt $consecutiveErrors)', error, StackTrace.current);
        if (consecutiveErrors >= 5) {
          _stopBackupJobPolling();
          if (mounted) {
            _showSnackBarError('Backup polling failed: $error');
          }
        }
      }
    });
  }

  void _stopBackupJobPolling() {
    _backupJobTimer?.cancel();
    _backupJobTimer = null;
    _backupJobId = null;
    if (mounted) {
      setState(() {
        _isBackupRunning = false;
      });
    }
  }

  Future<void> _cancelBackupJob() async {
    final jobId = _backupJobId;
    if (jobId == null) {
      return;
    }
    try {
      await _agentApiClient.cancelJob(jobId);
      _showSnackBarInfo('Backup cancel requested.');
    } catch (error) {
      _showSnackBarError('Backup cancel failed: $error');
    }
  }

  void _applyRestoreJobStatus(AgentJobStatus status) {
    if (!mounted) {
      return;
    }
    setState(() {
      _isRestoring = status.state == AgentJobState.running;
      _restoreStatusMessage = status.message;
      _restoreTotalBytes = status.totalUnits;
      _restoreBytesTransferred = status.bytesTransferred;
      _restoreSpeedBytesPerSec = status.speedBytesPerSec;
    });
  }

  void _applySanityJobStatus(AgentJobStatus status) {
    if (!mounted) {
      return;
    }
    setState(() {
      _isSanityChecking = status.state == AgentJobState.running;
      _sanityStatusMessage = status.message;
      _sanityTotalBytes = status.totalBytes;
      _sanityBytesTransferred = status.bytesTransferred;
      _sanitySpeedBytesPerSec = status.speedBytesPerSec;
    });
  }

  void _startRestoreJobPolling(String jobId) {
    _restoreUiTimer?.cancel();
    _restoreJobId = jobId;
    var consecutiveErrors = 0;
    _applyRestoreJobStatus(
      AgentJobStatus(
        id: jobId,
        type: AgentJobType.restore,
        state: AgentJobState.running,
        message: 'Preparing restore...',
        totalUnits: 0,
        completedUnits: 0,
        bytesTransferred: 0,
        speedBytesPerSec: 0,
        physicalBytesTransferred: 0,
        physicalSpeedBytesPerSec: 0,
        totalBytes: 0,
        sanityBytesTransferred: 0,
        sanitySpeedBytesPerSec: 0,
      ),
    );
    _restoreUiTimer = Timer.periodic(const Duration(seconds: 1), (_) async {
      try {
        final status = await _agentApiClient.fetchJob(jobId);
        if (consecutiveErrors > 0) {
          _logInfo('Restore polling recovered after $consecutiveErrors error(s).');
          consecutiveErrors = 0;
        }
        _applyRestoreJobStatus(status);
        if (status.state != AgentJobState.running) {
          _stopRestoreJobPolling();
          if (mounted) {
            final message = status.message.isEmpty ? (status.state == AgentJobState.success ? 'Restore completed.' : 'Restore failed.') : status.message;
            if (status.state == AgentJobState.success) {
              _showSnackBarInfo(message);
            } else {
              _showSnackBarError(message);
            }
          }
        }
      } catch (error) {
        consecutiveErrors += 1;
        _logError('Restore polling error (attempt $consecutiveErrors)', error, StackTrace.current);
        if (consecutiveErrors >= 5) {
          _stopRestoreJobPolling();
          if (mounted) {
            _showSnackBarError('Restore polling failed: $error');
          }
        }
      }
    });
  }

  void _startSanityJobPolling(String jobId) {
    _sanityJobTimer?.cancel();
    _sanityJobId = jobId;
    var consecutiveErrors = 0;
    _applySanityJobStatus(
      AgentJobStatus(
        id: jobId,
        type: AgentJobType.sanity,
        state: AgentJobState.running,
        message: 'Sanity check...',
        totalUnits: 0,
        completedUnits: 0,
        bytesTransferred: 0,
        speedBytesPerSec: 0,
        physicalBytesTransferred: 0,
        physicalSpeedBytesPerSec: 0,
        totalBytes: 0,
        sanityBytesTransferred: 0,
        sanitySpeedBytesPerSec: 0,
      ),
    );
    _sanityJobTimer = Timer.periodic(const Duration(seconds: 1), (_) async {
      try {
        final status = await _agentApiClient.fetchJob(jobId);
        if (consecutiveErrors > 0) {
          _logInfo('Sanity polling recovered after $consecutiveErrors error(s).');
          consecutiveErrors = 0;
        }
        _applySanityJobStatus(status);
        if (status.state != AgentJobState.running) {
          _stopSanityJobPolling();
          if (mounted) {
            final message = status.message.isEmpty ? (status.state == AgentJobState.success ? 'Sanity check completed.' : 'Sanity check failed.') : status.message;
            if (status.state == AgentJobState.success || status.state == AgentJobState.canceled) {
              _showSnackBarInfo(message);
            } else {
              _showSnackBarError(message);
            }
          }
        }
      } catch (error) {
        consecutiveErrors += 1;
        _logError('Sanity polling error (attempt $consecutiveErrors)', error, StackTrace.current);
        if (consecutiveErrors >= 5) {
          _stopSanityJobPolling();
          if (mounted) {
            _showSnackBarError('Sanity polling failed: $error');
          }
        }
      }
    });
  }

  void _stopRestoreJobPolling() {
    _restoreUiTimer?.cancel();
    _restoreUiTimer = null;
    _restoreJobId = null;
    if (mounted) {
      setState(() {
        _isRestoring = false;
        _restoreStatusMessage = '';
      });
    }
  }

  void _stopSanityJobPolling() {
    _sanityJobTimer?.cancel();
    _sanityJobTimer = null;
    _sanityJobId = null;
    if (mounted) {
      setState(() {
        _isSanityChecking = false;
        _sanityStatusMessage = '';
        _sanityBytesTransferred = 0;
        _sanitySpeedBytesPerSec = 0;
        _sanityTotalBytes = 0;
      });
    }
  }

  Future<void> _cancelSanityJob() async {
    final jobId = _sanityJobId;
    if (jobId == null) {
      return;
    }
    try {
      await _agentApiClient.cancelJob(jobId);
      _showSnackBarInfo('Sanity check cancel requested.');
    } catch (error) {
      _showSnackBarError('Sanity check cancel failed: $error');
    }
  }

  Future<void> _cancelRestoreJob() async {
    final jobId = _restoreJobId;
    if (jobId == null) {
      return;
    }
    try {
      await _agentApiClient.cancelJob(jobId);
      _showSnackBarInfo('Restore cancel requested.');
    } catch (error) {
      _showSnackBarError('Restore cancel failed: $error');
    }
  }

  void _attachFieldListeners() {
    _serverNameController.addListener(_handleFieldChanged);
    _sshHostController.addListener(_handleFieldChanged);
    _sshPortController.addListener(_handleFieldChanged);
    _sshUserController.addListener(_handleFieldChanged);
    _apiBaseUrlController.addListener(_handleFieldChanged);
    _backupPathController.addListener(_handleFieldChanged);
    _ntfymeTokenController.addListener(_handleFieldChanged);
  }

  void _handleFieldChanged() {
    if (mounted) {
      setState(() {});
    }
  }

  bool _isSelectedServerVerified() => _connectionVerified;

  Future<void> _configureGuiLogWriter({String? backupPath, bool rotateOnStartup = false}) async {
    final prefs = await SharedPreferences.getInstance();
    final configuredLevel = (prefs.getString(_guiLogLevelPrefKey) ?? '').trim();
    final level = configuredLevel.isEmpty ? 'info' : configuredLevel;
    final path = LogWriter.defaultPathForSource('gui', basePath: backupPath);
    await LogWriter.configureSourcePath(source: 'gui', path: path);
    LogWriter.configureSourceLevel(source: 'gui', level: level);
    if (rotateOnStartup && !_guiLogRotated) {
      await LogWriter.rotateSource('gui');
      _guiLogRotated = true;
    }
  }

  void _logInfo(String message) {
    LogWriter.logGuiSync(level: 'info', message: message);
  }

  void _logError(String message, Object error, StackTrace stackTrace) {
    LogWriter.logGuiSync(level: 'info', message: '$message $error');
    LogWriter.logGuiSync(level: 'debug', message: stackTrace.toString());
  }

  String _buildLibvirtHost() {
    final host = _sshHostController.text.trim();
    final user = _sshUserController.text.trim();
    if (host.isEmpty || user.isEmpty) {
      return '';
    }
    final port = int.tryParse(_sshPortController.text.trim()) ?? 22;
    final portSuffix = port == 22 ? '' : '?port=$port';
    return 'qemu+ssh://$user@$host/system$portSuffix';
  }

  void _updateUi(VoidCallback updates) {
    if (!mounted) {
      return;
    }
    setState(updates);
  }

  bool _requiresVerifiedIndex(int index) => index == 1 || index == 3;
  bool _requiresVmInventory(int index) => index == 1 || index == 2;

  void _applyServerToForm(ServerConfig server) {
    _serverNameController.text = server.name;
    _sshHostController.text = server.sshHost;
    _sshPortController.text = server.sshPort;
    _sshUserController.text = server.sshUser;
    _sshPasswordController.text = server.sshPassword;
    _apiBaseUrlController.text = '';
    _apiTokenController.text = '';
  }

  void _resetServerForm() {
    _serverNameController.text = '';
    _sshHostController.text = '';
    _sshPortController.text = '22';
    _sshUserController.text = '';
    _sshPasswordController.text = '';
    _apiBaseUrlController.text = '';
    _apiTokenController.text = '';
  }

  Future<void> _createNewServer() async {
    final newId = DateTime.now().millisecondsSinceEpoch.toString();
    final newServer = ServerConfig(id: newId, name: 'New server', connectionType: ConnectionType.ssh, sshHost: '', sshPort: '22', sshUser: '', sshPassword: '', apiBaseUrl: '', apiToken: '');
    setState(() {
      _servers.add(newServer);
      _editingServerId = newId;
      _resetServerForm();
    });
  }

  Future<void> _selectServer(ServerConfig server) async {
    setState(() {
      _editingServerId = server.id;
      _applyServerToForm(server);
    });
    if (_isPersistedServer(server.id)) {
      await _persistServers(selectedId: server.id);
    }
    _vmHasOverlayByName
      ..clear()
      ..addAll(_overlayByServerId[server.id] ?? {});
    if (_requiresVmInventory(_selectedMenuIndex)) {
      await _loadVmInventory(server);
    }
    if (_selectedMenuIndex == 3) {
      await _loadRestoreEntries();
    }
  }

  Future<void> _selectRestoreServerId(String? serverId) async {
    _updateUi(() {
      _restoreServerId = serverId;
    });
    if (_selectedMenuIndex == 3) {
      await _loadRestoreEntries();
    }
  }

  Future<void> _deleteServer(ServerConfig server) async {
    _servers.removeWhere((item) => item.id == server.id);
    if (_editingServerId == server.id) {
      if (_servers.isNotEmpty) {
        _editingServerId = _servers.first.id;
        _applyServerToForm(_servers.first);
      } else {
        _editingServerId = null;
        _resetServerForm();
      }
    }
    if (_restoreServerId == server.id) {
      _restoreServerId = _editingServerId;
    }
    await _persistServers(selectedId: _editingServerId);
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _confirmDeleteServer(ServerConfig server) async {
    final shouldDelete = await showDialog<bool>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text('Delete server'),
          content: Text('Remove "${server.name}" from the list?'),
          actions: [
            TextButton(onPressed: () => Navigator.of(dialogContext).pop(false), child: const Text('Cancel')),
            FilledButton(onPressed: () => Navigator.of(dialogContext).pop(true), child: const Text('Delete')),
          ],
        );
      },
    );
    if (!mounted) {
      return;
    }
    if (shouldDelete == true) {
      await _deleteServer(server);
    }
  }

  ServerConfig _buildServerFromForm({String? id}) {
    return ServerConfig(
      id: id ?? DateTime.now().millisecondsSinceEpoch.toString(),
      name: _serverNameController.text.trim(),
      connectionType: ConnectionType.ssh,
      sshHost: _sshHostController.text.trim(),
      sshPort: _sshPortController.text.trim(),
      sshUser: _sshUserController.text.trim(),
      sshPassword: _sshPasswordController.text,
      apiBaseUrl: '',
      apiToken: '',
    );
  }

  bool _isSameServer(ServerConfig a, ServerConfig b) {
    return a.name == b.name &&
        a.connectionType == b.connectionType &&
        a.sshHost == b.sshHost &&
        a.sshPort == b.sshPort &&
        a.sshUser == b.sshUser &&
        a.sshPassword == b.sshPassword &&
        a.apiBaseUrl == b.apiBaseUrl &&
        a.apiToken == b.apiToken;
  }

  ServerConfig? _findEditingServer() {
    if (_editingServerId == null) {
      return null;
    }
    try {
      return _servers.firstWhere((server) => server.id == _editingServerId);
    } catch (_) {
      return null;
    }
  }

  ServerConfig? _getSelectedServer() {
    if (_editingServerId == null) {
      return null;
    }
    try {
      return _servers.firstWhere((server) => server.id == _editingServerId);
    } catch (_) {
      return null;
    }
  }

  bool _hasConnectionData() {
    return _serverNameController.text.trim().isNotEmpty || _sshHostController.text.trim().isNotEmpty || _sshUserController.text.trim().isNotEmpty || _sshPasswordController.text.isNotEmpty;
  }

  bool _connectionHasChanges() {
    final existing = _findEditingServer();
    if (existing == null) {
      return _hasConnectionData();
    }
    final current = _buildServerFromForm(id: existing.id);
    return !_isSameServer(current, existing);
  }

  bool _localHasChanges() {
    return _backupPathController.text.trim() != _savedBackupPath || _ntfymeTokenController.text.trim() != _savedNtfymeToken;
  }

  bool _hasAnyChanges() {
    return _connectionHasChanges() || _localHasChanges();
  }

  Future<void> _testConnection() async {
    var isValid = false;
    try {
      _allowEmptyServerNameForTest = true;
      isValid = _connectionFormKey.currentState!.validate();
    } finally {
      _allowEmptyServerNameForTest = false;
    }
    if (!isValid) {
      return;
    }
    setState(() {
      _isTesting = true;
    });
    _logInfo('Starting connection test (ssh).');
    try {
      await _saveConnectionAgentSettings(showSnackBar: false);
      await _testSshConnection();
      _logInfo('Connection test successful.');
      await _setConnectionVerified(true);
      if (mounted) {
        _showSnackBarInfo('Connection test successful. Settings saved.');
      }
    } catch (error, stackTrace) {
      _logError('Connection test failed.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Connection test failed: $error');
      }
    } finally {
      if (mounted) {
        setState(() {
          _isTesting = false;
        });
      }
    }
  }

  Future<void> _saveConnectionAgentSettings({bool showSnackBar = true}) async {
    _ensureServerNameFilled();
    if (!_connectionFormKey.currentState!.validate()) {
      return;
    }
    try {
      final config = _buildServerFromForm(id: _editingServerId);
      final existingIndex = _servers.indexWhere((item) => item.id == config.id);
      if (existingIndex >= 0) {
        _servers[existingIndex] = config;
      } else {
        _servers.add(config);
      }
      _editingServerId = config.id;
      await _persistServers(selectedId: config.id);
      if (mounted) {
        if (showSnackBar) {
          _showSnackBarInfo('Connection settings saved locally');
        }
        setState(() {});
      }
    } finally {}
  }

  Future<void> _saveLocalAgentSettings({bool showSnackBar = true}) async {
    if (!_localFormKey.currentState!.validate()) {
      return;
    }
    try {
      final trimmedPath = _backupPathController.text.trim();
      final trimmedToken = _ntfymeTokenController.text.trim();
      final updatedDestinations = List<BackupDestination>.from(_agentSettings.destinations);
      final filesystemIndex = updatedDestinations.indexWhere((destination) => destination.id == AppSettings.filesystemDestinationId);
      if (filesystemIndex >= 0) {
        final current = updatedDestinations[filesystemIndex];
        updatedDestinations[filesystemIndex] = BackupDestination(
          id: AppSettings.filesystemDestinationId,
          name: AppSettings.filesystemDestinationName,
          driverId: 'filesystem',
          enabled: true,
          disableFresh: current.disableFresh,
          params: <String, dynamic>{'path': trimmedPath},
        );
        if (current.params['path']?.toString().trim() != trimmedPath) {
          _selectedBackupDestinationId ??= AppSettings.filesystemDestinationId;
        }
      }
      _agentSettings = _agentSettings.copyWith(backupPath: trimmedPath, ntfymeToken: trimmedToken, backupDestinationId: _selectedBackupDestinationId, destinations: updatedDestinations);
      await _pushAgentSettings();
      _savedBackupPath = trimmedPath;
      unawaited(_configureGuiLogWriter(backupPath: trimmedPath));
      _savedNtfymeToken = trimmedToken;
      if (mounted && showSnackBar) {
        _showSnackBarInfo('Local settings saved');
      }
    } finally {}
  }

  Future<void> _sendNtfymeTestMessage() async {
    final token = _ntfymeTokenController.text.trim();
    if (token.isEmpty) {
      _showSnackBarError('Enter an Ntfy me token first.');
      return;
    }
    if (!_agentReachable || _agentAuthFailed || _agentTokenMissing) {
      _showSnackBarError('Agent is not reachable.');
      return;
    }
    setState(() {
      _isSendingNtfymeTest = true;
    });
    try {
      final result = await _agentApiClient.sendNtfymeTest(token: token);
      if (result.success) {
        _showSnackBarInfo(result.message);
      } else {
        _showSnackBarError('Ntfy me test failed: ${result.message}');
      }
    } catch (error) {
      _showSnackBarError('Ntfy me test failed: $error');
    } finally {
      if (mounted) {
        setState(() {
          _isSendingNtfymeTest = false;
        });
      }
    }
  }

  Future<void> _openNtfymeDocs() async {
    final uri = Uri.parse('https://ntfyme.net/');
    try {
      final launched = await launchUrl(uri, mode: LaunchMode.externalApplication);
      if (!launched && mounted) {
        _showSnackBarError('Unable to open Ntfy me docs.');
      }
    } on PlatformException {
      if (mounted) {
        _showSnackBarError('Unable to open Ntfy me docs.');
      }
    }
  }

  bool _isDestinationGdriveConnected(String refreshToken) {
    return refreshToken.trim().isNotEmpty;
  }

  Future<void> _startGdriveDestinationOAuth({
    required String scope,
    required String existingRefreshToken,
    required void Function({required String accessToken, required String refreshToken, required String accountEmail, required int? expiresAtMs}) onConnected,
    bool forceConsent = false,
  }) async {
    if (_isGdriveConnecting) {
      return;
    }
    GoogleOAuthInstalledClient oauth;
    try {
      oauth = await _resolveGdriveOAuthClient();
    } catch (error) {
      _showSnackBarError('Google Drive OAuth client config is missing or invalid: $error');
      return;
    }
    final clientId = oauth.clientId.trim();
    final clientSecret = oauth.clientSecret.trim();
    if (clientId.isEmpty) {
      _showSnackBarError('Google Drive client ID is not configured.');
      return;
    }
    final trimmedScope = scope.trim();
    if (trimmedScope.isEmpty) {
      _showSnackBarError('Google Drive scope is required.');
      return;
    }
    _logInfo('Google Drive OAuth client_id: $clientId');
    _updateUi(() {
      _isGdriveConnecting = true;
    });

    HttpServer? server;
    try {
      server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final redirectUri = 'http://127.0.0.1:${server.port}/oauth/callback';
      final codeVerifier = _generateOAuthVerifier();
      final codeChallenge = _generateOAuthChallenge(codeVerifier);
      final state = _generateOAuthState();
      final shouldPrompt = forceConsent || existingRefreshToken.trim().isEmpty;
      final query = <String, String>{
        'client_id': clientId,
        'redirect_uri': redirectUri,
        'response_type': 'code',
        'scope': _buildGdriveOAuthScope(trimmedScope),
        'access_type': 'offline',
        'include_granted_scopes': 'true',
        'state': state,
        'code_challenge': codeChallenge,
        'code_challenge_method': 'S256',
      };
      if (shouldPrompt) {
        query['prompt'] = 'consent';
      }
      final authUri = Uri.https('accounts.google.com', '/o/oauth2/v2/auth', query);
      final callback = _waitForOAuthCallback(server);
      final launched = await launchUrl(authUri, mode: LaunchMode.externalApplication);
      if (!launched) {
        throw 'Unable to open the browser for Google Drive sign-in.';
      }
      final result = await callback.timeout(
        const Duration(minutes: 5),
        onTimeout: () {
          throw 'Google Drive sign-in timed out.';
        },
      );
      if (result.error != null && result.error!.isNotEmpty) {
        throw 'Google Drive sign-in failed: ${result.error}';
      }
      if (result.state != state) {
        throw 'Google Drive sign-in state mismatch.';
      }
      final code = result.code;
      if (code == null || code.isEmpty) {
        throw 'Google Drive sign-in did not return a code.';
      }
      final tokenResponse = await http.post(
        Uri.parse('https://oauth2.googleapis.com/token'),
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        body: {
          'client_id': clientId,
          if (clientSecret.isNotEmpty) 'client_secret': clientSecret,
          'code': code,
          'code_verifier': codeVerifier,
          'redirect_uri': redirectUri,
          'grant_type': 'authorization_code',
        },
      );
      if (tokenResponse.statusCode != 200) {
        throw 'Token exchange failed: ${tokenResponse.body}';
      }
      final decoded = jsonDecode(tokenResponse.body);
      if (decoded is! Map) {
        throw 'Token exchange failed: invalid response.';
      }
      final accessToken = decoded['access_token']?.toString() ?? '';
      final returnedRefreshToken = decoded['refresh_token']?.toString() ?? '';
      final refreshToken = returnedRefreshToken.isNotEmpty ? returnedRefreshToken : existingRefreshToken.trim();
      if (refreshToken.isEmpty) {
        throw 'No refresh token received. Disconnect and connect again.';
      }
      final expiresAtMs = _calculateExpiresAt(decoded['expires_in'])?.toUtc().millisecondsSinceEpoch;
      var accountEmail = '';
      if (accessToken.isNotEmpty) {
        try {
          final userinfo = await http.get(Uri.parse('https://openidconnect.googleapis.com/v1/userinfo'), headers: {'Authorization': 'Bearer $accessToken'});
          if (userinfo.statusCode == 200) {
            final userJson = jsonDecode(userinfo.body);
            if (userJson is Map) {
              accountEmail = userJson['email']?.toString() ?? '';
            }
          }
        } catch (_) {}
      }
      onConnected(accessToken: accessToken, refreshToken: refreshToken, accountEmail: accountEmail, expiresAtMs: expiresAtMs);
      if (mounted) {
        _showSnackBarInfo('Google Drive connected.');
      }
    } catch (error, stackTrace) {
      _logError('Google Drive OAuth failed.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Google Drive sign-in failed: $error');
      }
    } finally {
      try {
        await server?.close(force: true);
      } catch (_) {}
      _updateUi(() {
        _isGdriveConnecting = false;
      });
    }
  }

  String _buildGdriveOAuthScope(String scope) {
    return '$scope openid email';
  }

  Future<_OAuthCallbackResult> _waitForOAuthCallback(HttpServer server) {
    final completer = Completer<_OAuthCallbackResult>();
    server.listen((request) async {
      if (request.uri.path != '/oauth/callback') {
        request.response.statusCode = 404;
        await request.response.close();
        return;
      }
      final params = request.uri.queryParameters;
      final code = params['code'];
      final state = params['state'];
      final error = params['error'];
      request.response.statusCode = 200;
      request.response.headers.contentType = ContentType.html;
      request.response.write('<html><body><h3>You can close this window and return to VirtBackup.</h3></body></html>');
      await request.response.close();
      if (!completer.isCompleted) {
        completer.complete(_OAuthCallbackResult(code: code, state: state, error: error));
      }
      await server.close(force: true);
    });
    return completer.future;
  }

  String _generateOAuthVerifier() {
    final bytes = _randomBytes(32);
    return base64Url.encode(bytes).replaceAll('=', '');
  }

  String _generateOAuthChallenge(String verifier) {
    final digest = sha256.convert(utf8.encode(verifier)).bytes;
    return base64Url.encode(digest).replaceAll('=', '');
  }

  String _generateOAuthState() {
    final bytes = _randomBytes(16);
    return base64Url.encode(bytes).replaceAll('=', '');
  }

  Uint8List _randomBytes(int length) {
    final random = Random.secure();
    final bytes = Uint8List(length);
    for (var i = 0; i < length; i++) {
      bytes[i] = random.nextInt(256);
    }
    return bytes;
  }

  DateTime? _calculateExpiresAt(Object? expiresIn) {
    if (expiresIn is num) {
      return DateTime.now().toUtc().add(Duration(seconds: expiresIn.toInt()));
    }
    final parsed = int.tryParse(expiresIn?.toString() ?? '');
    if (parsed == null || parsed <= 0) {
      return null;
    }
    return DateTime.now().toUtc().add(Duration(seconds: parsed));
  }

  String _formatGdriveExpiryFromEpochMs(int? expiresAtMs) {
    if (expiresAtMs == null || expiresAtMs <= 0) {
      return 'Unknown';
    }
    final instant = DateTime.fromMillisecondsSinceEpoch(expiresAtMs, isUtc: true).toLocal();
    final paddedMonth = instant.month.toString().padLeft(2, '0');
    final paddedDay = instant.day.toString().padLeft(2, '0');
    final paddedHour = instant.hour.toString().padLeft(2, '0');
    final paddedMinute = instant.minute.toString().padLeft(2, '0');
    return '${instant.year}-$paddedMonth-$paddedDay $paddedHour:$paddedMinute';
  }

  Future<void> _saveAllAgentSettings() async {
    if (!_hasAnyChanges()) {
      return;
    }
    setState(() {
      _isSavingAll = true;
    });
    try {
      final needsConnectionSave = _connectionHasChanges();
      final needsLocalSave = _localHasChanges();
      var savedSomething = false;

      if (needsConnectionSave) {
        _ensureServerNameFilled();
        if (!_connectionFormKey.currentState!.validate()) {
          return;
        }
        await _saveConnectionAgentSettings(showSnackBar: false);
        savedSomething = true;
      }

      if (needsLocalSave) {
        if (!_localFormKey.currentState!.validate()) {
          return;
        }
        await _saveLocalAgentSettings(showSnackBar: false);
        savedSomething = true;
      }

      if (mounted && savedSomething) {
        _showSnackBarInfo('Settings saved');
      }
    } finally {
      if (mounted) {
        setState(() {
          _isSavingAll = false;
        });
      }
    }
  }

  String _generateDestinationId(String driverId) {
    final normalized = driverId.trim().isEmpty ? 'driver' : driverId.trim();
    final stamp = DateTime.now().microsecondsSinceEpoch;
    return 'dest_${normalized}_$stamp';
  }

  Future<void> _openDestinationEditor() async {
    var working = List<BackupDestination>.from(_agentSettings.destinations);
    await showDialog<void>(
      context: context,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setStateDialog) {
            Future<void> editDestination({BackupDestination? existing}) async {
              final nameController = TextEditingController(text: existing?.name ?? '');
              var driverId = existing?.driverId ?? (_backupDrivers.isNotEmpty ? _backupDrivers.first.id : 'filesystem');
              var enabled = existing?.enabled ?? true;
              final params = Map<String, dynamic>.from(existing?.params ?? const <String, dynamic>{});
              final pathController = TextEditingController(text: params['path']?.toString() ?? '');
              final sftpHostController = TextEditingController(text: params['host']?.toString() ?? '');
              final sftpPortController = TextEditingController(text: (params['port'] ?? 22).toString());
              final sftpUserController = TextEditingController(text: params['username']?.toString() ?? '');
              final sftpPasswordController = TextEditingController(text: params['password']?.toString() ?? '');
              final sftpBasePathController = TextEditingController(text: params['basePath']?.toString() ?? '');
              final gdriveRootController = TextEditingController(text: params['rootPath']?.toString() ?? '/');
              final gdriveScopeController = TextEditingController(text: params['scope']?.toString() ?? _gdriveScopeFile);
              final gdriveAccessController = TextEditingController(text: params['accessToken']?.toString() ?? '');
              final gdriveRefreshController = TextEditingController(text: params['refreshToken']?.toString() ?? '');
              final gdriveEmailController = TextEditingController(text: params['accountEmail']?.toString() ?? '');
              var gdriveExpiresAtMs = params['expiresAt'] is num ? (params['expiresAt'] as num).toInt() : int.tryParse((params['expiresAt'] ?? '').toString());
              var gdriveDisconnected = false;
              final formKey = GlobalKey<FormState>();
              final result = await showDialog<BackupDestination>(
                context: context,
                builder: (context) {
                  return StatefulBuilder(
                    builder: (context, setStateDestination) {
                      return AlertDialog(
                        title: Text(existing == null ? 'New destination' : 'Edit destination'),
                        content: SizedBox(
                          width: 520,
                          child: Form(
                            key: formKey,
                            child: SingleChildScrollView(
                              child: Column(
                                mainAxisSize: MainAxisSize.min,
                                children: [
                                  TextFormField(
                                    controller: nameController,
                                    decoration: const InputDecoration(labelText: 'Name'),
                                    validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a name' : null,
                                  ),
                                  const SizedBox(height: 12),
                                  Builder(
                                    builder: (context) {
                                      final items = <DropdownMenuItem<String>>[];
                                      final seenDriverIds = <String>{};
                                      for (final driver in _backupDrivers) {
                                        final id = driver.id.trim();
                                        if (id.isEmpty || seenDriverIds.contains(id)) {
                                          continue;
                                        }
                                        seenDriverIds.add(id);
                                        items.add(DropdownMenuItem<String>(value: id, child: Text(driver.label)));
                                      }
                                      final normalizedDriverId = driverId.trim();
                                      if (normalizedDriverId.isNotEmpty && !seenDriverIds.contains(normalizedDriverId)) {
                                        items.add(DropdownMenuItem<String>(value: normalizedDriverId, child: Text(normalizedDriverId)));
                                      }
                                      return DropdownButtonFormField<String>(
                                        initialValue: normalizedDriverId.isEmpty ? null : normalizedDriverId,
                                        items: items,
                                        onChanged: existing != null
                                            ? null
                                            : (value) {
                                                if (value == null || value.isEmpty) {
                                                  return;
                                                }
                                                setStateDestination(() {
                                                  driverId = value;
                                                });
                                              },
                                        decoration: const InputDecoration(labelText: 'Driver'),
                                      );
                                    },
                                  ),
                                  const SizedBox(height: 12),
                                  SwitchListTile(value: enabled, onChanged: (value) => setStateDestination(() => enabled = value), title: const Text('Enabled')),
                                  if (driverId == 'filesystem') ...[
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: pathController,
                                      decoration: const InputDecoration(labelText: 'Path'),
                                      validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a path' : null,
                                    ),
                                  ],
                                  if (driverId == 'sftp') ...[
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: sftpHostController,
                                      decoration: const InputDecoration(labelText: 'Host'),
                                      validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a host' : null,
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: sftpPortController,
                                      decoration: const InputDecoration(labelText: 'Port'),
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: sftpUserController,
                                      decoration: const InputDecoration(labelText: 'Username'),
                                      validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a username' : null,
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: sftpPasswordController,
                                      decoration: const InputDecoration(labelText: 'Password'),
                                      obscureText: true,
                                      validator: (value) => (value == null || value.isEmpty) ? 'Enter a password' : null,
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: sftpBasePathController,
                                      decoration: const InputDecoration(labelText: 'Base path'),
                                      validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a base path' : null,
                                    ),
                                  ],
                                  if (driverId == 'gdrive') ...[
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: gdriveRootController,
                                      decoration: const InputDecoration(labelText: 'Root path'),
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: gdriveScopeController,
                                      decoration: const InputDecoration(labelText: 'Scope'),
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: gdriveAccessController,
                                      decoration: const InputDecoration(labelText: 'Access token'),
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: gdriveRefreshController,
                                      decoration: const InputDecoration(labelText: 'Refresh token'),
                                    ),
                                    const SizedBox(height: 12),
                                    TextFormField(
                                      controller: gdriveEmailController,
                                      decoration: const InputDecoration(labelText: 'Account email'),
                                    ),
                                    const SizedBox(height: 12),
                                    Row(
                                      children: [
                                        FilledButton.icon(
                                          onPressed: _isGdriveConnecting
                                              ? null
                                              : () async {
                                                  await _startGdriveDestinationOAuth(
                                                    scope: gdriveScopeController.text.trim(),
                                                    existingRefreshToken: gdriveRefreshController.text,
                                                    onConnected: ({required accessToken, required refreshToken, required accountEmail, required expiresAtMs}) {
                                                      gdriveAccessController.text = accessToken;
                                                      gdriveRefreshController.text = refreshToken;
                                                      gdriveEmailController.text = accountEmail;
                                                      gdriveExpiresAtMs = expiresAtMs;
                                                      gdriveDisconnected = false;
                                                      if (context.mounted) {
                                                        setStateDestination(() {});
                                                      }
                                                    },
                                                  );
                                                  if (context.mounted) {
                                                    setStateDestination(() {});
                                                  }
                                                },
                                          icon: const Icon(Icons.link_outlined),
                                          label: Text(_isGdriveConnecting ? 'Connecting...' : 'Connect in browser'),
                                        ),
                                        const SizedBox(width: 12),
                                        TextButton.icon(
                                          onPressed: _isDestinationGdriveConnected(gdriveRefreshController.text) && !_isGdriveConnecting
                                              ? () {
                                                  gdriveAccessController.clear();
                                                  gdriveRefreshController.clear();
                                                  gdriveEmailController.clear();
                                                  gdriveExpiresAtMs = null;
                                                  gdriveDisconnected = true;
                                                  setStateDestination(() {});
                                                }
                                              : null,
                                          icon: const Icon(Icons.link_off_outlined),
                                          label: const Text('Disconnect'),
                                        ),
                                      ],
                                    ),
                                    const SizedBox(height: 8),
                                    if (_isDestinationGdriveConnected(gdriveRefreshController.text)) ...[
                                      Text(
                                        'Connected account: ${gdriveEmailController.text.trim().isEmpty ? 'Unknown' : gdriveEmailController.text.trim()}',
                                        style: Theme.of(context).textTheme.bodyMedium,
                                      ),
                                      const SizedBox(height: 4),
                                      Text('Access token expires: ${_formatGdriveExpiryFromEpochMs(gdriveExpiresAtMs)}', style: Theme.of(context).textTheme.bodySmall),
                                    ] else ...[
                                      Text('Not connected. Backups will fail until you connect Google Drive.', style: Theme.of(context).textTheme.bodyMedium),
                                    ],
                                  ],
                                ],
                              ),
                            ),
                          ),
                        ),
                        actions: [
                          TextButton(onPressed: () => Navigator.of(context).pop(), child: const Text('Cancel')),
                          FilledButton(
                            onPressed: () {
                              if (!formKey.currentState!.validate()) {
                                return;
                              }
                              final destinationParams = <String, dynamic>{};
                              if (driverId == 'filesystem') {
                                destinationParams['path'] = pathController.text.trim();
                              } else if (driverId == 'sftp') {
                                destinationParams['host'] = sftpHostController.text.trim();
                                destinationParams['port'] = int.tryParse(sftpPortController.text.trim()) ?? 22;
                                destinationParams['username'] = sftpUserController.text.trim();
                                final sftpPassword = sftpPasswordController.text;
                                destinationParams['password'] = sftpPassword;
                                if (sftpPassword.isEmpty) {
                                  final encryptedPassword = params['passwordEnc']?.toString() ?? '';
                                  if (encryptedPassword.isNotEmpty) {
                                    destinationParams['passwordEnc'] = encryptedPassword;
                                  }
                                }
                                destinationParams['basePath'] = sftpBasePathController.text.trim();
                              } else if (driverId == 'gdrive') {
                                final accessToken = gdriveAccessController.text.trim();
                                final refreshToken = gdriveRefreshController.text.trim();
                                destinationParams['rootPath'] = gdriveRootController.text.trim();
                                destinationParams['scope'] = gdriveScopeController.text.trim();
                                destinationParams['accessToken'] = accessToken;
                                destinationParams['refreshToken'] = refreshToken;
                                destinationParams['accountEmail'] = gdriveEmailController.text.trim();
                                if (gdriveExpiresAtMs != null) {
                                  destinationParams['expiresAt'] = gdriveExpiresAtMs;
                                }
                                if (!gdriveDisconnected) {
                                  if (accessToken.isEmpty) {
                                    final encryptedAccessToken = params['accessTokenEnc']?.toString() ?? '';
                                    if (encryptedAccessToken.isNotEmpty) {
                                      destinationParams['accessTokenEnc'] = encryptedAccessToken;
                                    }
                                  }
                                  if (refreshToken.isEmpty) {
                                    final encryptedRefreshToken = params['refreshTokenEnc']?.toString() ?? '';
                                    if (encryptedRefreshToken.isNotEmpty) {
                                      destinationParams['refreshTokenEnc'] = encryptedRefreshToken;
                                    }
                                  }
                                }
                              }
                              Navigator.of(context).pop(
                                BackupDestination(
                                  id: existing?.id ?? _generateDestinationId(driverId),
                                  name: nameController.text.trim(),
                                  driverId: driverId,
                                  enabled: enabled,
                                  disableFresh: existing?.disableFresh ?? false,
                                  params: destinationParams,
                                ),
                              );
                            },
                            child: const Text('Save'),
                          ),
                        ],
                      );
                    },
                  );
                },
              );
              if (result == null) {
                return;
              }
              setStateDialog(() {
                final index = working.indexWhere((item) => item.id == result.id);
                if (index >= 0) {
                  working[index] = result;
                } else {
                  working.add(result);
                }
              });
            }

            return AlertDialog(
              title: const Text('Destinations'),
              content: SizedBox(
                width: 640,
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    if (working.isEmpty)
                      const Padding(padding: EdgeInsets.symmetric(vertical: 16), child: Text('No destinations configured.'))
                    else
                      Flexible(
                        child: ListView.separated(
                          shrinkWrap: true,
                          itemCount: working.length,
                          separatorBuilder: (_, _) => const Divider(height: 1),
                          itemBuilder: (context, index) {
                            final destination = working[index];
                            final isMandatoryFilesystem = destination.id == AppSettings.filesystemDestinationId;
                            return ListTile(
                              title: Text(destination.name),
                              subtitle: Text('${destination.driverId} · ${destination.enabled ? 'enabled' : 'disabled'}'),
                              trailing: Wrap(
                                spacing: 8,
                                children: [
                                  IconButton(
                                    onPressed: () => editDestination(existing: destination),
                                    icon: const Icon(Icons.edit_outlined),
                                  ),
                                  IconButton(
                                    onPressed: isMandatoryFilesystem
                                        ? null
                                        : () {
                                            setStateDialog(() {
                                              working.removeAt(index);
                                            });
                                          },
                                    icon: const Icon(Icons.delete_outline),
                                    tooltip: isMandatoryFilesystem ? 'Filesystem destination is required' : null,
                                  ),
                                ],
                              ),
                            );
                          },
                        ),
                      ),
                    const SizedBox(height: 12),
                    Align(
                      alignment: Alignment.centerLeft,
                      child: OutlinedButton.icon(onPressed: () => editDestination(), icon: const Icon(Icons.add), label: const Text('New destination')),
                    ),
                  ],
                ),
              ),
              actions: [
                TextButton(onPressed: () => Navigator.of(context).pop(), child: const Text('Cancel')),
                FilledButton(
                  onPressed: () async {
                    try {
                      final nextBackupId = _selectedBackupDestinationId != null && working.any((item) => item.id == _selectedBackupDestinationId)
                          ? _selectedBackupDestinationId
                          : (working.isEmpty ? null : working.first.id);
                      final updated = _agentSettings.copyWith(destinations: working, backupDestinationId: nextBackupId);
                      await _agentApiClient.updateConfig(updated);
                      if (!mounted) {
                        return;
                      }
                      setState(() {
                        _agentSettings = updated;
                        _selectedBackupDestinationId = nextBackupId;
                        _ensureBackupDestinationSelection();
                      });
                      _showSnackBarInfo('Destinations saved');
                      if (!context.mounted) {
                        return;
                      }
                      Navigator.of(context).pop();
                    } catch (error, stackTrace) {
                      _logError('Saving destinations failed.', error, stackTrace);
                      if (mounted) {
                        _showSnackBarError('Saving destinations failed: $error');
                      }
                    }
                  },
                  child: const Text('Save'),
                ),
              ],
            );
          },
        );
      },
    );
  }

  Future<void> _refreshSelectedServer() async {
    final server = _getSelectedServer();
    if (server == null) {
      _showSnackBarInfo('Select a server first.');
      return;
    }
    if (_isRefreshingServer) {
      return;
    }
    setState(() {
      _isRefreshingServer = true;
    });
    try {
      if (server.connectionType == ConnectionType.ssh) {
        await _agentApiClient.refreshServer(server.id);
      }
      await _loadVmInventory(server);
      _lastRefreshByServerId[server.id] = DateTime.now();
      if (mounted) {
        _showSnackBarInfo('Server refreshed.');
      }
    } catch (error, stackTrace) {
      _logError('Server refresh failed.', error, stackTrace);
      _showSnackBarError('Refresh failed: $error');
    } finally {
      if (mounted) {
        setState(() {
          _isRefreshingServer = false;
        });
      }
    }
  }

  Future<void> _loadVmInventory(ServerConfig server) async {
    _updateUi(() {});
    try {
      if (server.connectionType == ConnectionType.ssh) {
        final statuses = await _agentApiClient.fetchVmStatus(server.id);
        final vms = statuses.map((entry) => entry.vm).toList();
        final overlayStatusByName = <String, bool>{for (final entry in statuses) entry.vm.name: entry.hasOverlay};
        _vmCacheByServerId[server.id] = List<VmEntry>.from(vms);
        _overlayByServerId[server.id] = overlayStatusByName;
        _lastRefreshByServerId[server.id] ??= DateTime.now();
        if (server.id == _editingServerId) {
          _vmHasOverlayByName
            ..clear()
            ..addAll(overlayStatusByName);
        }
      } else {
        final vms = await _loadVmInventoryViaApi(server);
        _vmCacheByServerId[server.id] = List<VmEntry>.from(vms);
        _overlayByServerId[server.id] = {};
      }
      if (server.id == _editingServerId) {
        if (server.connectionType != ConnectionType.ssh) {
          _vmHasOverlayByName.clear();
        }
      }
      if (mounted) {
        setState(() {});
      }
    } catch (error, stackTrace) {
      _logError('VM inventory load failed.', error, stackTrace);
      if (server.connectionType == ConnectionType.ssh) {
        _notifyAgentErrorOnce('VM load failed: $error');
      } else {
        _showSnackBarError('VM load failed: $error');
      }
    } finally {
      if (mounted) {
        _updateUi(() {});
      }
    }
  }

  void _notifyAgentErrorOnce(String message) {
    if (!mounted) {
      return;
    }
    if (_agentErrorNotified) {
      return;
    }
    _agentErrorNotified = true;
    _showSnackBarError(message);
  }

  void _showSnackBar(String message, {Duration? duration}) {
    if (!mounted) {
      return;
    }
    _logInfo('snackbar: $message');
    final messenger = ScaffoldMessenger.of(context);
    messenger.clearSnackBars();
    messenger.showSnackBar(SnackBar(content: Text(message), duration: duration ?? const Duration(seconds: 4)));
  }

  void _showSnackBarInfo(String message, {Duration? duration}) {
    _showSnackBar(message, duration: duration ?? const Duration(seconds: 2));
  }

  void _showSnackBarError(String message, {Duration? duration}) {
    _showSnackBar(message, duration: duration ?? const Duration(seconds: 4));
  }

  Future<void> _runVmAction(ServerConfig server, VmEntry vm, VmAction action) async {
    if (_isVmActionRunning) {
      return;
    }
    setState(() {
      _isVmActionRunning = true;
    });
    try {
      if (server.connectionType == ConnectionType.ssh) {
        final ok = await _agentApiClient.runVmAction(server.id, action, vm.name);
        if (!ok) {
          throw 'Agent did not accept the VM action.';
        }
      } else {
        throw 'API VM actions not configured.';
      }
      if (mounted) {
        _showSnackBarInfo('VM action sent');
      }
    } catch (error, stackTrace) {
      _logError('VM action failed.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('VM action failed: $error');
      }
    } finally {
      if (mounted) {
        setState(() {
          _isVmActionRunning = false;
        });
      }
    }
  }

  VmPowerState _parseVmState(String state) {
    final normalized = state.toLowerCase();
    if (normalized.contains('running')) {
      return VmPowerState.running;
    }
    return VmPowerState.stopped;
  }

  Future<List<VmEntry>> _loadVmInventoryViaApi(ServerConfig server) async {
    final baseUrl = server.apiBaseUrl.trim();
    if (baseUrl.isEmpty) {
      throw 'API base URL is empty.';
    }
    final uri = Uri.parse(baseUrl);
    final headers = <String, String>{};
    if (server.apiToken.isNotEmpty) {
      headers['Authorization'] = 'Bearer ${server.apiToken}';
    }
    final response = await http.get(uri, headers: headers);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw 'HTTP ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      throw 'Unexpected API response.';
    }
    return decoded.map<VmEntry>((item) {
      if (item is String) {
        return VmEntry(id: item, name: item, powerState: VmPowerState.stopped);
      }
      if (item is Map) {
        final data = Map<String, dynamic>.from(item);
        final id = (data['id'] ?? data['name'] ?? '').toString();
        final name = (data['name'] ?? id).toString();
        final state = (data['state'] ?? data['powerState'] ?? data['status'] ?? '').toString();
        return VmEntry(id: id.isEmpty ? name : id, name: name.isEmpty ? id : name, powerState: _parseVmState(state));
      }
      return VmEntry(id: 'unknown', name: 'unknown', powerState: VmPowerState.stopped);
    }).toList();
  }

  Future<void> _pickBackupFolder() async {
    final selectedDirectory = await getDirectoryPath();
    if (!mounted) {
      return;
    }
    if (selectedDirectory == null) {
      _showSnackBarInfo('No folder selected.');
      return;
    }
    setState(() {
      _backupPathController.text = selectedDirectory;
    });
  }

  void _ensureServerNameFilled() {
    if (_serverNameController.text.trim().isNotEmpty) {
      return;
    }
    final fallbackName = _sshHostController.text.trim();
    _serverNameController.text = fallbackName.isEmpty ? 'Server' : fallbackName;
  }

  Widget _buildRailItem({
    required int index,
    required IconData icon,
    required IconData selectedIcon,
    required String label,
    required ColorScheme colorScheme,
    required NavigationRailThemeData railTheme,
    bool enabled = true,
  }) {
    final selected = _selectedMenuIndex == index;
    final useIndicator = railTheme.useIndicator ?? true;
    final indicatorColor = railTheme.indicatorColor ?? colorScheme.secondaryContainer;
    final indicatorShape = railTheme.indicatorShape ?? const StadiumBorder();
    final selectedIconTheme = railTheme.selectedIconTheme ?? IconThemeData(color: colorScheme.primary);
    final unselectedIconTheme = railTheme.unselectedIconTheme ?? IconThemeData(color: colorScheme.onSurfaceVariant);
    final selectedLabelStyle = railTheme.selectedLabelTextStyle ?? Theme.of(context).textTheme.labelMedium?.copyWith(color: selectedIconTheme.color);
    final unselectedLabelStyle = railTheme.unselectedLabelTextStyle ?? Theme.of(context).textTheme.labelMedium?.copyWith(color: unselectedIconTheme.color);

    final disabledColor = colorScheme.outline;

    return Material(
      color: Colors.transparent,
      child: InkWell(
        onTap: enabled ? () => _selectMenuIndex(index) : null,
        customBorder: indicatorShape,
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 12, horizontal: 8),
          decoration: selected && useIndicator ? ShapeDecoration(color: indicatorColor, shape: indicatorShape) : null,
          child: Column(
            children: [
              Icon(selected ? selectedIcon : icon, color: enabled ? (selected ? selectedIconTheme.color : unselectedIconTheme.color) : disabledColor),
              const SizedBox(height: 6),
              Text(label, style: enabled ? (selected ? selectedLabelStyle : unselectedLabelStyle) : unselectedLabelStyle?.copyWith(color: disabledColor)),
            ],
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;
    final railTheme = NavigationRailTheme.of(context);
    final railWidth = railTheme.minWidth ?? 72;
    final isSaveEnabled = _selectedMenuIndex == 0 && !_isSavingAll && _hasAnyChanges();
    final isSettingsEnabled = true;
    final isAgentReady = _agentReachable && !_agentAuthFailed && !_agentTokenMissing;
    return Scaffold(
      floatingActionButton: _selectedMenuIndex == 0
          ? FloatingActionButton.extended(
              onPressed: isSaveEnabled ? _saveAllAgentSettings : null,
              label: Text(_isSavingAll ? 'Saving...' : 'Save settings'),
              icon: const Icon(Icons.save_outlined),
              backgroundColor: isSaveEnabled ? null : colorScheme.surfaceContainerHighest,
              foregroundColor: isSaveEnabled ? null : colorScheme.outline,
              elevation: 4,
              disabledElevation: 4,
            )
          : null,
      body: SafeArea(
        top: false,
        child: Stack(
          children: [
            Row(
              children: [
                Container(
                  width: railWidth,
                  color: colorScheme.surface,
                  child: Column(
                    children: [
                      const SizedBox(height: 16),
                      _buildRailItem(
                        index: 1,
                        icon: Icons.dns_outlined,
                        selectedIcon: Icons.dns,
                        label: 'Manage',
                        colorScheme: colorScheme,
                        railTheme: railTheme,
                        enabled: isAgentReady && _isSelectedServerVerified(),
                      ),
                      const SizedBox(height: 8),
                      _buildRailItem(index: 2, icon: Icons.backup_outlined, selectedIcon: Icons.backup, label: 'Backup', colorScheme: colorScheme, railTheme: railTheme, enabled: isAgentReady),
                      const SizedBox(height: 8),
                      _buildRailItem(
                        index: 3,
                        icon: Icons.restore_outlined,
                        selectedIcon: Icons.restore,
                        label: 'Restore',
                        colorScheme: colorScheme,
                        railTheme: railTheme,
                        enabled: isAgentReady && _isSelectedServerVerified(),
                      ),
                      const Spacer(),
                      _buildRailItem(
                        index: 0,
                        icon: Icons.settings_outlined,
                        selectedIcon: Icons.settings,
                        label: 'Settings',
                        colorScheme: colorScheme,
                        railTheme: railTheme,
                        enabled: isSettingsEnabled,
                      ),
                      const SizedBox(height: 16),
                    ],
                  ),
                ),
                const VerticalDivider(width: 1),
                Expanded(
                  child: Container(
                    decoration: BoxDecoration(
                      gradient: LinearGradient(begin: Alignment.topLeft, end: Alignment.bottomRight, colors: [colorScheme.primaryContainer, colorScheme.surface]),
                    ),
                    child: CustomScrollView(
                      slivers: [
                        if (!_agentReachable)
                          SliverToBoxAdapter(
                            child: Container(
                              width: double.infinity,
                              padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 12),
                              color: colorScheme.errorContainer,
                              child: Row(
                                children: [
                                  Icon(Icons.cloud_off, color: colorScheme.onErrorContainer),
                                  const SizedBox(width: 12),
                                  Expanded(
                                    child: Text(
                                      'Agent unreachable. Check that the agent is running and reachable.',
                                      style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: colorScheme.onErrorContainer),
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          ),
                        if (_agentReachable && _nativeSftpAvailable == false && (_selectedMenuIndex == 2 || _selectedMenuIndex == 3))
                          SliverToBoxAdapter(
                            child: Container(
                              width: double.infinity,
                              padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 12),
                              color: colorScheme.tertiaryContainer,
                              child: Row(
                                children: [
                                  Icon(Icons.warning_amber_rounded, color: colorScheme.onTertiaryContainer),
                                  const SizedBox(width: 12),
                                  Expanded(
                                    child: Text(
                                      'Native SCP/SFTP library not available. Backup and restore will use the slower Dart fallback.',
                                      style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: colorScheme.onTertiaryContainer),
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          ),
                        SliverPadding(
                          padding: _contentPadding,
                          sliver: SliverToBoxAdapter(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(_menuTitle(_selectedMenuIndex), style: Theme.of(context).textTheme.headlineMedium),
                                const SizedBox(height: _contentTitleSpacing),
                                Text(_menuSubtitle(_selectedMenuIndex), style: Theme.of(context).textTheme.bodyLarge),
                                const SizedBox(height: _contentSectionSpacing),
                                if (_selectedMenuIndex == 0) ..._buildSettingsSection(colorScheme),
                                if (_selectedMenuIndex == 1) ..._buildManageSection(colorScheme),
                                if (_selectedMenuIndex == 2) ..._buildBackupSection(colorScheme),
                                if (_selectedMenuIndex == 3) ..._buildRestoreSection(colorScheme),
                              ],
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            ),
            if (_isBackupRunning && _selectedMenuIndex == 2)
              Positioned(
                top: 12,
                left: railWidth + 1 + _contentPadding.left,
                right: _contentPadding.right,
                child: Material(
                  elevation: 6,
                  color: colorScheme.surface.withValues(alpha: 0.96),
                  borderRadius: BorderRadius.circular(14),
                  child: SafeArea(
                    bottom: false,
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          ClipRRect(
                            borderRadius: BorderRadius.circular(6),
                            child: LinearProgressIndicator(
                              value: (_backupSanityBytesTransferred > 0 || _backupSanitySpeedBytesPerSec > 0)
                                  ? (_backupTotalBytes > 0 ? (_backupSanityBytesTransferred / _backupTotalBytes).clamp(0, 1).toDouble() : null)
                                  : (_backupTotalBytes > 0
                                        ? (_backupBytesTransferred / _backupTotalBytes).clamp(0, 1).toDouble()
                                        : (_backupTotalDisks > 0 ? _backupCompletedDisks / _backupTotalDisks : null)),
                            ),
                          ),
                          const SizedBox(height: 8),
                          Row(
                            children: [
                              Expanded(child: Text(_backupStatusMessage, style: Theme.of(context).textTheme.bodyMedium)),
                              TextButton.icon(onPressed: _backupJobId == null ? null : _cancelBackupJob, icon: const Icon(Icons.cancel_outlined), label: const Text('Cancel')),
                            ],
                          ),
                          if (_backupSanityBytesTransferred <= 0 && _backupSanitySpeedBytesPerSec <= 0)
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Progress: ${_formatTotalSizeWithTotal(_backupBytesTransferred, _backupTotalBytes)} • Avg: ${_formatSpeed(_backupAverageSpeedBytesPerSec)} • ETA: ${_formatEta(_backupEtaSeconds)}',
                                  style: Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace'),
                                ),
                                Text(
                                  'Flush: ${_backupPhysicalProgressPercent.toStringAsFixed(1)}% (${_formatTotalSizeWithTotal(_backupPhysicalBytesTransferred, _backupPhysicalTotalBytes)}) • Speed: ${_formatSpeed(_backupPhysicalSpeedBytesPerSec)} • Remaining: ${_formatTotalSize(_backupPhysicalRemainingBytes)}',
                                  style: Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace'),
                                ),
                              ],
                            ),
                          if (_backupSanityBytesTransferred > 0 || _backupSanitySpeedBytesPerSec > 0)
                            Text(
                              'Speed: ${_formatSpeed(_backupSanitySpeedBytesPerSec)} • Total: ${_formatTotalSizeWithTotal(_backupSanityBytesTransferred, _backupTotalBytes)}',
                              style: Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace'),
                            ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
            if (_isRestoring && _selectedMenuIndex == 3)
              Positioned(
                top: 12,
                left: railWidth + 1 + _contentPadding.left,
                right: _contentPadding.right,
                child: Material(
                  elevation: 6,
                  color: colorScheme.surface.withValues(alpha: 0.96),
                  borderRadius: BorderRadius.circular(14),
                  child: SafeArea(
                    bottom: false,
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          ClipRRect(
                            borderRadius: BorderRadius.circular(6),
                            child: LinearProgressIndicator(value: _restoreTotalBytes > 0 ? (_restoreBytesTransferred / _restoreTotalBytes).clamp(0, 1).toDouble() : null),
                          ),
                          const SizedBox(height: 8),
                          Row(
                            children: [
                              Expanded(child: Text(_restoreStatusMessage, style: Theme.of(context).textTheme.bodyMedium)),
                              TextButton.icon(onPressed: _restoreJobId == null ? null : _cancelRestoreJob, icon: const Icon(Icons.cancel_outlined), label: const Text('Cancel')),
                            ],
                          ),
                          Text(
                            'Speed: ${_formatSpeed(_restoreSpeedBytesPerSec)} • Total: ${_formatTotalSizeWithTotal(_restoreBytesTransferred, _restoreTotalBytes)}',
                            style: Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace'),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
            if (_isSanityChecking && _selectedMenuIndex == 3)
              Positioned(
                top: 12,
                left: railWidth + 1 + _contentPadding.left,
                right: _contentPadding.right,
                child: Material(
                  elevation: 6,
                  color: colorScheme.surface.withValues(alpha: 0.96),
                  borderRadius: BorderRadius.circular(14),
                  child: SafeArea(
                    bottom: false,
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          ClipRRect(
                            borderRadius: BorderRadius.circular(6),
                            child: LinearProgressIndicator(value: _sanityTotalBytes > 0 ? (_sanityBytesTransferred / _sanityTotalBytes).clamp(0, 1).toDouble() : null),
                          ),
                          const SizedBox(height: 8),
                          Row(
                            children: [
                              Expanded(child: Text(_sanityStatusMessage, style: Theme.of(context).textTheme.bodyMedium)),
                              TextButton.icon(onPressed: _sanityJobId == null ? null : _cancelSanityJob, icon: const Icon(Icons.cancel_outlined), label: const Text('Cancel')),
                            ],
                          ),
                          Text(
                            'Speed: ${_formatSpeed(_sanitySpeedBytesPerSec)} • Total: ${_formatTotalSizeWithTotal(_sanityBytesTransferred, _sanityTotalBytes)}',
                            style: Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace'),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }
}
