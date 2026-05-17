import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

class AgentServerInventory {
  const AgentServerInventory({required this.statuses, required this.missingTools});

  final List<VmStatus> statuses;
  final List<String> missingTools;
}

class AgentHealth {
  const AgentHealth({required this.nativeSftpAvailable, required this.storageWritable, required this.storageWriteError});

  final bool? nativeSftpAvailable;
  final bool storageWritable;
  final String storageWriteError;
}

class AgentApiClient {
  AgentApiClient({Uri? baseUri}) : _baseUri = baseUri ?? Uri.parse('https://127.0.0.1:33551');

  Uri _baseUri;
  String? _lastSeenCertFingerprint;
  String? _trustedCertFingerprint;
  bool _allowUntrustedCerts = true;
  String? _authToken;

  Uri get baseUri => _baseUri;

  void setBaseUri(Uri uri) {
    _baseUri = uri;
  }

  String? get lastSeenCertFingerprint => _lastSeenCertFingerprint;

  void setTrustedCertFingerprint(String? fingerprint) {
    _trustedCertFingerprint = fingerprint;
  }

  void setAllowUntrustedCerts(bool allow) {
    _allowUntrustedCerts = allow;
  }

  void setAuthToken(String? token) {
    _authToken = token;
  }

  Future<bool> ping() async {
    final response = await _get('/health');
    return response.statusCode == 200;
  }

  Future<bool?> fetchNativeSftpAvailable() async {
    return (await fetchHealth()).nativeSftpAvailable;
  }

  Future<AgentHealth> fetchHealth() async {
    final response = await _get('/health');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    bool? nativeSftpAvailable;
    var storageWritable = false;
    var storageWriteError = '';
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map) {
        if (decoded['nativeSftpAvailable'] is bool) {
          nativeSftpAvailable = decoded['nativeSftpAvailable'] as bool;
        }
        if (decoded['storageWritable'] is bool) {
          storageWritable = decoded['storageWritable'] as bool;
        }
        storageWriteError = decoded['storageWriteError']?.toString() ?? '';
      }
    } catch (_) {}
    return AgentHealth(nativeSftpAvailable: nativeSftpAvailable, storageWritable: storageWritable, storageWriteError: storageWriteError);
  }

  Future<String> fetchAgentHostname() async {
    final response = await _get('/health');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is Map) {
      final hostname = decoded['hostname']?.toString().trim() ?? '';
      if (hostname.isNotEmpty) {
        return hostname;
      }
    }
    throw 'Agent did not return a hostname';
  }

  Future<AppSettings> fetchConfig() async {
    final response = await _get('/config');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AppSettings.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<List<BackupDriverInfo>> fetchDrivers() async {
    final response = await _get('/drivers');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return [];
    }
    return decoded.whereType<Map>().map((item) => BackupDriverInfo.fromMap(Map<String, dynamic>.from(item))).toList();
  }

  Future<void> updateConfig(AppSettings agentSettings) async {
    final response = await _post('/config', agentSettings.toMap());
    if (response.statusCode != 200) {
      throw _agentErrorMessage(response);
    }
  }

  Future<AgentJobStart> runSchedule(String scheduleId) async {
    final response = await _post('/schedules/${Uri.encodeComponent(scheduleId)}/run', {});
    if (response.statusCode != 200) {
      throw _agentErrorMessage(response);
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  String _agentErrorMessage(_AgentResponse response) {
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map) {
        final error = decoded['error']?.toString().trim();
        if (error != null && error.isNotEmpty) {
          return error;
        }
      }
    } catch (_) {}
    return 'Agent responded ${response.statusCode}';
  }

  Future<void> storeGoogleOAuth({required String accessToken, required String refreshToken, required String scope, required String accountEmail, required DateTime? expiresAt}) async {
    final response = await _post('/oauth/google', {
      'accessToken': accessToken,
      'refreshToken': refreshToken,
      'scope': scope,
      'accountEmail': accountEmail,
      'expiresAt': expiresAt?.toUtc().millisecondsSinceEpoch,
    });
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<void> clearGoogleOAuth() async {
    final response = await _post('/oauth/google/clear', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<void> storeVirtBackupAccount({
    required String email,
    required String accountBaseUrl,
    required String accessToken,
    required DateTime? accessTokenExpiresAt,
    required String refreshToken,
    required DateTime? refreshTokenExpiresAt,
  }) async {
    final response = await _post('/account/virtbackup', {
      'email': email,
      'accountBaseUrl': accountBaseUrl,
      'accessToken': accessToken,
      'accessTokenExpiresAt': accessTokenExpiresAt?.toUtc().toIso8601String(),
      'refreshToken': refreshToken,
      'refreshTokenExpiresAt': refreshTokenExpiresAt?.toUtc().toIso8601String(),
    });
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<void> clearVirtBackupAccount() async {
    final response = await _post('/account/virtbackup/clear', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<NtfymeTestResult> sendNtfymeTest({required String token}) async {
    final response = await _post('/ntfyme/test', {'token': token});
    if (response.statusCode == 200) {
      try {
        final decoded = jsonDecode(response.body);
        if (decoded is Map) {
          final message = decoded['message']?.toString() ?? 'Ntfy me test notification sent.';
          return NtfymeTestResult(success: decoded['success'] == true, message: message);
        }
      } catch (_) {}
      return const NtfymeTestResult(success: true, message: 'Ntfy me test notification sent.');
    }
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map) {
        final message = decoded['error']?.toString() ?? 'Agent responded ${response.statusCode}';
        return NtfymeTestResult(success: false, message: message);
      }
    } catch (_) {}
    return NtfymeTestResult(success: false, message: 'Agent responded ${response.statusCode}');
  }

  Future<EmailTestResult> sendEmailTest({required String to}) async {
    final response = await _post('/notifications/email/test', {'to': to});
    if (response.statusCode == 200) {
      try {
        final decoded = jsonDecode(response.body);
        if (decoded is Map) {
          final message = decoded['message']?.toString() ?? 'Test email sent.';
          return EmailTestResult(success: decoded['success'] == true, message: message);
        }
      } catch (_) {}
      return const EmailTestResult(success: true, message: 'Test email sent.');
    }
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map) {
        final message = _emailTestErrorMessage(response.statusCode, decoded);
        return EmailTestResult(success: false, message: message);
      }
    } catch (_) {}
    return EmailTestResult(success: false, message: 'Agent responded ${response.statusCode}');
  }

  String _emailTestErrorMessage(int agentStatusCode, Map<dynamic, dynamic> decoded) {
    final parts = <String>[];
    final error = decoded['error']?.toString().trim();
    if (error != null && error.isNotEmpty) {
      parts.add(error);
    }
    final statusCode = decoded['statusCode']?.toString().trim();
    if (statusCode != null && statusCode.isNotEmpty && !parts.any((part) => part.contains(statusCode))) {
      parts.add('status $statusCode');
    }
    final backendBody = _emailTestBackendBodyMessage(decoded['body']);
    if (backendBody != null && backendBody.isNotEmpty && !parts.contains(backendBody)) {
      parts.add(backendBody);
    }
    if (parts.isEmpty) {
      return 'Agent responded $agentStatusCode';
    }
    return parts.join(': ');
  }

  String? _emailTestBackendBodyMessage(Object? body) {
    final rawBody = body?.toString().trim();
    if (rawBody == null || rawBody.isEmpty) {
      return null;
    }
    try {
      final decodedBody = jsonDecode(rawBody);
      if (decodedBody is Map) {
        final fields = ['error', 'message', 'detail', 'details'];
        for (final field in fields) {
          final value = decodedBody[field]?.toString().trim();
          if (value != null && value.isNotEmpty) {
            return value;
          }
        }
      }
    } catch (_) {}
    return rawBody;
  }

  Future<SftpTestResult> testSftpConnection({required String host, required int port, required String username, required String password, required String basePath}) async {
    final response = await _post('/sftp/test', {'host': host, 'port': port, 'username': username, 'password': password, 'basePath': basePath});
    if (response.statusCode == 200) {
      try {
        final decoded = jsonDecode(response.body);
        if (decoded is Map) {
          final message = decoded['message']?.toString() ?? 'SFTP test completed.';
          return SftpTestResult(success: decoded['success'] == true, message: message);
        }
      } catch (_) {}
      return const SftpTestResult(success: true, message: 'SFTP test completed.');
    }
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map) {
        final message = decoded['error']?.toString() ?? 'Agent responded ${response.statusCode}';
        return SftpTestResult(success: false, message: message);
      }
    } catch (_) {}
    return SftpTestResult(success: false, message: 'Agent responded ${response.statusCode}');
  }

  Future<List<VmStatus>> fetchVmStatus(String serverId) async {
    return (await fetchServerInventory(serverId)).statuses;
  }

  Future<AgentServerInventory> fetchServerInventory(String serverId) async {
    final response = await _get('/servers/$serverId/vms');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is List) {
      return AgentServerInventory(statuses: decoded.whereType<Map>().map((item) => VmStatus.fromMap(Map<String, dynamic>.from(item))).toList(), missingTools: const []);
    }
    if (decoded is Map) {
      final map = Map<String, dynamic>.from(decoded);
      final rawItems = map['items'];
      final rawMissingTools = map['missingTools'];
      final statuses = rawItems is List ? rawItems.whereType<Map>().map((item) => VmStatus.fromMap(Map<String, dynamic>.from(item))).toList() : <VmStatus>[];
      final missingTools = rawMissingTools is List ? rawMissingTools.map((item) => item.toString()).where((item) => item.trim().isNotEmpty).toList() : <String>[];
      return AgentServerInventory(statuses: statuses, missingTools: missingTools);
    }
    return const AgentServerInventory(statuses: [], missingTools: []);
  }

  Future<void> refreshServer(String serverId) async {
    final response = await _post('/servers/$serverId/refresh', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! Map || decoded['success'] != true) {
      throw 'Agent refresh failed.';
    }
  }

  Future<bool> testConnection(String serverId) async {
    final response = await _post('/servers/$serverId/test', {});
    if (response.statusCode != 200) {
      return false;
    }
    final decoded = jsonDecode(response.body);
    return decoded is Map && decoded['success'] == true;
  }

  Future<bool> runVmAction(String serverId, VmAction action, String vmName) async {
    final response = await _post('/servers/$serverId/actions', {'action': action.name, 'vmName': vmName});
    if (response.statusCode != 200) {
      return false;
    }
    final decoded = jsonDecode(response.body);
    return decoded is Map && decoded['success'] == true;
  }

  Future<Map<String, dynamic>> previewVmRename(String serverId, String vmName) async {
    final response = await _post('/servers/$serverId/rename/preview', {'vmName': vmName});
    if (response.statusCode != 200) {
      final decoded = jsonDecode(response.body);
      if (decoded is Map && decoded['error'] != null) {
        throw decoded['error'].toString();
      }
      throw 'Agent responded ${response.statusCode}';
    }
    return Map<String, dynamic>.from(jsonDecode(response.body) as Map);
  }

  Future<void> applyVmRename(String serverId, {required String vmName, required String newVmName, required List<Map<String, String>> disks}) async {
    final response = await _post('/servers/$serverId/rename/apply', {'vmName': vmName, 'newVmName': newVmName, 'disks': disks});
    final decoded = jsonDecode(response.body);
    if (response.statusCode != 200 || decoded is! Map || decoded['success'] != true) {
      if (decoded is Map && decoded['error'] != null) {
        throw decoded['error'].toString();
      }
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<bool> cleanupOverlays(String serverId, String vmName) async {
    final response = await _post('/servers/$serverId/cleanup', {'vmName': vmName});
    if (response.statusCode != 200) {
      return false;
    }
    final decoded = jsonDecode(response.body);
    return decoded is Map && decoded['success'] == true;
  }

  Future<AgentJobStart> startBackup(String serverId, String vmName, {required String storageId, Map<String, dynamic>? driverParams}) async {
    final payload = <String, dynamic>{'vmName': vmName, 'storageId': storageId};
    if (driverParams != null && driverParams.isNotEmpty) {
      payload['driverParams'] = driverParams;
    }
    final response = await _post('/servers/$serverId/backup', payload);
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStatus> fetchJob(String jobId) async {
    final response = await _get('/jobs/$jobId');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStatus.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<List<AgentJobStatus>> fetchJobs() async {
    final response = await _get('/jobs');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return [];
    }
    return decoded.whereType<Map>().map((item) => AgentJobStatus.fromMap(Map<String, dynamic>.from(item))).toList();
  }

  Future<List<ScheduleQueueEntry>> fetchScheduleQueue() async {
    final response = await _get('/schedule-queue');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return <ScheduleQueueEntry>[];
    }
    return decoded.whereType<Map>().map((item) => ScheduleQueueEntry.fromMap(Map<String, dynamic>.from(item))).toList();
  }

  Future<void> cancelJob(String jobId) async {
    final response = await _post('/jobs/$jobId/cancel', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<void> removeQueuedScheduleRun(String scheduleId) async {
    final response = await _post('/schedule-queue/${Uri.encodeComponent(scheduleId)}/remove', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<List<RestoreEntry>> fetchRestoreEntries({required String storageId}) async {
    final normalizedStorageId = storageId.trim();
    final query = '?storageId=${Uri.encodeQueryComponent(normalizedStorageId)}';
    final response = await _get('/restore/entries$query');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return [];
    }
    return decoded.whereType<Map>().map((item) => RestoreEntry.fromMap(Map<String, dynamic>.from(item))).toList();
  }

  Future<RestorePrecheckResult> restorePrecheck(String serverId, String xmlPath, {required String storageId}) async {
    final normalizedStorageId = storageId.trim();
    final payload = <String, dynamic>{'xmlPath': xmlPath, 'storageId': normalizedStorageId};
    final response = await _post('/servers/$serverId/restore/precheck', payload);
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return RestorePrecheckResult.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStart> startRestore(String serverId, String xmlPath, String decision, {required String storageId}) async {
    final normalizedStorageId = storageId.trim();
    final payload = <String, dynamic>{'xmlPath': xmlPath, 'decision': decision, 'storageId': normalizedStorageId};
    final response = await _post('/servers/$serverId/restore/start', payload);
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStart> startSanityCheck(String xmlPath, String timestamp, {required String storageId}) async {
    final response = await _post('/restore/sanity', {'xmlPath': xmlPath, 'timestamp': timestamp, 'storageId': storageId.trim()});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStart> startQuickCheck(String xmlPath, String timestamp, {required String storageId}) async {
    final response = await _post('/restore/quick-check', {'xmlPath': xmlPath, 'timestamp': timestamp, 'storageId': storageId.trim()});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<int> deleteRestoreManifests({required String xmlPath, required String timestamp, required String storageId}) async {
    final response = await _post('/restore/manifests/delete', {'xmlPath': xmlPath, 'timestamp': timestamp, 'storageId': storageId.trim()});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = Map<String, dynamic>.from(jsonDecode(response.body) as Map);
    final deletedCountValue = decoded['deletedCount'];
    if (deletedCountValue is num) {
      return deletedCountValue.toInt();
    }
    return int.tryParse((deletedCountValue ?? '').toString()) ?? 0;
  }

  Stream<AgentEvent> eventStream() async* {
    final client = _createHttpClient();
    try {
      final request = await client.getUrl(_baseUri.resolve('/events'));
      _applyAuthHeaders(request.headers);
      final response = await request.close();
      if (response.statusCode != 200) {
        throw 'Agent responded ${response.statusCode}';
      }
      String? eventName;
      final dataLines = <String>[];
      await for (final line in response.transform(utf8.decoder).transform(const LineSplitter())) {
        if (line.isEmpty) {
          if (dataLines.isNotEmpty) {
            final data = dataLines.join('\n');
            final decoded = _decodeEventPayload(data, eventName);
            if (decoded != null) {
              yield decoded;
            }
          }
          eventName = null;
          dataLines.clear();
          continue;
        }
        if (line.startsWith(':')) {
          continue;
        }
        if (line.startsWith('event:')) {
          eventName = line.substring('event:'.length).trim();
          continue;
        }
        if (line.startsWith('data:')) {
          dataLines.add(line.substring('data:'.length).trim());
        }
      }
    } finally {
      client.close();
    }
  }

  AgentEvent? _decodeEventPayload(String data, String? eventName) {
    if (eventName == 'ready') {
      return null;
    }
    try {
      final decoded = jsonDecode(data);
      if (decoded is! Map) {
        return null;
      }
      final type = (decoded['type'] ?? eventName ?? '').toString();
      final payload = decoded['payload'];
      if (payload is! Map) {
        return null;
      }
      return AgentEvent(type: type, payload: Map<String, dynamic>.from(payload));
    } catch (_) {
      return null;
    }
  }

  Future<_AgentResponse> _get(String path) async {
    final client = _createHttpClient();
    try {
      final request = await client.getUrl(_baseUri.resolve(path));
      _applyAuthHeaders(request.headers);
      final response = await request.close();
      final body = await response.transform(utf8.decoder).join();
      return _AgentResponse(response.statusCode, body);
    } on SocketException catch (error) {
      throw 'Connection failed to ${_baseUri.host}:${_baseUri.port} (${error.message})';
    } finally {
      client.close();
    }
  }

  Future<_AgentResponse> _post(String path, Map<String, dynamic> body) async {
    final client = _createHttpClient();
    try {
      final request = await client.postUrl(_baseUri.resolve(path));
      request.headers.contentType = ContentType.json;
      _applyAuthHeaders(request.headers);
      request.write(jsonEncode(body));
      final response = await request.close();
      final responseBody = await response.transform(utf8.decoder).join();
      return _AgentResponse(response.statusCode, responseBody);
    } on SocketException catch (error) {
      throw 'Connection failed to ${_baseUri.host}:${_baseUri.port} (${error.message})';
    } finally {
      client.close();
    }
  }

  HttpClient _createHttpClient() {
    final client = HttpClient();
    if (_baseUri.scheme == 'https') {
      client.badCertificateCallback = (cert, host, port) {
        _lastSeenCertFingerprint = _fingerprintFromCert(cert);
        if (_allowUntrustedCerts) {
          return true;
        }
        return _trustedCertFingerprint != null && _trustedCertFingerprint == _lastSeenCertFingerprint;
      };
    }
    return client;
  }

  void _applyAuthHeaders(HttpHeaders headers) {
    final token = _authToken;
    if (token == null || token.isEmpty) {
      return;
    }
    headers.set(HttpHeaders.authorizationHeader, 'Bearer $token');
  }

  String _fingerprintFromCert(X509Certificate cert) {
    final bytes = utf8.encode(cert.pem);
    return sha256.convert(bytes).toString();
  }
}

class _AgentResponse {
  _AgentResponse(this.statusCode, this.body);

  final int statusCode;
  final String body;
}

class AgentEvent {
  AgentEvent({required this.type, required this.payload});

  final String type;
  final Map<String, dynamic> payload;
}

class NtfymeTestResult {
  const NtfymeTestResult({required this.success, required this.message});

  final bool success;
  final String message;
}

class EmailTestResult {
  const EmailTestResult({required this.success, required this.message});

  final bool success;
  final String message;
}

class SftpTestResult {
  const SftpTestResult({required this.success, required this.message});

  final bool success;
  final String message;
}
