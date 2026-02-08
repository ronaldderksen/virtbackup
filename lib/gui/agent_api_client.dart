import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

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
    final response = await _get('/health');
    if (response.statusCode != 200) {
      return null;
    }
    try {
      final decoded = jsonDecode(response.body);
      if (decoded is Map && decoded['nativeSftpAvailable'] is bool) {
        return decoded['nativeSftpAvailable'] as bool;
      }
    } catch (_) {}
    return null;
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
      throw 'Agent responded ${response.statusCode}';
    }
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

  Future<List<VmStatus>> fetchVmStatus(String serverId) async {
    final response = await _get('/servers/$serverId/vms');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return [];
    }
    return decoded.whereType<Map>().map((item) => VmStatus.fromMap(Map<String, dynamic>.from(item))).toList();
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

  Future<bool> cleanupOverlays(String serverId, String vmName) async {
    final response = await _post('/servers/$serverId/cleanup', {'vmName': vmName});
    if (response.statusCode != 200) {
      return false;
    }
    final decoded = jsonDecode(response.body);
    return decoded is Map && decoded['success'] == true;
  }

  Future<AgentJobStart> startBackup(String serverId, String vmName, String backupPath, {Map<String, dynamic>? driverParams}) async {
    final payload = <String, dynamic>{'vmName': vmName, 'backupPath': backupPath};
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

  Future<void> cancelJob(String jobId) async {
    final response = await _post('/jobs/$jobId/cancel', {});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
  }

  Future<List<RestoreEntry>> fetchRestoreEntries() async {
    final response = await _get('/restore/entries');
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    final decoded = jsonDecode(response.body);
    if (decoded is! List) {
      return [];
    }
    return decoded.whereType<Map>().map((item) => RestoreEntry.fromMap(Map<String, dynamic>.from(item))).toList();
  }

  Future<RestorePrecheckResult> restorePrecheck(String serverId, String xmlPath) async {
    final response = await _post('/servers/$serverId/restore/precheck', {'xmlPath': xmlPath});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return RestorePrecheckResult.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStart> startRestore(String serverId, String xmlPath, String decision) async {
    final response = await _post('/servers/$serverId/restore/start', {'xmlPath': xmlPath, 'decision': decision});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
  }

  Future<AgentJobStart> startSanityCheck(String xmlPath, String timestamp) async {
    final response = await _post('/restore/sanity', {'xmlPath': xmlPath, 'timestamp': timestamp});
    if (response.statusCode != 200) {
      throw 'Agent responded ${response.statusCode}';
    }
    return AgentJobStart.fromMap(Map<String, dynamic>.from(jsonDecode(response.body)));
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
