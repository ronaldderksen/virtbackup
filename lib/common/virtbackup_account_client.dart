import 'dart:convert';

import 'package:http/http.dart' as http;

class VirtBackupAccountSession {
  const VirtBackupAccountSession({required this.email, required this.sessionToken});

  final String email;
  final String sessionToken;
}

class VirtBackupAccountClientException implements Exception {
  const VirtBackupAccountClientException(this.message);

  final String message;

  @override
  String toString() => message;
}

class VirtBackupAccountClient {
  VirtBackupAccountClient({required Uri baseUri, http.Client? httpClient}) : _baseUri = baseUri, _httpClient = httpClient ?? http.Client();

  final Uri _baseUri;
  final http.Client _httpClient;

  Uri browserLoginUri({required Uri redirectUri, required String state}) {
    return _resolve('/app-login').replace(queryParameters: <String, String>{'redirect_uri': redirectUri.toString(), 'state': state});
  }

  Future<VirtBackupAccountSession> exchangeAppLoginCode(String code) async {
    final response = await _httpClient.post(
      _resolve('/api/auth/exchange'),
      headers: const <String, String>{'content-type': 'application/json; charset=utf-8'},
      body: jsonEncode(<String, String>{'code': code}),
    );
    final Map<String, dynamic> body = _decodeJsonObject(response.body);
    if (response.statusCode != 200) {
      throw VirtBackupAccountClientException(_messageForError(body['error']?.toString()));
    }
    final sessionEmail = body['email']?.toString() ?? '';
    final sessionToken = body['sessionToken']?.toString() ?? '';
    if (sessionEmail.isEmpty || sessionToken.isEmpty) {
      throw const VirtBackupAccountClientException('The server returned an invalid login response.');
    }
    return VirtBackupAccountSession(email: sessionEmail, sessionToken: sessionToken);
  }

  Future<VirtBackupAccountSession> fetchSession(String sessionToken) async {
    final response = await _httpClient.get(_resolve('/api/auth/session'), headers: <String, String>{'authorization': 'Bearer $sessionToken'});
    final Map<String, dynamic> body = _decodeJsonObject(response.body);
    if (response.statusCode != 200) {
      throw VirtBackupAccountClientException(_messageForError(body['error']?.toString()));
    }
    final sessionEmail = body['email']?.toString() ?? '';
    if (sessionEmail.isEmpty) {
      throw const VirtBackupAccountClientException('The server returned an invalid session response.');
    }
    return VirtBackupAccountSession(email: sessionEmail, sessionToken: sessionToken);
  }

  Future<void> logout(String sessionToken) async {
    final response = await _httpClient.post(_resolve('/api/auth/logout'), headers: <String, String>{'authorization': 'Bearer $sessionToken'});
    if (response.statusCode != 200) {
      final Map<String, dynamic> body = _decodeJsonObject(response.body);
      throw VirtBackupAccountClientException(_messageForError(body['error']?.toString()));
    }
  }

  Uri _resolve(String path) {
    return _baseUri.replace(path: path, queryParameters: null);
  }

  Map<String, dynamic> _decodeJsonObject(String body) {
    try {
      final dynamic decoded = jsonDecode(body);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
    } catch (_) {}
    throw const VirtBackupAccountClientException('The server returned an invalid response.');
  }

  String _messageForError(String? error) {
    switch (error) {
      case 'invalid_email':
        return 'Enter a valid email address.';
      case 'password_required':
        return 'Enter your password.';
      case 'invalid_credentials':
        return 'Invalid email or password.';
      case 'email_not_verified':
        return 'Your email is not verified yet.';
      case 'unauthorized':
        return 'Your session has expired. Sign in again.';
      case 'server_error':
        return 'The account server could not complete the request.';
      case 'method_not_allowed':
        return 'The account server rejected this request method.';
      case 'invalid_json':
        return 'The account server rejected the request body.';
      case 'code_required':
        return 'The browser login code is missing.';
      case 'invalid_code':
        return 'The browser login code is invalid or expired.';
      default:
        return 'The account request failed.';
    }
  }
}
