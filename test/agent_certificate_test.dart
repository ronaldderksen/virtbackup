import 'dart:convert';
import 'dart:io';

import 'package:basic_utils/basic_utils.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/gui/agent_api_client.dart';

SecurityContext _certificateContext() {
  final pair = CryptoUtils.generateRSAKeyPair(keySize: 2048);
  final privateKey = pair.privateKey as RSAPrivateKey;
  final csr = X509Utils.generateRsaCsrPem({'CN': 'virtbackup-test'}, privateKey, pair.publicKey as RSAPublicKey, san: ['localhost', '127.0.0.1']);
  final certificate = X509Utils.generateSelfSignedCertificate(privateKey, csr, 1);
  return SecurityContext()
    ..useCertificateChainBytes(utf8.encode(certificate))
    ..usePrivateKeyBytes(utf8.encode(CryptoUtils.encodeRSAPrivateKeyToPem(privateKey)));
}

void main() {
  late SecurityContext firstCertificate;
  late SecurityContext replacementCertificate;
  late HttpServer server;
  late AgentApiClient client;
  final receivedTokens = <String?>[];

  Future<HttpServer> serve(SecurityContext certificate, int port) async {
    final server = await HttpServer.bindSecure(InternetAddress.loopbackIPv4, port, certificate);
    server.listen((request) async {
      receivedTokens.add(request.headers.value(HttpHeaders.authorizationHeader));
      request.response.headers.contentType = ContentType.json;
      request.response.write('{"ok":true}');
      await request.response.close();
    }, onError: (Object _) {});
    return server;
  }

  setUpAll(() {
    firstCertificate = _certificateContext();
    replacementCertificate = _certificateContext();
  });
  setUp(() async {
    receivedTokens.clear();
    server = await serve(firstCertificate, 0);
    client = AgentApiClient(baseUri: Uri.parse('https://127.0.0.1:${server.port}'));
    client.setAuthToken('test-token');
  });
  tearDown(() async => server.close(force: true));

  test('certificate discovery sends no HTTP request or auth token before trust', () async {
    final fingerprint = await client.inspectCertificate();
    expect(fingerprint, matches(RegExp(r'^[0-9a-f]{64}$')));
    expect(receivedTokens, isEmpty);
    await expectLater(client.ping(), throwsA(isA<HandshakeException>()));
    expect(receivedTokens, isEmpty);
    client.setTrustedCertFingerprint(fingerprint);
    expect(await client.ping(), isTrue);
    expect(receivedTokens, ['Bearer test-token']);
  });

  test('a replacement certificate is rejected before the saved token is sent', () async {
    final trusted = await client.inspectCertificate();
    client.setTrustedCertFingerprint(trusted);
    expect(await client.ping(), isTrue);
    receivedTokens.clear();
    final port = server.port;
    await server.close(force: true);
    server = await serve(replacementCertificate, port);
    await expectLater(client.ping(), throwsA(isA<HandshakeException>()));
    expect(receivedTokens, isEmpty);
    final replacement = await client.inspectCertificate();
    expect(replacement, isNot(trusted));
    expect(receivedTokens, isEmpty);
    await expectLater(client.ping(), throwsA(isA<HandshakeException>()));
    client.setTrustedCertFingerprint(replacement);
    expect(await client.ping(), isTrue);
    expect(receivedTokens, ['Bearer test-token']);
  });
}
