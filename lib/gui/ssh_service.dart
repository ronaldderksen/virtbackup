part of 'main_screen.dart';

extension _BackupServerSetupSshService on _BackupServerSetupScreenState {
  Future<void> _testSshConnection() async {
    final server = _findEditingServer();
    if (server == null) {
      throw 'Save the server settings first.';
    }
    final ok = await _agentApiClient.testConnection(server.id);
    if (!ok) {
      throw 'Connection test failed.';
    }
  }
}
