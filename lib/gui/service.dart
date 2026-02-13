part of 'main_screen.dart';

extension _BackupServerSetupBackupService on _BackupServerSetupScreenState {
  Future<void> _cleanupVmOverlays(ServerConfig server, VmEntry vm) async {
    if (_isBackupRunning) {
      return;
    }
    if (server.connectionType != ConnectionType.ssh) {
      _showSnackBarInfo('Overlay cleanup requires SSH.');
      return;
    }
    try {
      _showSnackBarInfo('Cleaning up overlays for ${vm.name}...');
      final ok = await _agentApiClient.cleanupOverlays(server.id, vm.name);
      if (!ok) {
        throw 'Cleanup failed';
      }
      await _loadVmInventory(server);
      final stillHasOverlay = _vmHasOverlayByName[vm.name] == true;
      if (stillHasOverlay) {
        _showSnackBarError('Cleanup incomplete for ${vm.name}. Try starting the VM and running cleanup again.');
        return;
      }
      _showSnackBarInfo('Cleanup completed for ${vm.name}');
    } catch (error, stackTrace) {
      _logError('Overlay cleanup failed.', error, stackTrace);
      _showSnackBarError('Cleanup failed: $error');
    }
  }

  Future<void> _runVmBackup(ServerConfig server, VmEntry vm) async {
    if (_isBackupRunning) {
      return;
    }
    if (server.connectionType != ConnectionType.ssh) {
      _showSnackBarInfo('Backup requires SSH.');
      return;
    }
    final basePath = _backupPathController.text.trim();
    if (basePath.isEmpty) {
      _showSnackBarInfo('Set a Backup base path in Settings first.');
      return;
    }
    final destinationId = _selectedBackupDestinationId?.trim() ?? '';
    if (destinationId.isEmpty) {
      _showSnackBarError('Select a destination first.');
      return;
    }
    try {
      final start = await _agentApiClient.startBackup(server.id, vm.name, destinationId: destinationId);
      _startBackupJobPolling(start.jobId);
    } catch (error) {
      _showSnackBarError('Backup failed: $error');
    }
  }
}
