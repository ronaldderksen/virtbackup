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
    final backupPath = _backupPathController.text.trim();
    if (_selectedDriverUsesPath() && backupPath.isEmpty) {
      _showSnackBarInfo('Enter a backup path first');
      return;
    }
    final driverParams = <String, dynamic>{};
    for (final param in _selectedDriverParams()) {
      if (param.type == DriverParamType.boolean) {
        driverParams[param.key] = _driverParamBoolValue(_backupDriverId, param);
        continue;
      }
      final controller = _driverParamController(_backupDriverId, param);
      final rawText = controller.text.trim();
      if (rawText.isEmpty) {
        if (param.defaultValue != null) {
          driverParams[param.key] = param.defaultValue;
        }
        continue;
      }
      if (param.type == DriverParamType.number) {
        final parsed = double.tryParse(rawText);
        if (parsed == null) {
          _showSnackBarError('Enter a valid value for ${param.label}.');
          return;
        }
        driverParams[param.key] = parsed;
        continue;
      }
      driverParams[param.key] = rawText;
    }
    try {
      final start = await _agentApiClient.startBackup(server.id, vm.name, _selectedDriverUsesPath() ? backupPath : '', driverParams: driverParams.isEmpty ? null : driverParams);
      _startBackupJobPolling(start.jobId);
    } catch (error) {
      _showSnackBarError('Backup failed: $error');
    }
  }
}
