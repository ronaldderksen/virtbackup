part of 'main_screen.dart';

extension _BackupServerSetupBackupSection on _BackupServerSetupScreenState {
  List<Widget> _buildBackupSection(ColorScheme colorScheme) {
    return [
      Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('Backup', style: Theme.of(context).textTheme.titleMedium),
              const SizedBox(height: 16),
              ...[
                DropdownButtonFormField<String>(
                  key: ValueKey(_editingServerId),
                  initialValue: _editingServerId,
                  decoration: const InputDecoration(labelText: 'Select server', prefixIcon: Icon(Icons.storage_outlined), border: OutlineInputBorder()),
                  items: _servers.map((server) => DropdownMenuItem(value: server.id, child: Text(server.name))).toList(),
                  onChanged: _servers.isEmpty
                      ? null
                      : (value) {
                          final server = _servers.firstWhere((item) => item.id == value, orElse: () => _servers.first);
                          _selectServer(server);
                        },
                ),
                const SizedBox(height: 16),
                Row(
                  children: [
                    Expanded(
                      child: DropdownButtonFormField<String>(
                        initialValue: _selectedBackupDestinationId,
                        decoration: const InputDecoration(labelText: 'Destination', prefixIcon: Icon(Icons.cloud_queue_outlined), border: OutlineInputBorder()),
                        items: _enabledDestinations().map((destination) => DropdownMenuItem<String>(value: destination.id, child: Text('${destination.name} (${destination.driverId})'))).toList(),
                        onChanged: _isBackupRunning
                            ? null
                            : (value) {
                                _updateUi(() {
                                  _selectedBackupDestinationId = value;
                                });
                              },
                      ),
                    ),
                    const SizedBox(width: 12),
                    OutlinedButton(onPressed: _isBackupRunning ? null : _openDestinationEditor, child: const Text('Manage')),
                  ],
                ),
                const SizedBox(height: 16),
                if (_servers.isEmpty)
                  Text('Add a server first to view its VMs.', style: Theme.of(context).textTheme.bodyMedium)
                else ...[
                  Builder(
                    builder: (context) {
                      final server = _getSelectedServer();
                      return Row(
                        children: [
                          Text('Virtual machines', style: Theme.of(context).textTheme.titleMedium),
                          const SizedBox(width: 12),
                          Expanded(child: Text(_formatLastRefresh(server), style: Theme.of(context).textTheme.bodySmall)),
                        ],
                      );
                    },
                  ),
                  const SizedBox(height: 12),
                  Builder(
                    builder: (context) {
                      final server = _getSelectedServer();
                      final vms = server == null ? null : _vmCacheByServerId[server.id];
                      if (vms == null || vms.isEmpty) {
                        return Text('No VM data loaded yet.', style: Theme.of(context).textTheme.bodyMedium);
                      }
                      return ListView.separated(
                        shrinkWrap: true,
                        physics: const NeverScrollableScrollPhysics(),
                        itemCount: vms.length,
                        separatorBuilder: (_, _) => const Divider(height: 1),
                        itemBuilder: (context, index) {
                          final vm = vms[index];
                          final isRunning = vm.powerState == VmPowerState.running;
                          final hasOverlay = _vmHasOverlayByName[vm.name] == true;
                          return Padding(
                            padding: const EdgeInsets.symmetric(vertical: 8),
                            child: Row(
                              crossAxisAlignment: CrossAxisAlignment.center,
                              children: [
                                Icon(isRunning ? Icons.play_circle_fill : Icons.stop_circle, color: isRunning ? colorScheme.primary : colorScheme.outline),
                                const SizedBox(width: 12),
                                Expanded(
                                  child: Column(
                                    crossAxisAlignment: CrossAxisAlignment.start,
                                    children: [
                                      Text(vm.name, style: Theme.of(context).textTheme.titleMedium),
                                      Text(isRunning ? 'Running' : 'Stopped', style: Theme.of(context).textTheme.bodyMedium),
                                    ],
                                  ),
                                ),
                                const SizedBox(width: 12),
                                Row(
                                  mainAxisSize: MainAxisSize.min,
                                  children: [
                                    if (!isRunning) TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.start), child: const Text('Run')),
                                    if (!isRunning) const SizedBox(width: 8),
                                    if (hasOverlay) TextButton(onPressed: _isBackupRunning || server == null ? null : () => _cleanupVmOverlays(server, vm), child: const Text('Cleanup')),
                                    if (hasOverlay) const SizedBox(width: 8),
                                    TextButton(onPressed: _isBackupRunning || server == null || hasOverlay ? null : () => _runVmBackup(server, vm), child: const Text('Backup')),
                                  ],
                                ),
                              ],
                            ),
                          );
                        },
                      );
                    },
                  ),
                ],
              ],
            ],
          ),
        ),
      ),
    ];
  }
}
