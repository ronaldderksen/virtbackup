part of 'main_screen.dart';

extension _BackupServerSetupRestoreSection on _BackupServerSetupScreenState {
  ServerConfig? _getRestoreServer() {
    if (_restoreServerId == null) {
      return null;
    }
    try {
      return _servers.firstWhere((server) => server.id == _restoreServerId);
    } catch (_) {
      return null;
    }
  }

  Future<void> _loadRestoreEntries() async {
    _updateUi(() {
      _isLoadingRestoreEntries = true;
    });
    try {
      final entries = await _agentApiClient.fetchRestoreEntries();
      final selectedStillExists =
          _selectedRestoreVmName != null &&
          _selectedRestoreTimestamp != null &&
          _selectedRestoreSourceServerId != null &&
          entries.any((entry) => entry.vmName == _selectedRestoreVmName && entry.timestamp == _selectedRestoreTimestamp && entry.sourceServerId == _selectedRestoreSourceServerId);
      _updateUi(() {
        _restoreEntries
          ..clear()
          ..addAll(entries);
        if (!selectedStillExists) {
          if (entries.isEmpty) {
            _selectedRestoreVmName = null;
            _selectedRestoreTimestamp = null;
            _selectedRestoreSourceServerId = null;
          } else {
            _selectedRestoreVmName = entries.first.vmName;
            _selectedRestoreTimestamp = entries.first.timestamp;
            _selectedRestoreSourceServerId = entries.first.sourceServerId;
          }
        }
      });
    } catch (error, stackTrace) {
      _logError('Restore inventory load failed.', error, stackTrace);
      _showSnackBarError('Restore load failed: $error');
    } finally {
      _updateUi(() {
        _isLoadingRestoreEntries = false;
      });
    }
  }

  RestoreEntry? _selectedRestoreEntry() {
    if (_selectedRestoreVmName == null || _selectedRestoreTimestamp == null || _selectedRestoreSourceServerId == null) {
      return null;
    }
    for (final entry in _restoreEntries) {
      if (entry.vmName == _selectedRestoreVmName && entry.timestamp == _selectedRestoreTimestamp && entry.sourceServerId == _selectedRestoreSourceServerId) {
        return entry;
      }
    }
    return null;
  }

  Future<String?> _confirmRestoreDecision(String vmName, {required bool canDefineOnly}) async {
    final result = await showDialog<String>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text('Overwrite existing VM?'),
          content: Text(
            canDefineOnly
                ? 'A VM named "$vmName" already exists on the restore server. You can overwrite everything or just redefine the XML.'
                : 'A VM named "$vmName" already exists on the restore server. Overwrite it?',
          ),
          actions: [
            TextButton(onPressed: () => Navigator.of(dialogContext).pop('cancel'), child: const Text('Cancel')),
            if (canDefineOnly) TextButton(onPressed: () => Navigator.of(dialogContext).pop('define'), child: const Text('Define XML only')),
            FilledButton(onPressed: () => Navigator.of(dialogContext).pop('overwrite'), child: const Text('Overwrite all')),
          ],
        );
      },
    );
    return result;
  }

  Future<void> _runRestore() async {
    if (_isRestoring) {
      return;
    }
    final server = _getRestoreServer();
    final entry = _selectedRestoreEntry();
    if (server == null) {
      _showSnackBarInfo('Select a restore server first.');
      return;
    }
    if (entry == null) {
      _showSnackBarInfo('Select a backup XML first.');
      return;
    }
    if (!entry.hasAllDisks) {
      _showSnackBarInfo('Cannot restore: some disks are missing for this backup.');
      return;
    }
    try {
      final precheck = await _agentApiClient.restorePrecheck(server.id, entry.xmlPath);
      var decision = 'overwrite';
      if (precheck.vmExists) {
        final selected = await _confirmRestoreDecision(entry.vmName, canDefineOnly: precheck.canDefineOnly);
        if (selected == null || selected == 'cancel') {
          return;
        }
        decision = selected;
      }
      final start = await _agentApiClient.startRestore(server.id, entry.xmlPath, decision);
      _startRestoreJobPolling(start.jobId);
    } catch (error, stackTrace) {
      _logError('Restore failed.', error, stackTrace);
      _showSnackBarError('Restore failed: $error');
    }
  }

  Future<void> _runSanityCheck() async {
    if (_isSanityChecking) {
      return;
    }
    final entry = _selectedRestoreEntry();
    if (entry == null) {
      _showSnackBarError('Select a restore entry first.');
      return;
    }
    try {
      final start = await _agentApiClient.startSanityCheck(entry.xmlPath, entry.timestamp);
      _startSanityJobPolling(start.jobId);
    } catch (error, stackTrace) {
      _logError('Sanity check failed.', error, stackTrace);
      _showSnackBarError('Sanity check failed: $error');
    }
  }

  List<Widget> _buildRestoreSection(ColorScheme colorScheme) {
    final backupPath = _backupPathController.text.trim();
    final selectedEntry = _selectedRestoreEntry();
    final restoreServer = _getRestoreServer();
    final canRestore = !_isRestoring && restoreServer != null && restoreServer.connectionType == ConnectionType.ssh && selectedEntry != null && selectedEntry.hasAllDisks;
    final canCheck = !_isSanityChecking && !_isRestoring && selectedEntry != null;
    final vmOptions = _restoreEntries.map((entry) => entry.vmName).toSet().toList()..sort();
    final selectedVm = _selectedRestoreVmName;
    final dateEntries = _restoreEntries.where((entry) => selectedVm == null ? true : entry.vmName == selectedVm).toList()..sort((a, b) => b.timestamp.compareTo(a.timestamp));
    final selectedKey = _selectedRestoreTimestamp != null && _selectedRestoreSourceServerId != null ? '${_selectedRestoreTimestamp!}|${_selectedRestoreSourceServerId!}' : null;
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
              Row(children: [Text('Restore', style: Theme.of(context).textTheme.titleMedium)]),
              const SizedBox(height: 16),
              if (_isRestoring) const SizedBox(height: 16),
              DropdownButtonFormField<String>(
                key: const ValueKey('restore_server'),
                initialValue: _restoreServerId,
                decoration: const InputDecoration(labelText: 'Restore server', prefixIcon: Icon(Icons.storage_outlined), border: OutlineInputBorder()),
                items: _servers.map((server) => DropdownMenuItem(value: server.id, child: Text(server.name))).toList(),
                onChanged: _servers.isEmpty ? null : _selectRestoreServerId,
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: const ValueKey('restore_vm'),
                initialValue: _selectedRestoreVmName,
                decoration: const InputDecoration(labelText: 'VM', prefixIcon: Icon(Icons.memory_outlined), border: OutlineInputBorder()),
                items: vmOptions.map((name) => DropdownMenuItem(value: name, child: Text(name))).toList(),
                onChanged: vmOptions.isEmpty
                    ? null
                    : (value) {
                        _updateUi(() {
                          _selectedRestoreVmName = value;
                          final matches = _restoreEntries.where((entry) => entry.vmName == value).toList()..sort((a, b) => b.timestamp.compareTo(a.timestamp));
                          if (matches.isEmpty) {
                            _selectedRestoreTimestamp = null;
                            _selectedRestoreSourceServerId = null;
                          } else {
                            _selectedRestoreTimestamp = matches.first.timestamp;
                            _selectedRestoreSourceServerId = matches.first.sourceServerId;
                          }
                        });
                      },
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: const ValueKey('restore_timestamp'),
                initialValue: selectedKey,
                decoration: const InputDecoration(labelText: 'Date', prefixIcon: Icon(Icons.event_outlined), border: OutlineInputBorder()),
                items: dateEntries.map((entry) => DropdownMenuItem(value: '${entry.timestamp}|${entry.sourceServerId}', child: Text('${entry.timestamp} • ${entry.sourceServerName}'))).toList(),
                onChanged: dateEntries.isEmpty
                    ? null
                    : (value) {
                        _updateUi(() {
                          if (value == null) {
                            _selectedRestoreTimestamp = null;
                            _selectedRestoreSourceServerId = null;
                          } else {
                            final parts = value.split('|');
                            _selectedRestoreTimestamp = parts.isNotEmpty ? parts.first : null;
                            _selectedRestoreSourceServerId = parts.length > 1 ? parts.last : null;
                          }
                        });
                      },
              ),
              const SizedBox(height: 12),
              Row(
                children: [
                  FilledButton.icon(onPressed: canRestore ? _runRestore : null, icon: const Icon(Icons.restore), label: Text(_isRestoring ? 'Restoring...' : 'Restore')),
                  const SizedBox(width: 12),
                  OutlinedButton.icon(onPressed: canCheck ? _runSanityCheck : null, icon: const Icon(Icons.check_circle_outline), label: Text(_isSanityChecking ? 'Checking...' : 'Check')),
                ],
              ),
              const SizedBox(height: 12),
              if (_selectedDriverUsesPath() && backupPath.isEmpty)
                Text('Set a Backup path in Settings first.', style: Theme.of(context).textTheme.bodyMedium)
              else if (_restoreEntries.isEmpty && !_isLoadingRestoreEntries)
                Text(_selectedDriverUsesPath() ? 'No backup XML files found under: $backupPath' : 'No backup XML files found.', style: Theme.of(context).textTheme.bodyMedium)
              else if (selectedEntry != null) ...[
                const SizedBox(height: 8),
                Text('VM: ${selectedEntry.vmName}', style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 4),
                Text('XML: ${selectedEntry.xmlPath}', style: Theme.of(context).textTheme.bodySmall),
                const SizedBox(height: 12),
                Text(
                  selectedEntry.hasAllDisks
                      ? 'All disks for ${selectedEntry.timestamp} seem present.'
                      : 'Missing disks for ${selectedEntry.timestamp}: ${selectedEntry.missingDiskBasenames.join(', ')}',
                  style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: selectedEntry.hasAllDisks ? colorScheme.primary : colorScheme.error),
                ),
                if (selectedEntry.diskBasenames.isNotEmpty) ...[
                  const SizedBox(height: 12),
                  Text('Disks in XML:', style: Theme.of(context).textTheme.titleSmall),
                  const SizedBox(height: 6),
                  ...selectedEntry.diskBasenames.map((disk) => Text('• $disk', style: Theme.of(context).textTheme.bodyMedium)),
                ],
              ],
            ],
          ),
        ),
      ),
    ];
  }
}
