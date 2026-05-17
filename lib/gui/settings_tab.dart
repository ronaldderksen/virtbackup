part of 'main_screen.dart';

extension _BackupServerSetupSettingsSection on _BackupServerSetupScreenState {
  List<Widget> _buildSettingsSection(ColorScheme colorScheme) {
    return [
      _buildVirtBackupAccountCard(colorScheme),
      const SizedBox(height: 24),
      Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text('Agent', style: Theme.of(context).textTheme.titleMedium),
                  FilledButton.icon(onPressed: _showAddAgentDialog, icon: const Icon(Icons.add), label: const Text('Add agent')),
                ],
              ),
              const SizedBox(height: 12),
              ListView.separated(
                shrinkWrap: true,
                physics: const NeverScrollableScrollPhysics(),
                itemCount: _agentEndpoints.length,
                separatorBuilder: (_, _) => const Divider(height: 1),
                itemBuilder: (context, index) {
                  final agent = _agentEndpoints[index];
                  final isSelected = agent.id == _selectedAgentId;
                  final isLocal = _isLocalAgentHost(agent.host);
                  final tokenLabel = isLocal || agent.useLocalToken ? 'local token' : (agent.token.trim().isEmpty ? 'token missing' : 'stored token');
                  final healthLabel = isSelected ? (!_agentReachable ? 'offline' : (_storageWritable ? 'online, storage writable' : 'online, storage not writable')) : 'not loaded';
                  return ListTile(
                    selected: isSelected,
                    leading: Icon(isSelected ? (_agentReachable && _storageWritable ? Icons.cloud_done : Icons.cloud_off_outlined) : Icons.cloud_outlined),
                    title: Text(agent.label),
                    subtitle: Text('$tokenLabel - $healthLabel'),
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        IconButton(tooltip: 'Edit agent', onPressed: () => _openEditAgentDialog(agent), icon: const Icon(Icons.edit_outlined)),
                        IconButton(tooltip: 'Remove agent', onPressed: _agentEndpoints.length > 1 ? () => _removeAgent(agent) : null, icon: const Icon(Icons.delete_outline)),
                      ],
                    ),
                    onTap: () => _switchAgent(agent.id),
                  );
                },
              ),
              if (_currentAgent() != null && !_isLocalAgentHost(_currentAgent()!.host) && _agentTokenMissing) ...[
                const SizedBox(height: 12),
                Text(
                  _currentAgent()!.useLocalToken ? 'Local token not available.' : 'Token is required for this remote agent.',
                  style: Theme.of(context).textTheme.bodySmall?.copyWith(color: Theme.of(context).colorScheme.error),
                ),
              ] else if (_currentAgent() != null && _isLocalAgentHost(_currentAgent()!.host)) ...[
                const SizedBox(height: 12),
                Text('Local agent uses the token from the local filesystem.', style: Theme.of(context).textTheme.bodyMedium),
              ],
            ],
          ),
        ),
      ),
      const SizedBox(height: 24),
      Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text('Servers', style: Theme.of(context).textTheme.titleMedium),
                  FilledButton.icon(
                    onPressed: _agentReachable && !_agentAuthFailed && !_agentTokenMissing && !_isLoadingAgentSettings ? _createNewServer : null,
                    icon: const Icon(Icons.add),
                    label: const Text('Add server'),
                  ),
                ],
              ),
              const SizedBox(height: 12),
              if (_servers.isEmpty)
                Text(_agentReachable ? 'No servers added yet.' : 'No servers loaded because the agent is unreachable.', style: Theme.of(context).textTheme.bodyMedium)
              else
                ListView.separated(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  itemCount: _servers.length,
                  separatorBuilder: (_, _) => const Divider(height: 1),
                  itemBuilder: (context, index) {
                    final server = _servers[index];
                    final isSelected = server.id == _editingServerId;
                    return ListTile(
                      selected: isSelected,
                      title: Text(server.name),
                      subtitle: Text('SSH ${server.sshUser}@${server.sshHost}'),
                      trailing: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          IconButton(tooltip: 'Edit server', onPressed: () => _openEditServerDialog(server), icon: const Icon(Icons.edit_outlined)),
                          IconButton(tooltip: 'Delete server', onPressed: () => _confirmDeleteServer(server), icon: const Icon(Icons.delete_outline)),
                        ],
                      ),
                    );
                  },
                ),
              if (_servers.isNotEmpty) ...[
                const SizedBox(height: 20),
                Divider(color: colorScheme.outline.withValues(alpha: 0.2), height: 24),
                const SizedBox(height: 8),
                DropdownButtonFormField<String>(
                  initialValue: _preferredServerDropdownValue(_preferredBackupServerId),
                  decoration: const InputDecoration(labelText: 'Preferred backup server', prefixIcon: Icon(Icons.backup_outlined), border: OutlineInputBorder()),
                  items: [
                    const DropdownMenuItem(value: '', child: Text('No preference')),
                    ..._servers.map((server) => DropdownMenuItem(value: server.id, child: Text(server.name))),
                  ],
                  onChanged: _setPreferredBackupServerId,
                ),
                const SizedBox(height: 12),
                DropdownButtonFormField<String>(
                  initialValue: _preferredServerDropdownValue(_preferredRestoreServerId),
                  decoration: const InputDecoration(labelText: 'Preferred restore server', prefixIcon: Icon(Icons.restore_outlined), border: OutlineInputBorder()),
                  items: [
                    const DropdownMenuItem(value: '', child: Text('No preference')),
                    ..._servers.map((server) => DropdownMenuItem(value: server.id, child: Text(server.name))),
                  ],
                  onChanged: _setPreferredRestoreServerId,
                ),
              ],
            ],
          ),
        ),
      ),
      const SizedBox(height: 24),
      Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text('Storage', style: Theme.of(context).textTheme.titleMedium),
                  FilledButton.icon(
                    onPressed: _agentReachable && _storageWritable && !_agentAuthFailed && !_agentTokenMissing && !_isLoadingAgentSettings ? () => _openStorageEditor(createNew: true) : null,
                    icon: const Icon(Icons.add),
                    label: const Text('New storage'),
                  ),
                ],
              ),
              const SizedBox(height: 12),
              if (_agentSettings.storage.isEmpty)
                Text('No storage configured.', style: Theme.of(context).textTheme.bodyMedium)
              else
                ListView.separated(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  itemCount: _agentSettings.storage.length,
                  separatorBuilder: (_, _) => const Divider(height: 1),
                  itemBuilder: (context, index) {
                    final storage = _agentSettings.storage[index];
                    final isSelected = storage.id == _selectedBackupStorageId;
                    final isMandatoryFilesystem = storage.id == AppSettings.filesystemStorageId;
                    final canManageStorage = _agentReachable && _storageWritable && !_agentAuthFailed && !_agentTokenMissing && !_isLoadingAgentSettings;
                    return ListTile(
                      selected: isSelected,
                      leading: Icon(isSelected ? Icons.cloud_done_outlined : Icons.cloud_queue_outlined),
                      title: Text(storage.name),
                      subtitle: Text('${storage.driverId} - ${storage.enabled ? 'enabled' : 'disabled'}'),
                      trailing: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          IconButton(
                            tooltip: 'Edit storage',
                            onPressed: canManageStorage ? () => _openStorageEditor(initialStorage: storage) : null,
                            icon: const Icon(Icons.edit_outlined),
                          ),
                          IconButton(
                            tooltip: isMandatoryFilesystem ? 'Filesystem storage is required' : 'Delete storage',
                            onPressed: canManageStorage && !isMandatoryFilesystem ? () => _deleteStorageFromSettings(storage) : null,
                            icon: const Icon(Icons.delete_outline),
                          ),
                        ],
                      ),
                      onTap: canManageStorage ? () => _openStorageEditor(initialStorage: storage) : null,
                    );
                  },
                ),
            ],
          ),
        ),
      ),
      const SizedBox(height: 48),
    ];
  }

  Widget _buildVirtBackupAccountCard(ColorScheme colorScheme) {
    final accountEmail = _accountEmail;
    final isSignedIn = accountEmail != null && accountEmail.trim().isNotEmpty;
    return Card(
      elevation: 2,
      shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(Icons.account_circle_outlined, color: colorScheme.primary),
                const SizedBox(width: 10),
                Text('Virt Backup account', style: Theme.of(context).textTheme.titleMedium),
              ],
            ),
            const SizedBox(height: 16),
            if (_isLoadingAccountSession)
              Row(
                children: [
                  SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2, color: colorScheme.primary)),
                  const SizedBox(width: 12),
                  Text('Checking account session...', style: Theme.of(context).textTheme.bodyMedium),
                ],
              )
            else if (isSignedIn) ...[
              ListTile(contentPadding: EdgeInsets.zero, leading: const Icon(Icons.verified_user_outlined), title: Text(accountEmail), subtitle: const Text('Signed in')),
              Wrap(
                spacing: 12,
                runSpacing: 12,
                children: [
                  FilledButton.icon(
                    onPressed: () => _openVirtBackupAccountPage('/settings.html', fragment: 'subscription'),
                    icon: const Icon(Icons.open_in_new),
                    label: const Text('Manage Subscription'),
                  ),
                  OutlinedButton.icon(
                    onPressed: _isSigningOutAccount ? null : _signOutVirtBackupAccount,
                    icon: const Icon(Icons.logout),
                    label: Text(_isSigningOutAccount ? 'Signing out...' : 'Sign out'),
                  ),
                ],
              ),
            ] else ...[
              if (_accountStatusMessage.isNotEmpty) ...[Text(_accountStatusMessage, style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: colorScheme.error)), const SizedBox(height: 16)],
              Wrap(
                spacing: 12,
                runSpacing: 12,
                crossAxisAlignment: WrapCrossAlignment.center,
                children: [
                  FilledButton.icon(
                    onPressed: _isSigningInAccount ? null : _signInVirtBackupAccount,
                    icon: const Icon(Icons.open_in_browser),
                    label: Text(_isSigningInAccount ? 'Waiting for browser...' : 'Sign in with browser'),
                  ),
                  TextButton.icon(onPressed: () => _openVirtBackupAccountPage('/register.html'), icon: const Icon(Icons.person_add_alt_outlined), label: const Text('Create account')),
                  TextButton.icon(onPressed: () => _openVirtBackupAccountPage('/reset.html'), icon: const Icon(Icons.help_outline), label: const Text('Reset password')),
                ],
              ),
            ],
          ],
        ),
      ),
    );
  }
}
