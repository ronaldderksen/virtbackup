part of 'main_screen.dart';

extension _BackupServerSetupSettingsSection on _BackupServerSetupScreenState {
  List<Widget> _buildSettingsSection(ColorScheme colorScheme) {
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
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text('Agent', style: Theme.of(context).textTheme.titleMedium),
                  Row(
                    children: [
                      IconButton(tooltip: 'Remove agent', onPressed: _agentEndpoints.length > 1 ? _removeSelectedAgent : null, icon: const Icon(Icons.delete_outline)),
                      const SizedBox(width: 4),
                      FilledButton.icon(onPressed: _showAddAgentDialog, icon: const Icon(Icons.add), label: const Text('Add agent')),
                    ],
                  ),
                ],
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: ValueKey(_selectedAgentId),
                initialValue: _selectedAgentId,
                items: _agentEndpoints.map((agent) => DropdownMenuItem<String>(value: agent.id, child: Text(agent.label))).toList(),
                onChanged: (value) => _switchAgent(value),
                decoration: const InputDecoration(labelText: 'Agent address', border: OutlineInputBorder(), prefixIcon: Icon(Icons.cloud_outlined)),
              ),
              const SizedBox(height: 16),
              if (_currentAgent() != null && !_isLocalAgentHost(_currentAgent()!.host)) ...[
                CheckboxListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('Use local token'),
                  value: _currentAgent()!.useLocalToken,
                  onChanged: (value) => _toggleUseLocalToken(value ?? false),
                ),
                if (_currentAgent()!.useLocalToken) ...[
                  Text('Using local agent.token from filesystem.', style: Theme.of(context).textTheme.bodyMedium),
                  if (_agentTokenMissing) ...[
                    const SizedBox(height: 8),
                    Text('Local token not available.', style: Theme.of(context).textTheme.bodySmall?.copyWith(color: Theme.of(context).colorScheme.error)),
                  ],
                ] else ...[
                  const SizedBox(height: 8),
                  TextFormField(
                    controller: _agentTokenController,
                    decoration: InputDecoration(
                      labelText: 'Agent token',
                      border: const OutlineInputBorder(),
                      prefixIcon: const Icon(Icons.key_outlined),
                      errorText: _agentTokenMissing ? 'Token is required for remote agents' : null,
                    ),
                    obscureText: true,
                    onChanged: _updateSelectedAgentToken,
                  ),
                ],
              ] else ...[
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
                  FilledButton.icon(onPressed: _createNewServer, icon: const Icon(Icons.add), label: const Text('Add server')),
                ],
              ),
              const SizedBox(height: 12),
              if (_servers.isEmpty)
                Text('No servers added yet.', style: Theme.of(context).textTheme.bodyMedium)
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
                      trailing: IconButton(tooltip: 'Delete server', onPressed: () => _confirmDeleteServer(server), icon: const Icon(Icons.delete_outline)),
                      onTap: () => _selectServer(server),
                    );
                  },
                ),
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
          child: Form(
            key: _connectionFormKey,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Connection', style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 16),
                TextFormField(
                  controller: _serverNameController,
                  decoration: const InputDecoration(labelText: 'Server name', hintText: 'Libvirt host', prefixIcon: Icon(Icons.badge_outlined), border: OutlineInputBorder()),
                  textInputAction: TextInputAction.next,
                  validator: (value) {
                    if (_allowEmptyServerNameForTest) {
                      return null;
                    }
                    if (value == null || value.trim().isEmpty) {
                      return 'Enter a server name';
                    }
                    return null;
                  },
                ),
                const SizedBox(height: 16),
                InputDecorator(
                  decoration: const InputDecoration(
                    labelText: 'Libvirt host (derived)',
                    helperText: 'Based on connection method',
                    prefixIcon: Icon(Icons.storage_outlined),
                    border: OutlineInputBorder(),
                  ),
                  child: SelectableText(_buildLibvirtHost().isEmpty ? 'Complete the connection details to generate the host.' : _buildLibvirtHost()),
                ),
                const SizedBox(height: 16),
                ...[
                  TextFormField(
                    controller: _sshHostController,
                    decoration: const InputDecoration(labelText: 'SSH host', hintText: '10.0.0.15', prefixIcon: Icon(Icons.dns_outlined), border: OutlineInputBorder()),
                    textInputAction: TextInputAction.next,
                    validator: (value) {
                      if (value == null || value.trim().isEmpty) {
                        return 'Enter an SSH host';
                      }
                      return null;
                    },
                  ),
                  const SizedBox(height: 16),
                  Row(
                    children: [
                      Expanded(
                        child: TextFormField(
                          controller: _sshUserController,
                          decoration: const InputDecoration(labelText: 'SSH user', hintText: 'root', prefixIcon: Icon(Icons.person_outline), border: OutlineInputBorder()),
                          textInputAction: TextInputAction.next,
                          validator: (value) {
                            if (value == null || value.trim().isEmpty) {
                              return 'Enter an SSH user';
                            }
                            return null;
                          },
                        ),
                      ),
                      const SizedBox(width: 16),
                      SizedBox(
                        width: 120,
                        child: TextFormField(
                          controller: _sshPortController,
                          decoration: const InputDecoration(labelText: 'Port', prefixIcon: Icon(Icons.pin_outlined), border: OutlineInputBorder()),
                          keyboardType: TextInputType.number,
                          textInputAction: TextInputAction.next,
                          validator: (value) {
                            final parsed = int.tryParse(value ?? '');
                            if (parsed == null || parsed <= 0 || parsed > 65535) {
                              return 'Use 1-65535';
                            }
                            return null;
                          },
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  TextFormField(
                    controller: _sshPasswordController,
                    decoration: const InputDecoration(labelText: 'SSH password', prefixIcon: Icon(Icons.lock_outline_rounded), border: OutlineInputBorder()),
                    obscureText: true,
                    textInputAction: TextInputAction.done,
                    validator: (value) {
                      if (value == null || value.trim().isEmpty) {
                        return 'Enter a password';
                      }
                      return null;
                    },
                  ),
                ],
                const SizedBox(height: 20),
                FilledButton.icon(onPressed: _isTesting ? null : _testConnection, icon: const Icon(Icons.link_outlined), label: Text(_isTesting ? 'Testing...' : 'Test connection')),
              ],
            ),
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
          child: Form(
            key: _localFormKey,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Local settings', style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 16),
                TextFormField(
                  controller: _backupPathController,
                  decoration: InputDecoration(
                    labelText: 'Backup base path',
                    hintText: '/mnt/backups',
                    helperText: 'VirtBackup will create a "VirtBackup" folder inside this path.',
                    prefixIcon: Icon(Icons.folder_outlined),
                    suffixIcon: SizedBox(
                      width: 48,
                      child: Row(
                        children: [IconButton(tooltip: 'Browse folders', onPressed: _pickBackupFolder, icon: const Icon(Icons.folder_open_outlined))],
                      ),
                    ),
                    border: OutlineInputBorder(),
                  ),
                  textInputAction: TextInputAction.done,
                  validator: (value) => (value == null || value.trim().isEmpty) ? 'Enter a base path' : null,
                ),
                const SizedBox(height: 16),
                Container(
                  width: double.infinity,
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.45), borderRadius: BorderRadius.circular(12)),
                  child: const Text('Backup destinations are managed from the Backup tab (Destination -> Manage).'),
                ),
                const SizedBox(height: 20),
                Divider(color: colorScheme.outline.withValues(alpha: 0.2), height: 24),
                const SizedBox(height: 8),
                TextFormField(
                  controller: _ntfymeTokenController,
                  decoration: const InputDecoration(
                    labelText: 'Ntfy me token',
                    helperText: 'Used to send Ntfy me job notifications.',
                    prefixIcon: Icon(Icons.notifications_outlined),
                    border: OutlineInputBorder(),
                  ),
                  textInputAction: TextInputAction.done,
                ),
                const SizedBox(height: 12),
                Row(
                  children: [
                    FilledButton.icon(
                      onPressed: _isSendingNtfymeTest ? null : _sendNtfymeTestMessage,
                      icon: const Icon(Icons.send_outlined),
                      label: Text(_isSendingNtfymeTest ? 'Sending...' : 'Send test message'),
                    ),
                    const SizedBox(width: 12),
                    TextButton.icon(onPressed: _openNtfymeDocs, icon: const Icon(Icons.open_in_new), label: const Text('Open Ntfy me docs')),
                  ],
                ),
              ],
            ),
          ),
        ),
      ),
      const SizedBox(height: 48),
      if (kDebugMode) ...[
        Card(
          elevation: 2,
          shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
          child: Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Debug', style: Theme.of(context).textTheme.titleMedium),
                const SizedBox(height: 12),
                Text('Development helpers for testing connection gating.', style: Theme.of(context).textTheme.bodyMedium),
                const SizedBox(height: 16),
                TextButton(onPressed: _clearConnectionVerified, child: const Text('Clear server_verified')),
              ],
            ),
          ),
        ),
        const SizedBox(height: 48),
      ],
    ];
  }
}
