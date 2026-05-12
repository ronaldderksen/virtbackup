part of 'main_screen.dart';

extension _BackupServerSetupManageSection on _BackupServerSetupScreenState {
  List<Widget> _buildManageSection(ColorScheme colorScheme) {
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
              Text('Manage', style: Theme.of(context).textTheme.titleMedium),
              const SizedBox(height: 16),
              ...[
                Row(
                  children: [
                    Expanded(
                      child: DropdownButtonFormField<String>(
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
                    ),
                    const SizedBox(width: 12),
                    IconButton(tooltip: 'Refresh selected server', onPressed: _editingServerId == null || _isRefreshingServer ? null : _refreshSelectedServer, icon: const Icon(Icons.refresh)),
                  ],
                ),
                const SizedBox(height: 16),
                if (_servers.isEmpty)
                  Text('Add a server first to view its VMs.', style: Theme.of(context).textTheme.bodyMedium)
                else ...[
                  Builder(
                    builder: (context) {
                      final server = _getSelectedServer();
                      final missingTools = server == null ? const <String>[] : _missingToolsByServerId[server.id] ?? const <String>[];
                      return Row(
                        children: [
                          Text('Virtual machines', style: Theme.of(context).textTheme.titleMedium),
                          const SizedBox(width: 12),
                          Expanded(
                            child: Text(
                              missingTools.isEmpty ? _formatLastRefresh(server) : '${_formatLastRefresh(server)} • Missing tools: ${missingTools.join(', ')}',
                              style: Theme.of(context).textTheme.bodySmall?.copyWith(color: missingTools.isEmpty ? null : colorScheme.error),
                            ),
                          ),
                        ],
                      );
                    },
                  ),
                  const SizedBox(height: 12),
                  if (_vmActionStatusMessage.isNotEmpty) ...[
                    Row(
                      children: [
                        const SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2)),
                        const SizedBox(width: 12),
                        Expanded(child: Text(_vmActionStatusMessage, style: Theme.of(context).textTheme.bodyMedium)),
                      ],
                    ),
                    const SizedBox(height: 12),
                  ],
                  Builder(
                    builder: (context) {
                      final server = _getSelectedServer();
                      final vms = server == null ? null : _vmCacheByServerId[server.id];
                      final missingTools = server == null ? const <String>[] : _missingToolsByServerId[server.id] ?? const <String>[];
                      if (missingTools.isNotEmpty) {
                        return Text('Missing required remote tools: ${missingTools.join(', ')}.', style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: colorScheme.error));
                      }
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
                                Wrap(
                                  spacing: 8,
                                  runSpacing: 4,
                                  alignment: WrapAlignment.end,
                                  children: [
                                    if (!isRunning) TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _renameVm(server, vm), child: const Text('Rename')),
                                    if (!isRunning) TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.start), child: const Text('Run')),
                                    if (isRunning) ...[
                                      TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.reboot), child: const Text('Reboot')),
                                      TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.shutdown), child: const Text('Shutdown')),
                                      TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.forceOff), child: const Text('Force off')),
                                    ],
                                    if (isRunning)
                                      TextButton(onPressed: _isVmActionRunning || server == null ? null : () => _runVmAction(server, vm, VmAction.forceReset), child: const Text('Force reset')),
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

  Future<void> _renameVm(ServerConfig server, VmEntry vm) async {
    if (_isVmActionRunning) {
      return;
    }
    _updateUi(() {
      _isVmActionRunning = true;
      _vmActionStatusMessage = 'Preparing rename for ${vm.name}...';
    });
    late final Map<String, dynamic> preview;
    try {
      if (server.connectionType != ConnectionType.ssh) {
        throw 'Rename is only available for SSH-managed VMs.';
      }
      preview = await _agentApiClient.previewVmRename(server.id, vm.name);
    } catch (error, stackTrace) {
      _logError('VM rename preview failed.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Rename preview failed: $error');
      }
      return;
    } finally {
      if (mounted) {
        _updateUi(() {
          _isVmActionRunning = false;
          _vmActionStatusMessage = '';
        });
      }
    }
    if (!mounted) {
      return;
    }
    final request = await _showVmRenameDialog(vm.name, preview);
    if (request == null || !mounted) {
      return;
    }
    _updateUi(() {
      _isVmActionRunning = true;
      _vmActionStatusMessage = 'Checking rename targets for ${vm.name}...';
    });
    try {
      _updateUi(() {
        _vmActionStatusMessage = 'Renaming ${vm.name}...';
      });
      await _agentApiClient.applyVmRename(server.id, vmName: vm.name, newVmName: request.vmName, disks: request.disks);
      _updateUi(() {
        _vmActionStatusMessage = 'Refreshing VM inventory...';
      });
      await _loadVmInventory(server);
      if (mounted) {
        _showSnackBarInfo('VM renamed.');
      }
    } catch (error, stackTrace) {
      _logError('VM rename failed.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Rename failed: $error');
      }
    } finally {
      if (mounted) {
        _updateUi(() {
          _isVmActionRunning = false;
          _vmActionStatusMessage = '';
        });
      }
    }
  }

  Future<_VmRenameRequest?> _showVmRenameDialog(String currentVmName, Map<String, dynamic> preview) async {
    final rawDisks = preview['disks'];
    if (rawDisks is! List) {
      _showSnackBarError('Rename preview did not include disks.');
      return null;
    }
    final disks = rawDisks.whereType<Map>().map(_VmRenameDiskPreview.fromMap).toList();
    if (disks.isEmpty) {
      _showSnackBarError('Rename preview did not include file disks.');
      return null;
    }
    final vmNameController = TextEditingController(text: currentVmName);
    final diskControllers = {for (final disk in disks) disk.target: TextEditingController(text: disk.fileName)};
    for (final disk in disks) {
      diskControllers[disk.target]!.text = _autoVmRenameDiskFileName(vmName: currentVmName, disk: disk);
    }
    try {
      return await showDialog<_VmRenameRequest>(
        context: context,
        barrierDismissible: false,
        builder: (dialogContext) {
          String? dialogError;
          var autoDiskName = true;
          void applyAutoDiskNames() {
            final vmName = vmNameController.text.trim();
            for (final disk in disks) {
              diskControllers[disk.target]!.text = _autoVmRenameDiskFileName(vmName: vmName, disk: disk);
            }
          }

          return StatefulBuilder(
            builder: (context, setDialogState) {
              return AlertDialog(
                title: const Text('Rename VM'),
                content: SizedBox(
                  width: 720,
                  child: SingleChildScrollView(
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        TextField(
                          controller: vmNameController,
                          decoration: const InputDecoration(labelText: 'VM name', border: OutlineInputBorder()),
                          onChanged: (_) {
                            if (!autoDiskName) {
                              return;
                            }
                            setDialogState(applyAutoDiskNames);
                          },
                        ),
                        const SizedBox(height: 16),
                        CheckboxListTile(
                          value: autoDiskName,
                          contentPadding: EdgeInsets.zero,
                          title: const Text('Auto disk names'),
                          controlAffinity: ListTileControlAffinity.leading,
                          onChanged: (value) {
                            setDialogState(() {
                              autoDiskName = value == true;
                              if (autoDiskName) {
                                applyAutoDiskNames();
                              }
                            });
                          },
                        ),
                        const SizedBox(height: 8),
                        Text('Disk file names', style: Theme.of(context).textTheme.titleSmall),
                        const SizedBox(height: 8),
                        for (final disk in disks) ...[
                          Container(
                            padding: const EdgeInsets.symmetric(vertical: 8),
                            decoration: BoxDecoration(
                              border: Border(top: BorderSide(color: Theme.of(context).dividerColor)),
                            ),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(disk.target, style: Theme.of(context).textTheme.labelLarge),
                                const SizedBox(height: 4),
                                Text(disk.directory, maxLines: 1, overflow: TextOverflow.ellipsis, style: Theme.of(context).textTheme.bodySmall),
                                const SizedBox(height: 8),
                                TextField(
                                  controller: diskControllers[disk.target],
                                  decoration: const InputDecoration(labelText: 'File name', border: OutlineInputBorder()),
                                ),
                              ],
                            ),
                          ),
                        ],
                        if (dialogError != null) ...[
                          const SizedBox(height: 12),
                          Text(dialogError!, style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: Theme.of(context).colorScheme.error)),
                        ],
                      ],
                    ),
                  ),
                ),
                actions: [
                  TextButton(onPressed: () => Navigator.of(dialogContext).pop(), child: const Text('Cancel')),
                  FilledButton(
                    onPressed: () {
                      final vmName = vmNameController.text.trim();
                      final error = _validateVmRenameDialogInput(currentVmName: currentVmName, newVmName: vmName, disks: disks, diskControllers: diskControllers);
                      if (error != null) {
                        setDialogState(() {
                          dialogError = error;
                        });
                        return;
                      }
                      Navigator.of(dialogContext).pop(
                        _VmRenameRequest(
                          vmName: vmName,
                          disks: [
                            for (final disk in disks) {'target': disk.target, 'fileName': diskControllers[disk.target]!.text.trim()},
                          ],
                        ),
                      );
                    },
                    child: const Text('Apply'),
                  ),
                ],
              );
            },
          );
        },
      );
    } finally {
      vmNameController.dispose();
      for (final controller in diskControllers.values) {
        controller.dispose();
      }
    }
  }

  String? _validateVmRenameDialogInput({
    required String currentVmName,
    required String newVmName,
    required List<_VmRenameDiskPreview> disks,
    required Map<String, TextEditingController> diskControllers,
  }) {
    if (newVmName.isEmpty || newVmName.contains('/') || newVmName.contains('\\') || newVmName.contains(RegExp(r'[\r\n\u0000]'))) {
      return 'Enter a valid VM name.';
    }
    var hasChanges = newVmName != currentVmName;
    for (final disk in disks) {
      final fileName = diskControllers[disk.target]!.text.trim();
      if (fileName.isEmpty || fileName == '.' || fileName == '..' || fileName.contains('/') || fileName.contains('\\') || fileName.contains(RegExp(r'[\r\n\u0000]'))) {
        return 'Enter a valid file name for ${disk.target}.';
      }
      if (fileName != disk.fileName) {
        hasChanges = true;
      }
    }
    if (!hasChanges) {
      return 'Change the VM name or at least one disk file name.';
    }
    return null;
  }

  String _autoVmRenameDiskFileName({required String vmName, required _VmRenameDiskPreview disk}) {
    final baseName = vmName.trim();
    final extension = _vmRenameDiskExtension(disk.fileName);
    return '$baseName-${disk.target}$extension';
  }

  String _vmRenameDiskExtension(String fileName) {
    final index = fileName.lastIndexOf('.');
    if (index <= 0 || index == fileName.length - 1) {
      return '';
    }
    return fileName.substring(index);
  }
}

class _VmRenameDiskPreview {
  const _VmRenameDiskPreview({required this.target, required this.directory, required this.fileName});

  final String target;
  final String directory;
  final String fileName;

  factory _VmRenameDiskPreview.fromMap(Map<dynamic, dynamic> value) {
    return _VmRenameDiskPreview(target: (value['target'] ?? '').toString(), directory: (value['directory'] ?? '').toString(), fileName: (value['fileName'] ?? '').toString());
  }
}

class _VmRenameRequest {
  const _VmRenameRequest({required this.vmName, required this.disks});

  final String vmName;
  final List<Map<String, String>> disks;
}
