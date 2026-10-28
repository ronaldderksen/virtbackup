import 'package:flutter/material.dart';
import 'package:virtbackup/common/models.dart';

class StorageDropdown extends StatelessWidget {
  const StorageDropdown({super.key, required this.storages, required this.selectedId, required this.decoration, required this.onChanged, this.showDriverId = true});

  final List<BackupStorage> storages;
  final String? selectedId;
  final InputDecoration decoration;
  final ValueChanged<String?>? onChanged;
  final bool showDriverId;

  @override
  Widget build(BuildContext context) {
    final value = selectedId == null || selectedId!.isEmpty ? null : selectedId;
    final unavailable = value != null && !storages.any((storage) => storage.id == value);
    return DropdownButtonFormField<String>(
      key: ValueKey(value),
      initialValue: value,
      isExpanded: true,
      decoration: unavailable ? decoration.copyWith(errorText: 'Selected storage is unavailable. Choose an available storage.') : decoration,
      items: [
        for (final storage in storages)
          DropdownMenuItem(
            value: storage.id,
            child: Text(showDriverId ? '${storage.name} (${storage.driverId})' : storage.name, overflow: TextOverflow.ellipsis),
          ),
        if (unavailable)
          DropdownMenuItem(
            value: value,
            enabled: false,
            child: Text('Unavailable storage ($value)', overflow: TextOverflow.ellipsis),
          ),
      ],
      onChanged: storages.isEmpty ? null : onChanged,
    );
  }
}
