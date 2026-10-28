import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/gui/storage_dropdown.dart';

BackupStorage _storage(String id) => BackupStorage(id: id, name: id, driverId: 'sftp', enabled: true, params: const {}, uploadConcurrency: 1, downloadConcurrency: 1);

Widget _screen({required List<BackupStorage> storages, required String? selectedId, required ValueChanged<String?> onChanged}) => MaterialApp(
  home: Scaffold(
    body: StorageDropdown(
      storages: storages,
      selectedId: selectedId,
      decoration: const InputDecoration(labelText: 'Storage'),
      onChanged: onChanged,
    ),
  ),
);

void main() {
  const removed = 'dest_sftp_1793101380368028';
  testWidgets('a removed storage remains visibly unavailable without selecting another destination', (tester) async {
    final changes = <String?>[];
    await tester.pumpWidget(_screen(storages: [_storage('available')], selectedId: removed, onChanged: changes.add));
    expect(tester.takeException(), isNull);
    expect(find.textContaining('Unavailable storage'), findsWidgets);
    expect(find.text('Selected storage is unavailable. Choose an available storage.'), findsOneWidget);
    final dropdown = tester.widget<DropdownButtonFormField<String>>(find.byType(DropdownButtonFormField<String>));
    expect(dropdown.initialValue, removed);
    final button = tester.widget<DropdownButton<String>>(find.byType(DropdownButton<String>));
    expect(button.items!.singleWhere((item) => item.value == removed).enabled, isFalse);
    expect(changes, isEmpty);
  });

  testWidgets('removing a selected storage while mounted allows an explicit replacement', (tester) async {
    String? selected = removed;
    final available = _storage('available');
    void changed(String? value) => selected = value;
    await tester.pumpWidget(_screen(storages: [_storage(removed), available], selectedId: selected, onChanged: changed));
    await tester.pumpWidget(_screen(storages: [available], selectedId: selected, onChanged: changed));
    expect(tester.takeException(), isNull);
    expect(selected, removed);
    await tester.tap(find.byType(DropdownButtonFormField<String>));
    await tester.pumpAndSettle();
    await tester.tap(find.text('available (sftp)').last);
    await tester.pumpAndSettle();
    expect(selected, 'available');
    await tester.pumpWidget(_screen(storages: [available], selectedId: selected, onChanged: changed));
    expect(tester.takeException(), isNull);
    expect(find.textContaining('Unavailable storage'), findsNothing);
    expect(find.text('Selected storage is unavailable. Choose an available storage.'), findsNothing);
  });

  testWidgets('an externally changed selection resets retained form field state', (tester) async {
    final storages = [_storage('first'), _storage('second')];
    await tester.pumpWidget(_screen(storages: storages, selectedId: 'first', onChanged: (_) {}));
    await tester.pumpWidget(_screen(storages: storages, selectedId: 'second', onChanged: (_) {}));
    expect(tester.state<FormFieldState<String>>(find.byType(DropdownButtonFormField<String>)).value, 'second');
    await tester.pumpWidget(_screen(storages: [storages.first], selectedId: null, onChanged: (_) {}));
    expect(tester.takeException(), isNull);
    expect(tester.state<FormFieldState<String>>(find.byType(DropdownButtonFormField<String>)).value, isNull);
  });

  testWidgets('no available storages disables selection and preserves the missing destination warning', (tester) async {
    await tester.pumpWidget(_screen(storages: [], selectedId: removed, onChanged: (_) => fail('No selection expected.')));
    expect(tester.takeException(), isNull);
    final dropdown = tester.widget<DropdownButtonFormField<String>>(find.byType(DropdownButtonFormField<String>));
    expect(dropdown.onChanged, isNull);
    expect(find.textContaining('Unavailable storage'), findsWidgets);
  });
}
