import 'package:flutter/material.dart';

import 'package:virtbackup/gui/main_screen.dart';

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Virt Backup',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(colorScheme: ColorScheme.fromSeed(seedColor: Colors.blueGrey), useMaterial3: true),
      home: const BackupServerSetupScreen(),
    );
  }
}
