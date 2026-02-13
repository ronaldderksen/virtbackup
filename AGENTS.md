# AGENTS.md

## Project Rules
- App-taal is altijd Engels in de UI.
- Onze communicatie is in het Nederlands.
- Volg altijd de regels uit `analysis_options.yaml` (inclusief de lints in `package:flutter_lints/flutter.yaml`).
- Formatteer alle Dart-files met `dart format --line-length 200`.
- Gebruik nooit environment variabelen voor defaults.
- Gebruik geen `if (driverId == ...)` checks; driver-specifieke opties lopen via `BackupDriverCapabilities` params.
- Maak NOOIT fallbacks (defaults, alternatieve paden/waarden of stilzwijgende terugval) zonder expliciete toestemming van de gebruiker.
- Draai na iedere change `dart analyze`.
- Los alle `dart analyze` infos/warnings/errors op.
- Houd de `doc/` directory actueel wanneer functionaliteit wijzigt of toegevoegd wordt.
