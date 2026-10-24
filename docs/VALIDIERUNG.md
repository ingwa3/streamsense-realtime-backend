# Technischer Validierungsstand

## Automatisierte statische Prüfungen

Folgende Prüfungen sind im Projekt reproduzierbar:

```bash
python -m compileall -q src tests scripts
python -m pytest -q
python scripts/validate_compose_and_schema.py
```

Der aktuelle Projektstand enthält zehn Unit-Tests. Sie prüfen Ereignismodell, Generator, reproduzierbare IDs, gezielt verspätete Ereignisse, Fenstergrenzen und JSON-Schema. Die zusätzliche Strukturprüfung kontrolliert Docker Compose, JSON-Schema, Datenbankschema und Rollenmodell.

## Container-, Ende-zu-Ende- und Lasttest

Der vollständige lokale Test wird über einen der Starter im Projektstamm oder direkt ausgeführt:

```bash
python scripts/abschluss_test.py
```

Der Ablauf startet Kafka, PostgreSQL, Persister, Aggregator und API, erzeugt standardmäßig genau 1.000.000 Ereignisse, wartet auf deren Speicherung und schreibt ein maschinenlesbares sowie ein lesbares Testprotokoll in den Ordner `docs`.

Die Messwerte sind hardware- und umgebungsabhängig. Deshalb enthält das Repository keine erfundenen Standardwerte. Nach dem lokalen Lauf stehen die tatsächlich gemessenen Werte in:

- `docs/TESTERGEBNISSE_AUTOMATISCH.md`
- `docs/TESTERGEBNISSE_AUTOMATISCH.json`
