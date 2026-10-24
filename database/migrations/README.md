# Schemaänderungen

Die Dateien in `database/init/` initialisieren eine neue lokale PostgreSQL-Datenbank. Sie werden vom offiziellen PostgreSQL-Container nur beim ersten Start eines leeren Volumes ausgeführt.

Für den Demonstrator werden Änderungen als nummerierte SQL- beziehungsweise Shell-Dateien versioniert. Nach einer Änderung ist lokal ein kontrollierter Neustart mit leerem Volume möglich:

```bash
make reset
make up
```

In einer Produktivumgebung würde ich statt eines Volume-Resets ein Migrationswerkzeug einsetzen. Migrationen müssten vor dem Deployment getestet, rückwärtskompatibel geplant und mit einem Rollback-Verfahren dokumentiert werden.
