# Data-Governance-Festlegungen für StreamSense

Die folgenden Festlegungen übertragen die im Studienskript beschriebenen Rollen, Standards, Verantwortlichkeiten und Kontrollmechanismen auf den lokalen Demonstrator.

| Bereich | Festlegung im Projekt |
|---|---|
| Zweck | Technische Demonstration einer Near-Real-Time-Pipeline für synthetische Raumluftdaten. |
| Data Owner | Im realen Einsatz: Gebäudemanagement; im Demonstrator: Projektverantwortliche Person. |
| Data Steward | Data-Engineering-Verantwortliche Person; prüft Schema, Qualitätsregeln, Retention und Dokumentation. |
| Data Consumer | Reporting-Anwendung beziehungsweise berechtigte API-Nutzende. |
| Zeitzone | Alle Zeitstempel werden in UTC verarbeitet und gespeichert. |
| Einheiten | Temperatur in °C, Luftfeuchtigkeit in %, CO₂ in ppm, Batteriestand in %. |
| Ereignisschema | Versioniertes JSON-Schema `sensor-reading-v1.json`; Änderungen erfolgen nicht still, sondern über eine neue Version. |
| Datenherkunft | Topic, Partition und Offset werden zusammen mit jedem angenommenen Ereignis gespeichert. |
| Qualitätsregeln | Technische Validierung über JSON-Schema und Pydantic; fachliche Warnungen über konfigurierbare Wertebereiche. |
| Abgelehnte Daten | Ungültige Ereignisse gehen in das DLQ. In PostgreSQL werden Herkunft, Fehlergrund und Payload-Hash dokumentiert. |
| Aufbewahrung | Rohdaten standardmäßig 30 Tage, Aggregate 365 Tage; Werte sind konfigurierbar. |
| Zugriff | Getrennte Rollen: Writer für Ingestion, Aggregator für Berechnung/Retention, Reader für die Reporting-API. |
| Kontrolle | Unit-Tests, CI, Prometheus-Metriken, Checkpoints, strukturierte Logs und Testprotokoll. |
| Schemaänderungen | Nummerierte Initialisierungsdateien im Demonstrator; produktiv wäre ein geprüftes Migrationsverfahren erforderlich. |

## Bewusste Grenze

Die Dienste verwenden lokal einen gemeinsamen PostgreSQL-Speicher. Das ist einfacher reproduzierbar, entspricht aber nicht vollständig dem Database-per-Service-Ideal. Die stärkere Kopplung wird durch klare Tabellenverantwortung, getrennte Rollen und eine ausschließlich lesende API begrenzt.
