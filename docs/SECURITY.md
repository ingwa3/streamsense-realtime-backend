# Sicherheitsbetrachtung

## Schutzbedarf

Der Demonstrator verarbeitet ausschließlich synthetische Sensorwerte und keine Namen oder direkten Personenkennungen. Trotzdem werden Integrität, Verfügbarkeit und nachvollziehbarer Zugriff geschützt. Bei einem realen Einsatz müsste zusätzlich geprüft werden, ob Raum- und Zeitmuster indirekte Rückschlüsse auf Anwesenheit ermöglichen.

## Rollen und Rechte

- `streamsense_writer`: nimmt valide Rohdaten und Metadaten abgelehnter Ereignisse auf.
- `streamsense_aggregator`: liest Rohdaten, schreibt Aggregate, Alarme und Checkpoints und führt Retention aus.
- `streamsense_reader`: besitzt nur Leserechte und wird von der Reporting-API verwendet.
- `streamsense_admin`: initialisiert die Datenbank und wird nicht an Anwendungscontainer weitergegeben.

Die Compose-Datei injiziert jedem Dienst nur die tatsächlich benötigten Umgebungsvariablen. App-Container laufen mit schreibgeschütztem Dateisystem, ohne zusätzliche Linux-Capabilities und mit `no-new-privileges`.

## Wesentliche Missbrauchs- und Fehlerszenarien

| Szenario | Gegenmaßnahme | Verbleibendes Risiko |
|---|---|---|
| Ungültiges oder manipuliertes Ereignis | JSON-Schema, Pydantic, DLQ und Rejection-Audit | Ein absichtlich sehr hohes Nachrichtenvolumen kann Ressourcen binden. |
| Wiederholung einer Kafka-Nachricht | Ereignis-ID sowie Topic/Partition/Offset sind eindeutig | DLQ-Nachrichten können bei einem Fehler zwischen DLQ-Zustellung und Offset-Commit erneut auftauchen. |
| Unbefugter API-Zugriff | API-Key und lokale Portbindung | Ein statischer Key ist für Produktion nicht ausreichend. |
| Zu weit reichender Datenbankzugriff | getrennte Rollen und Read-only-API | Alle Rollen teilen weiterhin dieselbe Datenbankinstanz. |
| Verlust eines lokalen Containers | persistente Volumes und Neustartregeln | Ein Broker und eine Datenbankinstanz bieten keine echte Hochverfügbarkeit. |
| Abhören interner Kafka-Kommunikation | isoliertes lokales Docker-Netzwerk | PLAINTEXT ist nur für den lokalen Demonstrator vertretbar; produktiv wären TLS/SASL nötig. |

## Produktive Erweiterungen

Für einen echten Betrieb wären TLS für Kafka und API, rotierende Secrets, zentraler Identity Provider, Audit-Logging, Backups, mehrere Kafka-Broker, Datenbank-Replikation, Netzwerkregeln und regelmäßige Wiederherstellungstests erforderlich.
