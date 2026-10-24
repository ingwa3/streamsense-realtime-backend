# StreamSense – Testprotokoll

Testdatum: 24.10.2026
Testumgebung: Windows, Docker Desktop, Docker Compose

## 1. Lasttest – 1.000.000 Ereignisse

- Testproducer: streamsense-millionentest
- Erzeugte und eingereihte Ereignisse: 1.000.000
- Nicht zugestellte Ereignisse: 0
- Zustellfehler: 0
- Producer-Exit-Code: 0
- Ergebnis: Erfolgreich

## 2. PostgreSQL

- Tabelle: sensor_readings
- Per SQL COUNT(*) nachgewiesene Messungen: 1.005.638
- Ergebnis: Mindestanforderung von 1.000.000 Datensaetzen erreicht

Hinweis: Der Tabellenbestand enthaelt auch Daten aus dem regulaeren Producer.
Die Zahl ist daher kein isolierter Zaehler des Millionentests.

## 3. Automatische Tests

- Testwerkzeug: pytest 9.0.2
- Ausfuehrung: separater Python-3.12-Docker-Container
- Testdateien: test_generator.py, test_json_schema.py, test_models.py, test_windowing.py
- Ergebnis: 10 Tests erfolgreich (100 %)

## 4. Docker und Kafka

- Kafka, PostgreSQL, Producer, Persister, Aggregator und API: healthy
- Kafka-Topic-Initialisierung: Exit-Code 0
- Topics: sensor-readings.v1 und sensor-readings.dlq.v1
- Prometheus: running, Bereitschaftsendpunkt erfolgreich

## 5. Fazit

Der durchgefuehrte Lasttest bestaetigt die erfolgreiche Uebertragung von
1.000.000 Ereignissen an Kafka. In PostgreSQL wurden mehr als 1.000.000
gespeicherte Sensormessungen nachgewiesen. Alle 10 automatischen Tests
waren erfolgreich. Ein vollstaendiger Nachweis der Ende-zu-Ende-Zuordnung
jedes einzelnen Testereignisses wurde nicht durchgefuehrt.
