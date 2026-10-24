# Testprotokoll – vor der Abgabe ausfüllen

**Rechner/Betriebssystem:** [eintragen]  
**Docker-Version:** [eintragen]  
**Datum/Uhrzeit:** [eintragen]  
**Commit-ID:** [eintragen]

## 1. Start und Reproduzierbarkeit

| Prüfung | Ergebnis / Messwert |
|---|---|
| `docker compose up -d --build` erfolgreich | [ja/nein + Hinweise] |
| Alle Healthchecks grün | [ja/nein + Wartezeit] |
| Smoke-Test erfolgreich | [ja/nein + Ausgabe] |
| Neustart einzelner Dienste getestet | [Dienst + Beobachtung] |

## 2. Million-Event-Test

| Messgröße | Eigener Wert |
|---|---|
| erzeugte Ereignisse | [eintragen] |
| gespeicherte Rohereignisse | [eintragen] |
| abgelehnte Ereignisse | [eintragen] |
| Gesamtlaufzeit | [eintragen] |
| mittlerer Durchsatz | [eintragen] |
| maximaler Consumer-Lag | [eintragen] |
| Dauer eines Aggregationslaufs | [p50/p95 oder Min/Max eintragen] |
| erzeugte 1-Minuten-Fenster | [eintragen] |
| erzeugte 5-Minuten-Fenster | [eintragen] |
| Warnungen | [eintragen] |

## 3. Late-Data-Test

```bash
make late-demo
```

Dokumentiere, ob verspätete Ereignisse nach dem nächsten Lookback-Lauf in den betreffenden Fenstern enthalten sind. Notiere mindestens ein konkretes Fenster vor und nach der Neuberechnung.

## 4. Fehlerfall

Stoppe während der Verarbeitung einen Dienst, zum Beispiel den Persister oder PostgreSQL, und beschreibe:

- Was ist ausgefallen?
- Welche Daten wurden gepuffert oder erneut verarbeitet?
- Gab es Duplikate in `sensor_readings`?
- Wie lange dauerte die Wiederherstellung?
- Welche Verbesserung würdest du daraus ableiten?

Keine Werte schätzen. Nur tatsächlich beobachtete Ergebnisse in das Full Abstract übernehmen.
