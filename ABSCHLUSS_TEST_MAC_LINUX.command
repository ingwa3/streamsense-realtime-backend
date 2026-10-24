#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if command -v python3 >/dev/null 2>&1; then
  python3 scripts/abschluss_test.py
elif command -v python >/dev/null 2>&1; then
  python scripts/abschluss_test.py
else
  echo "Python 3 wurde nicht gefunden."
  exit 1
fi
echo "Das Ergebnis steht in docs/TESTERGEBNISSE_AUTOMATISCH.md"
