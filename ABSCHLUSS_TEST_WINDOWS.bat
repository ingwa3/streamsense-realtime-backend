@echo off
setlocal
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (
  py -3 scripts\abschluss_test.py
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    python scripts\abschluss_test.py
  ) else (
    echo Python 3 wurde nicht gefunden. Bitte Python 3 installieren oder den Test in VS Code starten.
    exit /b 1
  )
)
echo.
echo Das Ergebnis steht in docs\TESTERGEBNISSE_AUTOMATISCH.md
pause
