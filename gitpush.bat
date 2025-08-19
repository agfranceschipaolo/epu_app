@echo off
REM =====================================================
REM  Script per fare commit + push automatico su GitHub
REM  Rileva automaticamente il branch corrente
REM =====================================================

:: Chiede messaggio di commit
set /p msg=Inserisci messaggio commit: 

:: Aggiunge tutti i file modificati
git add .

:: Crea il commit
git commit -m "%msg%"

:: Recupera il branch corrente
for /f "tokens=*" %%i in ('git rev-parse --abbrev-ref HEAD') do set branch=%%i

echo Branch corrente: %branch%

:: Fa il push sul branch corrente
git push origin %branch%

pause
