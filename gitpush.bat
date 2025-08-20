@echo off
REM =====================================================
REM  Commit + Push automatico su GitHub (branch corrente)
REM =====================================================

:: Vai nella cartella del repo
cd /d "C:\Users\francpao\Desktop\Programmi code\epu_app"

:: Chiede messaggio di commit
set /p msg=Inserisci messaggio commit: 

:: Aggiunge tutti i file modificati
git add .

:: Crea il commit
git commit -m "%msg%"

:: Recupera il branch corrente (in modo semplice)
for /f %%i in ('git symbolic-ref --short HEAD') do set branch=%%i

echo Branch corrente: %branch%

:: Fa il push sul branch corrente
git push origin %branch%

pause
