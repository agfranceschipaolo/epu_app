@echo off
cd /d "C:\Users\francpao\Desktop\Programmi code\epu_app"
echo Avvio ambiente virtuale...
call venv\Scripts\activate.bat
echo Avvio applicazione Streamlit...
python -m streamlit run App.py
pause
