import os, shutil, sys, subprocess
from datetime import datetime
from pathlib import Path

BASE_DIR   = Path(r"C:/Users/taban/Programmi prova/epu_app")
APP_FILE   = BASE_DIR / "app.py"
DB_FILE    = BASE_DIR / "epu.db"
BACKUP_DIR = BASE_DIR / "backup_epu"

BACKUP_DIR.mkdir(exist_ok=True)

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
app_backup = BACKUP_DIR / f"app_{ts}.py"
db_backup  = BACKUP_DIR / f"epu_{ts}.db"

shutil.copy2(APP_FILE, app_backup)
shutil.copy2(DB_FILE, db_backup)
print(f"✅ Backup completato:\n- {app_backup}\n- {db_backup}")

# Avvia Streamlit in background, staccato dalla console (Windows)
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
creationflags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP

# Usa l'interprete corrente del venv
py = sys.executable
cmd = [py, "-m", "streamlit", "run", str(APP_FILE)]

# Inoltra le variabili d'ambiente correnti (include il venv)
env = os.environ.copy()

try:
    subprocess.Popen(cmd, creationflags=creationflags, env=env, cwd=str(BASE_DIR))
    print("🚀 Streamlit avviato in background. Apri: http://localhost:8501")
except Exception as e:
    print(f"⚠️ Errore nell'avvio di Streamlit: {e}")
