import os, shutil, sys, subprocess
from datetime import datetime
from pathlib import Path

# === CONFIG ===
BASE_DIR   = Path(r"C:/Users/taban/Programmi prova/epu_app")
PORT       = 8501
HEADLESS   = True

# Trova App file in modo robusto (App.py o app.py)
_candidates = [BASE_DIR / "App.py", BASE_DIR / "app.py"]
APP_FILE = next((p for p in _candidates if p.exists()), None)
if not APP_FILE:
    raise FileNotFoundError(f"File Streamlit non trovato in {BASE_DIR} (cercati: App.py, app.py)")

# DB: usa epu.db di default (o quello che preferisci)
DB_FILE    = BASE_DIR / "epu.db"
BACKUP_DIR = BASE_DIR / "backup_epu"
BACKUP_DIR.mkdir(exist_ok=True)

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
app_backup = BACKUP_DIR / f"app_{ts}.py"
db_backup  = BACKUP_DIR / f"epu_{ts}.db"

# === BACKUP SICURI ===
if APP_FILE.exists():
    shutil.copy2(APP_FILE, app_backup)
else:
    print(f"⚠️ App non trovata per backup: {APP_FILE}")

if DB_FILE.exists():
    shutil.copy2(DB_FILE, db_backup)
    print(f"✅ Backup completato:\n- {app_backup}\n- {db_backup}")
else:
    print(f"ℹ️ Nessun DB da backuppare (creato al volo all'avvio se serve): {DB_FILE}")

# === AVVIO STREAMLIT DETACHED (WINDOWS) ===
DETACHED_PROCESS        = 0x00000008
CREATE_NEW_PROCESS_GROUP= 0x00000200
CREATE_NO_WINDOW        = 0x08000000  # niente console window
creationflags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW

# Interprete del venv corrente
py = sys.executable

# Costruisci comando
cmd = [
    py, "-m", "streamlit", "run", str(APP_FILE),
    "--server.port", str(PORT),
]
if HEADLESS:
    cmd += ["--server.headless", "true"]

# Inoltra env e punta il DB giusto
env = os.environ.copy()
env["SQLITE_PATH"] = str(DB_FILE)

# Nascondi stdout/stderr del processo figlio
with open(os.devnull, "wb") as devnull:
    try:
        subprocess.Popen(
            cmd,
            creationflags=creationflags,
            env=env,
            cwd=str(BASE_DIR),
            stdout=devnull,
            stderr=devnull,
            close_fds=True  # harmless su Windows
        )
        print(f"🚀 Streamlit avviato in background. Apri: http://localhost:{PORT}")
        print(f"   App: {APP_FILE.name} | DB: {DB_FILE.name}")
    except Exception as e:
        print(f"⚠️ Errore nell'avvio di Streamlit: {e}")

