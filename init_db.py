import sqlite3
from pathlib import Path

DB_PATH = "epu.db"

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS categorie (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fornitori (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    codice TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS materiali (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    categoria TEXT,
    fornitore_id INTEGER REFERENCES fornitori(id) ON DELETE SET NULL,
    codice_fornitore TEXT,
    descrizione TEXT NOT NULL,
    unita TEXT,
    prezzo REAL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS capitoli (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codice TEXT,
    nome TEXT NOT NULL,
    cg_def REAL DEFAULT 0,
    ut_def REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS voci (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capitolo_id INTEGER NOT NULL REFERENCES capitoli(id) ON DELETE CASCADE,
    codice TEXT,
    descrizione TEXT NOT NULL,
    um TEXT,
    qta_voce REAL DEFAULT 1,
    cg_pct REAL DEFAULT 0,
    utile_pct REAL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS distinte (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    voce_id INTEGER REFERENCES voci(id) ON DELETE CASCADE,
    materiale_id INTEGER REFERENCES materiali(id) ON DELETE SET NULL,
    descrizione TEXT,
    um TEXT,
    qta REAL,
    prezzo_unit REAL
);

CREATE TABLE IF NOT EXISTS clienti (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    piva TEXT,
    indirizzo TEXT,
    cap TEXT,
    citta TEXT,
    provincia TEXT,
    nazione TEXT,
    email TEXT,
    telefono TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Testata preventivo (nota: i campi agreggati che usi nel DOCX
-- vengono calcolati via query/join; qui salviamo i "dati sorgente")
CREATE TABLE IF NOT EXISTS preventivi (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero TEXT,                 -- opzionale, se lo usi
    cliente_id INTEGER REFERENCES clienti(id) ON DELETE SET NULL,
    data TEXT,                   -- ISO yyyy-mm-dd
    titolo TEXT,
    note TEXT,
    iva_percentuale REAL DEFAULT 22,  -- alias di iva_pct per compatibilità export
    imponibile REAL DEFAULT 0,
    iva_importo REAL DEFAULT 0,
    totale REAL DEFAULT 0,
    note_finali TEXT
);

CREATE TABLE IF NOT EXISTS preventivo_righe (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    preventivo_id INTEGER REFERENCES preventivi(id) ON DELETE CASCADE,
    voce_id INTEGER REFERENCES voci(id) ON DELETE SET NULL,
    capitolo_codice TEXT,       -- utile per export
    capitolo_nome TEXT,         -- utile per export
    voce_codice TEXT,           -- utile per export
    descrizione TEXT,
    um TEXT,
    quantita REAL DEFAULT 1,
    prezzo_unitario REAL DEFAULT 0,
    prezzo_totale REAL DEFAULT 0,
    note TEXT
);

CREATE TABLE IF NOT EXISTS materiali_prezzi_storico (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    materiale_id INTEGER REFERENCES materiali(id) ON DELETE CASCADE,
    prezzo_vecchio REAL NOT NULL,
    prezzo_nuovo REAL NOT NULL,
    changed_at TEXT DEFAULT (datetime('now'))
);

-- Indici principali
CREATE INDEX IF NOT EXISTS idx_voci_capitolo ON voci(capitolo_id);
CREATE INDEX IF NOT EXISTS idx_distinte_voce ON distinte(voce_id);
CREATE INDEX IF NOT EXISTS idx_distinte_materiale ON distinte(materiale_id);
CREATE INDEX IF NOT EXISTS idx_prev_righe_prev ON preventivo_righe(preventivo_id);
"""

def main():
    created = not Path(DB_PATH).exists()
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(DDL)
        con.commit()
        print("✅ Schema creato/aggiornato su", DB_PATH)
        if created:
            print("ℹ️ Nuovo DB creato (vuoto).")
        else:
            print("ℹ️ DB esistente aggiornato in modo idempotente.")
    finally:
        con.close()

if __name__ == "__main__":
    main()
