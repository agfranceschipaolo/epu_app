# EPU Builder v1.2 – Streamlit + SQLite
# Novità v1.2:
# - Capitoli: Spese generali (%) e Utile (%) di default a livello capitolo
# - Voci: precompilano CG%/Utile% dal capitolo ma restano modificabili per singola voce
# - Voci: aggiunti UM della VOCE e Quantità della VOCE (la “misura prodotta” della voce)
# - UM estese: Mt, Mtq2, Hr, Nr, Lt, GG, KG, QL, AC
# - Sommario: mostra anche Quantità Voce e UM Voce
# - Import Fornitori da CSV (nome obbligatorio + campi anagrafici opzionali)
# - Mantiene tutte le funzioni di v1.1 (inline edit, clone voce, import materiali, export Excel)

import io
import sqlite3
from contextlib import contextmanager
from typing import Optional, Dict

import pandas as pd
import streamlit as st

DB_PATH = "epu.db"
UM_CHOICES = ["Mt", "Mtq2", "Hr", "Nr", "Lt", "GG", "KG", "QL", "AC"]

st.set_page_config(page_title="EPU Builder v1.2", layout="wide")

# ------------------------------------------------------------------
# DB
# ------------------------------------------------------------------
def _exec(con, sql, params=None):
    cur = con.cursor()
    cur.execute(sql, params or [])
    return cur

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()

        # Tabelle di dominio
        cur.execute("""
        CREATE TABLE IF NOT EXISTS categorie (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fornitori (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            piva TEXT,
            indirizzo TEXT,
            email TEXT,
            telefono TEXT
        )""")

        # Materiali
        cur.execute("""
        CREATE TABLE IF NOT EXISTS materiali_base (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria_id INTEGER NOT NULL,
            fornitore_id INTEGER NOT NULL,
            codice_fornitore TEXT NOT NULL,
            descrizione TEXT NOT NULL,
            unita_misura TEXT NOT NULL,
            quantita_default REAL DEFAULT 1.0,
            prezzo_unitario REAL NOT NULL,
            FOREIGN KEY(categoria_id) REFERENCES categorie(id),
            FOREIGN KEY(fornitore_id) REFERENCES fornitori(id),
            UNIQUE(fornitore_id, codice_fornitore)
        )""")

        # Capitoli: aggiungiamo default per SG% e Utile%
        cur.execute("""
        CREATE TABLE IF NOT EXISTS capitoli (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codice TEXT NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            cg_default_percentuale REAL DEFAULT 0.0,
            utile_default_percentuale REAL DEFAULT 0.0
        )""")

        # Voci: aggiungiamo UM/Quantità VOCE + utile%
        cur.execute("""
        CREATE TABLE IF NOT EXISTS voci_analisi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capitolo_id INTEGER NOT NULL,
            codice TEXT NOT NULL,
            descrizione TEXT NOT NULL,
            costi_generali_percentuale REAL DEFAULT 0.0,
            utile_percentuale REAL DEFAULT 0.0,
            voce_unita_misura TEXT,
            voce_quantita REAL DEFAULT 1.0,
            FOREIGN KEY(capitolo_id) REFERENCES capitoli(id),
            UNIQUE(capitolo_id, codice)
        )""")

        # Righe distinta
        cur.execute("""
        CREATE TABLE IF NOT EXISTS righe_distinta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            voce_analisi_id INTEGER NOT NULL,
            materiale_id INTEGER NOT NULL,
            quantita REAL NOT NULL,
            FOREIGN KEY(voce_analisi_id) REFERENCES voci_analisi(id),
            FOREIGN KEY(materiale_id) REFERENCES materiali_base(id)
        )""")
        con.commit()

        # Seed categorie e fornitore iniziale
        if _exec(con, "SELECT COUNT(*) FROM categorie").fetchone()[0] == 0:
            _exec(con, "INSERT INTO categorie (nome) VALUES (?), (?), (?), (?)",
                  ["Edile", "Ferramenta", "Noleggi", "Pose"])
        if _exec(con, "SELECT COUNT(*) FROM fornitori").fetchone()[0] == 0:
            _exec(con, "INSERT INTO fornitori (nome) VALUES (?)", ["Fornitore Sconosciuto"])
        con.commit()

@contextmanager
def get_con():
    con = sqlite3.connect(DB_PATH)
    try:
        yield con
    finally:
        con.close()

# ------------------------------------------------------------------
# Query helpers
# ------------------------------------------------------------------
def df_categorie():
    with get_con() as con:
        return pd.read_sql_query("SELECT id, nome FROM categorie ORDER BY nome", con)

def df_fornitori():
    with get_con() as con:
        return pd.read_sql_query("SELECT id, nome, piva, indirizzo, email, telefono FROM fornitori ORDER BY nome", con)

def df_materiali():
    with get_con() as con:
        return pd.read_sql_query("""
            SELECT m.id,
                   m.categoria_id, c.nome AS categoria,
                   m.fornitore_id, f.nome AS fornitore,
                   m.codice_fornitore, m.descrizione, m.unita_misura,
                   IFNULL(m.quantita_default,1.0) AS quantita_default,
                   m.prezzo_unitario
            FROM materiali_base m
            JOIN categorie c ON c.id = m.categoria_id
            JOIN fornitori f ON f.id = m.fornitore_id
            ORDER BY c.nome, f.nome, m.codice_fornitore
        """, con)

def df_capitoli():
    with get_con() as con:
        return pd.read_sql_query("""
            SELECT id, codice, nome, 
                   IFNULL(cg_default_percentuale,0) AS cg_def,
                   IFNULL(utile_default_percentuale,0) AS ut_def
            FROM capitoli ORDER BY codice
        """, con)

def df_voci(capitolo_id: Optional[int] = None):
    with get_con() as con:
        if capitolo_id:
            q = """
            SELECT v.id, v.capitolo_id, c.codice AS capitolo_codice, c.nome AS capitolo_nome,
                   v.codice, v.descrizione,
                   IFNULL(v.costi_generali_percentuale,0) AS cg_pct,
                   IFNULL(v.utile_percentuale,0) AS utile_pct,
                   v.voce_unita_misura AS um_voce,
                   IFNULL(v.voce_quantita,1.0) AS q_voce
            FROM voci_analisi v
            JOIN capitoli c ON c.id = v.capitolo_id
            WHERE v.capitolo_id = ?
            ORDER BY c.codice, v.codice
            """
            return pd.read_sql_query(q, con, params=[capitolo_id])
        else:
            q = """
            SELECT v.id, v.capitolo_id, c.codice AS capitolo_codice, c.nome AS capitolo_nome,
                   v.codice, v.descrizione,
                   IFNULL(v.costi_generali_percentuale,0) AS cg_pct,
                   IFNULL(v.utile_percentuale,0) AS utile_pct,
                   v.voce_unita_misura AS um_voce,
                   IFNULL(v.voce_quantita,1.0) AS q_voce
            FROM voci_analisi v
            JOIN capitoli c ON c.id = v.capitolo_id
            ORDER BY c.codice, v.codice
            """
            return pd.read_sql_query(q, con)

def df_righe(voce_id: int):
    with get_con() as con:
        return pd.read_sql_query("""
            SELECT r.id, r.voce_analisi_id, r.materiale_id, r.quantita,
                   m.descrizione AS materiale_descrizione,
                   m.unita_misura, m.prezzo_unitario,
                   c.nome AS categoria, f.nome AS fornitore, m.codice_fornitore,
                   (r.quantita * m.prezzo_unitario) AS subtotale
            FROM righe_distinta r
            JOIN materiali_base m ON m.id = r.materiale_id
            JOIN categorie c ON c.id = m.categoria_id
            JOIN fornitori f ON f.id = m.fornitore_id
            WHERE r.voce_analisi_id = ?
            ORDER BY r.id
        """, con, params=[voce_id])

def get_voce(voce_id: int) -> Optional[dict]:
    with get_con() as con:
        row = _exec(con, """
            SELECT v.id, v.capitolo_id, c.codice, c.nome,
                   v.codice, v.descrizione,
                   IFNULL(v.costi_generali_percentuale,0),
                   IFNULL(v.utile_percentuale,0),
                   v.voce_unita_misura,
                   IFNULL(v.voce_quantita,1.0)
            FROM voci_analisi v
            JOIN capitoli c ON c.id = v.capitolo_id
            WHERE v.id = ?
        """, (voce_id,)).fetchone()
        if not row: return None
        return {
            "id": row[0], "capitolo_id": row[1],
            "capitolo_codice": row[2], "capitolo_nome": row[3],
            "codice": row[4], "descrizione": row[5],
            "cg_pct": float(row[6]), "utile_pct": float(row[7]),
            "um_voce": row[8], "q_voce": float(row[9]),
        }

# ------------------------------------------------------------------
# Calcoli
# ------------------------------------------------------------------
def compute_totali_voce(voce_id: int) -> Dict[str, float]:
    df = df_righe(voce_id)
    costo_materie = float(df["subtotale"].sum()) if not df.empty else 0.0
    voce = get_voce(voce_id) or {"cg_pct": 0.0, "utile_pct": 0.0}
    cg = costo_materie * (voce["cg_pct"] / 100.0)
    base = costo_materie + cg
    utile = base * (voce["utile_pct"] / 100.0)
    totale = base + utile
    return {
        "costo_materie": costo_materie,
        "costi_generali": cg,
        "utile": utile,
        "cg_pct": voce["cg_pct"],
        "utile_pct": voce["utile_pct"],
        "totale": totale,
    }

# ------------------------------------------------------------------
# Mutations
# ------------------------------------------------------------------
def add_categoria(nome: str):
    with get_con() as con:
        try:
            _exec(con, "INSERT INTO categorie (nome) VALUES (?)", (nome.strip(),))
            con.commit(); st.success("Categoria aggiunta.")
        except sqlite3.IntegrityError:
            st.warning("Categoria già esistente.")

def delete_categoria(cid: int):
    with get_con() as con:
        used = _exec(con, "SELECT COUNT(*) FROM materiali_base WHERE categoria_id=?", (cid,)).fetchone()[0]
        if used: st.warning("Impossibile eliminare: categoria in uso."); return
        _exec(con, "DELETE FROM categorie WHERE id=?", (cid,)); con.commit(); st.success("Categoria eliminata.")

def add_fornitore(nome, piva, indirizzo, email, telefono):
    with get_con() as con:
        try:
            _exec(con, """INSERT INTO fornitori (nome,piva,indirizzo,email,telefono) VALUES (?,?,?,?,?)""",
                  (nome.strip(), piva, indirizzo, email, telefono))
            con.commit(); st.success("Fornitore aggiunto.")
        except sqlite3.IntegrityError:
            st.warning("Fornitore già esistente.")

def delete_fornitore(fid: int):
    with get_con() as con:
        used = _exec(con, "SELECT COUNT(*) FROM materiali_base WHERE fornitore_id=?", (fid,)).fetchone()[0]
        if used: st.warning("Impossibile eliminare: fornitore in uso."); return
        _exec(con, "DELETE FROM fornitori WHERE id=?", (fid,)); con.commit(); st.success("Fornitore eliminato.")

def add_materiale(categoria_id, fornitore_id, codice_fornitore, descrizione, um, qdef, prezzo):
    try:
        with get_con() as con:
            _exec(con, """
                INSERT INTO materiali_base (categoria_id, fornitore_id, codice_fornitore, descrizione, unita_misura, quantita_default, prezzo_unitario)
                VALUES (?,?,?,?,?,?,?)
            """, (int(categoria_id), int(fornitore_id), codice_fornitore.strip(), descrizione.strip(),
                  um, float(qdef or 1.0), float(prezzo)))
            con.commit(); st.success("Materiale inserito.")
    except sqlite3.IntegrityError:
        st.error("Codice fornitore già presente per questo fornitore.")

def update_materiali_bulk(df_edit: pd.DataFrame, df_orig: pd.DataFrame):
    changes = []
    for _, row in df_edit.iterrows():
        orig = df_orig[df_orig["id"] == row["id"]].iloc[0]
        fields = ["descrizione", "unita_misura", "quantita_default", "prezzo_unitario"]
        updates = {f: row[f] for f in fields if str(row[f]) != str(orig[f])}
        if updates: changes.append((int(row["id"]), updates))
    if not changes: st.info("Nessuna modifica."); return
    with get_con() as con:
        for mid, upd in changes:
            sets = ", ".join([f"{k}=?" for k in upd.keys()])
            vals = list(upd.values()) + [mid]
            _exec(con, f"UPDATE materiali_base SET {sets} WHERE id=?", vals)
        con.commit()
    st.success(f"Salvate {len(changes)} modifiche.")

def add_capitolo(codice, nome, cg_def, ut_def):
    try:
        with get_con() as con:
            _exec(con, "INSERT INTO capitoli (codice, nome, cg_default_percentuale, utile_default_percentuale) VALUES (?,?,?,?)",
                  (codice.strip(), nome.strip(), float(cg_def or 0.0), float(ut_def or 0.0)))
            con.commit(); st.success("Capitolo inserito.")
    except sqlite3.IntegrityError:
        st.error("Codice capitolo già esistente.")

def update_capitolo_defaults(cid: int, cg_def: float, ut_def: float):
    with get_con() as con:
        _exec(con, "UPDATE capitoli SET cg_default_percentuale=?, utile_default_percentuale=? WHERE id=?",
              (float(cg_def or 0.0), float(ut_def or 0.0), int(cid)))
        con.commit(); st.success("Default aggiornati (solo nuove voci o voci modificate singolarmente).")

def delete_capitolo(cid: int):
    with get_con() as con:
        used = _exec(con, "SELECT COUNT(*) FROM voci_analisi WHERE capitolo_id=?", (cid,)).fetchone()[0]
        if used: st.warning("Impossibile eliminare: ha voci collegate."); return
        _exec(con, "DELETE FROM capitoli WHERE id=?", (cid,)); con.commit(); st.success("Capitolo eliminato.")

def add_voce(capitolo_id, codice, descrizione, cg_pct, utile_pct, um_voce, q_voce):
    try:
        with get_con() as con:
            _exec(con, """INSERT INTO voci_analisi 
                (capitolo_id, codice, descrizione, costi_generali_percentuale, utile_percentuale, voce_unita_misura, voce_quantita)
                VALUES (?,?,?,?,?,?,?)""",
                (int(capitolo_id), codice.strip(), descrizione.strip(),
                 float(cg_pct or 0.0), float(utile_pct or 0.0), um_voce, float(q_voce or 1.0)))
            con.commit(); st.success("Voce creata.")
    except sqlite3.IntegrityError:
        st.error("Codice voce già esistente nel capitolo.")

def update_voce_perc_umqty(vid: int, cg_pct: float, utile_pct: float, um_voce: str, q_voce: float):
    with get_con() as con:
        _exec(con, "UPDATE voci_analisi SET costi_generali_percentuale=?, utile_percentuale=?, voce_unita_misura=?, voce_quantita=? WHERE id=?",
              (float(cg_pct or 0.0), float(utile_pct or 0.0), um_voce, float(q_voce or 1.0), int(vid)))
        con.commit(); st.success("Voce aggiornata.")

def add_riga(voce_id: int, materiale_id: int, quantita: float):
    with get_con() as con:
        _exec(con, "INSERT INTO righe_distinta (voce_analisi_id, materiale_id, quantita) VALUES (?,?,?)",
              (int(voce_id), int(materiale_id), float(quantita)))
        con.commit(); st.success("Riga aggiunta.")

def update_quantita_righe(voce_id: int, edited: pd.DataFrame, original: pd.DataFrame):
    diffs = []
    for _, r in edited.iterrows():
        o = original[original["id"] == r["id"]].iloc[0]
        if float(r["quantita"]) != float(o["quantita"]):
            diffs.append((float(r["quantita"]), int(r["id"])))
    if not diffs: st.info("Nessuna quantità modificata."); return
    with get_con() as con:
        for q, rid in diffs:
            _exec(con, "UPDATE righe_distinta SET quantita=? WHERE id=?", (q, rid))
        con.commit()
    st.success(f"Aggiornate {len(diffs)} righe.")

def delete_riga(rid: int):
    with get_con() as con:
        _exec(con, "DELETE FROM righe_distinta WHERE id=?", (rid,))
        con.commit(); st.success("Riga eliminata.")

def delete_voce(vid: int):
    with get_con() as con:
        _exec(con, "DELETE FROM righe_distinta WHERE voce_analisi_id=?", (vid,))
        _exec(con, "DELETE FROM voci_analisi WHERE id=?", (vid,))
        con.commit(); st.success("Voce eliminata.")

def clone_voce(vid: int):
    v = get_voce(vid)
    if not v: st.error("Voce non trovata."); return
    new_code = f"{v['codice']}-COPY"
    with get_con() as con:
        try:
            _exec(con, """INSERT INTO voci_analisi 
                (capitolo_id, codice, descrizione, costi_generali_percentuale, utile_percentuale, voce_unita_misura, voce_quantita)
                VALUES (?,?,?,?,?,?,?)""",
                (v["capitolo_id"], new_code, v["descrizione"], v["cg_pct"], v["utile_pct"], v["um_voce"], v["q_voce"]))
            new_id = _exec(con, "SELECT last_insert_rowid()").fetchone()[0]
            rows = _exec(con, "SELECT materiale_id, quantita FROM righe_distinta WHERE voce_analisi_id=?", (vid,)).fetchall()
            for m_id, q in rows:
                _exec(con, "INSERT INTO righe_distinta (voce_analisi_id, materiale_id, quantita) VALUES (?,?,?)",
                      (new_id, m_id, q))
            con.commit(); st.success(f"Voce clonata come codice {new_code}.")
        except sqlite3.IntegrityError:
            st.error("Esiste già una voce con quel codice; riprova.")

# ------------------------------------------------------------------
# Import/Export
# ------------------------------------------------------------------
def export_excel():
    voci = df_voci()
    if voci.empty:
        st.warning("Non ci sono voci da esportare."); return None

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        # 1) Sommario EPU con Nome Capitolo
        rows = []
        for _, r in voci.iterrows():
            tot = compute_totali_voce(int(r.id))
            rows.append({
                "Codice Capitolo": r.capitolo_codice,
                "Nome Capitolo": r.capitolo_nome,
                "Cod. Voce": r.codice,
                "Descrizione Voce": r.descrizione,
                "UM Voce": r.um_voce,
                "Q.tà Voce": r.q_voce,
                "CG %": r.cg_pct,
                "Utile %": r.utile_pct,
                "Materie (€)": round(tot["costo_materie"], 2),
                "Spese generali (€)": round(tot["costi_generali"], 2),
                "Utile (€)": round(tot["utile"], 2),
                "Totale (€)": round(tot["totale"], 2),
            })
        df_sommario = pd.DataFrame(rows).sort_values(["Codice Capitolo", "Cod. Voce"])
        df_sommario.to_excel(writer, index=False, sheet_name="Sommario EPU")

    buffer.seek(0)
    return buffer



def import_materiali_csv(file):
    # accetta csv o xlsx
    try:
        df = pd.read_csv(file)
    except Exception:
        file.seek(0); df = pd.read_excel(file)

    required = {"categoria","fornitore","codice_fornitore","descrizione","unita_misura","prezzo_unitario"}
    cols_lower = {c.lower(): c for c in df.columns}
    if not required.issubset(set(cols_lower.keys())):
        missing = required - set(cols_lower.keys())
        st.error(f"Colonne mancanti: {', '.join(missing)}"); return

    # normalizza nomi
    df = df.rename(columns={v: k.lower() for k, v in cols_lower.items()})
    if "quantita_default" not in df.columns: df["quantita_default"] = 1.0

    with get_con() as con:
        for _, r in df.iterrows():
            um = str(r["unita_misura"]).strip()
            if um not in UM_CHOICES:
                st.warning(f"UM non valida '{um}' per codice {r['codice_fornitore']} → saltato."); continue

            # cat
            cat = str(r["categoria"]).strip()
            row = _exec(con, "SELECT id FROM categorie WHERE nome=?", (cat,)).fetchone()
            if not row:
                _exec(con, "INSERT INTO categorie (nome) VALUES (?)", (cat,))
                cat_id = _exec(con, "SELECT last_insert_rowid()").fetchone()[0]
            else:
                cat_id = row[0]
            # fornitore
            forn = str(r["fornitore"]).strip()
            row = _exec(con, "SELECT id FROM fornitori WHERE nome=?", (forn,)).fetchone()
            if not row:
                _exec(con, "INSERT INTO fornitori (nome) VALUES (?)", (forn,))
                forn_id = _exec(con, "SELECT last_insert_rowid()").fetchone()[0]
            else:
                forn_id = row[0]
            try:
                _exec(con, """INSERT INTO materiali_base
                              (categoria_id, fornitore_id, codice_fornitore, descrizione, unita_misura, quantita_default, prezzo_unitario)
                              VALUES (?,?,?,?,?,?,?)""",
                      (cat_id, forn_id, str(r["codice_fornitore"]).strip(), str(r["descrizione"]).strip(),
                       um, float(r.get("quantita_default", 1.0)), float(r["prezzo_unitario"])))
                con.commit()
            except sqlite3.IntegrityError:
                st.warning(f"Duplicato: {forn} / {r['codice_fornitore']} → saltato.")
    st.success("Import materiali completato.")

def import_fornitori_csv(file):
    # accetta csv o xlsx
    try:
        df = pd.read_csv(file)
    except Exception:
        file.seek(0); df = pd.read_excel(file)

    # schema richiesto
    # nome (obbl.), piva, indirizzo, email, telefono
    cols = {c.lower(): c for c in df.columns}
    if "nome" not in cols:
        st.error("Colonna obbligatoria mancante: 'nome'"); return

    df = df.rename(columns={v: k.lower() for k, v in cols.items()})
    with get_con() as con:
        inserted, skipped = 0, 0
        for _, r in df.iterrows():
            name = str(r["nome"]).strip()
            if not name: skipped += 1; continue
            exists = _exec(con, "SELECT 1 FROM fornitori WHERE nome=?", (name,)).fetchone()
            if exists:
                skipped += 1; continue
            _exec(con, """INSERT INTO fornitori (nome,piva,indirizzo,email,telefono)
                          VALUES (?,?,?,?,?)""",
                  (name, str(r.get("piva") or ""), str(r.get("indirizzo") or ""),
                   str(r.get("email") or ""), str(r.get("telefono") or "")))
            inserted += 1
        con.commit()
    st.success(f"Import fornitori completato. Inseriti: {inserted}, saltati: {skipped} (duplicati o senza nome).")

# ------------------------------------------------------------------
# UI – Categorie e Fornitori
# ------------------------------------------------------------------
def ui_categorie():
    st.subheader("Categorie")
    df = df_categorie()
    left, right = st.columns([2, 1])
    with left:
        st.dataframe(df, use_container_width=True, hide_index=True)
    with right:
        nome = st.text_input("Nuova categoria")
        if st.button("➕ Aggiungi categoria") and nome:
            add_categoria(nome); st.rerun()
        if not df.empty:
            del_id = st.selectbox("Elimina categoria", options=[None]+df["id"].tolist(),
                                  format_func=lambda x: "—" if x is None else df[df["id"]==x]["nome"].iloc[0])
            if del_id and st.button("Elimina"): delete_categoria(int(del_id)); st.rerun()

def ui_fornitori():
    st.subheader("Fornitori")
    df = df_fornitori()
    st.dataframe(df, use_container_width=True, hide_index=True, height=260)
    with st.expander("➕ Nuovo fornitore"):
        c1, c2 = st.columns(2)
        nome = c1.text_input("Nome *")
        piva = c1.text_input("P.IVA")
        indirizzo = c2.text_input("Indirizzo")
        email = c2.text_input("Email")
        telefono = c2.text_input("Telefono")
        if st.button("Aggiungi") and nome:
            add_fornitore(nome, piva, indirizzo, email, telefono); st.rerun()

    with st.expander("📥 Import fornitori da CSV/Excel"):
        st.markdown("**Schema richiesto**: `nome` (obbl.), `piva`, `indirizzo`, `email`, `telefono` (opzionali).")
        up = st.file_uploader("Carica .csv o .xlsx", type=["csv","xlsx"], key="up_fornitori")
        if up is not None:
            import_fornitori_csv(up); st.rerun()

    if not df.empty:
        fid = st.selectbox("Elimina fornitore", options=[None]+df["id"].tolist(),
                           format_func=lambda x: "—" if x is None else df[df["id"]==x]["nome"].iloc[0])
        if fid and st.button("Elimina fornitore"):
            delete_fornitore(int(fid)); st.rerun()

# ------------------------------------------------------------------
# UI – Materiali (inline edit + import)
# ------------------------------------------------------------------
def ui_materiali():
    st.subheader("Archivio prezzi base (Materiali)")

    cat_df = df_categorie(); forn_df = df_fornitori()
    if cat_df.empty or forn_df.empty:
        st.info("Servono almeno 1 Categoria e 1 Fornitore."); return

    with st.form("form_materiale"):
        c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 1.6])
        categoria_id = c1.selectbox("Categoria", options=cat_df["id"],
                                    format_func=lambda i: cat_df[cat_df["id"]==i]["nome"].iloc[0])
        fornitore_id = c2.selectbox("Fornitore", options=forn_df["id"],
                                    format_func=lambda i: forn_df[forn_df["id"]==i]["nome"].iloc[0])
        codice_fornitore = c3.text_input("Codice fornitore *")
        um = c4.selectbox("UM", UM_CHOICES)

        descrizione = st.text_input("Descrizione *")
        colq, colp = st.columns(2)
        qdef = colq.number_input("Quantità default", min_value=0.0, value=1.0, step=1.0)
        prezzo = colp.number_input("Prezzo unitario (€) *", min_value=0.0, value=0.0, step=0.01, format="%.2f")

        if st.form_submit_button("➕ Aggiungi materiale"):
            if not (codice_fornitore and descrizione and prezzo > 0):
                st.warning("Compila i campi obbligatori contrassegnati con *.")
            else:
                add_materiale(categoria_id, fornitore_id, codice_fornitore, descrizione, um, qdef, prezzo); st.rerun()

    # Lista + inline edit
    df = df_materiali()
    st.caption(f"{len(df)} materiali")
    view = df[["id","categoria","fornitore","codice_fornitore","descrizione","unita_misura","quantita_default","prezzo_unitario"]].copy()
    edited = st.data_editor(view, use_container_width=True, num_rows="fixed",
                            column_config={"unita_misura": st.column_config.SelectboxColumn("UM", options=UM_CHOICES)})
    if st.button("💾 Salva modifiche materiali"):
        update_materiali_bulk(edited, view); st.rerun()

    with st.expander("📥 Import materiali da CSV/Excel"):
        st.markdown("Colonne richieste: **categoria, fornitore, codice_fornitore, descrizione, unita_misura, prezzo_unitario** (+ opz. `quantita_default`).")
        up = st.file_uploader("Carica file .csv o .xlsx", type=["csv","xlsx"], key="up_materiali")
        if up is not None:
            import_materiali_csv(up); st.rerun()

# ------------------------------------------------------------------
# UI – Capitoli
# ------------------------------------------------------------------
def ui_capitoli():
    st.subheader("Capitoli (Categorie lavorazioni)")
    with st.form("form_capitolo"):
        c1, c2 = st.columns([1, 2.2])
        c3, c4 = st.columns(2)
        codice = c1.text_input("Codice capitolo", placeholder="Es. CAP.1")
        nome = c2.text_input("Nome capitolo", placeholder="Es. CANTIERAMENTO")
        cg_def = c3.number_input("Spese generali default (%)", min_value=0.0, value=0.0, step=0.5)
        ut_def = c4.number_input("Utile impresa default (%)", min_value=0.0, value=0.0, step=0.5)
        if st.form_submit_button("➕ Aggiungi capitolo"):
            if not (codice and nome):
                st.warning("Inserisci codice e nome.")
            else:
                add_capitolo(codice, nome, cg_def, ut_def); st.rerun()

    df = df_capitoli()
    st.dataframe(df.rename(columns={"cg_def":"CG% default","ut_def":"Utile% default"}),
                 use_container_width=True, hide_index=True)

    if not df.empty:
        st.divider()
        st.markdown("**Aggiorna default capitolo (influenza nuove voci; le esistenti restano invariate)**")
        cid = st.selectbox("Capitolo", options=[None]+df["id"].tolist(),
                           format_func=lambda x: "—" if x is None else f"{df[df['id']==x]['codice'].iloc[0]} – {df[df['id']==x]['nome'].iloc[0]}")
        if cid:
            row = df[df["id"]==cid].iloc[0]
            c1, c2, c3 = st.columns(3)
            new_cg = c1.number_input("CG% default", min_value=0.0, value=float(row["cg_def"]), step=0.5, key=f"cgdef_{cid}")
            new_ut = c2.number_input("Utile% default", min_value=0.0, value=float(row["ut_def"]), step=0.5, key=f"utdef_{cid}")
            if c3.button("💾 Aggiorna default"):
                update_capitolo_defaults(int(cid), new_cg, new_ut); st.rerun()

        del_id = st.selectbox("Elimina capitolo", options=[None]+df["id"].tolist(),
                              format_func=lambda x: "—" if x is None else f"{df[df['id']==x]['codice'].iloc[0]} – {df[df['id']==x]['nome'].iloc[0]}")
        if del_id and st.button("Elimina"):
            delete_capitolo(int(del_id)); st.rerun()

# ------------------------------------------------------------------
# UI – Voci di analisi
# ------------------------------------------------------------------
def ui_voci():
    st.subheader("Voci di analisi")
    cap = df_capitoli(); mats = df_materiali()
    if cap.empty: st.info("Crea almeno un capitolo."); return
    if mats.empty: st.info("Aggiungi almeno un materiale."); return

    with st.form("form_voce"):
        c1, c2 = st.columns([2, 3])
        cap_map = {int(r.id): f"{r.codice} – {r.nome}" for _, r in cap.iterrows()}
        capitolo_id = c1.selectbox("Capitolo", options=list(cap_map.keys()), format_func=lambda x: cap_map[x])
        # default dal capitolo scelto
        rowc = cap[cap["id"]==capitolo_id].iloc[0]
        cg_def, ut_def = float(rowc["cg_def"]), float(rowc["ut_def"])

        codice = c1.text_input("Codice voce", placeholder="Es. 01")
        descrizione = c2.text_input("Descrizione voce", placeholder="Es. Recinzione di cantiere")
        cg_pct = c1.number_input("Spese generali (%)", min_value=0.0, value=cg_def, step=0.5)
        utile_pct = c1.number_input("Utile impresa (%)", min_value=0.0, value=ut_def, step=0.5,
                                    help="Calcolato su (materie + spese generali).")
        um_voce = c2.selectbox("UM della VOCE (misura prodotta)", UM_CHOICES, index=UM_CHOICES.index("Mt") if "Mt" in UM_CHOICES else 0)
        q_voce = c2.number_input("Quantità della VOCE (misura prodotta)", min_value=0.0, value=1.0, step=0.1)

        if st.form_submit_button("➕ Crea voce"):
            if not (codice and descrizione and um_voce and q_voce > 0):
                st.warning("Compila codice, descrizione, UM voce e quantità voce (>0).")
            else:
                add_voce(capitolo_id, codice, descrizione, cg_pct, utile_pct, um_voce, q_voce); st.rerun()

    filtro = st.selectbox("Filtra per capitolo", options=[0]+list(cap_map.keys()),
                          format_func=lambda x: "Tutti" if x==0 else cap_map[x])
    voci = df_voci(None if filtro==0 else filtro)
    if voci.empty: st.info("Nessuna voce trovata."); return

    left, right = st.columns([2, 3], vertical_alignment="top")
    with left:
        st.write("Voci disponibili")
        st.dataframe(voci.rename(columns={"cg_pct":"CG %","utile_pct":"Utile %","um_voce":"UM Voce","q_voce":"Q.tà Voce"})
                     [["id","capitolo_codice","codice","descrizione","UM Voce","Q.tà Voce","CG %","Utile %"]],
                     use_container_width=True, hide_index=True, height=320)
        ids = voci["id"].tolist()
        label = [f"{r.capitolo_codice} {r.codice} – {r.descrizione[:50]}" for _, r in voci.iterrows()]
        voce_sel = st.selectbox("Seleziona voce", options=[None]+ids,
                                format_func=lambda x: "—" if x is None else label[ids.index(x)])
        colA, colB, colC = st.columns(3)
        if voce_sel and colA.button("🗑️ Elimina voce"): delete_voce(int(voce_sel)); st.rerun()
        if voce_sel and colB.button("🧬 Duplica voce"): clone_voce(int(voce_sel)); st.rerun()

    if voce_sel:
        with right:
            v = get_voce(int(voce_sel))
            st.markdown(f"**Voce:** {v['capitolo_codice']} {v['codice']} – {v['descrizione']}")
            c1, c2, c3, c4, c5 = st.columns(5)
            new_cg = c1.number_input("Spese generali (%)", min_value=0.0, value=float(v["cg_pct"]), step=0.5, key=f"cg_{voce_sel}")
            new_ut = c2.number_input("Utile impresa (%)", min_value=0.0, value=float(v["utile_pct"]), step=0.5, key=f"ut_{voce_sel}")
            new_um = c3.selectbox("UM Voce", UM_CHOICES, index=UM_CHOICES.index(v["um_voce"]) if v["um_voce"] in UM_CHOICES else 0, key=f"um_{voce_sel}")
            new_qv = c4.number_input("Q.tà Voce", min_value=0.0, value=float(v["q_voce"]), step=0.1, key=f"qv_{voce_sel}")
            if c5.button("💾 Aggiorna voce"):
                update_voce_perc_umqty(int(voce_sel), new_cg, new_ut, new_um, new_qv); st.rerun()

            st.divider()
            st.write("Distinta base – aggiungi riga")
            mats = df_materiali()
            mat_map = {int(r.id): f"{r.categoria} | {r.fornitore} | {r.codice_fornitore} – {r.descrizione[:50]}" for _, r in mats.iterrows()}
            co1, co2, co3 = st.columns([3, 1, 1])
            mat_id = co1.selectbox("Materiale", options=list(mat_map.keys()), format_func=lambda x: mat_map[x], key=f"m_{voce_sel}")
            qta = co2.number_input("Quantità", min_value=0.0, value=1.0, step=0.1, key=f"q_{voce_sel}")
            if co3.button("➕ Aggiungi", key=f"add_{voce_sel}"):
                if qta <= 0: st.warning("Quantità > 0")
                else: add_riga(int(voce_sel), int(mat_id), float(qta)); st.rerun()

            # Inline edit quantità righe
            righe = df_righe(int(voce_sel))
            if righe.empty: st.info("Nessuna riga in distinta.")
            else:
                view = righe[["id","categoria","fornitore","codice_fornitore","materiale_descrizione",
                              "unita_misura","prezzo_unitario","quantita","subtotale"]].copy()
                edited = st.data_editor(view, use_container_width=True, num_rows="fixed",
                                        column_config={"quantita": st.column_config.NumberColumn("quantita", step=0.1)})
                colx, coly = st.columns([1,1])
                if colx.button("💾 Salva quantità modificate"):
                    update_quantita_righe(int(voce_sel), edited, view); st.rerun()
                rid = coly.selectbox("Elimina riga", options=[None]+righe["id"].tolist(), format_func=lambda x: "—" if x is None else f"riga #{x}")
                if rid and st.button("Elimina selezionata"): delete_riga(int(rid)); st.rerun()

            tot = compute_totali_voce(int(voce_sel))
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Materie (€)", f"{tot['costo_materie']:.2f}")
            m2.metric("Spese generali (€)", f"{tot['costi_generali']:.2f}", help=f"{tot['cg_pct']}%")
            m3.metric("Utile (€)", f"{tot['utile']:.2f}", help=f"{tot['utile_pct']}%")
            m4.metric("Totale voce (€)", f"{tot['totale']:.2f}")

# ------------------------------------------------------------------
# UI – Sommario + Export
# ------------------------------------------------------------------

def ui_sommario():
    st.subheader("Sommario EPU")

    voci = df_voci()
    if voci.empty:
        st.info("Nessuna voce disponibile.")
        return

    # Tabella sintetica (manteniamola)
    rows = []
    for _, r in voci.iterrows():
        tot = compute_totali_voce(int(r.id))
        rows.append({
            "Capitolo": r.capitolo_codice,
            "CapitoloNome": r.capitolo_nome,
            "Cod. Voce": r.codice,
            "Descrizione": r.descrizione,
            "UM Voce": r.um_voce,
            "Q.tà Voce": r.q_voce,
            "CG %": r.cg_pct,
            "Utile %": r.utile_pct,
            "Materie (€)": round(tot["costo_materie"], 2),
            "Spese generali (€)": round(tot["costi_generali"], 2),
            "Utile (€)": round(tot["utile"], 2),
            "Totale (€)": round(tot["totale"], 2),
            "voce_id": int(r.id),
        })
    df_sum = pd.DataFrame(rows).sort_values(["Capitolo","Cod. Voce"])
    st.dataframe(df_sum.drop(columns=["voce_id","CapitoloNome"]), use_container_width=True, hide_index=True)

    st.markdown("### Dettaglio a livelli")

    # Livello 1: Capitolo (solo codice, nome, n. voci) → Livello 2: Voce → Dettaglio righe
    for (cap_code, cap_name), grp in df_sum.groupby(["Capitolo","CapitoloNome"]):
        with st.expander(f"📁 Capitolo {cap_code} — {cap_name} | Voci: {len(grp)}", expanded=False):
            # NIENTE totalizzatori di prezzo a livello capitolo
            for _, r in grp.iterrows():
                titolo_voce = f"🧩 Voce {r['Cod. Voce']} – {r['Descrizione']} ({r['Q.tà Voce']} {r['UM Voce']}) | Totale € {r['Totale (€)']:.2f}"
                with st.expander(titolo_voce, expanded=False):
                    righe = df_righe(int(r["voce_id"]))
                    if righe.empty:
                        st.info("Nessuna riga.")
                    else:
                        show = righe[["categoria","fornitore","codice_fornitore","materiale_descrizione",
                                      "unita_misura","prezzo_unitario","quantita","subtotale"]]
                        st.dataframe(show, use_container_width=True, hide_index=True)
                    # metriche voce (ok mostrarle qui)
                    st.caption(f"Materie € {r['Materie (€)']:.2f} | Spese generali € {r['Spese generali (€)']:.2f} | Utile € {r['Utile (€)']:.2f}")

    # Export (già include tutto il dettaglio)
    buf = export_excel()
    if buf:
        st.download_button("⬇️ Esporta Excel (Sommario + Distinte + Dettaglio voci + Capitoli)",
                           data=buf.getvalue(),
                           file_name="EPU_export.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    init_db()
    st.title("🏗️ EPU Builder v1.2")
    st.caption("Capitoli con CG%/Utile% di default; voci con UM e quantità della VOCE; inline edit; import/export; clone voci.")

    pagina = st.sidebar.radio("Navigazione", [
        "Categorie", "Fornitori", "Archivio materiali", "Capitoli", "Voci di analisi", "Sommario EPU"
    ])

    if pagina == "Categorie":
        ui_categorie()
    elif pagina == "Fornitori":
        ui_fornitori()
    elif pagina == "Archivio materiali":
        ui_materiali()
    elif pagina == "Capitoli":
        ui_capitoli()
    elif pagina == "Voci di analisi":
        ui_voci()
    elif pagina == "Sommario EPU":
        ui_sommario()

if __name__ == "__main__":
    main()
