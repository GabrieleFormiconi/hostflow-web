import io
import os
import requests
import re
import json
import smtplib
import ssl
from email.message import EmailMessage
from io import BytesIO
import sqlite3

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None

import hashlib
import secrets
from pathlib import Path
from datetime import date, timedelta, datetime

import pandas as pd
import streamlit as st


try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from pricing_service import run_pricing_analysis


# ---------------------------
# Configurazione esterna
# ---------------------------
def testo_bool(valore, default=True):
    if valore is None or (isinstance(valore, float) and pd.isna(valore)):
        return default
    return str(valore).strip().lower() in ["true", "1", "si", "sì", "yes", "y"]


def unisci_dict(base, override):
    risultato = dict(base)
    for chiave, valore in override.items():
        risultato[chiave] = valore
    return risultato


def carica_configurazione():
    testi_default = {
        "titolo_app": "HostFlow v5",
        "sottotitolo_app": "Dashboard host con import Booking, netto reale, immobile, analisi mercato e pricing regolabile",
        "titolo_login": "Accedi a HostFlow",
        "sottotitolo_login": "Entra o crea un account per salvare il profilo immobile e ritrovare i dati già compilati.",
        "titolo_onboarding": "Completa la scheda immobile",
        "sottotitolo_onboarding": "Prima di usare la piattaforma, inserisci i dati base della struttura e quelli utili per i messaggi automatici.",
        "tab_login": "Accedi",
        "tab_registrazione": "Registrati",
        "label_email": "Email",
        "label_password": "Password",
        "label_conferma_password": "Conferma password",
        "bottone_login": "Accedi",
        "bottone_registrazione": "Crea account",
        "tab_password_dimenticata": "Password dimenticata",
        "bottone_reset_password": "Reimposta password",
        "messaggio_reset_password_ok": "Password aggiornata con successo. Ora puoi accedere con la nuova password.",
        "errore_email_non_trovata": "Nessun account trovato con questa email.",
        "sicurezza_header": "Account e sicurezza",
        "sicurezza_password_attuale": "Password attuale",
        "sicurezza_nuova_password": "Nuova password",
        "sicurezza_conferma_password": "Conferma nuova password",
        "sicurezza_bottone": "Aggiorna password",
        "sicurezza_successo": "Password aggiornata correttamente.",
        "sicurezza_errore_password_attuale": "La password attuale non è corretta.",
        "messaggio_login_ok": "Accesso effettuato con successo.",
        "messaggio_registrazione_ok": "Account creato con successo. Ora puoi accedere.",
        "logout_label": "Esci",
        "utente_connesso_label": "Utente connesso",
        "voce_menu_profilo": "Profilo",
        "voce_menu_dati_struttura": "Dati struttura",
        "voce_menu_logout": "Esci",
        "tab_dashboard": "Dashboard",
        "tab_immobile": "Immobile",
        "tab_analisi_mercato": "Analisi mercato",
        "tab_pricing": "Pricing",
        "tab_messaggi": "Messaggi",
        "tab_pulizie_servizi": "Pulizie",
        "tab_dati": "Dati",
        "sidebar_import_header": "Import",
        "sidebar_tipo_import": "Tipo import",
        "sidebar_carica_file": "Carica file prenotazioni",
        "sidebar_pulizie_header": "Pulizie",
        "sidebar_modalita_pulizie": "Modalità pulizie",
        "sidebar_pulizie_prenotazione": "Pulizie per prenotazione (€)",
        "sidebar_pulizie_mensili": "Costo pulizie mensile (€)",
        "sidebar_finanza_header": "Impostazioni finanziarie",
        "sidebar_includi_tassa_soggiorno": "Includi tassa di soggiorno nel netto",
        "sidebar_tassa_soggiorno": "Tassa soggiorno (€ per persona/notte)",
        "sidebar_costo_transazione": "Costo transazione",
        "sidebar_costo_transazione_percentuale": "Costo transazione (%)",
        "sidebar_vat": "VAT servizi piattaforma (%)",
        "sidebar_ritenuta_checkbox": "Applica ritenuta locazione breve",
        "sidebar_ritenuta": "Ritenuta locazione breve (%)",
        "sidebar_periodo_header": "Periodo",
        "sidebar_anno_dashboard": "Anno dashboard",
        "sidebar_mese_dashboard": "Mese dashboard",
        "metrica_prenotazioni": "Prenotazioni",
        "metrica_occupazione": "Occupazione",
        "metrica_fatturato": "Fatturato",
        "metrica_netto_operativo": "Netto operativo",
        "metrica_netto_reale": "Netto reale",
        "metrica_adr_medio": "ADR medio",
        "sottotab_dashboard": "Dashboard",
        "sottotab_storico": "Storico",
        "dashboard_prenotazioni_elaborate": "Prenotazioni elaborate",
        "dashboard_andamento_annuale": "Andamento annuale",
        "dashboard_nessun_dato_anno": "Nessun dato disponibile per l'anno selezionato.",
        "immobile_titolo": "Scheda immobile",
        "immobile_blocco_dati": "Dati immobile",
        "immobile_blocco_messaggi": "Dati per messaggi automatici",
        "immobile_info": "Compila e salva una volta questi dati: li ritroverai già inseriti ai prossimi accessi.",
        "immobile_salva_bottone": "Salva dati immobile",
        "immobile_salvataggio_ok": "Dati immobile salvati con successo.",
        "immobile_tabella_titolo": "Riepilogo dati salvati",
        "analisi_mercato_titolo": "Analisi mercato",
        "analisi_checkin": "Check-in analisi",
        "analisi_checkout": "Check-out analisi",
        "analisi_raggio": "Raggio analisi (km)",
        "analisi_ospiti": "Ospiti analisi",
        "analisi_tipologia": "Tipologia da confrontare",
        "analisi_pricing_titolo": "Analisi mercato e pricing",
        "analisi_indirizzo": "Indirizzo immobile",
        "analisi_indirizzo_placeholder": "Es. Via del Corso 1, Roma",
        "analisi_bottone": "Analizza mercato e pricing",
        "analisi_warning_indirizzo": "Inserisci un indirizzo",
        "analisi_warning_date": "Il check-out deve essere successivo al check-in.",
        "analisi_info_iniziale": "Imposta i filtri e premi “Analizza mercato e pricing” per vedere competitor reali.",
        "analisi_successo": "Analisi completata",
        "analisi_metrica_indirizzo": "Indirizzo",
        "analisi_metrica_latlon": "Lat / Lon",
        "analisi_metrica_competitor_trovati": "Competitor trovati",
        "analisi_metrica_prezzo_base": "Prezzo base",
        "analisi_metrica_mediana_competitor": "Mediana competitor",
        "analisi_metrica_ratio_disponibilita": "Ratio disponibilità",
        "analisi_metrica_prezzo_suggerito": "Prezzo suggerito",
        "analisi_metrica_disponibili": "Disponibili",
        "analisi_metrica_non_prenotabili": "Non prenotabili",
        "analisi_metrica_prezzo_medio": "Prezzo medio",
        "analisi_metrica_min_max": "Min / Max",
        "analisi_competitor_titolo": "Competitor trovati",
        "analisi_tabella_competitor_titolo": "Tabella competitor",
        "analisi_nessun_competitor": "Nessun competitor trovato nel raggio selezionato.",
        "pricing_titolo": "Pricing engine",
        "pricing_base_price": "Base price",
        "pricing_prezzo_manuale": "Prezzo manuale notte (€)",
        "pricing_data_prezzo": "Data da prezzare",
        "pricing_evento": "C'è un evento/ponte/fiera?",
        "pricing_weekend": "Definizione weekend",
        "pricing_regole_titolo": "Regole pricing",
        "pricing_weekend_markup": "Weekend markup (%)",
        "pricing_evento_markup": "Evento markup (%)",
        "pricing_last_minute_sconto": "Last minute sconto (%)",
        "pricing_last_minute_giorni": "Last minute entro giorni",
        "pricing_early_booking_markup": "Early booking markup (%)",
        "pricing_early_booking_giorni": "Early booking oltre giorni",
        "pricing_occupazione_alta_da": "Occupazione alta da (%)",
        "pricing_markup_occupazione_alta": "Markup occupazione alta (%)",
        "pricing_occupazione_bassa_fino": "Occupazione bassa fino a (%)",
        "pricing_sconto_occupazione_bassa": "Sconto occupazione bassa (%)",
        "pricing_risultato_titolo": "Risultato pricing",
        "pricing_metrica_base_usato": "Prezzo base usato",
        "pricing_metrica_fonte": "Fonte base price",
        "pricing_metrica_suggerito": "Prezzo suggerito",
        "pricing_motivazioni": "Motivazioni del prezzo",
        "pricing_nessuna_regola": "Nessuna regola speciale applicata",
        "messaggi_titolo": "Messaggi automatici",
        "messaggi_info_profilo": "I campi qui sotto vengono precompilati dai dati salvati nella Scheda immobile.",
        "messaggi_template": "Template",
        "messaggi_output": "Messaggio",
        "messaggi_nessuna_prenotazione": "Nessuna prenotazione attiva disponibile.",
        "dati_titolo": "Scarica CSV elaborato",
        "dati_bottone_scarica": "Scarica CSV",
        "dati_colonne_titolo": "Colonne calcolate",
        "stato_senza_file_dashboard": "Carica un file Booking o CSV per iniziare.",
        "stato_senza_file_pricing": "Carica prima un file per usare il pricing.",
        "stato_senza_file_messaggi": "Carica prima un file per generare i messaggi.",
        "stato_senza_file_dati": "Carica prima un file per scaricare l'export.",
        "errore_email_necessaria": "Inserisci un indirizzo email valido.",
        "errore_password_vuota": "Inserisci una password.",
        "errore_password_corte": "La password deve contenere almeno 6 caratteri.",
        "errore_password_diverse": "Le password non coincidono.",
        "errore_login": "Email o password non corretti.",
        "errore_email_esistente": "Esiste già un account con questa email.",
        "errore_campi_onboarding": "Compila almeno nome immobile, indirizzo completo, città e nome host.",
    }

    sezioni_default = {
        "mostra_tab_dashboard": True,
        "mostra_tab_immobile": True,
        "mostra_tab_analisi_mercato": True,
        "mostra_tab_pricing": True,
        "mostra_tab_messaggi": True,
        "mostra_tab_dati": True,
        "mostra_storico_annuale": True,
        "mostra_grafico_annuale": True,
        "mostra_tabella_immobile": True,
        "mostra_card_competitor": True,
        "mostra_tabella_competitor": True,
        "mostra_motivazioni_pricing": True,
        "mostra_colonne_calcolate": True,
        "mostra_metrica_prenotazioni": True,
        "mostra_metrica_occupazione": True,
        "mostra_metrica_fatturato": True,
        "mostra_metrica_netto_operativo": True,
        "mostra_metrica_netto_reale": True,
        "mostra_metrica_adr_medio": True,
    }

    colori_default = {
        "colore_primario": "#1F4FFF",
        "colore_sfondo": "#0E1117",
        "colore_testo": "#FFFFFF",
        "colore_card": "#171A21",
        "colore_bordo": "#2A2F3A",
        "colore_accent": "#3B82F6",
        "colore_box_input": "#11151C",
    }

    campi_default = {
        "nome_immobile": {"sezione": "immobile_dati", "etichetta": "Nome immobile", "visibile": True},
        "indirizzo_completo": {"sezione": "immobile_dati", "etichetta": "Indirizzo completo", "visibile": True},
        "citta": {"sezione": "immobile_dati", "etichetta": "Città", "visibile": True},
        "cap": {"sezione": "immobile_dati", "etichetta": "CAP", "visibile": True},
        "tipologia_immobile": {"sezione": "immobile_dati", "etichetta": "Tipologia immobile", "visibile": True},
        "ospiti_massimi": {"sezione": "immobile_dati", "etichetta": "Ospiti massimi", "visibile": True},
        "camere": {"sezione": "immobile_dati", "etichetta": "Camere", "visibile": True},
        "bagni": {"sezione": "immobile_dati", "etichetta": "Bagni", "visibile": True},
        "fascia_qualita": {"sezione": "immobile_dati", "etichetta": "Fascia qualità", "visibile": True},
        "raggio_competitor_km": {"sezione": "immobile_dati", "etichetta": "Raggio competitor (km)", "visibile": True},
        "nome_host": {"sezione": "immobile_messaggi", "etichetta": "Nome host", "visibile": True},
        "numero_whatsapp": {"sezione": "immobile_messaggi", "etichetta": "Numero WhatsApp", "visibile": True},
        "checkin_da": {"sezione": "immobile_messaggi", "etichetta": "Check-in dalle", "visibile": True},
        "checkin_fino": {"sezione": "immobile_messaggi", "etichetta": "Check-in fino alle", "visibile": True},
        "checkout_entro": {"sezione": "immobile_messaggi", "etichetta": "Check-out entro", "visibile": True},
        "wifi_nome": {"sezione": "immobile_messaggi", "etichetta": "Nome Wi‑Fi", "visibile": True},
        "wifi_password": {"sezione": "immobile_messaggi", "etichetta": "Password Wi‑Fi", "visibile": True},
        "animali_ammessi": {"sezione": "immobile_messaggi", "etichetta": "Animali ammessi", "visibile": True},
        "fumatori_ammessi": {"sezione": "immobile_messaggi", "etichetta": "Fumatori ammessi", "visibile": True},
        "parcheggio_disponibile": {"sezione": "immobile_messaggi", "etichetta": "Parcheggio disponibile", "visibile": True},
        "tassa_soggiorno": {"sezione": "immobile_messaggi", "etichetta": "Tassa di soggiorno (€ per persona/notte)", "visibile": True},
        "istruzioni_ingresso": {"sezione": "immobile_messaggi", "etichetta": "Istruzioni ingresso", "visibile": False},
        "note_finali": {"sezione": "immobile_messaggi", "etichetta": "Note standard finali", "visibile": False},
        "messaggi_nome_struttura": {"sezione": "messaggi", "etichetta": "Nome struttura", "visibile": True},
        "messaggi_ora_checkin": {"sezione": "messaggi", "etichetta": "Ora check-in", "visibile": True},
        "messaggi_wifi_nome": {"sezione": "messaggi", "etichetta": "Wi‑Fi nome", "visibile": True},
        "messaggi_wifi_password": {"sezione": "messaggi", "etichetta": "Wi‑Fi password", "visibile": True},
        "analisi_raggio": {"sezione": "analisi_mercato", "etichetta": "Raggio analisi (km)", "visibile": True},
        "analisi_ospiti": {"sezione": "analisi_mercato", "etichetta": "Ospiti analisi", "visibile": True},
        "analisi_successo_box": {"sezione": "analisi_risultati", "etichetta": "Analisi completata", "visibile": True},
        "analisi_card_indirizzo": {"sezione": "analisi_risultati", "etichetta": "Indirizzo", "visibile": True},
        "analisi_card_latlon": {"sezione": "analisi_risultati", "etichetta": "Lat / Lon", "visibile": True},
        "analisi_card_competitor_trovati": {"sezione": "analisi_risultati", "etichetta": "Competitor trovati", "visibile": True},
        "analisi_card_prezzo_base": {"sezione": "analisi_risultati", "etichetta": "Prezzo base", "visibile": True},
        "analisi_card_mediana_competitor": {"sezione": "analisi_risultati", "etichetta": "Mediana competitor", "visibile": True},
        "analisi_card_ratio_disponibilita": {"sezione": "analisi_risultati", "etichetta": "Ratio disponibilità", "visibile": True},
        "analisi_card_prezzo_suggerito": {"sezione": "analisi_risultati", "etichetta": "Prezzo suggerito", "visibile": True},
        "analisi_card_disponibili": {"sezione": "analisi_risultati", "etichetta": "Disponibili", "visibile": True},
        "analisi_card_non_prenotabili": {"sezione": "analisi_risultati", "etichetta": "Non prenotabili", "visibile": True},
        "analisi_card_prezzo_medio": {"sezione": "analisi_risultati", "etichetta": "Prezzo medio", "visibile": True},
        "analisi_card_min_max": {"sezione": "analisi_risultati", "etichetta": "Min / Max", "visibile": True},
        "analisi_sezione_competitor_titolo": {"sezione": "analisi_risultati", "etichetta": "Competitor trovati", "visibile": True},
        "analisi_card_competitor_singolo": {"sezione": "analisi_risultati", "etichetta": "Card competitor", "visibile": True},
        "analisi_tabella_competitor_titolo_config": {"sezione": "analisi_risultati", "etichetta": "Tabella competitor", "visibile": True},
    }

    configurazione = {
        "testi": testi_default,
        "sezioni": sezioni_default,
        "colori": colori_default,
        "campi": campi_default,
    }

    percorso = Path("settings.xlsx")
    if not percorso.exists():
        return configurazione

    try:
        xls = pd.ExcelFile(percorso)

        if "testi" in xls.sheet_names:
            df_testi = pd.read_excel(percorso, sheet_name="testi")
            override_testi = {}
            for _, riga in df_testi.iterrows():
                chiave = str(riga.get("chiave", "")).strip()
                valore = riga.get("valore", "")
                if chiave:
                    override_testi[chiave] = "" if pd.isna(valore) else str(valore)
            configurazione["testi"] = unisci_dict(testi_default, override_testi)

        if "sezioni" in xls.sheet_names:
            df_sezioni = pd.read_excel(percorso, sheet_name="sezioni")
            override_sezioni = {}
            for _, riga in df_sezioni.iterrows():
                chiave = str(riga.get("chiave", "")).strip()
                valore = riga.get("valore", "")
                if chiave:
                    default_val = sezioni_default.get(chiave, True)
                    override_sezioni[chiave] = testo_bool(valore, default=default_val)
            configurazione["sezioni"] = unisci_dict(sezioni_default, override_sezioni)

        if "colori" in xls.sheet_names:
            df_colori = pd.read_excel(percorso, sheet_name="colori")
            override_colori = {}
            for _, riga in df_colori.iterrows():
                chiave = str(riga.get("chiave", "")).strip()
                valore = riga.get("valore", "")
                if chiave:
                    override_colori[chiave] = "" if pd.isna(valore) else str(valore)
            configurazione["colori"] = unisci_dict(colori_default, override_colori)

        if "campi" in xls.sheet_names:
            df_campi = pd.read_excel(percorso, sheet_name="campi")
            override_campi = {}
            for _, riga in df_campi.iterrows():
                chiave = str(riga.get("chiave", "")).strip()
                if not chiave:
                    continue
                base = campi_default.get(chiave, {"sezione": "", "etichetta": chiave, "visibile": True})
                override_campi[chiave] = {
                    "sezione": str(riga.get("sezione", base["sezione"])).strip() or base["sezione"],
                    "etichetta": str(riga.get("etichetta", base["etichetta"])).strip() or base["etichetta"],
                    "visibile": testo_bool(riga.get("visibile", base["visibile"]), default=base["visibile"]),
                }
            merged = dict(campi_default)
            merged.update(override_campi)
            configurazione["campi"] = merged

    except Exception as exc:
        st.warning(f"Errore caricamento settings.xlsx: {exc}")

    return configurazione


CONFIG = carica_configurazione()
TESTI = CONFIG["testi"]
SEZIONI = CONFIG["sezioni"]
COLORI = CONFIG["colori"]
CAMPI = CONFIG["campi"]

st.set_page_config(page_title=TESTI.get("titolo_app", "HostFlow"), layout="wide")

DB_PATH = "hostflow_auth.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)

SESSION_TIMEOUT_MINUTES = 10


class PostgresDictCursor(RealDictCursor if RealDictCursor else object):
    def execute(self, query, vars=None):
        # Il codice storico usa i placeholder SQLite "?".
        # In Postgres/psycopg2 devono diventare "%s".
        if isinstance(query, str):
            query = query.replace("?", "%s")
        return super().execute(query, vars)


def compone_indirizzo_ricerca(profilo):
    indirizzo = str(profilo.get("indirizzo_completo", "") or "").strip()
    cap = str(profilo.get("cap", "") or "").strip()
    citta = str(profilo.get("citta", "") or "").strip()

    parti = []
    if indirizzo:
        parti.append(indirizzo)

    cap_citta = " ".join([p for p in [cap, citta] if p]).strip()
    if cap_citta:
        parti.append(cap_citta)

    return ", ".join(parti).strip()



def campo_visibile(chiave):
    return CAMPI.get(chiave, {}).get("visibile", True)


def campo_etichetta(chiave, fallback=""):
    return CAMPI.get(chiave, {}).get("etichetta", fallback or chiave)


def render_metriche_configurabili(items, cards_per_row=4):
    visibili = [item for item in items if item.get("visibile", True)]
    for i in range(0, len(visibili), cards_per_row):
        row = visibili[i:i + cards_per_row]
        cols = st.columns(len(row))
        for col, item in zip(cols, row):
            with col:
                col.metric(item["label"], item["value"])


def get_conn():
    if USE_POSTGRES:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 non installato. Aggiungi psycopg2-binary a requirements.txt.")
        return psycopg2.connect(DATABASE_URL, cursor_factory=PostgresDictCursor)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn




def hf_date(value):
    """Converte Timestamp/datetime/date/string in datetime.date per confronti sicuri."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def hf_date_series(series):
    """Serie normalizzata a mezzanotte per confronti sicuri con date/timestamp."""
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def hf_bound(value):
    return pd.Timestamp(value).normalize()

def calculate_hours_worked(start_time_str, end_time_str):
    try:
        start_dt = datetime.strptime(str(start_time_str), "%H:%M")
        end_dt = datetime.strptime(str(end_time_str), "%H:%M")
        diff_hours = (end_dt - start_dt).total_seconds() / 3600
        return round(max(diff_hours, 0), 2)
    except Exception:
        return 0.0


def calculate_cleaning_total(hours_worked, hourly_rate, extra_cost=0.0, custom_total_override=None):
    if custom_total_override not in [None, ""]:
        try:
            return round(float(custom_total_override), 2)
        except Exception:
            pass
    return round((float(hours_worked) * float(hourly_rate)) + float(extra_cost), 2)


def save_cleaning_service(utente_id, data):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cleaning_services (
            utente_id, service_date, booking_ref, guest_name, service_type, cleaner_name,
            start_time, end_time, hours_worked, hourly_rate, extra_cost,
            custom_total_override, total_cost, payment_status, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            int(utente_id),
            str(data.get("service_date", "")),
            str(data.get("booking_ref", "") or ""),
            str(data.get("guest_name", "") or ""),
            str(data.get("service_type", "check_out") or "check_out"),
            str(data.get("cleaner_name", "") or ""),
            str(data.get("start_time", "") or ""),
            str(data.get("end_time", "") or ""),
            float(data.get("hours_worked", 0) or 0),
            float(data.get("hourly_rate", 0) or 0),
            float(data.get("extra_cost", 0) or 0),
            (None if data.get("custom_total_override", None) in [None, ""] else float(data.get("custom_total_override"))),
            float(data.get("total_cost", 0) or 0),
            str(data.get("payment_status", "Da pagare") or "Da pagare"),
            str(data.get("notes", "") or ""),
        ),
    )
    conn.commit()
    conn.close()


def load_cleaning_services(utente_id):
    conn = get_conn()
    query = """
        SELECT id, service_date, booking_ref, guest_name, service_type, cleaner_name,
               start_time, end_time, hours_worked, hourly_rate, extra_cost,
               custom_total_override, total_cost, payment_status, notes, created_at
        FROM cleaning_services
        WHERE utente_id = ?
        ORDER BY date(service_date) DESC, id DESC
    """
    df = pd.read_sql_query(query, conn, params=(int(utente_id),))
    conn.close()
    return df


def update_cleaning_payment_status(utente_id, service_id, payment_status):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cleaning_services
        SET payment_status = ?, updated_at = CURRENT_TIMESTAMP
        WHERE utente_id = ? AND id = ?
        """,
        (str(payment_status), int(utente_id), int(service_id)),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def update_cleaning_service(utente_id, service_id, data):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cleaning_services
        SET service_date = ?, booking_ref = ?, guest_name = ?, service_type = ?, cleaner_name = ?,
            start_time = ?, end_time = ?, hours_worked = ?, hourly_rate = ?, extra_cost = ?,
            custom_total_override = ?, total_cost = ?, payment_status = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
        WHERE utente_id = ? AND id = ?
        """,
        (
            str(data.get("service_date", "")),
            str(data.get("booking_ref", "") or ""),
            str(data.get("guest_name", "") or ""),
            str(data.get("service_type", "check_out") or "check_out"),
            str(data.get("cleaner_name", "") or ""),
            str(data.get("start_time", "") or ""),
            str(data.get("end_time", "") or ""),
            float(data.get("hours_worked", 0) or 0),
            float(data.get("hourly_rate", 0) or 0),
            float(data.get("extra_cost", 0) or 0),
            (None if data.get("custom_total_override", None) in [None, ""] else float(data.get("custom_total_override"))),
            float(data.get("total_cost", 0) or 0),
            str(data.get("payment_status", "Da pagare") or "Da pagare"),
            str(data.get("notes", "") or ""),
            int(utente_id),
            int(service_id),
        ),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def delete_cleaning_service(utente_id, service_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM cleaning_services WHERE utente_id = ? AND id = ?",
        (int(utente_id), int(service_id)),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def delete_all_cleaning_services(utente_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM cleaning_services WHERE utente_id = ?",
        (int(utente_id),),
    )
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return deleted


def build_cleaning_movements(valid_df):
    rows = []
    for _, row in valid_df.iterrows():
        booking_ref_value = booking_reference(row)
        rows.append({
            "Data": pd.to_datetime(row["check_out"]).strftime("%d/%m/%Y"),
            "Movimento": "Check-out",
            "Ospite": str(row.get("guest_name", "")),
            "Booking ref": booking_ref_value,
        })
        rows.append({
            "Data": pd.to_datetime(row["check_in"]).strftime("%d/%m/%Y"),
            "Movimento": "Check-in",
            "Ospite": str(row.get("guest_name", "")),
            "Booking ref": booking_ref_value,
        })
    movement_df = pd.DataFrame(rows)
    if not movement_df.empty:
        movement_df["sort_date"] = pd.to_datetime(movement_df["Data"], format="%d/%m/%Y", errors="coerce")
        movement_df = movement_df.sort_values(["sort_date", "Movimento", "Ospite"]).drop(columns=["sort_date"])
    return movement_df


def init_db():
    id_pk = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS utenti (
            id {id_pk},
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS profili_immobile (
            utente_id INTEGER PRIMARY KEY,
            nome_immobile TEXT,
            indirizzo_completo TEXT,
            citta TEXT,
            cap TEXT,
            tipologia_immobile TEXT,
            ospiti_massimi INTEGER,
            camere INTEGER,
            bagni INTEGER,
            fascia_qualita TEXT,
            raggio_competitor_km REAL,
            nome_host TEXT,
            numero_whatsapp TEXT,
            checkin_da TEXT,
            checkin_fino TEXT,
            checkout_entro TEXT,
            wifi_nome TEXT,
            wifi_password TEXT,
            animali_ammessi INTEGER,
            fumatori_ammessi INTEGER,
            parcheggio_disponibile INTEGER,
            tassa_soggiorno REAL,
            istruzioni_ingresso TEXT,
            note_finali TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessioni_accesso (
            token TEXT PRIMARY KEY,
            utente_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS password_reset_codes (
            email TEXT PRIMARY KEY,
            code_hash TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS file_prenotazioni (
            utente_id INTEGER PRIMARY KEY,
            nome_file TEXT,
            contenuto BYTEA,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS sidebar_settings (
            utente_id INTEGER PRIMARY KEY,
            settings_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id {id_pk},
            utente_id INTEGER NOT NULL,
            booking_ref TEXT,
            platform TEXT,
            guest_name TEXT,
            guest_phone TEXT,
            guest_email TEXT,
            check_in TEXT,
            check_out TEXT,
            message_type TEXT NOT NULL,
            message_text TEXT NOT NULL,
            send_at TEXT NOT NULL,
            channel TEXT DEFAULT 'whatsapp',
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT,
            error_message TEXT,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS message_settings (
            utente_id INTEGER PRIMARY KEY,
            settings_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS cleaning_services (
            id {id_pk},
            utente_id INTEGER NOT NULL,
            service_date TEXT NOT NULL,
            booking_ref TEXT,
            guest_name TEXT,
            service_type TEXT DEFAULT 'check_out',
            cleaner_name TEXT,
            start_time TEXT,
            end_time TEXT,
            hours_worked REAL DEFAULT 0,
            hourly_rate REAL DEFAULT 0,
            extra_cost REAL DEFAULT 0,
            custom_total_override REAL,
            total_cost REAL DEFAULT 0,
            payment_status TEXT DEFAULT 'Da pagare',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS custom_bookings (
            id {id_pk},
            utente_id INTEGER NOT NULL,
            platform TEXT DEFAULT 'Custom',
            guest_name TEXT NOT NULL,
            guest_phone TEXT,
            check_in TEXT NOT NULL,
            check_out TEXT NOT NULL,
            total_price REAL DEFAULT 0,
            cleaning_cost REAL DEFAULT 0,
            platform_fee REAL DEFAULT 0,
            transaction_cost REAL DEFAULT 0,
            raw_booking_status TEXT DEFAULT 'confirmed',
            status TEXT DEFAULT 'confirmed',
            guests INTEGER DEFAULT 1,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (utente_id) REFERENCES utenti(id)
        )
    """)
    try:
        cur.execute("ALTER TABLE custom_bookings ADD COLUMN guest_phone TEXT")
    except Exception:
        pass

    try:
        cur.execute("ALTER TABLE scheduled_messages ADD COLUMN guest_phone TEXT")
    except Exception:
        pass

    conn.commit()
    conn.close()


def hash_password(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 100000).hex()


def crea_utente(email, password):
    conn = get_conn()
    cur = conn.cursor()
    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)
    try:
        cur.execute("INSERT INTO utenti (email, password_hash, salt) VALUES (?, ?, ?)", (email.strip().lower(), password_hash, salt))
        conn.commit()
        return True, None
    except sqlite3.IntegrityError:
        return False, TESTI["errore_email_esistente"]
    finally:
        conn.close()


def autentica_utente(email, password):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM utenti WHERE email = ?", (email.strip().lower(),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    expected = hash_password(password, row["salt"])
    if expected == row["password_hash"]:
        return {"id": row["id"], "email": row["email"]}
    return None



def aggiorna_password_utente(utente_id, password_attuale, nuova_password):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT password_hash, salt FROM utenti WHERE id = ?", (utente_id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return False, TESTI["errore_login"]

    expected = hash_password(password_attuale, row["salt"])
    if expected != row["password_hash"]:
        conn.close()
        return False, TESTI["sicurezza_errore_password_attuale"]

    nuovo_salt = secrets.token_hex(16)
    nuovo_hash = hash_password(nuova_password, nuovo_salt)
    cur.execute(
        "UPDATE utenti SET password_hash = ?, salt = ? WHERE id = ?",
        (nuovo_hash, nuovo_salt, utente_id),
    )
    conn.commit()
    conn.close()
    return True, TESTI["sicurezza_successo"]


def get_secret_value(name, default=None):
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return os.getenv(name, default)

def smtp_config_disponibile():
    host = str(get_secret_value("SMTP_HOST", "") or "").strip()
    username = str(get_secret_value("SMTP_USERNAME", "") or "").strip()
    password = str(get_secret_value("SMTP_PASSWORD", "") or "").strip()
    from_email = str(get_secret_value("SMTP_FROM_EMAIL", username) or "").strip()
    return all([host, username, password, from_email])

def invia_email_reset_password(destinatario_email, codice):
    host = str(get_secret_value("SMTP_HOST", "") or "").strip()
    port_raw = get_secret_value("SMTP_PORT", 587)
    username = str(get_secret_value("SMTP_USERNAME", "") or "").strip()
    password = str(get_secret_value("SMTP_PASSWORD", "") or "").strip()
    from_email = str(get_secret_value("SMTP_FROM_EMAIL", username) or "").strip()
    from_name = str(get_secret_value("SMTP_FROM_NAME", "HostFlow") or "HostFlow").strip()
    use_tls_raw = str(get_secret_value("SMTP_USE_TLS", "true") or "true").strip().lower()
    use_tls = use_tls_raw in ["1", "true", "yes", "si", "sì", "y"]

    if not all([host, username, password, from_email]):
        return False, "Configurazione email mancante. Imposta SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD e SMTP_FROM_EMAIL."

    try:
        port = int(port_raw)
    except Exception:
        port = 587

    subject = "Codice recupero password HostFlow"
    body = (
        f"Ciao,\n\n"
        f"il tuo codice di recupero password per HostFlow è: {codice}\n\n"
        f"Il codice è valido per 15 minuti.\n"
        f"Se non hai richiesto tu questa operazione, ignora questa email.\n\n"
        f"HostFlow"
    )

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    message["To"] = str(destinatario_email).strip().lower()
    message.set_content(body)

    try:
        if use_tls:
            context = ssl.create_default_context()
            with smtplib.SMTP(host, port, timeout=20) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(username, password)
                server.send_message(message)
        else:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=20) as server:
                server.login(username, password)
                server.send_message(message)
        return True, "Codice inviato via email."
    except Exception as exc:
        return False, f"Invio email non riuscito: {exc}"


def crea_codice_reset_password(email):
    email_norm = email.strip().lower()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM utenti WHERE email = ?", (email_norm,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return False, TESTI["errore_email_non_trovata"], None

    code = f"{secrets.randbelow(1000000):06d}"
    code_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    expires_at = (datetime.utcnow() + timedelta(minutes=15)).isoformat()

    cur.execute(
        """
        INSERT INTO password_reset_codes (email, code_hash, expires_at, created_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(email) DO UPDATE SET
            code_hash = excluded.code_hash,
            expires_at = excluded.expires_at,
            created_at = CURRENT_TIMESTAMP
        """,
        (email_norm, code_hash, expires_at),
    )
    conn.commit()

    ok_email, msg_email = invia_email_reset_password(email_norm, code)
    if not ok_email:
        cur.execute("DELETE FROM password_reset_codes WHERE email = ?", (email_norm,))
        conn.commit()
        conn.close()
        return False, msg_email, None

    conn.close()
    return True, "Codice di recupero inviato via email.", None


def reimposta_password_con_codice(email, codice, nuova_password):
    email_norm = email.strip().lower()
    code_hash = hashlib.sha256(str(codice).strip().encode("utf-8")).hexdigest()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT code_hash, expires_at FROM password_reset_codes WHERE email = ?",
        (email_norm,),
    )
    reset_row = cur.fetchone()

    if not reset_row:
        conn.close()
        return False, "Richiedi prima un codice di recupero."

    try:
        expires_at = datetime.fromisoformat(reset_row["expires_at"])
    except Exception:
        cur.execute("DELETE FROM password_reset_codes WHERE email = ?", (email_norm,))
        conn.commit()
        conn.close()
        return False, "Codice non valido. Richiedine uno nuovo."

    if expires_at < datetime.utcnow():
        cur.execute("DELETE FROM password_reset_codes WHERE email = ?", (email_norm,))
        conn.commit()
        conn.close()
        return False, "Codice scaduto. Richiedine uno nuovo."

    if code_hash != reset_row["code_hash"]:
        conn.close()
        return False, "Codice di recupero non corretto."

    cur.execute("SELECT id FROM utenti WHERE email = ?", (email_norm,))
    user_row = cur.fetchone()
    if not user_row:
        cur.execute("DELETE FROM password_reset_codes WHERE email = ?", (email_norm,))
        conn.commit()
        conn.close()
        return False, TESTI["errore_email_non_trovata"]

    nuovo_salt = secrets.token_hex(16)
    nuovo_hash = hash_password(nuova_password, nuovo_salt)
    cur.execute(
        "UPDATE utenti SET password_hash = ?, salt = ? WHERE id = ?",
        (nuovo_hash, nuovo_salt, user_row["id"]),
    )
    cur.execute("DELETE FROM password_reset_codes WHERE email = ?", (email_norm,))
    conn.commit()
    conn.close()
    return True, TESTI["messaggio_reset_password_ok"]




def crea_sessione_accesso(utente_id):
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(minutes=SESSION_TIMEOUT_MINUTES)).isoformat()

    conn = get_conn()
    cur = conn.cursor()
    if USE_POSTGRES:
        cur.execute(
            """
            INSERT INTO sessioni_accesso (token, utente_id, expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(token) DO UPDATE SET
                utente_id = excluded.utente_id,
                expires_at = excluded.expires_at,
                created_at = CURRENT_TIMESTAMP
            """,
            (token, utente_id, expires_at),
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO sessioni_accesso (token, utente_id, expires_at) VALUES (?, ?, ?)",
            (token, utente_id, expires_at),
        )
    conn.commit()
    conn.close()
    return token


def elimina_sessione_accesso(token):
    if not token:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessioni_accesso WHERE token = ?", (token,))
    conn.commit()
    conn.close()


def autentica_da_token(token):
    if not token:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.token, s.utente_id, s.expires_at, u.email
        FROM sessioni_accesso s
        JOIN utenti u ON u.id = s.utente_id
        WHERE s.token = ?
        """,
        (token,),
    )
    row = cur.fetchone()

    if not row:
        conn.close()
        return None

    try:
        expires_at = datetime.fromisoformat(row["expires_at"])
    except Exception:
        cur.execute("DELETE FROM sessioni_accesso WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        return None

    if expires_at < datetime.utcnow():
        cur.execute("DELETE FROM sessioni_accesso WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        return None

    new_expiry = (datetime.utcnow() + timedelta(minutes=SESSION_TIMEOUT_MINUTES)).isoformat()
    cur.execute("UPDATE sessioni_accesso SET expires_at = ? WHERE token = ?", (new_expiry, token))
    conn.commit()
    conn.close()

    return {"id": row["utente_id"], "email": row["email"], "token": token}


def profilo_default():
    return {
        "nome_immobile": "",
        "indirizzo_completo": "",
        "citta": "",
        "cap": "",
        "tipologia_immobile": "Appartamento intero",
        "ospiti_massimi": 4,
        "camere": 1,
        "bagni": 1,
        "fascia_qualita": "Basic",
        "raggio_competitor_km": 1.0,
        "nome_host": "",
        "numero_whatsapp": "",
        "checkin_da": "15:00",
        "checkin_fino": "22:00",
        "checkout_entro": "10:00",
        "wifi_nome": "",
        "wifi_password": "",
        "animali_ammessi": False,
        "fumatori_ammessi": False,
        "parcheggio_disponibile": False,
        "tassa_soggiorno": 4.0,
        "istruzioni_ingresso": "",
        "note_finali": "",
    }


def profilo_completo(profilo):
    return all([
        str(profilo.get("nome_immobile", "")).strip(),
        str(profilo.get("indirizzo_completo", "")).strip(),
        str(profilo.get("citta", "")).strip(),
        str(profilo.get("nome_host", "")).strip(),
    ])


def carica_profilo_immobile(utente_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM profili_immobile WHERE utente_id = ?", (utente_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return profilo_default()
    profilo = profilo_default()
    profilo.update(dict(row))
    profilo["animali_ammessi"] = bool(profilo.get("animali_ammessi", 0))
    profilo["fumatori_ammessi"] = bool(profilo.get("fumatori_ammessi", 0))
    profilo["parcheggio_disponibile"] = bool(profilo.get("parcheggio_disponibile", 0))
    return profilo


def salva_profilo_immobile(utente_id, dati):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO profili_immobile (
            utente_id, nome_immobile, indirizzo_completo, citta, cap, tipologia_immobile,
            ospiti_massimi, camere, bagni, fascia_qualita, raggio_competitor_km,
            nome_host, numero_whatsapp, checkin_da, checkin_fino, checkout_entro,
            wifi_nome, wifi_password, animali_ammessi, fumatori_ammessi,
            parcheggio_disponibile, tassa_soggiorno, istruzioni_ingresso, note_finali,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(utente_id) DO UPDATE SET
            nome_immobile=excluded.nome_immobile,
            indirizzo_completo=excluded.indirizzo_completo,
            citta=excluded.citta,
            cap=excluded.cap,
            tipologia_immobile=excluded.tipologia_immobile,
            ospiti_massimi=excluded.ospiti_massimi,
            camere=excluded.camere,
            bagni=excluded.bagni,
            fascia_qualita=excluded.fascia_qualita,
            raggio_competitor_km=excluded.raggio_competitor_km,
            nome_host=excluded.nome_host,
            numero_whatsapp=excluded.numero_whatsapp,
            checkin_da=excluded.checkin_da,
            checkin_fino=excluded.checkin_fino,
            checkout_entro=excluded.checkout_entro,
            wifi_nome=excluded.wifi_nome,
            wifi_password=excluded.wifi_password,
            animali_ammessi=excluded.animali_ammessi,
            fumatori_ammessi=excluded.fumatori_ammessi,
            parcheggio_disponibile=excluded.parcheggio_disponibile,
            tassa_soggiorno=excluded.tassa_soggiorno,
            istruzioni_ingresso=excluded.istruzioni_ingresso,
            note_finali=excluded.note_finali,
            updated_at=CURRENT_TIMESTAMP
    """, (
        utente_id, dati["nome_immobile"], dati["indirizzo_completo"], dati["citta"], dati["cap"], dati["tipologia_immobile"],
        int(dati["ospiti_massimi"]), int(dati["camere"]), int(dati["bagni"]), dati["fascia_qualita"], float(dati["raggio_competitor_km"]),
        dati["nome_host"], dati["numero_whatsapp"], dati["checkin_da"], dati["checkin_fino"], dati["checkout_entro"],
        dati["wifi_nome"], dati["wifi_password"], int(bool(dati["animali_ammessi"])), int(bool(dati["fumatori_ammessi"])),
        int(bool(dati["parcheggio_disponibile"])), float(dati["tassa_soggiorno"]), dati["istruzioni_ingresso"], dati["note_finali"]
    ))
    conn.commit()
    conn.close()



def salva_file_prenotazioni(utente_id, uploaded_file):
    if uploaded_file is None:
        return

    contenuto = uploaded_file.getvalue()
    nome_file = uploaded_file.name

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO file_prenotazioni (utente_id, nome_file, contenuto, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(utente_id) DO UPDATE SET
            nome_file = excluded.nome_file,
            contenuto = excluded.contenuto,
            updated_at = CURRENT_TIMESTAMP
        """,
        (utente_id, nome_file, contenuto),
    )
    conn.commit()
    conn.close()


def carica_file_prenotazioni(utente_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT nome_file, contenuto FROM file_prenotazioni WHERE utente_id = ?", (utente_id,))
    row = cur.fetchone()
    conn.close()

    if not row or row["contenuto"] is None:
        return None

    buffer_file = BytesIO(row["contenuto"])
    buffer_file.name = row["nome_file"] or "prenotazioni.xlsx"
    return buffer_file


def save_message_settings(utente_id, settings_dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO message_settings (utente_id, settings_json, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(utente_id) DO UPDATE SET
            settings_json = excluded.settings_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (int(utente_id), json.dumps(settings_dict, ensure_ascii=False)),
    )
    conn.commit()
    conn.close()


def load_message_settings(utente_id):
    defaults = {
        "msg_rule_checkin_reminder_days": 1,
        "msg_rule_checkin_reminder_time": "18:00",
        "msg_rule_checkin_instr_time": "15:00",
        "msg_rule_checkout_reminder_days": 1,
        "msg_rule_checkout_reminder_time": "18:00",
        "msg_rule_review_days_after": 1,
        "msg_rule_review_time": "12:00",
        "template_booking_confirmation": "Ciao {nome_ospite}, grazie per aver prenotato {nome_struttura}.\n\nIl tuo soggiorno è confermato dal {data_checkin} al {data_checkout}.\n\nCheck-in dalle {checkin_da} alle {checkin_fino}\nCheck-out entro le {checkout_entro}\n\nPer qualsiasi necessità puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "template_checkin_reminder": "Ciao {nome_ospite}, ti ricordiamo che il check-in presso {nome_struttura} sarà il {data_checkin}.\n\nL’orario di arrivo è previsto dalle {checkin_da} alle {checkin_fino}.\n\nSe hai bisogno di assistenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "template_checkin_instructions": "Ciao {nome_ospite}, benvenuto a {nome_struttura}.\n\nTi ricordiamo che oggi il check-in è disponibile dalle {checkin_da} alle {checkin_fino}.\n\nDati Wi-Fi:\nNome rete: {wifi_nome}\nPassword: {wifi_password}\n\nHost di riferimento: {nome_host}\nContatto WhatsApp: {numero_whatsapp}{testo_parcheggio}{testo_tassa_soggiorno}",
        "template_checkout_reminder": "Ciao {nome_ospite}, ti ricordiamo che il check-out di {nome_struttura} è previsto il {data_checkout} entro le {checkout_entro}.\n\nPer qualsiasi necessità prima della partenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "template_review_request": "Ciao {nome_ospite}, grazie per aver scelto {nome_struttura}.\n\nSperiamo che il soggiorno sia stato piacevole. Se ti sei trovato bene, ci farebbe molto piacere ricevere una tua recensione.\n\nUn saluto da {nome_host}.",
    }

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT settings_json FROM message_settings WHERE utente_id = ?", (int(utente_id),))
    row = cur.fetchone()
    conn.close()

    if not row or not row["settings_json"]:
        return defaults

    try:
        loaded = json.loads(row["settings_json"])
        if isinstance(loaded, dict):
            defaults.update(loaded)
    except Exception:
        pass

    return defaults



def save_custom_booking(utente_id, data):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO custom_bookings (
            utente_id, platform, guest_name, guest_phone, check_in, check_out, total_price,
            cleaning_cost, platform_fee, transaction_cost, raw_booking_status,
            status, guests, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            int(utente_id),
            "Custom",
            str(data.get("guest_name", "") or "").strip(),
            str(data.get("guest_phone", "") or "").strip(),
            str(data.get("check_in", "")),
            str(data.get("check_out", "")),
            float(data.get("total_price", 0) or 0),
            float(data.get("cleaning_cost", 0) or 0),
            float(data.get("platform_fee", 0) or 0),
            float(data.get("transaction_cost", 0) or 0),
            str(data.get("raw_booking_status", "confirmed") or "confirmed"),
            str(data.get("status", "confirmed") or "confirmed"),
            int(data.get("guests", 1) or 1),
            str(data.get("notes", "") or ""),
        ),
    )
    conn.commit()
    conn.close()


def update_custom_booking(utente_id, booking_id, data):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE custom_bookings
        SET guest_name = ?, guest_phone = ?, check_in = ?, check_out = ?, total_price = ?,
            cleaning_cost = ?, platform_fee = ?, transaction_cost = ?,
            raw_booking_status = ?, status = ?, guests = ?, notes = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND utente_id = ?
        """,
        (
            str(data.get("guest_name", "") or "").strip(),
            str(data.get("guest_phone", "") or "").strip(),
            str(data.get("check_in", "")),
            str(data.get("check_out", "")),
            float(data.get("total_price", 0) or 0),
            float(data.get("cleaning_cost", 0) or 0),
            float(data.get("platform_fee", 0) or 0),
            float(data.get("transaction_cost", 0) or 0),
            str(data.get("raw_booking_status", "confirmed") or "confirmed"),
            str(data.get("status", "confirmed") or "confirmed"),
            int(data.get("guests", 1) or 1),
            str(data.get("notes", "") or ""),
            int(booking_id),
            int(utente_id),
        ),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def delete_custom_booking(utente_id, booking_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM custom_bookings WHERE id = ? AND utente_id = ?",
        (int(booking_id), int(utente_id)),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def load_custom_bookings(utente_id):
    conn = get_conn()
    query = """
        SELECT id, platform, guest_name, guest_phone, check_in, check_out, total_price,
               cleaning_cost, platform_fee, transaction_cost,
               raw_booking_status, status, guests, notes
        FROM custom_bookings
        WHERE utente_id = ?
        ORDER BY date(check_in) ASC, id ASC
    """
    df = pd.read_sql_query(query, conn, params=(int(utente_id),))
    conn.close()

    if df.empty:
        return df

    df["check_in"] = pd.to_datetime(df["check_in"], errors="coerce").dt.date
    df["check_out"] = pd.to_datetime(df["check_out"], errors="coerce").dt.date
    for col in ["id", "total_price", "cleaning_cost", "platform_fee", "transaction_cost", "guests"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["id"] = df["id"].astype(int)
    df["guests"] = df["guests"].astype(int).clip(lower=1)
    df["status"] = df["status"].apply(normalize_status)
    if "raw_booking_status" not in df.columns:
        df["raw_booking_status"] = df["status"]
    if "guest_phone" not in df.columns:
        df["guest_phone"] = ""
    df["guest_phone"] = df["guest_phone"].fillna("").astype(str)
    return df

    df["check_in"] = pd.to_datetime(df["check_in"], errors="coerce").dt.date
    df["check_out"] = pd.to_datetime(df["check_out"], errors="coerce").dt.date
    for col in ["id", "total_price", "cleaning_cost", "platform_fee", "transaction_cost", "guests"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["id"] = df["id"].astype(int)
    df["guests"] = df["guests"].astype(int).clip(lower=1)
    df["status"] = df["status"].apply(normalize_status)
    if "raw_booking_status" not in df.columns:
        df["raw_booking_status"] = df["status"]
    if "guest_phone" not in df.columns:
        df["guest_phone"] = ""
    return df

    df["check_in"] = pd.to_datetime(df["check_in"], errors="coerce").dt.date
    df["check_out"] = pd.to_datetime(df["check_out"], errors="coerce").dt.date
    for col in ["id", "total_price", "cleaning_cost", "platform_fee", "transaction_cost", "guests"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["id"] = df["id"].astype(int)
    df["guests"] = df["guests"].astype(int).clip(lower=1)
    df["status"] = df["status"].apply(normalize_status)
    if "raw_booking_status" not in df.columns:
        df["raw_booking_status"] = df["status"]
    return df


def merge_booking_sources(base_df, custom_df):

    if base_df is None or len(base_df) == 0:
        return custom_df.copy() if custom_df is not None else pd.DataFrame()
    if custom_df is None or len(custom_df) == 0:
        return base_df.copy()
    return pd.concat([base_df.copy(), custom_df.copy()], ignore_index=True, sort=False)


def sidebar_defaults():
    today = date.today()
    return {
        "import_mode": "Auto",
        "cleaning_mode": "Per prenotazione",
        "cleaning_cost_default": 0.0,
        "monthly_cleaning_cost": 0.0,
        "include_city_tax": True,
        "city_tax_rate": 4.0,
        "transaction_mode": "Percentuale",
        "transaction_pct": 1.5,
        "vat_pct": 22.0,
        "include_withholding": True,
        "withholding_pct": 21.0,
        "selected_year": int(today.year),
        "selected_month": int(today.month),
        "dashboard_period_mode": "Mensile",
        "selected_quarter": 1,
        "selected_semester": 1,
        "custom_start_date": today.replace(day=1).isoformat(),
        "custom_end_date": today.isoformat(),
    }


def carica_sidebar_settings(utente_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT settings_json FROM sidebar_settings WHERE utente_id = ?", (utente_id,))
    row = cur.fetchone()
    conn.close()

    defaults = sidebar_defaults()
    if not row or not row["settings_json"]:
        return defaults

    try:
        saved = json.loads(row["settings_json"])
        if not isinstance(saved, dict):
            return defaults
    except Exception:
        return defaults

    defaults.update(saved)
    return defaults


def salva_sidebar_settings(utente_id, settings):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sidebar_settings (utente_id, settings_json, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(utente_id) DO UPDATE SET
            settings_json = excluded.settings_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (utente_id, json.dumps(settings)),
    )
    conn.commit()
    conn.close()


def inizializza_sidebar_state(utente_id):
    defaults = carica_sidebar_settings(utente_id)
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def inizializza_sessione():
    if "utente" not in st.session_state:
        st.session_state.utente = None
    if "auth_token" not in st.session_state:
        st.session_state.auth_token = None
    if "profilo_immobile" not in st.session_state:
        st.session_state.profilo_immobile = None
    if "market_result" not in st.session_state:
        st.session_state.market_result = None
    if "file_prenotazioni_virtuale" not in st.session_state:
        st.session_state.file_prenotazioni_virtuale = None
    if "file_prenotazioni_nome" not in st.session_state:
        st.session_state.file_prenotazioni_nome = None
    if "msg_rule_confirm_offset_days" not in st.session_state:
        st.session_state.msg_rule_confirm_offset_days = 0
    if "msg_rule_confirm_time" not in st.session_state:
        st.session_state.msg_rule_confirm_time = "10:00"
    if "msg_rule_checkin_reminder_days" not in st.session_state:
        st.session_state.msg_rule_checkin_reminder_days = 1
    if "msg_rule_checkin_reminder_time" not in st.session_state:
        st.session_state.msg_rule_checkin_reminder_time = "18:00"
    if "msg_rule_checkin_instr_time" not in st.session_state:
        st.session_state.msg_rule_checkin_instr_time = "15:00"
    if "msg_rule_checkout_reminder_days" not in st.session_state:
        st.session_state.msg_rule_checkout_reminder_days = 1
    if "msg_rule_checkout_reminder_time" not in st.session_state:
        st.session_state.msg_rule_checkout_reminder_time = "18:00"
    if "msg_rule_review_days_after" not in st.session_state:
        st.session_state.msg_rule_review_days_after = 1
    if "msg_rule_review_time" not in st.session_state:
        st.session_state.msg_rule_review_time = "12:00"
    if "template_booking_confirmation" not in st.session_state:
        st.session_state.template_booking_confirmation = "Ciao {nome_ospite}, grazie per aver prenotato {nome_struttura}.\n\nIl tuo soggiorno è confermato dal {data_checkin} al {data_checkout}.\n\nCheck-in dalle {checkin_da} alle {checkin_fino}\nCheck-out entro le {checkout_entro}\n\nPer qualsiasi necessità puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}."
    if "template_checkin_reminder" not in st.session_state:
        st.session_state.template_checkin_reminder = "Ciao {nome_ospite}, ti ricordiamo che il check-in presso {nome_struttura} sarà il {data_checkin}.\n\nL’orario di arrivo è previsto dalle {checkin_da} alle {checkin_fino}.\n\nSe hai bisogno di assistenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}."
    if "template_checkin_instructions" not in st.session_state:
        st.session_state.template_checkin_instructions = "Ciao {nome_ospite}, benvenuto a {nome_struttura}.\n\nTi ricordiamo che oggi il check-in è disponibile dalle {checkin_da} alle {checkin_fino}.\n\nDati Wi-Fi:\nNome rete: {wifi_nome}\nPassword: {wifi_password}\n\nHost di riferimento: {nome_host}\nContatto WhatsApp: {numero_whatsapp}{testo_parcheggio}{testo_tassa_soggiorno}"
    if "template_checkout_reminder" not in st.session_state:
        st.session_state.template_checkout_reminder = "Ciao {nome_ospite}, ti ricordiamo che il check-out di {nome_struttura} è previsto il {data_checkout} entro le {checkout_entro}.\n\nPer qualsiasi necessità prima della partenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}."
    if "template_review_request" not in st.session_state:
        st.session_state.template_review_request = "Ciao {nome_ospite}, grazie per aver scelto {nome_struttura}.\n\nSperiamo che il soggiorno sia stato piacevole. Se ti sei trovato bene, ci farebbe molto piacere ricevere una tua recensione.\n\nUn saluto da {nome_host}."
    if "message_settings_loaded" not in st.session_state:
        st.session_state.message_settings_loaded = False
    if "file_prenotazioni_signature" not in st.session_state:
        st.session_state.file_prenotazioni_signature = None


def logout():
    token = st.session_state.get("auth_token")
    elimina_sessione_accesso(token)

    st.session_state.utente = None
    st.session_state.auth_token = None
    st.session_state.profilo_immobile = None
    st.session_state.market_result = None
    st.session_state.file_prenotazioni_virtuale = None
    st.session_state.file_prenotazioni_nome = None
    st.session_state.message_settings_loaded = False

    try:
        st.query_params.clear()
    except Exception:
        pass

    st.rerun()


st.markdown(f"""
<style>
.stApp {{
    background-color: {COLORI["colore_sfondo"]};
    color: {COLORI["colore_testo"]};
}}
[data-testid="stSidebar"] {{
    background-color: {COLORI["colore_box_input"]};
}}
div[data-testid="stMetric"] {{
    background-color: {COLORI["colore_card"]};
    border: 1px solid {COLORI["colore_bordo"]};
    padding: 14px;
    border-radius: 16px;
}}
.stButton > button, .stDownloadButton > button {{
    background-color: {COLORI["colore_primario"]};
    color: white;
    border: none;
    border-radius: 10px;
    padding: 0.55rem 1rem;
    font-weight: 600;
}}
.block-container {{
    padding-top:18px 0 18px 0;
}}
.hf-auth-box, .hf-onboarding-box {{
    max-width: 920px;
    margin: 0 auto 1.5rem auto;
    padding: 26px;
    border-radius: 18px;
    background: {COLORI["colore_card"]};
    border: 1px solid {COLORI["colore_bordo"]};
}}
</style>
""", unsafe_allow_html=True)


# ---------------------------
# Helpers import/pricing
# ---------------------------
def clean_money(value):
    if pd.isna(value):
        return 0.0
    s = str(value).strip().upper().replace("EUR", "").replace("€", "").strip()
    s = s.replace("\xa0", " ").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(match.group()) if match else 0.0


def clean_int(value, default=1):
    if pd.isna(value):
        return default
    s = str(value).strip().replace(",", ".")
    match = re.search(r"\d+", s)
    return int(match.group()) if match else default


def normalize_status(value):
    if pd.isna(value):
        return "confirmed"

    s = str(value).strip().lower()
    s = re.sub(r"\s+", " ", s)

    if not s:
        return "confirmed"

    cancelled_terms = [
        "cancelled", "canceled", "annullata", "annullato", "annull", "no-show",
        "noshow", "no show", "cancellata", "cancellato"
    ]
    confirmed_terms = [
        "confirmed", "confermata", "confermato", "ok", "booked"
    ]

    if any(term in s for term in cancelled_terms):
        return "cancelled"
    if any(term in s for term in confirmed_terms):
        return "confirmed"

    return "confirmed"


def detect_booking_export(columns):
    cols = [str(c).strip().lower() for c in columns]
    return (
        any("nome osp" in c for c in cols)
        and any("arrivo" in c for c in cols)
        and any("partenza" in c for c in cols)
        and any("prezzo" in c for c in cols)
        and any("persone" in c for c in cols)
    )


def load_booking_file(uploaded_file, cleaning_cost_default):
    ext = Path(uploaded_file.name).suffix.lower()
    if ext == ".csv":
        raw = pd.read_csv(uploaded_file)
    elif ext in [".xls", ".xlsx"]:
        raw = pd.read_excel(uploaded_file)
    else:
        raise ValueError("Formato non supportato. Usa CSV, XLS o XLSX.")

    raw_columns = {str(c).strip().lower(): c for c in raw.columns}

    def find_col(possible_fragments, exclude_fragments=None, prefer_exact=None):
        exclude_fragments = exclude_fragments or []
        prefer_exact = prefer_exact or []

        for exact in prefer_exact:
            for low, original in raw_columns.items():
                if low == exact:
                    return original

        for frag in possible_fragments:
            for low, original in raw_columns.items():
                if frag in low and not any(ex in low for ex in exclude_fragments):
                    return original
        return None

    guest_col = find_col(["nome osp"])
    checkin_col = find_col(["arrivo"])
    checkout_col = find_col(["partenza"])
    price_col = find_col(["prezzo"])
    fee_col = find_col(["importo c", "commission"])
    status_col = find_col(
        ["stato prenotazione", "booking status", "reservation status", "stato"],
        exclude_fragments=["pagamento", "payment", "commission", "commissione", "camera", "stanza"],
        prefer_exact=["stato prenotazione", "booking status", "reservation status", "stato"]
    )
    guests_col = find_col(["persone"])

    required = {
        "Nome ospite": guest_col,
        "Arrivo": checkin_col,
        "Partenza": checkout_col,
        "Prezzo": price_col,
        "Importo commissione": fee_col,
        "Stato": status_col,
        "Persone": guests_col,
    }
    missing = [name for name, col in required.items() if col is None]
    if missing:
        raise ValueError(f"Nel file Booking mancano queste colonne obbligatorie: {', '.join(missing)}")

    df = pd.DataFrame({
        "platform": "Booking",
        "guest_name": raw[guest_col].astype(str).str.strip(),
        "guest_phone": "",
        "check_in": pd.to_datetime(raw[checkin_col], errors="coerce").dt.date,
        "check_out": pd.to_datetime(raw[checkout_col], errors="coerce").dt.date,
        "total_price": raw[price_col].apply(clean_money),
        "cleaning_cost": float(cleaning_cost_default),
        "platform_fee": raw[fee_col].apply(clean_money),
        "transaction_cost": 0.0,
        "raw_booking_status": raw[status_col].astype(str).str.strip(),
        "status": raw[status_col].apply(normalize_status),
        "guests": raw[guests_col].apply(lambda x: clean_int(x, 1)).astype(int).clip(lower=1),
    })

    df = df.dropna(subset=["check_in", "check_out"]).copy()
    df = df[df["guest_name"].str.strip() != ""].copy()
    return df


def load_generic_csv(uploaded_file):
    df = pd.read_csv(uploaded_file)
    required = [
        "platform", "guest_name", "check_in", "check_out", "total_price",
        "cleaning_cost", "platform_fee", "transaction_cost", "status", "guests"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mancano queste colonne nel CSV: {', '.join(missing)}")

    if "guest_phone" not in df.columns:
        df["guest_phone"] = ""

    df["check_in"] = pd.to_datetime(df["check_in"]).dt.date
    df["check_out"] = pd.to_datetime(df["check_out"]).dt.date
    for col in ["total_price", "cleaning_cost", "platform_fee", "transaction_cost", "guests"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["guests"] = df["guests"].astype(int).clip(lower=1)
    df["status"] = df["status"].apply(normalize_status)
    return df


def load_data(uploaded_file, cleaning_cost_default, import_mode):
    if import_mode == "Booking export":
        return load_booking_file(uploaded_file, cleaning_cost_default)

    ext = Path(uploaded_file.name).suffix.lower()

    if ext == ".csv":
        try:
            return load_generic_csv(uploaded_file)
        except Exception:
            uploaded_file.seek(0)
            return load_booking_file(uploaded_file, cleaning_cost_default)

    if ext in [".xls", ".xlsx"]:
        preview = pd.read_excel(uploaded_file, nrows=5)
        uploaded_file.seek(0)
        if detect_booking_export(preview.columns):
            return load_booking_file(uploaded_file, cleaning_cost_default)
        raise ValueError("Excel non riconosciuto. Per ora supporto diretto solo per export Booking.")

    raise ValueError("Formato non supportato. Usa CSV, XLS o XLSX.")


def get_weekend_days(mode):
    if mode == "Ven-Sab-Dom":
        return {4, 5, 6}
    if mode == "Ven-Sab":
        return {4, 5}
    if mode == "Solo sabato":
        return {5}
    return {4, 5, 6}


def get_hourly_cleaning_totals_by_booking(utente_id):
    if utente_id in [None, ""]:
        return {}, {}

    cleaning_df = load_cleaning_services(utente_id)
    if cleaning_df.empty:
        return {}, {}

    cleaning_df = cleaning_df.copy()
    cleaning_df["booking_ref"] = cleaning_df.get("booking_ref", "").astype(str).str.strip()
    cleaning_df["guest_name"] = cleaning_df.get("guest_name", "").astype(str).str.strip()
    cleaning_df["service_type"] = cleaning_df.get("service_type", "").astype(str).str.strip().str.lower()
    cleaning_df["total_cost"] = pd.to_numeric(cleaning_df.get("total_cost", 0), errors="coerce").fillna(0.0)
    cleaning_df["id"] = pd.to_numeric(cleaning_df.get("id", 0), errors="coerce").fillna(0).astype(int)
    cleaning_df["service_date_dt"] = pd.to_datetime(cleaning_df.get("service_date"), errors="coerce").dt.date

    cleaning_df = cleaning_df[cleaning_df["service_type"] == "check_out"].copy()
    if cleaning_df.empty:
        return {}, {}

    booking_ref_map = {}
    booking_ref_df = cleaning_df[cleaning_df["booking_ref"] != ""].copy()
    if not booking_ref_df.empty:
        booking_ref_df = booking_ref_df.sort_values(["booking_ref", "id"])
        latest_booking_ref_records = booking_ref_df.groupby("booking_ref", as_index=False).tail(1)
        booking_ref_map = dict(zip(latest_booking_ref_records["booking_ref"], latest_booking_ref_records["total_cost"]))

    fallback_map = {}
    fallback_df = cleaning_df[
        (cleaning_df["guest_name"] != "")
        & cleaning_df["service_date_dt"].notna()
    ].copy()
    if not fallback_df.empty:
        fallback_df["fallback_key"] = fallback_df.apply(
            lambda row: f"{str(row['guest_name']).strip()}||{row['service_date_dt'].isoformat()}",
            axis=1,
        )
        fallback_df = fallback_df.sort_values(["fallback_key", "id"])
        latest_fallback_records = fallback_df.groupby("fallback_key", as_index=False).tail(1)
        fallback_map = dict(zip(latest_fallback_records["fallback_key"], latest_fallback_records["total_cost"]))

    return booking_ref_map, fallback_map


def enrich_financials(
    df,
    city_tax_rate,
    include_city_tax,
    transaction_mode,
    transaction_pct,
    vat_pct,
    withholding_pct,
    include_withholding,
    cleaning_mode,
    monthly_cleaning_cost,
    selected_year=None,
    selected_month=None,
    utente_id=None,
):
    out = df.copy()
    out["nights"] = (pd.to_datetime(out["check_out"]) - pd.to_datetime(out["check_in"])).dt.days.clip(lower=1)

    if include_city_tax:
        out["city_tax"] = (out["guests"] * out["nights"] * float(city_tax_rate)).round(2)
    else:
        out["city_tax"] = 0.0

    out["vat_platform_services"] = (out["platform_fee"] * float(vat_pct) / 100).round(2)

    if transaction_mode == "Percentuale":
        out["transaction_cost"] = (out["total_price"] * float(transaction_pct) / 100).round(2)
    else:
        out["transaction_cost"] = pd.to_numeric(out["transaction_cost"], errors="coerce").fillna(0.0).round(2)

    out["cleaning_cost"] = pd.to_numeric(out["cleaning_cost"], errors="coerce").fillna(0.0).round(2)

    if cleaning_mode == "Per prenotazione":
        out["cleaning_allocated"] = out["cleaning_cost"].round(2)
    elif cleaning_mode == "Ad ore":
        hourly_cleaning_map, hourly_cleaning_fallback_map = get_hourly_cleaning_totals_by_booking(utente_id)
        out["booking_ref_calc"] = out.apply(booking_reference, axis=1)
        out["cleaning_allocated"] = out["booking_ref_calc"].map(hourly_cleaning_map)

        missing_mask = out["cleaning_allocated"].isna()
        if missing_mask.any():
            out.loc[missing_mask, "cleaning_fallback_key"] = out.loc[missing_mask].apply(
                lambda row: f"{str(row.get('guest_name', '')).strip()}||{pd.to_datetime(row.get('check_out')).date().isoformat()}",
                axis=1,
            )
            out.loc[missing_mask, "cleaning_allocated"] = out.loc[missing_mask, "cleaning_fallback_key"].map(hourly_cleaning_fallback_map)

        out["cleaning_allocated"] = out["cleaning_allocated"].fillna(0.0).round(2)
        drop_cols = [col for col in ["booking_ref_calc", "cleaning_fallback_key"] if col in out.columns]
        if drop_cols:
            out = out.drop(columns=drop_cols)
        out["cleaning_cost"] = out["cleaning_allocated"].round(2)
    else:
        out["cleaning_allocated"] = 0.0
        active = out[out["status"].str.lower() != "cancelled"].copy()
        if selected_year and selected_month:
            active = active[
                (pd.to_datetime(active["check_in"]).dt.year == selected_year) &
                (pd.to_datetime(active["check_in"]).dt.month == selected_month)
            ]
        total_nights = active["nights"].sum()
        if total_nights > 0:
            per_night = float(monthly_cleaning_cost) / total_nights
            out["cleaning_allocated"] = (out["nights"] * per_night).round(2)
        out["cleaning_cost"] = out["cleaning_allocated"].round(2)

    out["withholding_tax"] = 0.0
    if include_withholding:
        out["withholding_tax"] = (out["total_price"] * float(withholding_pct) / 100).round(2)

    out["net_operating"] = (
        out["total_price"]
        - out["platform_fee"]
        - out["vat_platform_services"]
        - out["transaction_cost"]
        - out["cleaning_allocated"]
        - out["city_tax"]
    ).round(2)

    out["net_real"] = (out["net_operating"] - out["withholding_tax"]).round(2)

    custom_mask = out["platform"].astype(str).str.lower().eq("custom")

    out.loc[custom_mask, "platform_fee"] = 0.0
    out.loc[custom_mask, "transaction_cost"] = 0.0
    out.loc[custom_mask, "vat_platform_services"] = 0.0
    out.loc[custom_mask, "withholding_tax"] = 0.0
    out.loc[custom_mask, "net_operating"] = (
        out.loc[custom_mask, "total_price"]
        - out.loc[custom_mask, "cleaning_allocated"]
        - out.loc[custom_mask, "city_tax"]
    ).round(2)
    out.loc[custom_mask, "net_real"] = out.loc[custom_mask, "net_operating"]

    cancelled_mask = out["status"].astype(str).str.lower().eq("cancelled")

    zero_columns = [
        "total_price",
        "platform_fee",
        "transaction_cost",
        "vat_platform_services",
        "withholding_tax",
        "city_tax",
        "cleaning_cost",
        "cleaning_allocated",
        "net_operating",
        "net_real",
    ]

    for col in zero_columns:
        out.loc[cancelled_mask, col] = 0.0

    out["adr"] = (out["total_price"] / out["nights"]).round(2)
    return out


def month_slice(df, year, month):
    month_start = date(year, month, 1)
    next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    active = df[
        (df["status"].str.lower() != "cancelled")
        & (hf_date_series(df["check_in"]) < hf_bound(next_month))
        & (hf_date_series(df["check_out"]) >= hf_bound(month_start))
    ].copy()
    return active, month_start, next_month


def month_stats(df, year, month):
    active, month_start, next_month = month_slice(df, year, month)
    days_in_month = (next_month - month_start).days

    occupied_dates = set()
    for _, row in active.iterrows():
        row_check_in = hf_date(row["check_in"])
        row_check_out = hf_date(row["check_out"])
        if row_check_in is None or row_check_out is None:
            continue
        start = max(row_check_in, month_start)
        end = min(row_check_out, next_month)
        for d in pd.date_range(start, end - timedelta(days=1), freq="D"):
            occupied_dates.add(d.date())

    occupied_nights = len(occupied_dates)
    occupancy = round((occupied_nights / days_in_month) * 100, 1) if days_in_month else 0

    month_res = active[
        (pd.to_datetime(active["check_in"]).dt.month == month)
        & (pd.to_datetime(active["check_in"]).dt.year == year)
    ]
    total_bookings = len(month_res)

    return {
        "occupied_nights": occupied_nights,
        "occupancy": occupancy,
        "bookings": total_bookings,
        "revenue": round(month_res["total_price"].sum(), 2),
        "net_operating": round(month_res["net_operating"].sum(), 2),
        "net_real": round(month_res["net_real"].sum(), 2),
        "adr": round(month_res["adr"].mean(), 2) if total_bookings else 0,
    }


def annual_summary(df, year):
    valid = df[
        (df["status"].str.lower() != "cancelled")
        & (pd.to_datetime(df["check_in"]).dt.year == year)
    ].copy()

    if valid.empty:
        return pd.DataFrame(columns=["month", "bookings", "revenue", "net_operating", "net_real", "adr"])

    valid["month_num"] = pd.to_datetime(valid["check_in"]).dt.month
    summary = valid.groupby("month_num", as_index=False).agg(
        bookings=("guest_name", "count"),
        revenue=("total_price", "sum"),
        net_operating=("net_operating", "sum"),
        net_real=("net_real", "sum"),
        adr=("adr", "mean"),
    )
    summary["month"] = summary["month_num"].map({
        1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mag", 6: "Giu",
        7: "Lug", 8: "Ago", 9: "Set", 10: "Ott", 11: "Nov", 12: "Dic"
    })
    return summary[["month", "bookings", "revenue", "net_operating", "net_real", "adr"]]


def get_period_bounds(year, period_mode="Mensile", month=1, quarter=1, semester=1, custom_start=None, custom_end=None):
    period_mode = str(period_mode or "Mensile")

    if period_mode == "Mensile":
        start_date = date(year, month, 1)
        end_date = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        label = datetime(year, month, 1).strftime("%B %Y").capitalize()
    elif period_mode == "Trimestrale":
        quarter = int(quarter)
        start_month = ((quarter - 1) * 3) + 1
        start_date = date(year, start_month, 1)
        end_month = start_month + 3
        end_date = date(year + 1, 1, 1) if end_month > 12 else date(year, end_month, 1)
        label = f"Q{quarter} {year}"
    elif period_mode == "Semestrale":
        semester = int(semester)
        start_month = 1 if semester == 1 else 7
        end_month = 7 if semester == 1 else 13
        start_date = date(year, start_month, 1)
        end_date = date(year + 1, 1, 1) if end_month == 13 else date(year, end_month, 1)
        label = f"S{semester} {year}"
    elif period_mode == "Annuale":
        start_date = date(year, 1, 1)
        end_date = date(year + 1, 1, 1)
        label = str(year)
    else:
        if isinstance(custom_start, datetime):
            custom_start = custom_start.date()
        if isinstance(custom_end, datetime):
            custom_end = custom_end.date()
        start_date = custom_start or date(year, 1, 1)
        inclusive_end = custom_end or date(year, 12, 31)
        if inclusive_end < start_date:
            inclusive_end = start_date
        end_date = inclusive_end + timedelta(days=1)
        label = f"{start_date.strftime('%d/%m/%Y')} - {inclusive_end.strftime('%d/%m/%Y')}"
    return start_date, end_date, label


def period_stats(df, start_date, end_date):
    active = df[
        (df["status"].str.lower() != "cancelled")
        & (hf_date_series(df["check_in"]) < hf_bound(end_date))
        & (hf_date_series(df["check_out"]) >= hf_bound(start_date))
    ].copy()

    days_in_period = (end_date - start_date).days
    occupied_dates = set()
    for _, row in active.iterrows():
        row_check_in = hf_date(row["check_in"])
        row_check_out = hf_date(row["check_out"])
        if row_check_in is None or row_check_out is None:
            continue
        start = max(row_check_in, start_date)
        end = min(row_check_out, end_date)
        for d in pd.date_range(start, end - timedelta(days=1), freq="D"):
            occupied_dates.add(d.date())

    occupied_nights = len(occupied_dates)
    occupancy = round((occupied_nights / days_in_period) * 100, 1) if days_in_period else 0

    arrivals = active[
        (hf_date_series(active["check_in"]) >= hf_bound(start_date))
        & (hf_date_series(active["check_in"]) < hf_bound(end_date))
    ].copy()
    total_bookings = len(arrivals)

    return {
        "occupied_nights": occupied_nights,
        "occupancy": occupancy,
        "bookings": total_bookings,
        "revenue": round(arrivals["total_price"].sum(), 2),
        "net_operating": round(arrivals["net_operating"].sum(), 2),
        "net_real": round(arrivals["net_real"].sum(), 2),
        "adr": round(arrivals["adr"].mean(), 2) if total_bookings else 0,
    }


def filter_df_by_period(df, start_date, end_date):
    return df[
        (hf_date_series(df["check_in"]) >= hf_bound(start_date))
        & (hf_date_series(df["check_in"]) < hf_bound(end_date))
    ].copy()


def build_period_summary(df, year, period_mode="Mensile", custom_start=None, custom_end=None):
    rows = []

    if period_mode == "Mensile":
        for month in range(1, 13):
            start_date, end_date, label = get_period_bounds(year, "Mensile", month=month)
            stats = period_stats(df, start_date, end_date)
            rows.append({
                "period": label,
                "bookings": stats["bookings"],
                "occupancy": stats["occupancy"],
                "revenue": stats["revenue"],
                "net_operating": stats["net_operating"],
                "net_real": stats["net_real"],
                "adr": stats["adr"],
            })
        return pd.DataFrame(rows)

    if period_mode == "Trimestrale":
        for quarter in range(1, 5):
            start_date, end_date, label = get_period_bounds(year, "Trimestrale", quarter=quarter)
            stats = period_stats(df, start_date, end_date)
            rows.append({
                "period": label,
                "bookings": stats["bookings"],
                "occupancy": stats["occupancy"],
                "revenue": stats["revenue"],
                "net_operating": stats["net_operating"],
                "net_real": stats["net_real"],
                "adr": stats["adr"],
            })
        return pd.DataFrame(rows)

    if period_mode == "Semestrale":
        for semester in [1, 2]:
            start_date, end_date, label = get_period_bounds(year, "Semestrale", semester=semester)
            stats = period_stats(df, start_date, end_date)
            rows.append({
                "period": label,
                "bookings": stats["bookings"],
                "occupancy": stats["occupancy"],
                "revenue": stats["revenue"],
                "net_operating": stats["net_operating"],
                "net_real": stats["net_real"],
                "adr": stats["adr"],
            })
        return pd.DataFrame(rows)

    if period_mode == "Annuale":
        start_date, end_date, label = get_period_bounds(year, "Annuale")
        stats = period_stats(df, start_date, end_date)
        return pd.DataFrame([{
            "period": label,
            "bookings": stats["bookings"],
            "occupancy": stats["occupancy"],
            "revenue": stats["revenue"],
            "net_operating": stats["net_operating"],
            "net_real": stats["net_real"],
            "adr": stats["adr"],
        }])

    start_date, end_date, label = get_period_bounds(year, "Personalizzato", custom_start=custom_start, custom_end=custom_end)
    stats = period_stats(df, start_date, end_date)
    return pd.DataFrame([{
        "period": label,
        "bookings": stats["bookings"],
        "occupancy": stats["occupancy"],
        "revenue": stats["revenue"],
        "net_operating": stats["net_operating"],
        "net_real": stats["net_real"],
        "adr": stats["adr"],
    }])


def get_period_bounds(period_mode, year, month=1, quarter=1, semester=1, custom_start_date=None, custom_end_date=None):
    period_mode = str(period_mode or "Mensile")

    if period_mode == "Mensile":
        start_date = date(int(year), int(month), 1)
        end_date = date(int(year) + 1, 1, 1) if int(month) == 12 else date(int(year), int(month) + 1, 1)
        label = start_date.strftime("%B %Y").capitalize()
        return start_date, end_date, label

    if period_mode == "Trimestrale":
        q = int(quarter)
        start_month = ((q - 1) * 3) + 1
        start_date = date(int(year), start_month, 1)
        end_date = date(int(year) + 1, 1, 1) if start_month == 10 else date(int(year), start_month + 3, 1)
        label = f"{q}° Trimestre {year}"
        return start_date, end_date, label

    if period_mode == "Semestrale":
        s = int(semester)
        start_month = 1 if s == 1 else 7
        start_date = date(int(year), start_month, 1)
        end_date = date(int(year) + 1, 1, 1) if s == 2 else date(int(year), 7, 1)
        label = f"{s}° Semestre {year}"
        return start_date, end_date, label

    if period_mode == "Annuale":
        start_date = date(int(year), 1, 1)
        end_date = date(int(year) + 1, 1, 1)
        label = str(year)
        return start_date, end_date, label

    start_date = custom_start_date if isinstance(custom_start_date, date) else pd.to_datetime(custom_start_date).date()
    end_inclusive = custom_end_date if isinstance(custom_end_date, date) else pd.to_datetime(custom_end_date).date()
    if end_inclusive < start_date:
        start_date, end_inclusive = end_inclusive, start_date
    end_date = end_inclusive + timedelta(days=1)
    label = f"{start_date.strftime('%d/%m/%Y')} - {end_inclusive.strftime('%d/%m/%Y')}"
    return start_date, end_date, label


def period_slice(df, start_date, end_date):
    active = df[
        (df["status"].str.lower() != "cancelled")
        & (hf_date_series(df["check_in"]) < hf_bound(end_date))
        & (hf_date_series(df["check_out"]) >= hf_bound(start_date))
    ].copy()
    return active


def period_stats(df, start_date, end_date):
    active = period_slice(df, start_date, end_date)
    days_in_period = max((end_date - start_date).days, 1)

    occupied_dates = set()
    for _, row in active.iterrows():
        row_check_in = hf_date(row["check_in"])
        row_check_out = hf_date(row["check_out"])
        if row_check_in is None or row_check_out is None:
            continue
        row_start = max(row_check_in, start_date)
        row_end = min(row_check_out, end_date)
        if row_end <= row_start:
            continue
        for d in pd.date_range(row_start, row_end - timedelta(days=1), freq="D"):
            occupied_dates.add(d.date())

    occupied_nights = len(occupied_dates)
    occupancy = round((occupied_nights / days_in_period) * 100, 1) if days_in_period else 0

    period_res = active[
        (hf_date_series(active["check_in"]) >= hf_bound(start_date))
        & (hf_date_series(active["check_in"]) < hf_bound(end_date))
    ].copy()
    total_bookings = len(period_res)

    return {
        "occupied_nights": occupied_nights,
        "occupancy": occupancy,
        "bookings": total_bookings,
        "revenue": round(period_res["total_price"].sum(), 2),
        "net_operating": round(period_res["net_operating"].sum(), 2),
        "net_real": round(period_res["net_real"].sum(), 2),
        "adr": round(period_res["adr"].mean(), 2) if total_bookings else 0,
    }


def build_dashboard_history(df, period_mode, year, month=1, quarter=1, semester=1):
    rows = []
    if period_mode == "Mensile":
        for m in range(1, 13):
            start_date, end_date, label = get_period_bounds("Mensile", year, month=m)
            stats = period_stats(df, start_date, end_date)
            rows.append({"period": label[:3], **stats})
    elif period_mode == "Trimestrale":
        for q in range(1, 5):
            start_date, end_date, label = get_period_bounds("Trimestrale", year, quarter=q)
            stats = period_stats(df, start_date, end_date)
            rows.append({"period": label, **stats})
    elif period_mode == "Semestrale":
        for s in (1, 2):
            start_date, end_date, label = get_period_bounds("Semestrale", year, semester=s)
            stats = period_stats(df, start_date, end_date)
            rows.append({"period": label, **stats})
    elif period_mode == "Annuale":
        for offset in range(-2, 1):
            y = int(year) + offset
            start_date, end_date, label = get_period_bounds("Annuale", y)
            stats = period_stats(df, start_date, end_date)
            rows.append({"period": label, **stats})
    else:
        return pd.DataFrame(columns=["period", "bookings", "revenue", "net_operating", "net_real", "adr", "occupancy"])

    summary = pd.DataFrame(rows)
    if not summary.empty:
        summary["bookings"] = summary["bookings"].astype(int)
    return summary[["period", "bookings", "occupancy", "revenue", "net_operating", "net_real", "adr"]]


def compute_base_price(df, selected_year, selected_month, source_mode, manual_value):
    valid = df[df["status"].str.lower() != "cancelled"].copy()

    if source_mode == "Manuale":
        return float(manual_value), "Prezzo manuale"

    if source_mode == "ADR mese dashboard":
        month_df = valid[
            (pd.to_datetime(valid["check_in"]).dt.year == selected_year) &
            (pd.to_datetime(valid["check_in"]).dt.month == selected_month)
        ]
        if len(month_df) > 0:
            return round(month_df["adr"].mean(), 2), "ADR medio mese dashboard"

    if source_mode == "ADR ultimi 30 giorni":
        max_date = pd.to_datetime(valid["check_in"]).max()
        if pd.notna(max_date):
            start = max_date - pd.Timedelta(days=30)
            recent = valid[
                (pd.to_datetime(valid["check_in"]) >= start) &
                (pd.to_datetime(valid["check_in"]) <= max_date)
            ]
            if len(recent) > 0:
                return round(recent["adr"].mean(), 2), "ADR medio ultimi 30 giorni"

    if len(valid) > 0:
        return round(valid["adr"].mean(), 2), "ADR medio generale"

    return float(manual_value), "Manuale fallback"


def pricing_suggestion(
    base_price,
    weekend,
    event,
    days_to_checkin,
    monthly_occupancy,
    weekend_markup,
    event_markup,
    last_minute_discount,
    last_minute_days,
    early_booking_markup,
    early_booking_days,
    high_occ_threshold,
    high_occ_markup,
    low_occ_threshold,
    low_occ_discount
):
    price = float(base_price)
    notes = []

    if weekend:
        price *= 1 + (float(weekend_markup) / 100)
        notes.append(f"Weekend: +{weekend_markup}%")

    if event:
        price *= 1 + (float(event_markup) / 100)
        notes.append(f"Evento: +{event_markup}%")

    if days_to_checkin <= int(last_minute_days):
        price *= 1 - (float(last_minute_discount) / 100)
        notes.append(f"Last minute entro {int(last_minute_days)} giorni: -{last_minute_discount}%")
    elif days_to_checkin >= int(early_booking_days):
        price *= 1 + (float(early_booking_markup) / 100)
        notes.append(f"Prenotazione anticipata oltre {int(early_booking_days)} giorni: +{early_booking_markup}%")

    if monthly_occupancy >= float(high_occ_threshold):
        price *= 1 + (float(high_occ_markup) / 100)
        notes.append(f"Occupazione alta ({high_occ_threshold}%+): +{high_occ_markup}%")
    elif monthly_occupancy <= float(low_occ_threshold):
        price *= 1 - (float(low_occ_discount) / 100)
        notes.append(f"Occupazione bassa (<={low_occ_threshold}%): -{low_occ_discount}%")

    return round(price, 2), notes




def parse_time_value(value, fallback="15:00"):
    raw = str(value or fallback).strip()
    if not raw:
        raw = fallback
    try:
        return datetime.strptime(raw, "%H:%M").time()
    except Exception:
        return datetime.strptime(fallback, "%H:%M").time()


def booking_reference(row):
    return "||".join([
        str(row.get("platform", "Booking")),
        str(row.get("guest_name", "")).strip(),
        str(row.get("check_in", "")),
        str(row.get("check_out", "")),
    ])


def render_message_from_type(message_type, guest_name, check_in, check_out, profilo):
    property_name = str(profilo.get("nome_immobile", "La tua struttura") or "La tua struttura")
    checkin_time = str(profilo.get("checkin_da", "15:00") or "15:00")
    checkout_time = str(profilo.get("checkout_entro", "10:00") or "10:00")
    wifi_name = str(profilo.get("wifi_nome", "") or "")
    wifi_password = str(profilo.get("wifi_password", "") or "")
    address = str(profilo.get("indirizzo_completo", "") or "")
    host_name = str(profilo.get("nome_host", "") or "")
    whatsapp = str(profilo.get("numero_whatsapp", "") or "")
    ingresso = str(profilo.get("istruzioni_ingresso", "") or "").strip()
    note_finali = str(profilo.get("note_finali", "") or "").strip()

    messages = {
        "booking_confirmed": (
            f"Ciao {guest_name}, grazie per aver prenotato {property_name}. "
            f"Ti aspettiamo dal {check_in.strftime('%d/%m/%Y')} al {check_out.strftime('%d/%m/%Y')}."
        ),
        "reminder_checkin": (
            f"Ciao {guest_name}, ti ricordiamo che il check-in per {property_name} è previsto domani "
            f"({check_in.strftime('%d/%m/%Y')}) a partire dalle {checkin_time}."
        ),
        "checkin_instructions": (
            f"Ciao {guest_name}, benvenuto a {property_name}. "
            f"Il check-in è disponibile da oggi alle {checkin_time}. "
            f"Indirizzo: {address}. "
            f"Wi‑Fi: {wifi_name} / Password: {wifi_password}. "
            f"{'Istruzioni ingresso: ' + ingresso + '. ' if ingresso else ''}"
            f"{'Host: ' + host_name + '. ' if host_name else ''}"
            f"{'Contatto: ' + whatsapp + '. ' if whatsapp else ''}"
        ),
        "reminder_checkout": (
            f"Ciao {guest_name}, ti ricordiamo che il check-out di {property_name} è previsto domani "
            f"entro le {checkout_time} ({check_out.strftime('%d/%m/%Y')})."
        ),
        "review_request": (
            f"Ciao {guest_name}, grazie ancora per il soggiorno presso {property_name}. "
            f"Se ti sei trovato bene, una recensione sarebbe davvero preziosa per noi. "
            f"{note_finali if note_finali else ''}".strip()
        ),
    }
    return messages.get(message_type, "")


def build_scheduled_messages_for_booking(row, profilo, scheduling_rules=None, template_base=None):
    guest_name = str(row.get("guest_name", "") or "").strip() or "Ospite"
    guest_phone = str(row.get("guest_phone", "") or "").strip()
    platform = str(row.get("platform", "") or "").strip() or "Booking"
    check_in = pd.to_datetime(row.get("check_in")).to_pydatetime()
    check_out = pd.to_datetime(row.get("check_out")).to_pydatetime()

    booking_ref = f"{platform}|{guest_name}|{check_in.date().isoformat()}|{check_out.date().isoformat()}"

    property_name = str(profilo.get("nome_immobile", "") or "La tua struttura").strip() or "La tua struttura"
    nome_host = str(profilo.get("nome_host", "") or "").strip()
    numero_whatsapp = str(profilo.get("numero_whatsapp", "") or "").strip()
    checkin_da = str(profilo.get("checkin_da", "") or "15:00").strip() or "15:00"
    checkin_fino = str(profilo.get("checkin_fino", "") or "22:00").strip() or "22:00"
    checkout_entro = str(profilo.get("checkout_entro", "") or "10:00").strip() or "10:00"
    wifi_name = str(profilo.get("wifi_nome", "") or "WIFI-GUEST").strip() or "WIFI-GUEST"
    wifi_password = str(profilo.get("wifi_password", "") or "password123").strip() or "password123"
    parcheggio_disponibile = bool(profilo.get("parcheggio_disponibile", False))
    tassa_soggiorno = float(profilo.get("tassa_soggiorno", 0) or 0)

    rules = scheduling_rules or {
        "confirm_offset_days": 0,
        "confirm_time": "10:00",
        "checkin_reminder_days": 1,
        "checkin_reminder_time": "18:00",
        "checkin_instr_time": "15:00",
        "checkout_reminder_days": 1,
        "checkout_reminder_time": "18:00",
        "review_days_after": 1,
        "review_time": "12:00",
    }

    template_base = template_base or {
        "booking_confirmation": "Ciao {nome_ospite}, grazie per aver prenotato {nome_struttura}.\n\nIl tuo soggiorno è confermato dal {data_checkin} al {data_checkout}.\n\nCheck-in dalle {checkin_da} alle {checkin_fino}\nCheck-out entro le {checkout_entro}\n\nPer qualsiasi necessità puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "checkin_reminder": "Ciao {nome_ospite}, ti ricordiamo che il check-in presso {nome_struttura} sarà il {data_checkin}.\n\nL’orario di arrivo è previsto dalle {checkin_da} alle {checkin_fino}.\n\nSe hai bisogno di assistenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "checkin_instructions": "Ciao {nome_ospite}, benvenuto a {nome_struttura}.\n\nTi ricordiamo che oggi il check-in è disponibile dalle {checkin_da} alle {checkin_fino}.\n\nDati Wi-Fi:\nNome rete: {wifi_nome}\nPassword: {wifi_password}\n\nHost di riferimento: {nome_host}\nContatto WhatsApp: {numero_whatsapp}{testo_parcheggio}{testo_tassa_soggiorno}",
        "checkout_reminder": "Ciao {nome_ospite}, ti ricordiamo che il check-out di {nome_struttura} è previsto il {data_checkout} entro le {checkout_entro}.\n\nPer qualsiasi necessità prima della partenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.",
        "review_request": "Ciao {nome_ospite}, grazie per aver scelto {nome_struttura}.\n\nSperiamo che il soggiorno sia stato piacevole. Se ti sei trovato bene, ci farebbe molto piacere ricevere una tua recensione.\n\nUn saluto da {nome_host}.",
    }

    context = {
        "nome_ospite": guest_name,
        "data_checkin": check_in.strftime("%d/%m/%Y"),
        "data_checkout": check_out.strftime("%d/%m/%Y"),
        "nome_struttura": property_name,
        "nome_host": nome_host,
        "numero_whatsapp": numero_whatsapp,
        "checkin_da": checkin_da,
        "checkin_fino": checkin_fino,
        "checkout_entro": checkout_entro,
        "ora_checkin": checkin_da,
        "wifi_nome": wifi_name,
        "wifi_password": wifi_password,
        "parcheggio_disponibile": "Sì" if parcheggio_disponibile else "No",
        "tassa_soggiorno": f"{tassa_soggiorno:.2f}".replace(".", ","),
        "testo_parcheggio": "\n\nParcheggio disponibile in struttura." if parcheggio_disponibile else "",
        "testo_tassa_soggiorno": f"\nTassa di soggiorno: € {tassa_soggiorno:.2f} per persona a notte.".replace(".", ",") if tassa_soggiorno > 0 else "",
    }

    confirm_hour, confirm_minute = parse_time_string(rules["confirm_time"], "10:00")
    checkin_reminder_hour, checkin_reminder_minute = parse_time_string(rules["checkin_reminder_time"], "18:00")
    checkin_instr_hour, checkin_instr_minute = parse_time_string(rules["checkin_instr_time"], "15:00")
    checkout_reminder_hour, checkout_reminder_minute = parse_time_string(rules["checkout_reminder_time"], "18:00")
    review_hour, review_minute = parse_time_string(rules["review_time"], "12:00")

    confirm_at = (check_in - timedelta(days=int(rules["confirm_offset_days"]))).replace(
        hour=confirm_hour, minute=confirm_minute, second=0, microsecond=0
    )
    reminder_checkin_at = (check_in - timedelta(days=int(rules["checkin_reminder_days"]))).replace(
        hour=checkin_reminder_hour, minute=checkin_reminder_minute, second=0, microsecond=0
    )
    instructions_at = check_in.replace(
        hour=checkin_instr_hour, minute=checkin_instr_minute, second=0, microsecond=0
    )
    reminder_checkout_at = (check_out - timedelta(days=int(rules["checkout_reminder_days"]))).replace(
        hour=checkout_reminder_hour, minute=checkout_reminder_minute, second=0, microsecond=0
    )
    review_request_at = (check_out + timedelta(days=int(rules["review_days_after"]))).replace(
        hour=review_hour, minute=review_minute, second=0, microsecond=0
    )

    messages = [
        {
            "booking_ref": booking_ref,
            "platform": platform,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "guest_email": "",
            "check_in": check_in.date().isoformat(),
            "check_out": check_out.date().isoformat(),
            "message_type": "booking_confirmation",
            "message_text": render_template_message(template_base["booking_confirmation"], context),
            "send_at": confirm_at.isoformat(),
            "channel": "whatsapp",
            "status": "pending",
        },
        {
            "booking_ref": booking_ref,
            "platform": platform,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "guest_email": "",
            "check_in": check_in.date().isoformat(),
            "check_out": check_out.date().isoformat(),
            "message_type": "checkin_reminder",
            "message_text": render_template_message(template_base["checkin_reminder"], context),
            "send_at": reminder_checkin_at.isoformat(),
            "channel": "whatsapp",
            "status": "pending",
        },
        {
            "booking_ref": booking_ref,
            "platform": platform,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "guest_email": "",
            "check_in": check_in.date().isoformat(),
            "check_out": check_out.date().isoformat(),
            "message_type": "checkin_instructions",
            "message_text": render_template_message(template_base["checkin_instructions"], context),
            "send_at": instructions_at.isoformat(),
            "channel": "whatsapp",
            "status": "pending",
        },
        {
            "booking_ref": booking_ref,
            "platform": platform,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "guest_email": "",
            "check_in": check_in.date().isoformat(),
            "check_out": check_out.date().isoformat(),
            "message_type": "checkout_reminder",
            "message_text": render_template_message(template_base["checkout_reminder"], context),
            "send_at": reminder_checkout_at.isoformat(),
            "channel": "whatsapp",
            "status": "pending",
        },
        {
            "booking_ref": booking_ref,
            "platform": platform,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "guest_email": "",
            "check_in": check_in.date().isoformat(),
            "check_out": check_out.date().isoformat(),
            "message_type": "review_request",
            "message_text": render_template_message(template_base["review_request"], context),
            "send_at": review_request_at.isoformat(),
            "channel": "whatsapp",
            "status": "pending",
        },
    ]
    return messages


def replace_scheduled_messages_for_user(utente_id, bookings_df, profilo, scheduling_rules=None, template_base=None):
    conn = get_conn()
    cur = conn.cursor()

    valid = bookings_df[bookings_df["status"].str.lower() != "cancelled"].copy()
    valid = valid.sort_values(["check_in", "check_out", "guest_name"])

    cur.execute(
        """
        SELECT id, booking_ref, message_type, send_at, status, message_text, sent_at
        FROM scheduled_messages
        WHERE utente_id = ?
        """,
        (utente_id,),
    )
    existing_rows = cur.fetchall()
    existing_map = {
        (row["booking_ref"], row["message_type"], row["send_at"]): dict(row)
        for row in existing_rows
    }

    current_keys = set()
    inserted = 0
    updated = 0

    for _, row in valid.iterrows():
        for msg in build_scheduled_messages_for_booking(
            row,
            profilo,
            scheduling_rules=scheduling_rules,
            template_base=template_base,
        ):
            key = (msg["booking_ref"], msg["message_type"], msg["send_at"])
            current_keys.add(key)
            existing = existing_map.get(key)

            if existing:
                if existing["status"] in ["sent", "cancelled"]:
                    cur.execute(
                        """
                        UPDATE scheduled_messages
                        SET platform = ?, guest_name = ?, guest_phone = ?, check_in = ?, check_out = ?, channel = ?
                        WHERE id = ?
                        """,
                        (
                            msg["platform"],
                            msg["guest_name"],
                            msg["guest_phone"],
                            msg["check_in"],
                            msg["check_out"],
                            msg["channel"],
                            existing["id"],
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE scheduled_messages
                        SET platform = ?, guest_name = ?, guest_phone = ?, check_in = ?, check_out = ?,
                            message_text = ?, channel = ?, status = 'pending', error_message = NULL
                        WHERE id = ?
                        """,
                        (
                            msg["platform"],
                            msg["guest_name"],
                            msg["guest_phone"],
                            msg["check_in"],
                            msg["check_out"],
                            msg["message_text"],
                            msg["channel"],
                            existing["id"],
                        ),
                    )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO scheduled_messages (
                        utente_id, booking_ref, platform, guest_name, guest_phone, guest_email,
                        check_in, check_out, message_type, message_text, send_at, channel, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        utente_id,
                        msg["booking_ref"],
                        msg["platform"],
                        msg["guest_name"],
                        msg["guest_phone"],
                        msg["guest_email"],
                        msg["check_in"],
                        msg["check_out"],
                        msg["message_type"],
                        msg["message_text"],
                        msg["send_at"],
                        msg["channel"],
                        msg["status"],
                    ),
                )
                inserted += 1

    obsolete_pending_ids = []
    for row in existing_rows:
        key = (row["booking_ref"], row["message_type"], row["send_at"])
        if key not in current_keys and row["status"] not in ["sent", "cancelled"]:
            obsolete_pending_ids.append(row["id"])

    if obsolete_pending_ids:
        placeholders = ",".join(["?"] * len(obsolete_pending_ids))
        cur.execute(
            f"DELETE FROM scheduled_messages WHERE utente_id = ? AND id IN ({placeholders})",
            [utente_id] + obsolete_pending_ids,
        )

    conn.commit()
    conn.close()
    return inserted + updated


def load_scheduled_messages(utente_id):
    conn = get_conn()
    query = """
        SELECT id, booking_ref, platform, guest_name, guest_phone, check_in, check_out, message_type, send_at,
               channel, status, message_text, created_at, sent_at, error_message
        FROM scheduled_messages
        WHERE utente_id = ?
        ORDER BY send_at ASC, id ASC
    """
    df = pd.read_sql_query(query, conn, params=(utente_id,))
    conn.close()
    return df


def get_scheduled_message_by_id(message_id, utente_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, booking_ref, platform, guest_name, guest_phone, check_in, check_out, message_type, send_at,
               channel, status, message_text, created_at, sent_at, error_message
        FROM scheduled_messages
        WHERE id = ? AND utente_id = ?
        """,
        (int(message_id), int(utente_id)),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def resolve_message_guest_phone(current_msg, bookings_df):
    if not current_msg:
        return ""

    existing_phone = str(current_msg.get("guest_phone", "") or "").strip()
    if existing_phone:
        return existing_phone

    if bookings_df is None or len(bookings_df) == 0:
        return ""

    booking_ref_value = str(current_msg.get("booking_ref", "") or "").strip()
    if booking_ref_value and "booking_ref_calc" not in bookings_df.columns:
        bookings_df = bookings_df.copy()
        bookings_df["booking_ref_calc"] = bookings_df.apply(booking_reference, axis=1)

    if booking_ref_value and "booking_ref_calc" in bookings_df.columns:
        match_ref = bookings_df[bookings_df["booking_ref_calc"].astype(str) == booking_ref_value]
        if not match_ref.empty:
            return str(match_ref.iloc[0].get("guest_phone", "") or "").strip()

    guest_name = str(current_msg.get("guest_name", "") or "").strip()
    check_in = str(current_msg.get("check_in", "") or "").strip()
    check_out = str(current_msg.get("check_out", "") or "").strip()

    fallback_match = bookings_df[
        (bookings_df["guest_name"].astype(str).str.strip() == guest_name)
        & (bookings_df["check_in"].astype(str) == check_in)
        & (bookings_df["check_out"].astype(str) == check_out)
    ]
    if not fallback_match.empty:
        return str(fallback_match.iloc[0].get("guest_phone", "") or "").strip()

    return ""


def update_scheduled_message_text(message_id, utente_id, new_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE scheduled_messages
        SET message_text = ?
        WHERE id = ? AND utente_id = ?
        """,
        (str(new_text), int(message_id), int(utente_id)),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def update_scheduled_message_status(message_id, utente_id, new_status, error_message=None, set_sent_now=False):
    conn = get_conn()
    cur = conn.cursor()

    sent_at = datetime.utcnow().isoformat() if set_sent_now else None

    cur.execute(
        """
        UPDATE scheduled_messages
        SET status = ?,
            error_message = ?,
            sent_at = CASE
                WHEN ? IS NOT NULL THEN ?
                WHEN ? = 'pending' THEN NULL
                ELSE sent_at
            END
        WHERE id = ? AND utente_id = ?
        """,
        (
            str(new_status),
            error_message,
            sent_at,
            sent_at,
            str(new_status),
            int(message_id),
            int(utente_id),
        ),
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()
    return changed > 0


def label_message_type(message_type):
    mapping = {
        "booking_confirmation": "Conferma prenotazione",
        "checkin_reminder": "Reminder check-in",
        "checkin_instructions": "Istruzioni check-in",
        "checkout_reminder": "Reminder check-out",
        "review_request": "Richiesta recensione",
    }
    return mapping.get(message_type, message_type)


def message_templates(guest_name, check_in, check_out, property_name, checkin_time, wifi_name, wifi_password):
    return {
        "Conferma prenotazione": f"Ciao {guest_name}, grazie per aver prenotato {property_name}. Ti aspettiamo dal {check_in.strftime('%d/%m/%Y')}.",
        "Istruzioni check-in": f"Ciao {guest_name}, il check-in per {property_name} è disponibile dalle {checkin_time} del {check_in.strftime('%d/%m/%Y')}. Wi‑Fi: {wifi_name} / password: {wifi_password}.",
        "Messaggio durante il soggiorno": f"Ciao {guest_name}, spero che il soggiorno stia andando bene. Se ti serve qualsiasi cosa, sono a disposizione.",
        "Promemoria check-out": f"Ciao {guest_name}, ti ricordo che il check-out è previsto entro le ore 10:00 del {check_out.strftime('%d/%m/%Y')}.",
        "Richiesta recensione": f"Ciao {guest_name}, grazie ancora per il soggiorno presso {property_name}. Se ti sei trovato bene, una recensione sarebbe davvero preziosa."
    }


def render_dashboard_dataframe(df_to_show, user_id):
    all_columns = list(df_to_show.columns)
    saved_settings = carica_sidebar_settings(user_id)
    saved_visible_columns = saved_settings.get("dashboard_visible_columns", all_columns)

    if not isinstance(saved_visible_columns, list) or not saved_visible_columns:
        saved_visible_columns = all_columns.copy()

    saved_visible_columns = [col for col in saved_visible_columns if col in all_columns]
    if not saved_visible_columns:
        saved_visible_columns = all_columns.copy()

    state_key = f"dashboard_visible_columns_draft_{user_id}"
    selector_key = f"dashboard_visible_columns_selector_{user_id}"
    manager_key = f"dashboard_column_manager_open_{user_id}"
    loaded_key = f"dashboard_visible_columns_loaded_{user_id}"

    if loaded_key not in st.session_state:
        st.session_state[state_key] = saved_visible_columns.copy()
        st.session_state[selector_key] = saved_visible_columns.copy()
        st.session_state[loaded_key] = True

    if manager_key not in st.session_state:
        st.session_state[manager_key] = False

    with st.expander("Gestisci colonne", expanded=st.session_state.get(manager_key, False)):
        st.session_state[manager_key] = False

        selected_columns = st.multiselect(
            "Scegli le colonne da mostrare",
            options=all_columns,
            default=st.session_state.get(state_key, saved_visible_columns.copy()),
            key=selector_key,
        )

        st.session_state[state_key] = selected_columns.copy() if selected_columns else []

        col_save, col_reset = st.columns(2)
        with col_save:
            save_layout_clicked = st.button(
                "Salva layout tabella",
                use_container_width=True,
                key=f"save_dashboard_table_layout_{user_id}",
            )
        with col_reset:
            reset_layout_clicked = st.button(
                "Ripristina layout default",
                use_container_width=True,
                key=f"reset_dashboard_table_layout_{user_id}",
            )

        if save_layout_clicked:
            columns_to_save = st.session_state.get(state_key, all_columns.copy())
            columns_to_save = [col for col in columns_to_save if col in all_columns]
            if not columns_to_save:
                st.warning("Seleziona almeno una colonna da mostrare prima di salvare.")
            else:
                merged_settings = carica_sidebar_settings(user_id)
                merged_settings["dashboard_visible_columns"] = columns_to_save
                salva_sidebar_settings(user_id, merged_settings)
                st.session_state[manager_key] = False
                st.success("Layout tabella salvato.")
                st.rerun()

        if reset_layout_clicked:
            merged_settings = carica_sidebar_settings(user_id)
            merged_settings["dashboard_visible_columns"] = all_columns.copy()
            salva_sidebar_settings(user_id, merged_settings)
            st.session_state[state_key] = all_columns.copy()
            st.session_state[selector_key] = all_columns.copy()
            st.session_state[manager_key] = False
            st.success("Layout tabella ripristinato.")
            st.rerun()

    custom_bookings_df = load_custom_bookings(user_id)

    with st.expander("Aggiungi prenotazione custom", expanded=False):
        st.caption("Usa questa sezione per aggiungere prenotazioni non arrivate da Booking. Verranno integrate nella dashboard e nelle altre analisi.")

        with st.form("custom_booking_create_form", clear_on_submit=False):
            cb1, cb2 = st.columns(2)
            with cb1:
                custom_guest_name = st.text_input("Nome ospite", key="custom_booking_guest_name")
                custom_guest_phone = st.text_input("Telefono ospite", key="custom_booking_guest_phone")
                custom_check_in = st.date_input("Check-in", value=date.today(), key="custom_booking_check_in")
                custom_check_out = st.date_input("Check-out", value=date.today() + timedelta(days=1), key="custom_booking_check_out")
                custom_guests = st.number_input("Numero ospiti", min_value=1, value=2, step=1, key="custom_booking_guests")
            with cb2:
                custom_total_price = st.number_input("Prezzo totale netto (€)", min_value=0.0, value=0.0, step=10.0, key="custom_booking_total_price")
                st.caption("Per le prenotazioni custom questo importo viene considerato già netto: non applichiamo commissioni piattaforma.")
                custom_cleaning_cost = st.number_input("Pulizie (€)", min_value=0.0, value=float(cleaning_cost_default), step=5.0, key="custom_booking_cleaning_cost")
                custom_status = st.selectbox("Stato prenotazione", ["confirmed", "cancelled"], key="custom_booking_status")

            custom_notes = st.text_area("Note", key="custom_booking_notes", height=80)
            create_custom_booking_clicked = st.form_submit_button("Salva prenotazione custom", use_container_width=True)

        if create_custom_booking_clicked:
            if not str(custom_guest_name).strip():
                st.error("Inserisci il nome ospite.")
            elif custom_check_out <= custom_check_in:
                st.error("Il check-out deve essere successivo al check-in.")
            else:
                save_custom_booking(
                    user_id,
                    {
                        "guest_name": custom_guest_name,
                        "guest_phone": custom_guest_phone,
                        "check_in": custom_check_in.isoformat(),
                        "check_out": custom_check_out.isoformat(),
                        "total_price": custom_total_price,
                        "cleaning_cost": custom_cleaning_cost,
                        "platform_fee": 0.0,
                        "transaction_cost": 0.0,
                        "raw_booking_status": custom_status,
                        "status": custom_status,
                        "guests": custom_guests,
                        "notes": custom_notes,
                    },
                )
                st.success("Prenotazione custom salvata correttamente.")
                st.rerun()

        st.markdown("#### Prenotazioni custom inserite")
        if custom_bookings_df.empty:
            st.info("Non hai ancora inserito prenotazioni custom.")
        else:
            edit_options = {
                f'#{int(row["id"])} · {row["guest_name"]} · {pd.to_datetime(row["check_in"]).strftime("%d/%m/%Y")} → {pd.to_datetime(row["check_out"]).strftime("%d/%m/%Y")}': int(row["id"])
                for _, row in custom_bookings_df.iterrows()
            }
            selected_custom_label = st.selectbox(
                "Seleziona prenotazione custom da modificare",
                options=list(edit_options.keys()),
                key="selected_custom_booking_to_edit",
            )
            selected_custom_id = edit_options[selected_custom_label]
            selected_custom_row = custom_bookings_df[custom_bookings_df["id"] == selected_custom_id].iloc[0]

            ec1, ec2 = st.columns(2)
            with ec1:
                edit_guest_name = st.text_input("Nome ospite", value=str(selected_custom_row["guest_name"]), key=f"edit_custom_guest_name_{selected_custom_id}")
                edit_guest_phone = st.text_input(
                    "Telefono ospite",
                    value=str(selected_custom_row.get("guest_phone", "") or ""),
                    key=f"edit_custom_guest_phone_{selected_custom_id}"
                )
                edit_check_in = st.date_input("Check-in", value=pd.to_datetime(selected_custom_row["check_in"]).date(), key=f"edit_custom_check_in_{selected_custom_id}")
                edit_check_out = st.date_input("Check-out", value=pd.to_datetime(selected_custom_row["check_out"]).date(), key=f"edit_custom_check_out_{selected_custom_id}")
                edit_guests = st.number_input("Numero ospiti", min_value=1, value=int(selected_custom_row["guests"]), step=1, key=f"edit_custom_guests_{selected_custom_id}")
            with ec2:
                edit_total_price = st.number_input("Prezzo totale netto (€)", min_value=0.0, value=float(selected_custom_row["total_price"]), step=10.0, key=f"edit_custom_total_price_{selected_custom_id}")
                edit_cleaning_cost = st.number_input("Pulizie (€)", min_value=0.0, value=float(selected_custom_row["cleaning_cost"]), step=5.0, key=f"edit_custom_cleaning_cost_{selected_custom_id}")
                current_status = str(selected_custom_row["status"])
                status_options = ["confirmed", "cancelled"]
                status_index = status_options.index(current_status) if current_status in status_options else 0
                edit_status = st.selectbox("Stato prenotazione", status_options, index=status_index, key=f"edit_custom_status_{selected_custom_id}")

            edit_notes = st.text_area("Note", value=str(selected_custom_row.get("notes", "") or ""), height=80, key=f"edit_custom_notes_{selected_custom_id}")

            ea1, ea2 = st.columns(2)
            with ea1:
                if st.button("Aggiorna prenotazione custom", use_container_width=True, key=f"update_custom_booking_button_{selected_custom_id}"):
                    if not str(edit_guest_name).strip():
                        st.error("Inserisci il nome ospite.")
                    elif edit_check_out <= edit_check_in:
                        st.error("Il check-out deve essere successivo al check-in.")
                    else:
                        ok = update_custom_booking(
                            user_id,
                            selected_custom_id,
                            {
                                "guest_name": edit_guest_name,
                                "guest_phone": edit_guest_phone,
                                "check_in": edit_check_in.isoformat(),
                                "check_out": edit_check_out.isoformat(),
                                "total_price": edit_total_price,
                                "cleaning_cost": edit_cleaning_cost,
                                "platform_fee": 0.0,
                                "transaction_cost": 0.0,
                                "raw_booking_status": edit_status,
                                "status": edit_status,
                                "guests": edit_guests,
                                "notes": edit_notes,
                            },
                        )
                        if ok:
                            st.success("Prenotazione custom aggiornata.")
                            st.rerun()
            with ea2:
                if st.button("Elimina prenotazione custom", use_container_width=True, key=f"delete_custom_booking_button_{selected_custom_id}"):
                    ok = delete_custom_booking(user_id, selected_custom_id)
                    if ok:
                        st.success("Prenotazione custom eliminata.")
                        st.rerun()


    visible_columns = carica_sidebar_settings(user_id).get("dashboard_visible_columns", saved_visible_columns.copy())
    visible_columns = [col for col in visible_columns if col in all_columns]
    if not visible_columns:
        visible_columns = saved_visible_columns.copy() if saved_visible_columns else all_columns.copy()

    st.dataframe(df_to_show[visible_columns], width="stretch")


def dataframe_download(df):
    out = io.BytesIO()
    df.to_csv(out, index=False)
    return out.getvalue()


def competitor_price_label(value):
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"€ {float(value):.2f}"
    except Exception:
        return str(value)


def competitor_rating_label(value):
    if value is None or pd.isna(value):
        return "N/D"
    return str(value)


def competitor_availability_label(value):
    if value is True:
        return "Disponibile"
    if value is False:
        return "Non prenotabile"
    return "N/D"


def parse_time_string(value, default="00:00"):
    s = str(value or default).strip()
    match = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not match:
        s = default
        match = re.match(r"^(\d{1,2}):(\d{2})$", s)
    hour = max(0, min(23, int(match.group(1))))
    minute = max(0, min(59, int(match.group(2))))
    return hour, minute


def render_template_message(template_text, context):
    text = str(template_text or "")
    for key, value in context.items():
        text = text.replace("{" + key + "}", str(value))
    return text


def preview_message_key(message_id, message_text):
    digest = hashlib.md5(str(message_text or "").encode("utf-8")).hexdigest()[:10]
    return f"view_message_text_{message_id}_{digest}"


def sync_template_editor_to_persistent(editor_key, persistent_key):
    st.session_state[persistent_key] = st.session_state.get(editor_key, "")


def ensure_template_editor_value(editor_key, persistent_key, force_reload=False):
    if force_reload or editor_key not in st.session_state:
        st.session_state[editor_key] = st.session_state.get(persistent_key, "")


def build_bookings_auto_signature(bookings_df):
    if bookings_df is None or len(bookings_df) == 0:
        return "no_bookings"

    signature_df = bookings_df.copy()
    required_cols = ["platform", "guest_name", "guest_phone", "check_in", "check_out", "status"]
    for col in required_cols:
        if col not in signature_df.columns:
            signature_df[col] = ""

    signature_df = signature_df[required_cols].copy()
    signature_df["platform"] = signature_df["platform"].fillna("").astype(str).str.strip()
    signature_df["guest_name"] = signature_df["guest_name"].fillna("").astype(str).str.strip()
    signature_df["guest_phone"] = signature_df["guest_phone"].fillna("").astype(str).str.strip()
    signature_df["check_in"] = pd.to_datetime(signature_df["check_in"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    signature_df["check_out"] = pd.to_datetime(signature_df["check_out"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    signature_df["status"] = signature_df["status"].fillna("").astype(str).str.strip().str.lower()
    signature_df = signature_df.sort_values(required_cols).reset_index(drop=True)

    payload = signature_df.to_json(orient="records", force_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def build_competitor_table(competitors, fallback_guests=2):
    rows = []
    for c in competitors:
        raw_price = c.get("price")
        try:
            price_num = float(raw_price) if raw_price is not None else None
        except Exception:
            price_num = None

        rows.append({
            "platform": c.get("platform", "-"),
            "listing_name": c.get("name", "-"),
            "distance_km": c.get("distance_km"),
            "guests": c.get("guests", fallback_guests),
            "bedrooms": c.get("bedrooms", "-"),
            "bathrooms": c.get("bathrooms", "-"),
            "rating": c.get("rating"),
            "reviews_count": c.get("reviews_count", "N/D"),
            "price_total": price_num,
            "price_per_night": price_num,
            "available_status": competitor_availability_label(c.get("available")),
        })
    return pd.DataFrame(rows)


def render_pricing_analysis_result(result, fallback_guests=2):
    if campo_visibile("analisi_successo_box"):
        st.success(campo_etichetta("analisi_successo_box", TESTI["analisi_successo"]))

    competitors = result.get("competitors", [])
    pricing = result.get("pricing", {})

    metriche_principali = [
        {
            "chiave": "analisi_card_indirizzo",
            "label": campo_etichetta("analisi_card_indirizzo", TESTI["analisi_metrica_indirizzo"]),
            "value": result.get("address", "-"),
            "visibile": campo_visibile("analisi_card_indirizzo"),
        },
        {
            "chiave": "analisi_card_latlon",
            "label": campo_etichetta("analisi_card_latlon", TESTI["analisi_metrica_latlon"]),
            "value": f'{result.get("lat", "-")} / {result.get("lon", "-")}',
            "visibile": campo_visibile("analisi_card_latlon"),
        },
        {
            "chiave": "analisi_card_competitor_trovati",
            "label": campo_etichetta("analisi_card_competitor_trovati", TESTI["analisi_metrica_competitor_trovati"]),
            "value": len(competitors),
            "visibile": campo_visibile("analisi_card_competitor_trovati"),
        },
        {
            "chiave": "analisi_card_prezzo_base",
            "label": campo_etichetta("analisi_card_prezzo_base", TESTI["analisi_metrica_prezzo_base"]),
            "value": f'€ {float(pricing.get("base_price", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_prezzo_base"),
        },
        {
            "chiave": "analisi_card_mediana_competitor",
            "label": campo_etichetta("analisi_card_mediana_competitor", TESTI["analisi_metrica_mediana_competitor"]),
            "value": f'€ {float(pricing.get("median_price", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_mediana_competitor"),
        },
        {
            "chiave": "analisi_card_ratio_disponibilita",
            "label": campo_etichetta("analisi_card_ratio_disponibilita", TESTI["analisi_metrica_ratio_disponibilita"]),
            "value": f'{float(pricing.get("available_ratio", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_ratio_disponibilita"),
        },
        {
            "chiave": "analisi_card_prezzo_suggerito",
            "label": campo_etichetta("analisi_card_prezzo_suggerito", TESTI["analisi_metrica_prezzo_suggerito"]),
            "value": f'€ {float(pricing.get("suggested_price", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_prezzo_suggerito"),
        },
        {
            "chiave": "analisi_card_disponibili",
            "label": campo_etichetta("analisi_card_disponibili", TESTI["analisi_metrica_disponibili"]),
            "value": int(pricing.get("available_count", 0) or 0),
            "visibile": campo_visibile("analisi_card_disponibili"),
        },
        {
            "chiave": "analisi_card_non_prenotabili",
            "label": campo_etichetta("analisi_card_non_prenotabili", TESTI["analisi_metrica_non_prenotabili"]),
            "value": int(pricing.get("unavailable_count", 0) or 0),
            "visibile": campo_visibile("analisi_card_non_prenotabili"),
        },
        {
            "chiave": "analisi_card_prezzo_medio",
            "label": campo_etichetta("analisi_card_prezzo_medio", TESTI["analisi_metrica_prezzo_medio"]),
            "value": f'€ {float(pricing.get("average_price", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_prezzo_medio"),
        },
        {
            "chiave": "analisi_card_min_max",
            "label": campo_etichetta("analisi_card_min_max", TESTI["analisi_metrica_min_max"]),
            "value": f'€ {float(pricing.get("min_price", 0) or 0):.2f} / € {float(pricing.get("max_price", 0) or 0):.2f}',
            "visibile": campo_visibile("analisi_card_min_max"),
        },
    ]

    render_metriche_configurabili(metriche_principali, cards_per_row=4)

    if campo_visibile("analisi_sezione_competitor_titolo"):
        st.markdown(f"### {campo_etichetta('analisi_sezione_competitor_titolo', TESTI['analisi_competitor_titolo'])}")

    if not competitors:
        st.info(TESTI["analisi_nessun_competitor"])
        return

    if SEZIONI["mostra_card_competitor"] and campo_visibile("analisi_card_competitor_singolo"):
        cards_per_row = 2
        for i in range(0, len(competitors), cards_per_row):
            row = competitors[i:i + cards_per_row]
            cols = st.columns(cards_per_row)

            for idx, comp in enumerate(row):
                with cols[idx]:
                    category = comp.get("category_label")
                    if not category:
                        categories = comp.get("category", [])
                        category = categories[0] if categories else "Accommodation"

                    st.markdown(
                        f"""
                        <div style="
                            border: 1px solid {COLORI["colore_bordo"]};
                            border-radius: 16px;
                            padding: 18px;
                            margin-bottom: 16px;
                            background: {COLORI["colore_card"]};
                        ">
                            <div style="font-size: 1.05rem; font-weight: 700; margin-bottom: 6px;">
                                {comp.get("name", "Struttura")}
                            </div>
                            <div style="font-size: 0.88rem; opacity: 0.75; margin-bottom: 10px;">
                                {comp.get("address", "-")}
                            </div>
                            <div style="display: inline-block; padding: 4px 10px; border-radius: 999px; background: {COLORI["colore_accent"]}22; font-size: 0.8rem; margin-bottom: 12px;">
                                {category}
                            </div>
                            <div style="font-size: 0.92rem; line-height: 1.8;">
                                <strong>Distanza:</strong> {comp.get("distance_km", "-")} km<br>
                                <strong>Prezzo:</strong> {competitor_price_label(comp.get("price"))}<br>
                                <strong>Rating:</strong> {competitor_rating_label(comp.get("rating"))}<br>
                                <strong>Disponibilità:</strong> {competitor_availability_label(comp.get("available"))}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    if SEZIONI["mostra_tabella_competitor"] and campo_visibile("analisi_tabella_competitor_titolo_config"):
        competitor_table = build_competitor_table(competitors, fallback_guests=fallback_guests)
        st.markdown(f"### {campo_etichetta('analisi_tabella_competitor_titolo_config', TESTI['analisi_tabella_competitor_titolo'])}")
        st.dataframe(competitor_table, width="stretch")


def render_auth():

    st.markdown(f"""
    <div class="hf-auth-box">
        <h1 style="margin-bottom:8px;">{TESTI["titolo_login"]}</h1>
        <p style="opacity:0.8; margin-bottom:0;">{TESTI["sottotitolo_login"]}</p>
    </div>
    """, unsafe_allow_html=True)
    tab_login, tab_reg, tab_reset = st.tabs([
        TESTI["tab_login"],
        TESTI["tab_registrazione"],
        TESTI["tab_password_dimenticata"],
    ])

    with tab_login:
        with st.form("login_form"):
            email = st.text_input(TESTI["label_email"])
            password = st.text_input(TESTI["label_password"], type="password")
            submitted = st.form_submit_button(TESTI["bottone_login"], use_container_width=True)
            if submitted:
                if not email.strip():
                    st.error(TESTI["errore_email_necessaria"])
                elif not password:
                    st.error(TESTI["errore_password_vuota"])
                else:
                    utente = autentica_utente(email, password)
                    if utente:
                        token = crea_sessione_accesso(utente["id"])
                        st.session_state.utente = utente
                        st.session_state.auth_token = token
                        st.session_state.profilo_immobile = carica_profilo_immobile(utente["id"])
                        st.query_params["session"] = token
                        st.rerun()
                    else:
                        st.error(TESTI["errore_login"])

    with tab_reg:
        with st.form("register_form"):
            email = st.text_input(TESTI["label_email"], key="register_email")
            password = st.text_input(TESTI["label_password"], type="password", key="register_password")
            password2 = st.text_input(TESTI["label_conferma_password"], type="password")
            submitted = st.form_submit_button(TESTI["bottone_registrazione"], use_container_width=True)
            if submitted:
                if not email.strip() or "@" not in email:
                    st.error(TESTI["errore_email_necessaria"])
                elif not password:
                    st.error(TESTI["errore_password_vuota"])
                elif len(password) < 6:
                    st.error(TESTI["errore_password_corte"])
                elif password != password2:
                    st.error(TESTI["errore_password_diverse"])
                else:
                    ok, errore = crea_utente(email, password)
                    if ok:
                        st.success(TESTI["messaggio_registrazione_ok"])
                    else:
                        st.error(errore)

    with tab_reset:
        if smtp_config_disponibile():
            st.caption("Inserisci la tua email e riceverai un codice di recupero valido 15 minuti.")
        else:
            st.warning("Configurazione email non trovata. Per usare il recupero password devi impostare SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD e SMTP_FROM_EMAIL.")

        with st.form("request_reset_code_form"):
            reset_request_email = st.text_input(TESTI["label_email"], key="reset_request_email")
            request_submitted = st.form_submit_button("Invia codice di recupero", use_container_width=True)

            if request_submitted:
                if not reset_request_email.strip() or "@" not in reset_request_email:
                    st.error(TESTI["errore_email_necessaria"])
                else:
                    ok, messaggio, _ = crea_codice_reset_password(reset_request_email)
                    if ok:
                        st.success(messaggio)
                    else:
                        st.error(messaggio)

        with st.form("reset_password_form"):
            email = st.text_input(TESTI["label_email"], key="reset_email")
            codice = st.text_input("Codice di recupero", key="reset_code")
            nuova_password = st.text_input(TESTI["sicurezza_nuova_password"], type="password", key="reset_new_password")
            conferma_password = st.text_input(TESTI["sicurezza_conferma_password"], type="password", key="reset_confirm_password")
            submitted = st.form_submit_button(TESTI["bottone_reset_password"], use_container_width=True)

            if submitted:
                if not email.strip() or "@" not in email:
                    st.error(TESTI["errore_email_necessaria"])
                elif not str(codice).strip():
                    st.error("Inserisci il codice di recupero.")
                elif not nuova_password:
                    st.error(TESTI["errore_password_vuota"])
                elif len(nuova_password) < 6:
                    st.error(TESTI["errore_password_corte"])
                elif nuova_password != conferma_password:
                    st.error(TESTI["errore_password_diverse"])
                else:
                    ok, messaggio = reimposta_password_con_codice(email, codice, nuova_password)
                    if ok:
                        st.success(messaggio)
                    else:
                        st.error(messaggio)


def render_profile_menu():
    st.markdown(
        f"""
        <div style="
            display:flex;
            align-items:center;
            padding:18px 0 18px 0;
            border-bottom:1px solid rgba(255,255,255,0.08);
            margin-bottom:10px;
        ">
            <div style="
                color:white;
                font-size:34px;
                font-weight:800;
                line-height:1;
                letter-spacing:-1px;
            ">
                {TESTI.get("titolo_app", "HostFlow v5")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_profile_form(profilo, onboarding_mode=False):
    if onboarding_mode:
        st.markdown(f"""
        <div class="hf-onboarding-box">
            <h1 style="margin-bottom:8px;">{TESTI["titolo_onboarding"]}</h1>
            <p style="opacity:0.85; margin-bottom:0;">{TESTI["sottotitolo_onboarding"]}</p>
        </div>
        """, unsafe_allow_html=True)

    st.subheader(TESTI["immobile_titolo"])
    st.info(TESTI["immobile_info"])

    tipi = ["Appartamento intero", "Stanza privata", "Casa vacanze", "Altro"]
    fasce = ["Basic", "Standard", "Premium", "Luxury"]

    st.markdown(f"#### {TESTI['immobile_blocco_dati']}")
    col1, col2 = st.columns(2)

    with col1:
        property_name = st.text_input(campo_etichetta("nome_immobile"), profilo.get("nome_immobile", "")) if campo_visibile("nome_immobile") else profilo.get("nome_immobile", "")
        address = st.text_input(campo_etichetta("indirizzo_completo"), profilo.get("indirizzo_completo", "")) if campo_visibile("indirizzo_completo") else profilo.get("indirizzo_completo", "")
        city = st.text_input(campo_etichetta("citta"), profilo.get("citta", "")) if campo_visibile("citta") else profilo.get("citta", "")
        cap = st.text_input(campo_etichetta("cap"), profilo.get("cap", "")) if campo_visibile("cap") else profilo.get("cap", "")
        if campo_visibile("tipologia_immobile"):
            property_type = st.selectbox(campo_etichetta("tipologia_immobile"), tipi, index=tipi.index(profilo.get("tipologia_immobile", "Appartamento intero")) if profilo.get("tipologia_immobile", "Appartamento intero") in tipi else 0)
        else:
            property_type = profilo.get("tipologia_immobile", "Appartamento intero")

    with col2:
        max_guests = st.number_input(campo_etichetta("ospiti_massimi"), min_value=1, value=int(profilo.get("ospiti_massimi", 4)), step=1) if campo_visibile("ospiti_massimi") else int(profilo.get("ospiti_massimi", 4))
        bedrooms = st.number_input(campo_etichetta("camere"), min_value=0, value=int(profilo.get("camere", 1)), step=1) if campo_visibile("camere") else int(profilo.get("camere", 1))
        bathrooms = st.number_input(campo_etichetta("bagni"), min_value=0, value=int(profilo.get("bagni", 1)), step=1) if campo_visibile("bagni") else int(profilo.get("bagni", 1))
        if campo_visibile("fascia_qualita"):
            quality_band = st.selectbox(campo_etichetta("fascia_qualita"), fasce, index=fasce.index(profilo.get("fascia_qualita", "Basic")) if profilo.get("fascia_qualita", "Basic") in fasce else 0)
        else:
            quality_band = profilo.get("fascia_qualita", "Basic")
        competitor_radius_km = st.number_input(campo_etichetta("raggio_competitor_km"), min_value=0.1, value=float(profilo.get("raggio_competitor_km", 1.0)), step=0.1) if campo_visibile("raggio_competitor_km") else float(profilo.get("raggio_competitor_km", 1.0))

    st.markdown(f"#### {TESTI['immobile_blocco_messaggi']}")
    col3, col4 = st.columns(2)

    with col3:
        nome_host = st.text_input(campo_etichetta("nome_host"), profilo.get("nome_host", "")) if campo_visibile("nome_host") else profilo.get("nome_host", "")
        numero_whatsapp = st.text_input(campo_etichetta("numero_whatsapp"), profilo.get("numero_whatsapp", "")) if campo_visibile("numero_whatsapp") else profilo.get("numero_whatsapp", "")
        checkin_da = st.text_input(campo_etichetta("checkin_da"), profilo.get("checkin_da", "15:00")) if campo_visibile("checkin_da") else profilo.get("checkin_da", "15:00")
        checkin_fino = st.text_input(campo_etichetta("checkin_fino"), profilo.get("checkin_fino", "22:00")) if campo_visibile("checkin_fino") else profilo.get("checkin_fino", "22:00")
        checkout_entro = st.text_input(campo_etichetta("checkout_entro"), profilo.get("checkout_entro", "10:00")) if campo_visibile("checkout_entro") else profilo.get("checkout_entro", "10:00")
        wifi_nome = st.text_input(campo_etichetta("wifi_nome"), profilo.get("wifi_nome", "")) if campo_visibile("wifi_nome") else profilo.get("wifi_nome", "")
        wifi_password = st.text_input(campo_etichetta("wifi_password"), profilo.get("wifi_password", "")) if campo_visibile("wifi_password") else profilo.get("wifi_password", "")

    with col4:
        animali_ammessi = st.checkbox(campo_etichetta("animali_ammessi"), value=bool(profilo.get("animali_ammessi", False))) if campo_visibile("animali_ammessi") else bool(profilo.get("animali_ammessi", False))
        fumatori_ammessi = st.checkbox(campo_etichetta("fumatori_ammessi"), value=bool(profilo.get("fumatori_ammessi", False))) if campo_visibile("fumatori_ammessi") else bool(profilo.get("fumatori_ammessi", False))
        parcheggio_disponibile = st.checkbox(campo_etichetta("parcheggio_disponibile"), value=bool(profilo.get("parcheggio_disponibile", False))) if campo_visibile("parcheggio_disponibile") else bool(profilo.get("parcheggio_disponibile", False))
        tassa_soggiorno_profilo = st.number_input(campo_etichetta("tassa_soggiorno"), min_value=0.0, value=float(profilo.get("tassa_soggiorno", 4.0)), step=0.5) if campo_visibile("tassa_soggiorno") else float(profilo.get("tassa_soggiorno", 4.0))

    nuovi_dati = {
        "nome_immobile": property_name,
        "indirizzo_completo": address,
        "citta": city,
        "cap": cap,
        "tipologia_immobile": property_type,
        "ospiti_massimi": max_guests,
        "camere": bedrooms,
        "bagni": bathrooms,
        "fascia_qualita": quality_band,
        "raggio_competitor_km": competitor_radius_km,
        "nome_host": nome_host,
        "numero_whatsapp": numero_whatsapp,
        "checkin_da": checkin_da,
        "checkin_fino": checkin_fino,
        "checkout_entro": checkout_entro,
        "wifi_nome": wifi_nome,
        "wifi_password": wifi_password,
        "animali_ammessi": animali_ammessi,
        "fumatori_ammessi": fumatori_ammessi,
        "parcheggio_disponibile": parcheggio_disponibile,
        "tassa_soggiorno": tassa_soggiorno_profilo,
        "istruzioni_ingresso": profilo.get("istruzioni_ingresso", ""),
        "note_finali": profilo.get("note_finali", ""),
    }

    if st.button(TESTI["immobile_salva_bottone"], use_container_width=True, key=f"save_profile_{'onboarding' if onboarding_mode else 'tab'}"):
        if not profilo_completo(nuovi_dati):
            st.error(TESTI["errore_campi_onboarding"])
        else:
            salva_profilo_immobile(st.session_state.utente["id"], nuovi_dati)
            st.session_state.profilo_immobile = carica_profilo_immobile(st.session_state.utente["id"])
            st.success(TESTI["immobile_salvataggio_ok"])
            if onboarding_mode:
                st.rerun()

    if SEZIONI["mostra_tabella_immobile"] and not onboarding_mode:
        st.markdown(f"### {TESTI['immobile_tabella_titolo']}")
        riepilogo = pd.DataFrame([{
            "nome_immobile": property_name,
            "indirizzo_completo": address,
            "citta": city,
            "cap": cap,
            "tipologia_immobile": property_type,
            "ospiti_massimi": max_guests,
            "camere": bedrooms,
            "bagni": bathrooms,
            "fascia_qualita": quality_band,
            "raggio_competitor_km": competitor_radius_km,
            "nome_host": nome_host,
            "numero_whatsapp": numero_whatsapp,
            "checkin_da": checkin_da,
            "checkin_fino": checkin_fino,
            "checkout_entro": checkout_entro,
        }])
        st.dataframe(riepilogo, width="stretch")


init_db()
inizializza_sessione()

if st.session_state.utente is None:
    token_qs = st.query_params.get("session")
    if token_qs:
        auth_data = autentica_da_token(token_qs)
        if auth_data:
            st.session_state.utente = {"id": auth_data["id"], "email": auth_data["email"]}
            st.session_state.auth_token = auth_data["token"]
            st.session_state.profilo_immobile = carica_profilo_immobile(auth_data["id"])
        else:
            try:
                st.query_params.clear()
            except Exception:
                pass

if st.session_state.utente is None:
    render_auth()
    st.stop()

profilo = st.session_state.profilo_immobile or carica_profilo_immobile(st.session_state.utente["id"])
st.session_state.profilo_immobile = profilo
inizializza_sidebar_state(st.session_state.utente["id"])
st.session_state.cleaning_cost_default = 0.0
st.session_state.monthly_cleaning_cost = 0.0

if st.session_state.get("auth_token"):
    st.query_params["session"] = st.session_state["auth_token"]

if st.session_state.get("file_prenotazioni_virtuale") is None:
    file_salvato = carica_file_prenotazioni(st.session_state.utente["id"])
    if file_salvato is not None:
        st.session_state.file_prenotazioni_virtuale = file_salvato
        st.session_state.file_prenotazioni_nome = getattr(file_salvato, "name", None)

if not st.session_state.get("message_settings_loaded", False):
    saved_message_settings = load_message_settings(st.session_state.utente["id"])
    for k, v in saved_message_settings.items():
        st.session_state[k] = v
    st.session_state.message_settings_loaded = True

if not profilo_completo(profilo):
    render_profile_menu()
    render_profile_form(profilo, onboarding_mode=True)
    st.stop()

render_profile_menu()
st.caption(TESTI["sottotitolo_app"])

with st.sidebar:
    st.header(TESTI["sidebar_import_header"])
    import_mode = st.radio(
        TESTI["sidebar_tipo_import"],
        ["Auto", "Booking export", "CSV standard"],
        key="import_mode"
    )
    uploaded_file_widget = st.file_uploader(TESTI["sidebar_carica_file"], type=["csv", "xls", "xlsx"])

    if uploaded_file_widget is not None:
        uploaded_bytes = uploaded_file_widget.getvalue()
        uploaded_signature = hashlib.md5(
            f'{uploaded_file_widget.name}|{len(uploaded_bytes)}'.encode("utf-8") + uploaded_bytes
        ).hexdigest()

        if st.session_state.get("file_prenotazioni_signature") != uploaded_signature:
            salva_file_prenotazioni(st.session_state.utente["id"], uploaded_file_widget)
            buffer_file = BytesIO(uploaded_bytes)
            buffer_file.name = uploaded_file_widget.name
            buffer_file.seek(0)
            st.session_state.file_prenotazioni_virtuale = buffer_file
            st.session_state.file_prenotazioni_nome = uploaded_file_widget.name
            st.session_state.file_prenotazioni_signature = uploaded_signature
            st.rerun()

    uploaded_file = st.session_state.get("file_prenotazioni_virtuale")

    with st.expander(TESTI["sidebar_pulizie_header"], expanded=False):
        cleaning_mode = st.radio(
            TESTI["sidebar_modalita_pulizie"],
            ["Per prenotazione", "Mensile", "Ad ore"],
            key="cleaning_mode"
        )
        cleaning_cost_default = st.number_input(
            TESTI["sidebar_pulizie_prenotazione"],
            min_value=0.0,
            value=0.0,
            step=1.0,
            key="cleaning_cost_default"
        )
        monthly_cleaning_cost = st.number_input(
            TESTI["sidebar_pulizie_mensili"],
            min_value=0.0,
            value=0.0,
            step=10.0,
            key="monthly_cleaning_cost"
        )

    with st.expander(TESTI["sidebar_finanza_header"], expanded=False):
        include_city_tax = st.checkbox(
            TESTI["sidebar_includi_tassa_soggiorno"],
            key="include_city_tax"
        )
        city_tax_rate = st.number_input(
            TESTI["sidebar_tassa_soggiorno"],
            min_value=0.0,
            step=0.5,
            key="city_tax_rate"
        )
        transaction_mode = st.radio(
            TESTI["sidebar_costo_transazione"],
            ["Percentuale", "Dal file"],
            key="transaction_mode"
        )
        transaction_pct = st.number_input(
            TESTI["sidebar_costo_transazione_percentuale"],
            min_value=0.0,
            step=0.1,
            key="transaction_pct"
        )
        vat_pct = st.number_input(
            TESTI["sidebar_vat"],
            min_value=0.0,
            step=1.0,
            key="vat_pct"
        )
        include_withholding = st.checkbox(
            TESTI["sidebar_ritenuta_checkbox"],
            key="include_withholding"
        )
        withholding_pct = st.number_input(
            TESTI["sidebar_ritenuta"],
            min_value=0.0,
            step=1.0,
            key="withholding_pct"
        )

    with st.expander(TESTI["sidebar_periodo_header"], expanded=False):
        period_mode = st.selectbox(
            "Vista periodo",
            ["Mensile", "Trimestrale", "Semestrale", "Annuale", "Personalizzato"],
            key="dashboard_period_mode"
        )
        selected_year = st.number_input(
    TESTI["sidebar_anno_dashboard"],
    min_value=2024,
    max_value=2035,
    value=datetime.now().year,
    step=1,
    key="selected_year"
)
        selected_month = int(st.session_state.get("selected_month", date.today().month))
        selected_quarter = int(st.session_state.get("selected_quarter", 1))
        selected_semester = int(st.session_state.get("selected_semester", 1))
        custom_start_date = pd.to_datetime(st.session_state.get("custom_start_date", date.today().replace(day=1).isoformat())).date()
        custom_end_date = pd.to_datetime(st.session_state.get("custom_end_date", date.today().isoformat())).date()

        if "selected_month_initialized" not in st.session_state:
            st.session_state["selected_month"] = date.today().month
            st.session_state["selected_month_initialized"] = True

        if period_mode == "Mensile":
            selected_month = st.selectbox(
                TESTI["sidebar_mese_dashboard"],
                list(range(1, 13)),
                key="selected_month"
            )
        elif period_mode == "Trimestrale":
            selected_quarter = st.selectbox(
                "Trimestre dashboard",
                [1, 2, 3, 4],
                format_func=lambda x: f"Q{x}",
                key="selected_quarter"
            )
        elif period_mode == "Semestrale":
            selected_semester = st.selectbox(
                "Semestre dashboard",
                [1, 2],
                format_func=lambda x: "Primo semestre" if x == 1 else "Secondo semestre",
                key="selected_semester"
            )
        elif period_mode == "Personalizzato":
            cstart, cend = st.columns(2)

            if isinstance(st.session_state.get("custom_start_date"), str):
                st.session_state["custom_start_date"] = pd.to_datetime(st.session_state["custom_start_date"]).date()

            if isinstance(st.session_state.get("custom_end_date"), str):
                st.session_state["custom_end_date"] = pd.to_datetime(st.session_state["custom_end_date"]).date()

            custom_start_date = cstart.date_input(
                "Data inizio",
                value=st.session_state["custom_start_date"],
                key="custom_start_date"
            )
            custom_end_date = cend.date_input(
                "Data fine",
                value=st.session_state["custom_end_date"],
                key="custom_end_date"
            )

    merged_sidebar_settings = carica_sidebar_settings(st.session_state.utente["id"])
    merged_sidebar_settings.update({
        "import_mode": import_mode,
        "cleaning_mode": cleaning_mode,
        "cleaning_cost_default": float(cleaning_cost_default),
        "monthly_cleaning_cost": float(monthly_cleaning_cost),
        "include_city_tax": bool(include_city_tax),
        "city_tax_rate": float(city_tax_rate),
        "transaction_mode": transaction_mode,
        "transaction_pct": float(transaction_pct),
        "vat_pct": float(vat_pct),
        "include_withholding": bool(include_withholding),
        "withholding_pct": float(withholding_pct),
        "selected_year": int(selected_year),
        "selected_month": int(selected_month),
        "dashboard_period_mode": period_mode,
        "selected_quarter": int(selected_quarter),
        "selected_semester": int(selected_semester),
        "custom_start_date": custom_start_date.isoformat() if hasattr(custom_start_date, "isoformat") else str(custom_start_date),
        "custom_end_date": custom_end_date.isoformat() if hasattr(custom_end_date, "isoformat") else str(custom_end_date),
    })
    salva_sidebar_settings(st.session_state.utente["id"], merged_sidebar_settings)

    st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)
    if st.button("Logout", use_container_width=True, key="logout_sidebar_bottom"):
        logout()

tab_order = []
if SEZIONI["mostra_tab_dashboard"]:
    tab_order.append(("dashboard", TESTI["tab_dashboard"]))
if SEZIONI["mostra_tab_immobile"]:
    tab_order.append(("immobile", TESTI["tab_immobile"]))
if SEZIONI["mostra_tab_analisi_mercato"]:
    tab_order.append(("mercato", TESTI["tab_analisi_mercato"]))
if SEZIONI["mostra_tab_pricing"]:
    tab_order.append(("pricing", TESTI["tab_pricing"]))
if SEZIONI["mostra_tab_messaggi"]:
    tab_order.append(("messaggi", TESTI["tab_messaggi"]))
tab_order.append(("pulizie_servizi", TESTI.get("tab_pulizie_servizi", "Pulizie")))
if SEZIONI["mostra_tab_dati"]:
    tab_order.append(("dati", TESTI["tab_dati"]))

tabs = st.tabs([label for _, label in tab_order])
tab_map = {key: tab for (key, _), tab in zip(tab_order, tabs)}

df = None
filtered_df = None
stats = None
annual = None

custom_bookings_df = load_custom_bookings(st.session_state.utente["id"])

if uploaded_file or not custom_bookings_df.empty:
    try:
        raw_df = pd.DataFrame()
        if uploaded_file:
            if hasattr(uploaded_file, "seek"):
                uploaded_file.seek(0)
            raw_df = load_data(uploaded_file, cleaning_cost_default, import_mode)

        merged_raw_df = merge_booking_sources(raw_df, custom_bookings_df)
        df = enrich_financials(
            merged_raw_df,
            city_tax_rate=city_tax_rate,
            include_city_tax=include_city_tax,
            transaction_mode=transaction_mode,
            transaction_pct=transaction_pct,
            vat_pct=vat_pct,
            withholding_pct=withholding_pct,
            include_withholding=include_withholding,
            cleaning_mode=cleaning_mode,
            monthly_cleaning_cost=monthly_cleaning_cost,
            selected_year=selected_year,
            selected_month=selected_month,
            utente_id=st.session_state.utente["id"],
        )
        period_start, period_end, period_label = get_period_bounds(
            period_mode,
            selected_year,
            month=selected_month,
            quarter=selected_quarter,
            semester=selected_semester,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date,
        )
        stats = period_stats(df, period_start, period_end)
        filtered_df = filter_df_by_period(df, period_start, period_end)
        annual = build_dashboard_history(
            df,
            period_mode,
            selected_year,
            month=selected_month,
            quarter=selected_quarter,
            semester=selected_semester,
        )
    except Exception as e:
        st.error(str(e))
        st.stop()

profilo = st.session_state.profilo_immobile or profilo

if "immobile" in tab_map:
    with tab_map["immobile"]:
        render_profile_form(profilo, onboarding_mode=False)

if "mercato" in tab_map:
    with tab_map["mercato"]:
        st.subheader(TESTI["analisi_mercato_titolo"])
        m1, m2, m3 = st.columns(3)
        market_checkin = m1.date_input(TESTI["analisi_checkin"], value=date.today(), key="market_in")
        market_checkout = m2.date_input(TESTI["analisi_checkout"], value=date.today() + timedelta(days=2), key="market_out")
        market_radius = m3.number_input(campo_etichetta("analisi_raggio"), min_value=0.1, value=float(profilo.get("raggio_competitor_km", 1.0)), step=0.1, key="market_radius") if campo_visibile("analisi_raggio") else float(profilo.get("raggio_competitor_km", 1.0))

        n1, n2 = st.columns(2)
        market_guests = n1.number_input(campo_etichetta("analisi_ospiti"), min_value=1, value=int(profilo.get("ospiti_massimi", 2) or 2), step=1, key="market_guests") if campo_visibile("analisi_ospiti") else int(profilo.get("ospiti_massimi", 2) or 2)
        tipi = ["Appartamento intero", "Stanza privata", "Casa vacanze", "Altro"]
        tipo_default = profilo.get("tipologia_immobile", "Appartamento intero")
        market_type = n2.selectbox(TESTI["analisi_tipologia"], tipi, index=tipi.index(tipo_default) if tipo_default in tipi else 0, key="market_type")

        st.markdown("---")
        st.subheader(TESTI["analisi_pricing_titolo"])
        default_market_address = compone_indirizzo_ricerca(profilo)
        address_input = st.text_input(TESTI["analisi_indirizzo"], value=default_market_address, placeholder=TESTI["analisi_indirizzo_placeholder"], key="pricing_address")

        if st.button(TESTI["analisi_bottone"], key="pricing_button"):
            if not address_input:
                st.warning(TESTI["analisi_warning_indirizzo"])
            elif market_checkout <= market_checkin:
                st.warning(TESTI["analisi_warning_date"])
            else:
                st.session_state.market_result = run_pricing_analysis(
                    address=address_input,
                    radius_km=market_radius,
                    base_price=0,
                    checkin=market_checkin.strftime("%Y-%m-%d"),
                    checkout=market_checkout.strftime("%Y-%m-%d"),
                    guests=market_guests,
                    property_type=market_type,
                )

        result = st.session_state.market_result
        if result:
            if "error" in result:
                st.error(result["error"])
            else:
                render_pricing_analysis_result(result, fallback_guests=market_guests)
        else:
            st.info(TESTI["analisi_info_iniziale"])

if "dashboard" in tab_map:
    with tab_map["dashboard"]:
        if df is None:
            st.info(TESTI["stato_senza_file_dashboard"])
        else:
            st.caption(f"Periodo selezionato: {period_label}")
            dashboard_metriche = [
                {
                    "label": TESTI["metrica_prenotazioni"],
                    "value": stats["bookings"],
                    "visibile": SEZIONI.get("mostra_metrica_prenotazioni", True),
                },
                {
                    "label": TESTI["metrica_occupazione"],
                    "value": f'{stats["occupancy"]}%',
                    "visibile": SEZIONI.get("mostra_metrica_occupazione", True),
                },
                {
                    "label": TESTI["metrica_fatturato"],
                    "value": f'€ {stats["revenue"]:.2f}',
                    "visibile": SEZIONI.get("mostra_metrica_fatturato", True),
                },
                {
                    "label": TESTI["metrica_netto_operativo"],
                    "value": f'€ {stats["net_operating"]:.2f}',
                    "visibile": SEZIONI.get("mostra_metrica_netto_operativo", True),
                },
                {
                    "label": TESTI["metrica_netto_reale"],
                    "value": f'€ {stats["net_real"]:.2f}',
                    "visibile": SEZIONI.get("mostra_metrica_netto_reale", True),
                },
                {
                    "label": TESTI["metrica_adr_medio"],
                    "value": f'€ {stats["adr"]:.2f}',
                    "visibile": SEZIONI.get("mostra_metrica_adr_medio", True),
                },
            ]
            render_metriche_configurabili(dashboard_metriche, cards_per_row=6)


            if SEZIONI["mostra_storico_annuale"]:
                dashboard_tab, storico_tab = st.tabs([TESTI["sottotab_dashboard"], TESTI["sottotab_storico"]])
                with dashboard_tab:
                    st.subheader(f'{TESTI["dashboard_prenotazioni_elaborate"]} · {period_label}')
                    render_dashboard_dataframe(filtered_df, st.session_state.utente["id"])
                with storico_tab:
                    st.subheader("Storico periodo")
                    if annual.empty:
                        st.info(TESTI["dashboard_nessun_dato_anno"])
                    else:
                        annual_display = annual.copy().rename(columns={
                            "period": "Periodo", "bookings": "Prenotazioni", "occupancy": "Occupazione",
                            "revenue": "Fatturato", "net_operating": "Netto operativo", "net_real": "Netto reale", "adr": "ADR medio"
                        })
                        st.dataframe(annual_display, width="stretch")
                        if SEZIONI["mostra_grafico_annuale"]:
                            st.line_chart(annual.set_index("period")[["revenue", "net_operating", "net_real"]])
            else:
                st.subheader(f'{TESTI["dashboard_prenotazioni_elaborate"]} · {period_label}')
                render_dashboard_dataframe(filtered_df, st.session_state.utente["id"])

if "pricing" in tab_map:
    with tab_map["pricing"]:
        if df is None:
            st.info(TESTI["stato_senza_file_pricing"])
        else:
            st.subheader(TESTI["pricing_titolo"])
            p1, p2 = st.columns(2)
            with p1:
                base_price_mode = st.radio(TESTI["pricing_base_price"], ["ADR mese dashboard", "ADR ultimi 30 giorni", "Manuale"], index=0)
                manual_base_price = st.number_input(TESTI["pricing_prezzo_manuale"], min_value=20.0, value=120.0, step=5.0)
                target_date = st.date_input(TESTI["pricing_data_prezzo"], value=date.today() + timedelta(days=7))
                event = st.checkbox(TESTI["pricing_evento"], value=False)
                weekend_mode = st.selectbox(TESTI["pricing_weekend"], ["Ven-Sab-Dom", "Ven-Sab", "Solo sabato"])
            with p2:
                st.markdown(f"**{TESTI['pricing_regole_titolo']}**")
                weekend_markup = st.number_input(TESTI["pricing_weekend_markup"], min_value=0.0, value=15.0, step=1.0)
                event_markup = st.number_input(TESTI["pricing_evento_markup"], min_value=0.0, value=20.0, step=1.0)
                last_minute_discount = st.number_input(TESTI["pricing_last_minute_sconto"], min_value=0.0, value=10.0, step=1.0)
                last_minute_days = st.number_input(TESTI["pricing_last_minute_giorni"], min_value=0, value=3, step=1)
                early_booking_markup = st.number_input(TESTI["pricing_early_booking_markup"], min_value=0.0, value=5.0, step=1.0)
                early_booking_days = st.number_input(TESTI["pricing_early_booking_giorni"], min_value=0, value=30, step=1)
                high_occ_threshold = st.number_input(TESTI["pricing_occupazione_alta_da"], min_value=0.0, value=75.0, step=1.0)
                high_occ_markup = st.number_input(TESTI["pricing_markup_occupazione_alta"], min_value=0.0, value=10.0, step=1.0)
                low_occ_threshold = st.number_input(TESTI["pricing_occupazione_bassa_fino"], min_value=0.0, value=35.0, step=1.0)
                low_occ_discount = st.number_input(TESTI["pricing_sconto_occupazione_bassa"], min_value=0.0, value=8.0, step=1.0)

            base_price, base_source = compute_base_price(df, selected_year, selected_month, base_price_mode, manual_base_price)
            days_to_checkin = (target_date - date.today()).days
            weekend = target_date.weekday() in get_weekend_days(weekend_mode)
            suggested, notes = pricing_suggestion(
                base_price, weekend, event, days_to_checkin, stats["occupancy"],
                weekend_markup, event_markup, last_minute_discount, last_minute_days,
                early_booking_markup, early_booking_days, high_occ_threshold, high_occ_markup,
                low_occ_threshold, low_occ_discount
            )

            st.markdown(f"### {TESTI['pricing_risultato_titolo']}")
            r1, r2, r3 = st.columns(3)
            r1.metric(TESTI["pricing_metrica_base_usato"], f"€ {base_price:.2f}")
            r2.metric(TESTI["pricing_metrica_fonte"], base_source)
            r3.metric(TESTI["pricing_metrica_suggerito"], f"€ {suggested:.2f}")

            if SEZIONI["mostra_motivazioni_pricing"]:
                with st.expander(TESTI["pricing_motivazioni"], expanded=False):
                    if notes:
                        for n in notes:
                            st.write(f"- {n}")
                    else:
                        st.write(f"- {TESTI['pricing_nessuna_regola']}")

if "messaggi" in tab_map:
    with tab_map["messaggi"]:
        if df is None:
            st.info(TESTI["stato_senza_file_messaggi"])
        else:
            st.markdown(
                f"""
                <style>
                .hf-message-main-card {{
                    border: 1px solid {COLORI["colore_bordo"]};
                    border-radius: 18px;
                    background: {COLORI["colore_card"]};
                    padding: 18px;
                    margin-bottom: 14px;
                }}
                .hf-message-status-pill {{
                    display:inline-block;
                    padding:6px 12px;
                    border-radius:999px;
                    font-size:0.86rem;
                    font-weight:700;
                    margin-bottom:10px;
                }}
                .hf-message-list-card {{
                    border: 1px solid {COLORI["colore_bordo"]};
                    border-radius: 14px;
                    background: rgba(255,255,255,0.01);
                    padding: 12px 14px;
                    margin-bottom: 10px;
                }}
                .hf-message-list-card-selected {{
                    border: 1px solid {COLORI["colore_primario"]};
                    box-shadow: 0 0 0 1px {COLORI["colore_primario"]}33 inset;
                    background: rgba(31,79,255,0.08);
                }}
                .hf-message-meta {{
                    color: rgba(255,255,255,0.72);
                    font-size: 0.92rem;
                }}
                .hf-section-title {{
                    font-size: 1.65rem;
                    font-weight: 800;
                    margin-bottom: 4px;
                }}
                </style>
                """,
                unsafe_allow_html=True,
            )

            st.subheader(TESTI["messaggi_titolo"])
            st.caption("Qui configuri i template e gestisci solo i messaggi davvero utili da controllare adesso.")

            current_message_settings = {
                "msg_rule_confirm_offset_days": int(st.session_state.get("msg_rule_confirm_offset_days", 0)),
                "msg_rule_confirm_time": str(st.session_state.get("msg_rule_confirm_time", "10:00")),
                "msg_rule_checkin_reminder_days": int(st.session_state.get("msg_rule_checkin_reminder_days", 1)),
                "msg_rule_checkin_reminder_time": str(st.session_state.get("msg_rule_checkin_reminder_time", "18:00")),
                "msg_rule_checkin_instr_time": str(st.session_state.get("msg_rule_checkin_instr_time", "15:00")),
                "msg_rule_checkout_reminder_days": int(st.session_state.get("msg_rule_checkout_reminder_days", 1)),
                "msg_rule_checkout_reminder_time": str(st.session_state.get("msg_rule_checkout_reminder_time", "18:00")),
                "msg_rule_review_days_after": int(st.session_state.get("msg_rule_review_days_after", 1)),
                "msg_rule_review_time": str(st.session_state.get("msg_rule_review_time", "12:00")),
                "template_booking_confirmation": st.session_state.get("template_booking_confirmation", ""),
                "template_checkin_reminder": st.session_state.get("template_checkin_reminder", ""),
                "template_checkin_instructions": st.session_state.get("template_checkin_instructions", ""),
                "template_checkout_reminder": st.session_state.get("template_checkout_reminder", ""),
                "template_review_request": st.session_state.get("template_review_request", ""),
            }
            saved_message_settings = load_message_settings(st.session_state.utente["id"])
            needs_regeneration = current_message_settings != saved_message_settings

            template_editor_map = {
                "template_booking_confirmation_editor": "template_booking_confirmation",
                "template_checkin_reminder_editor": "template_checkin_reminder",
                "template_checkin_instructions_editor": "template_checkin_instructions",
                "template_checkout_reminder_editor": "template_checkout_reminder",
                "template_review_request_editor": "template_review_request",
            }
            show_templates_box = st.toggle("Mostra template", value=False, key="show_message_templates_toggle")
            templates_were_open = bool(st.session_state.get("show_message_templates_prev", False))
            if show_templates_box and not templates_were_open:
                for editor_key, persistent_key in template_editor_map.items():
                    ensure_template_editor_value(editor_key, persistent_key, force_reload=True)
            elif show_templates_box:
                for editor_key, persistent_key in template_editor_map.items():
                    ensure_template_editor_value(editor_key, persistent_key)
            st.session_state["show_message_templates_prev"] = show_templates_box

            if show_templates_box:
                with st.container(border=True):
                    st.markdown("### Template messaggi")
                    st.caption("I dati della struttura vengono presi automaticamente dalla Scheda immobile. Modifica solo il testo che vuoi far vedere all'ospite.")
                    st.info(
                        "Variabili disponibili: {nome_ospite}, {data_checkin}, {data_checkout}, {nome_struttura}, {nome_host}, {numero_whatsapp}, {checkin_da}, {checkin_fino}, {checkout_entro}, {wifi_nome}, {wifi_password}, {parcheggio_disponibile}, {tassa_soggiorno}, {testo_parcheggio}, {testo_tassa_soggiorno}"
                    )

                    template_tabs = st.tabs([
                        "Conferma prenotazione",
                        "Reminder check-in",
                        "Istruzioni check-in",
                        "Reminder check-out",
                        "Richiesta recensione",
                    ])

                    with template_tabs[0]:
                        st.text_area(
                            "Testo conferma prenotazione",
                            key="template_booking_confirmation_editor",
                            height=170,
                            label_visibility="collapsed",
                            on_change=sync_template_editor_to_persistent,
                            args=("template_booking_confirmation_editor", "template_booking_confirmation"),
                        )
                    with template_tabs[1]:
                        st.text_area(
                            "Testo reminder check-in",
                            key="template_checkin_reminder_editor",
                            height=170,
                            label_visibility="collapsed",
                            on_change=sync_template_editor_to_persistent,
                            args=("template_checkin_reminder_editor", "template_checkin_reminder"),
                        )
                    with template_tabs[2]:
                        st.text_area(
                            "Testo istruzioni check-in",
                            key="template_checkin_instructions_editor",
                            height=190,
                            label_visibility="collapsed",
                            on_change=sync_template_editor_to_persistent,
                            args=("template_checkin_instructions_editor", "template_checkin_instructions"),
                        )
                    with template_tabs[3]:
                        st.text_area(
                            "Testo reminder check-out",
                            key="template_checkout_reminder_editor",
                            height=170,
                            label_visibility="collapsed",
                            on_change=sync_template_editor_to_persistent,
                            args=("template_checkout_reminder_editor", "template_checkout_reminder"),
                        )
                    with template_tabs[4]:
                        st.text_area(
                            "Testo richiesta recensione",
                            key="template_review_request_editor",
                            height=170,
                            label_visibility="collapsed",
                            on_change=sync_template_editor_to_persistent,
                            args=("template_review_request_editor", "template_review_request"),
                        )

                    generate_button_col, _ = st.columns([1, 2.6])
                    with generate_button_col:
                        generate_clicked = st.button(
                            "Genera / aggiorna messaggi programmati",
                            use_container_width=True,
                            key="generate_scheduled_messages_top",
                        )
            else:
                generate_clicked = False

            if needs_regeneration:
                st.warning("Hai modificato regole o template. Attiva 'Mostra template' e premi 'Genera / aggiorna messaggi programmati' per aggiornare i messaggi già creati.")

            st.markdown("### Orari messaggi")
            reg1, reg2 = st.columns(2)

            with reg1:
                st.markdown("**Conferma prenotazione**")
                conf1, conf2 = st.columns([1, 1.2])
                with conf1:
                    confirm_offset_days = st.number_input(
                        "Giorni prima / stesso giorno",
                        min_value=0,
                        max_value=30,
                        step=1,
                        key="msg_rule_confirm_offset_days"
                    )
                with conf2:
                    confirm_time = st.text_input(
                        "Orario conferma prenotazione",
                        key="msg_rule_confirm_time"
                    )

                st.markdown("**Reminder check-in**")
                rc1, rc2 = st.columns([1, 1.2])
                with rc1:
                    checkin_reminder_days = st.number_input(
                        "Giorni prima",
                        min_value=0,
                        max_value=30,
                        step=1,
                        key="msg_rule_checkin_reminder_days"
                    )
                with rc2:
                    checkin_reminder_time = st.text_input(
                        "Orario reminder check-in",
                        key="msg_rule_checkin_reminder_time"
                    )

                st.markdown("**Istruzioni check-in**")
                checkin_instr_time = st.text_input(
                    "Orario del giorno check-in",
                    key="msg_rule_checkin_instr_time"
                )

            with reg2:
                st.markdown("**Reminder check-out**")
                rco1, rco2 = st.columns([1, 1.2])
                with rco1:
                    checkout_reminder_days = st.number_input(
                        "Giorni prima",
                        min_value=0,
                        max_value=30,
                        step=1,
                        key="msg_rule_checkout_reminder_days"
                    )
                with rco2:
                    checkout_reminder_time = st.text_input(
                        "Orario reminder check-out",
                        key="msg_rule_checkout_reminder_time"
                    )

                st.markdown("**Richiesta recensione**")
                rr1, rr2 = st.columns([1, 1.2])
                with rr1:
                    review_days_after = st.number_input(
                        "Giorni dopo il check-out",
                        min_value=0,
                        max_value=30,
                        step=1,
                        key="msg_rule_review_days_after"
                    )
                with rr2:
                    review_time = st.text_input(
                        "Orario richiesta recensione",
                        key="msg_rule_review_time"
                    )

            scheduling_rules = {
                "confirm_offset_days": confirm_offset_days,
                "confirm_time": confirm_time,
                "checkin_reminder_days": checkin_reminder_days,
                "checkin_reminder_time": checkin_reminder_time,
                "checkin_instr_time": checkin_instr_time,
                "checkout_reminder_days": checkout_reminder_days,
                "checkout_reminder_time": checkout_reminder_time,
                "review_days_after": review_days_after,
                "review_time": review_time,
            }

            template_booking_confirmation = st.session_state.get("template_booking_confirmation", "")
            template_checkin_reminder = st.session_state.get("template_checkin_reminder", "")
            template_checkin_instructions = st.session_state.get("template_checkin_instructions", "")
            template_checkout_reminder = st.session_state.get("template_checkout_reminder", "")
            template_review_request = st.session_state.get("template_review_request", "")

            template_base = {
                "booking_confirmation": template_booking_confirmation,
                "checkin_reminder": template_checkin_reminder,
                "checkin_instructions": template_checkin_instructions,
                "checkout_reminder": template_checkout_reminder,
                "review_request": template_review_request,
            }

            auto_messages_signature_key = f"scheduled_messages_bookings_signature_{st.session_state.utente['id']}"
            current_bookings_signature = build_bookings_auto_signature(df)

            if (
                not needs_regeneration
                and st.session_state.get(auto_messages_signature_key) != current_bookings_signature
            ):
                replace_scheduled_messages_for_user(
                    st.session_state.utente["id"],
                    df,
                    profilo,
                    scheduling_rules=scheduling_rules,
                    template_base=template_base,
                )
                st.session_state[auto_messages_signature_key] = current_bookings_signature
                st.session_state["selected_scheduled_message_id"] = None
                st.rerun()

            if generate_clicked:
                for editor_key, persistent_key in template_editor_map.items():
                    if editor_key in st.session_state:
                        st.session_state[persistent_key] = st.session_state.get(editor_key, st.session_state.get(persistent_key, ""))

                template_booking_confirmation = st.session_state.get("template_booking_confirmation", "")
                template_checkin_reminder = st.session_state.get("template_checkin_reminder", "")
                template_checkin_instructions = st.session_state.get("template_checkin_instructions", "")
                template_checkout_reminder = st.session_state.get("template_checkout_reminder", "")
                template_review_request = st.session_state.get("template_review_request", "")

                template_base = {
                    "booking_confirmation": template_booking_confirmation,
                    "checkin_reminder": template_checkin_reminder,
                    "checkin_instructions": template_checkin_instructions,
                    "checkout_reminder": template_checkout_reminder,
                    "review_request": template_review_request,
                }

                save_message_settings(
                    st.session_state.utente["id"],
                    {
                        "msg_rule_confirm_offset_days": confirm_offset_days,
                        "msg_rule_confirm_time": confirm_time,
                        "msg_rule_checkin_reminder_days": checkin_reminder_days,
                        "msg_rule_checkin_reminder_time": checkin_reminder_time,
                        "msg_rule_checkin_instr_time": checkin_instr_time,
                        "msg_rule_checkout_reminder_days": checkout_reminder_days,
                        "msg_rule_checkout_reminder_time": checkout_reminder_time,
                        "msg_rule_review_days_after": review_days_after,
                        "msg_rule_review_time": review_time,
                        "template_booking_confirmation": template_booking_confirmation,
                        "template_checkin_reminder": template_checkin_reminder,
                        "template_checkin_instructions": template_checkin_instructions,
                        "template_checkout_reminder": template_checkout_reminder,
                        "template_review_request": template_review_request,
                    }
                )
                totale = replace_scheduled_messages_for_user(
                    st.session_state.utente["id"],
                    df,
                    profilo,
                    scheduling_rules=scheduling_rules,
                    template_base=template_base,
                )
                st.session_state[auto_messages_signature_key] = current_bookings_signature
                st.session_state["selected_scheduled_message_id"] = None
                st.success(f"Messaggi programmati aggiornati: {totale}")
                st.rerun()

            scheduled_df = load_scheduled_messages(st.session_state.utente["id"])
            valid = df[df["status"].str.lower() != "cancelled"].sort_values("check_in")

            if not scheduled_df.empty:
                valid_keys = set(
                    valid.apply(
                        lambda r: f"{str(r.get('guest_name', '')).strip().lower()}|{pd.to_datetime(r.get('check_in')).date()}|{pd.to_datetime(r.get('check_out')).date()}",
                        axis=1,
                    )
                )
                scheduled_df = scheduled_df.copy()
                scheduled_df["booking_key"] = scheduled_df.apply(
                    lambda r: f"{str(r.get('guest_name', '')).strip().lower()}|{pd.to_datetime(r.get('check_in')).date()}|{pd.to_datetime(r.get('check_out')).date()}",
                    axis=1,
                )
                scheduled_df = scheduled_df[scheduled_df["booking_key"].isin(valid_keys)].copy()
                scheduled_df = scheduled_df.drop(columns=["booking_key"], errors="ignore")
            if len(valid) == 0:
                st.warning(TESTI["messaggi_nessuna_prenotazione"])
            else:
                st.markdown("### Messaggi programmati")
                if scheduled_df.empty:
                    st.info("Nessun messaggio programmato ancora. Premi il bottone sopra per generarli dalle prenotazioni.")
                else:
                    scheduled_df = scheduled_df.copy()
                    scheduled_df["send_at_dt"] = pd.to_datetime(scheduled_df["send_at"], errors="coerce")
                    scheduled_df["check_in_dt"] = pd.to_datetime(scheduled_df["check_in"], errors="coerce")
                    scheduled_df["check_out_dt"] = pd.to_datetime(scheduled_df["check_out"], errors="coerce")
                    now_ts = pd.Timestamp.now()
                    today_ts = pd.Timestamp.now().normalize()

                    pending_count = int((scheduled_df["status"] == "pending").sum())
                    sent_count = int((scheduled_df["status"] == "sent").sum())
                    failed_count = int((scheduled_df["status"] == "failed").sum())
                    cancelled_count = int((scheduled_df["status"] == "cancelled").sum())

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Da inviare", pending_count)
                    m2.metric("Inviati", sent_count)
                    m3.metric("Falliti", failed_count)
                    m4.metric("Annullati", cancelled_count)

                    upcoming_base_df = scheduled_df[
                        (
                            scheduled_df["status"].isin(["pending", "failed", "cancelled"])
                            & (scheduled_df["send_at_dt"] >= now_ts)
                        )
                        | (
                            (scheduled_df["status"] == "sent")
                            & (scheduled_df["send_at_dt"] >= now_ts)
                        )
                    ].copy()


                    upcoming_base_df["tipo"] = upcoming_base_df["message_type"].apply(label_message_type)

                    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1.1, 0.9])
                    with filter_col1:
                        stato_filtro = st.selectbox(
                            "Stato",
                            ["Tutti", "pending", "failed", "cancelled", "sent"],
                            index=0,
                            key="scheduled_status_filter",
                        )
                    with filter_col2:
                        tipo_filtro = st.selectbox(
                            "Tipo",
                            ["Tutti"] + [label_message_type(x) for x in sorted(upcoming_base_df["message_type"].dropna().unique())],
                            index=0,
                            key="scheduled_type_filter",
                        )
                    with filter_col3:
                        ricerca_ospite = st.text_input("Cerca ospite", key="scheduled_guest_search")
                    with filter_col4:
                        mostra_storico = st.toggle("Mostra storico", value=False, key="scheduled_show_history")

                    display_df = scheduled_df.copy() if mostra_storico else upcoming_base_df.copy()
                    display_df["tipo"] = display_df["message_type"].apply(label_message_type)

                    if stato_filtro != "Tutti":
                        display_df = display_df[display_df["status"] == stato_filtro]
                    if tipo_filtro != "Tutti":
                        display_df = display_df[display_df["tipo"] == tipo_filtro]
                    if ricerca_ospite.strip():
                        display_df = display_df[
                            display_df["guest_name"].astype(str).str.contains(ricerca_ospite.strip(), case=False, na=False)
                        ]

                    display_df = display_df.sort_values(["check_in_dt", "send_at_dt", "id"]).copy()

                    if not mostra_storico:
                        st.caption("Di default vedi solo i messaggi ancora utili da gestire per soggiorni in corso o futuri. I messaggi vecchi restano nello storico.")

                    if display_df.empty:
                        if not mostra_storico and upcoming_base_df.empty:
                            st.info("Non ci sono prossimi messaggi da gestire.")
                        else:
                            st.info("Nessun messaggio corrisponde ai filtri selezionati.")
                    else:
                        left_col, right_col = st.columns([1.02, 1.55], gap="large")

                        def get_status_ui(status_value, sent_at_value=None, error_text=None):
                            mapping = {
                                "pending": {
                                    "label": "In attesa di invio",
                                    "pill_bg": "rgba(245, 194, 66, 0.16)",
                                    "pill_border": "#F5C242",
                                    "pill_text": "#FFD76A",
                                },
                                "sent": {
                                    "label": "Inviato",
                                    "pill_bg": "rgba(72, 187, 120, 0.16)",
                                    "pill_border": "#48BB78",
                                    "pill_text": "#7EE2A8",
                                },
                                "failed": {
                                    "label": "Invio fallito",
                                    "pill_bg": "rgba(239, 68, 68, 0.16)",
                                    "pill_border": "#EF4444",
                                    "pill_text": "#FF8F8F",
                                },
                                "cancelled": {
                                    "label": "Annullato",
                                    "pill_bg": "rgba(148, 163, 184, 0.18)",
                                    "pill_border": "#94A3B8",
                                    "pill_text": "#D1D5DB",
                                },
                            }
                            base = mapping.get(str(status_value), mapping["pending"])
                            detail = base["label"]
                            if str(status_value) == "sent" and sent_at_value:
                                try:
                                    detail = f"Inviato il {pd.to_datetime(sent_at_value).strftime('%d/%m/%Y %H:%M')}"
                                except Exception:
                                    detail = "Inviato"
                            if str(status_value) == "failed" and error_text:
                                detail = "Invio fallito"
                            return base, detail

                        message_ids = [int(x) for x in display_df["id"].tolist()]
                        current_selection = st.session_state.get("selected_scheduled_message_id")
                        if current_selection not in message_ids:
                            current_selection = message_ids[0]
                            st.session_state["selected_scheduled_message_id"] = current_selection

                        with left_col:
                            st.markdown("<div class='hf-section-title'>Prossimi messaggi</div>", unsafe_allow_html=True)
                            st.caption("Lista essenziale, ordinata per soggiorno e data di invio.")

                            selected_id = st.radio(
                                "Messaggi disponibili",
                                options=message_ids,
                                index=message_ids.index(current_selection),
                                key="selected_scheduled_message_id",
                                format_func=lambda mid: (
                                    f'#{int(mid)} · '
                                    f'{display_df.loc[display_df["id"] == mid, "guest_name"].iloc[0]} · '
                                    f'{label_message_type(display_df.loc[display_df["id"] == mid, "message_type"].iloc[0])} · '
                                    f'{pd.to_datetime(display_df.loc[display_df["id"] == mid, "send_at"].iloc[0]).strftime("%d/%m/%Y %H:%M")}'
                                ),
                            )

                        current_msg = get_scheduled_message_by_id(selected_id, st.session_state.utente["id"])

                        with right_col:
                            st.markdown("<div class='hf-section-title'>Dettaglio messaggio</div>", unsafe_allow_html=True)

                            if current_msg:
                                status_ui, status_detail = get_status_ui(
                                    current_msg.get("status"),
                                    current_msg.get("sent_at"),
                                    current_msg.get("error_message"),
                                )
                                try:
                                    send_at_label = pd.to_datetime(current_msg["send_at"]).strftime("%d/%m/%Y %H:%M")
                                except Exception:
                                    send_at_label = str(current_msg.get("send_at", "-"))

                                st.markdown(
                                    f"""
                                    <div class="hf-message-main-card" style="border-color:{status_ui['pill_border']};">
                                        <div class="hf-message-status-pill" style="background:{status_ui['pill_bg']}; border:1px solid {status_ui['pill_border']}; color:{status_ui['pill_text']};">
                                            {status_detail}
                                        </div>
                                        <div style="font-size:2rem; font-weight:800; margin-bottom:6px;">{current_msg['guest_name']}</div>
                                        <div class="hf-message-meta">{label_message_type(current_msg['message_type'])} · {current_msg['channel']} · Invio previsto: {send_at_label}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )

                                resolved_guest_phone = resolve_message_guest_phone(current_msg, df)

                                info1, info2 = st.columns(2)
                                with info1:
                                    st.text_input("Telefono", value=resolved_guest_phone, disabled=True, key=f"msg_phone_{selected_id}")
                                with info2:
                                    st.text_input("Piattaforma", value=str(current_msg.get("platform", "") or ""), disabled=True, key=f"msg_platform_{selected_id}")

                                st.text_area(
                                    "Anteprima messaggio",
                                    value=current_msg["message_text"],
                                    height=190,
                                    disabled=True,
                                    key=preview_message_key(selected_id, current_msg["message_text"]),
                                )

                                if current_msg.get("error_message"):
                                    st.error(f"Errore ultimo invio: {current_msg['error_message']}")


                                status_value = str(current_msg.get("status", "pending"))


                                if status_value == "failed":


                                    action1, action2 = st.columns(2)


                                    with action1:


                                        if st.button("Ripristina", use_container_width=True, key=f"retry_msg_{selected_id}"):


                                            ok = update_scheduled_message_status(


                                                selected_id,


                                                st.session_state.utente["id"],


                                                "pending",


                                                error_message=None,


                                                set_sent_now=False,


                                            )


                                            if ok:


                                                st.success("Messaggio rimesso in attesa di invio.")


                                                st.rerun()


                                    with action2:


                                        if st.button("Annulla", use_container_width=True, key=f"cancel_msg_{selected_id}"):


                                            ok = update_scheduled_message_status(


                                                selected_id,


                                                st.session_state.utente["id"],


                                                "cancelled",


                                                error_message=None,


                                                set_sent_now=False,


                                            )


                                            if ok:


                                                st.info("Messaggio annullato.")


                                                st.rerun()


                                elif status_value == "cancelled":


                                    if st.button("Ripristina", use_container_width=True, key=f"restore_msg_{selected_id}"):


                                        ok = update_scheduled_message_status(


                                            selected_id,


                                            st.session_state.utente["id"],


                                            "pending",


                                            error_message=None,


                                            set_sent_now=False,


                                        )


                                        if ok:


                                            st.success("Messaggio ripristinato.")


                                            st.rerun()


                                elif status_value == "sent":


                                    st.success("Questo messaggio risulta già inviato. Se vuoi rivedere quelli vecchi puoi lasciare attivo 'Mostra storico'.")


                                else:


                                    st.info("L'invio automatico verrà gestito dal provider WhatsApp integrato online. Da qui puoi solo monitorare o annullare il messaggio.")


                                    if st.button("Annulla", use_container_width=True, key=f"cancel_msg_{selected_id}"):


                                        ok = update_scheduled_message_status(


                                            selected_id,


                                            st.session_state.utente["id"],


                                            "cancelled",


                                            error_message=None,


                                            set_sent_now=False,


                                        )


                                        if ok:


                                            st.info("Messaggio annullato.")


                                            st.rerun()

if "pulizie_servizi" in tab_map:
    with tab_map["pulizie_servizi"]:
        st.subheader("Servizi di pulizia")
        if df is None:
            st.info("Carica prima un file prenotazioni per collegare pulizie, check-in e check-out reali.")
        else:
            valid_cleaning = df[df["status"].str.lower() != "cancelled"].copy()
            valid_cleaning = valid_cleaning.sort_values(["check_out", "check_in", "guest_name"])

            cleaning_df = load_cleaning_services(st.session_state.utente["id"])

            current_period_cleaning = pd.DataFrame()
            if not cleaning_df.empty:
                current_period_cleaning = cleaning_df.copy()
                current_period_cleaning["service_date_dt"] = pd.to_datetime(current_period_cleaning["service_date"], errors="coerce").dt.date
                current_period_cleaning = current_period_cleaning[
                    (hf_date_series(current_period_cleaning["service_date"]) >= hf_bound(period_start)) &
                    (hf_date_series(current_period_cleaning["service_date"]) < hf_bound(period_end))
                ].copy()

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Ore totali periodo", f'{current_period_cleaning["hours_worked"].sum():.2f}' if not current_period_cleaning.empty else "0.00")
            k2.metric("Costo totale periodo", f'€ {current_period_cleaning["total_cost"].sum():.2f}' if not current_period_cleaning.empty else "€ 0.00")
            k3.metric("Pulizie registrate", int(len(current_period_cleaning)))
            avg_cleaning_cost = current_period_cleaning["total_cost"].mean() if not current_period_cleaning.empty else 0
            k4.metric("Costo medio pulizia", f'€ {avg_cleaning_cost:.2f}')

            cleaning_services_display = current_period_cleaning.copy()

            if not cleaning_services_display.empty:
                cleaning_services_display = cleaning_services_display.sort_values(
                    by="service_date_dt",
                    ascending=False
                )

                st.dataframe(
                    cleaning_services_display[
                        [
                            "id",
                            "service_date",
                            "guest_name",
                            "service_type",
                            "cleaner_name",
                            "start_time",
                            "end_time",
                            "hours_worked",
                            "hourly_rate",
                            "extra_cost",
                            "total_cost",
                            "payment_status",
                            "notes",
                        ]
                    ].rename(columns={
                        "id": "ID",
                        "service_date": "Data",
                        "guest_name": "Cliente",
                        "service_type": "Tipo servizio",
                        "cleaner_name": "Donna pulizie",
                        "start_time": "Ora inizio",
                        "end_time": "Ora fine",
                        "hours_worked": "Ore",
                        "hourly_rate": "Tariffa oraria",
                        "extra_cost": "Extra",
                        "total_cost": "Totale",
                        "payment_status": "Stato pagamento",
                        "notes": "Note",
                    }),
                    width="stretch",
                    hide_index=True
                )
            else:
                st.info("Nessun servizio di pulizia registrato nel periodo selezionato.")

            st.markdown("### Inserimento servizio pulizia")
            dashboard_cleaning_source = filtered_df if filtered_df is not None and not filtered_df.empty else valid_cleaning
            dashboard_cleaning_source = dashboard_cleaning_source[dashboard_cleaning_source["status"].str.lower() != "cancelled"].copy()
            dashboard_cleaning_source = dashboard_cleaning_source.sort_values(["check_out", "check_in", "guest_name"])

            booking_options = []
            for _, row in dashboard_cleaning_source.iterrows():
                booking_ref_value = booking_reference(row)
                label = f'{row["guest_name"]} · check-out {pd.to_datetime(row["check_out"]).strftime("%d/%m/%Y")} · check-in {pd.to_datetime(row["check_in"]).strftime("%d/%m/%Y")}'
                booking_options.append({
                    "booking_ref": booking_ref_value,
                    "label": label,
                    "guest_name": str(row.get("guest_name", "")),
                    "checkout_date": pd.to_datetime(row.get("check_out")).date(),
                    "checkin_date": pd.to_datetime(row.get("check_in")).date(),
                })

            selected_booking = None
            if booking_options:
                today_date = date.today()
                default_booking_index = min(
                    range(len(booking_options)),
                    key=lambda i: abs((booking_options[i]["checkout_date"] - today_date).days)
                )
                selected_booking_label = st.selectbox(
                    "Prenotazione associata",
                    options=[x["label"] for x in booking_options],
                    index=default_booking_index,
                    key="cleaning_selected_booking",
                )
                selected_booking = next((x for x in booking_options if x["label"] == selected_booking_label), None)

            form_col1, form_col2 = st.columns(2)
            with form_col1:
                service_date = st.date_input(
                    "Data pulizia",
                    value=(selected_booking["checkout_date"] if selected_booking else date.today()),
                    key="cleaning_service_date",
                )
                service_type = st.selectbox(
                    "Tipo servizio",
                    ["check_out", "check_in", "extra"],
                    format_func=lambda x: {"check_out": "Pulizia check-out", "check_in": "Preparazione check-in", "extra": "Pulizia extra"}.get(x, x),
                    key="cleaning_service_type",
                )
                cleaner_name = st.text_input("Donna delle pulizie", key="cleaner_name")
                start_time = st.text_input("Ora inizio", value="11:00", key="cleaning_start_time")
                end_time = st.text_input("Ora fine", value="12:30", key="cleaning_end_time")
            with form_col2:
                hourly_rate = st.number_input("Tariffa oraria (€)", min_value=0.0, value=10.0, step=0.5, key="cleaning_hourly_rate")
                extra_cost = st.number_input("Extra / materiali (€)", min_value=0.0, value=0.0, step=1.0, key="cleaning_extra_cost")
                use_custom_total = st.checkbox("Totale custom", value=False, key="cleaning_use_custom_total")
                custom_total_override = st.number_input(
                    "Totale personalizzato (€)",
                    min_value=0.0,
                    value=0.0,
                    step=1.0,
                    key="cleaning_custom_total_override",
                    disabled=not use_custom_total,
                )
                payment_status = st.selectbox("Stato pagamento", ["Da pagare", "Pagato"], key="cleaning_payment_status")

            hours_worked = calculate_hours_worked(start_time, end_time)
            total_cost = calculate_cleaning_total(
                hours_worked=hours_worked,
                hourly_rate=hourly_rate,
                extra_cost=extra_cost,
                custom_total_override=(custom_total_override if use_custom_total else None),
            )

            guest_name_for_service = selected_booking["guest_name"] if selected_booking else ""
            booking_ref_for_service = selected_booking["booking_ref"] if selected_booking else ""

            st.caption(
                f"Cliente associato: {guest_name_for_service or '-'} · Ore calcolate: {hours_worked:.2f} · Totale calcolato: € {total_cost:.2f}"
            )

            cleaning_notes = st.text_area("Note", key="cleaning_notes", height=90)

            if st.button("Salva servizio pulizia", use_container_width=True, key="save_cleaning_service_button"):
                save_cleaning_service(
                    st.session_state.utente["id"],
                    {
                        "service_date": service_date.isoformat(),
                        "booking_ref": booking_ref_for_service,
                        "guest_name": guest_name_for_service,
                        "service_type": service_type,
                        "cleaner_name": cleaner_name,
                        "start_time": start_time,
                        "end_time": end_time,
                        "hours_worked": hours_worked,
                        "hourly_rate": hourly_rate,
                        "extra_cost": extra_cost,
                        "custom_total_override": (custom_total_override if use_custom_total else None),
                        "total_cost": total_cost,
                        "payment_status": payment_status,
                        "notes": cleaning_notes,
                    },
                )
                st.success("Servizio pulizia salvato.")
                st.rerun()

            st.markdown("### Modifica pulizie")
            cleaning_df = load_cleaning_services(st.session_state.utente["id"])
            if cleaning_df.empty:
                st.info("Nessun servizio pulizia registrato.")
            else:
                action_col1, action_col2 = st.columns(2)
                with action_col1:
                    selected_cleaning_id = st.selectbox(
                        "Seleziona servizio da modificare",
                        options=cleaning_df["id"].astype(int).tolist(),
                        format_func=lambda sid: (
                            f'#{int(sid)} · '
                            f'{cleaning_df.loc[cleaning_df["id"] == sid, "guest_name"].iloc[0]} · '
                            f'{pd.to_datetime(cleaning_df.loc[cleaning_df["id"] == sid, "service_date"], errors="coerce").dt.strftime("%d/%m/%Y").iloc[0]}'
                        ),
                        key="selected_cleaning_service_id",
                    )
                with action_col2:
                    if st.button("Azzera tutto il registro pulizie", use_container_width=True, key="delete_all_cleaning_services_button"):
                        deleted_rows = delete_all_cleaning_services(st.session_state.utente["id"])
                        st.success(f"Registro pulizie azzerato. Record eliminati: {deleted_rows}")
                        st.rerun()

                selected_cleaning_row = cleaning_df[cleaning_df["id"] == int(selected_cleaning_id)].iloc[0]

                with st.expander("Modifica servizio pulizia selezionato", expanded=False):
                    edit_booking_index = 0
                    selected_cleaning_booking_ref = str(selected_cleaning_row.get("booking_ref", "") or "")
                    if booking_options:
                        matching_booking_indices = [
                            idx for idx, opt in enumerate(booking_options)
                            if str(opt["booking_ref"]) == selected_cleaning_booking_ref
                        ]
                        if matching_booking_indices:
                            edit_booking_index = matching_booking_indices[0]

                    edit_booking = None
                    if booking_options:
                        edit_booking_label = st.selectbox(
                            "Prenotazione associata",
                            options=[x["label"] for x in booking_options],
                            index=edit_booking_index,
                            key=f"edit_cleaning_selected_booking_{int(selected_cleaning_id)}",
                        )
                        edit_booking = next((x for x in booking_options if x["label"] == edit_booking_label), None)

                    edit_col1, edit_col2 = st.columns(2)
                    with edit_col1:
                        edit_service_date = st.date_input(
                            "Data pulizia",
                            value=pd.to_datetime(selected_cleaning_row["service_date"], errors="coerce").date() if pd.notna(pd.to_datetime(selected_cleaning_row["service_date"], errors="coerce")) else date.today(),
                            key=f"edit_cleaning_service_date_{int(selected_cleaning_id)}",
                        )
                        service_type_options = ["check_out", "check_in", "extra"]
                        current_service_type = str(selected_cleaning_row.get("service_type", "check_out") or "check_out")
                        edit_service_type = st.selectbox(
                            "Tipo servizio",
                            service_type_options,
                            index=service_type_options.index(current_service_type) if current_service_type in service_type_options else 0,
                            format_func=lambda x: {"check_out": "Pulizia check-out", "check_in": "Preparazione check-in", "extra": "Pulizia extra"}.get(x, x),
                            key=f"edit_cleaning_service_type_{int(selected_cleaning_id)}",
                        )
                        edit_cleaner_name = st.text_input(
                            "Donna delle pulizie",
                            value=str(selected_cleaning_row.get("cleaner_name", "") or ""),
                            key=f"edit_cleaner_name_{int(selected_cleaning_id)}",
                        )
                        edit_start_time = st.text_input(
                            "Ora inizio",
                            value=str(selected_cleaning_row.get("start_time", "") or "11:00"),
                            key=f"edit_cleaning_start_time_{int(selected_cleaning_id)}",
                        )
                        edit_end_time = st.text_input(
                            "Ora fine",
                            value=str(selected_cleaning_row.get("end_time", "") or "13:00"),
                            key=f"edit_cleaning_end_time_{int(selected_cleaning_id)}",
                        )
                    with edit_col2:
                        edit_hourly_rate = st.number_input(
                            "Tariffa oraria (€)",
                            min_value=0.0,
                            value=float(selected_cleaning_row.get("hourly_rate", 0) or 0),
                            step=0.5,
                            key=f"edit_cleaning_hourly_rate_{int(selected_cleaning_id)}",
                        )
                        edit_extra_cost = st.number_input(
                            "Extra / materiali (€)",
                            min_value=0.0,
                            value=float(selected_cleaning_row.get("extra_cost", 0) or 0),
                            step=1.0,
                            key=f"edit_cleaning_extra_cost_{int(selected_cleaning_id)}",
                        )
                        existing_custom_total = selected_cleaning_row.get("custom_total_override", None)
                        edit_use_custom_total = st.checkbox(
                            "Totale custom",
                            value=existing_custom_total not in [None, ""] and not pd.isna(existing_custom_total),
                            key=f"edit_cleaning_use_custom_total_{int(selected_cleaning_id)}",
                        )
                        edit_custom_total_override = st.number_input(
                            "Totale personalizzato (€)",
                            min_value=0.0,
                            value=float(existing_custom_total or 0),
                            step=1.0,
                            key=f"edit_cleaning_custom_total_override_{int(selected_cleaning_id)}",
                            disabled=not edit_use_custom_total,
                        )
                        payment_status_options = ["Da pagare", "Pagato"]
                        current_payment_status = str(selected_cleaning_row.get("payment_status", "Da pagare") or "Da pagare")
                        edit_payment_status = st.selectbox(
                            "Stato pagamento",
                            payment_status_options,
                            index=payment_status_options.index(current_payment_status) if current_payment_status in payment_status_options else 0,
                            key=f"edit_cleaning_payment_status_{int(selected_cleaning_id)}",
                        )

                    edit_hours_worked = calculate_hours_worked(edit_start_time, edit_end_time)
                    edit_total_cost = calculate_cleaning_total(
                        hours_worked=edit_hours_worked,
                        hourly_rate=edit_hourly_rate,
                        extra_cost=edit_extra_cost,
                        custom_total_override=(edit_custom_total_override if edit_use_custom_total else None),
                    )

                    edit_guest_name_for_service = edit_booking["guest_name"] if edit_booking else str(selected_cleaning_row.get("guest_name", "") or "")
                    edit_booking_ref_for_service = edit_booking["booking_ref"] if edit_booking else str(selected_cleaning_row.get("booking_ref", "") or "")

                    st.caption(
                        f"Cliente associato: {edit_guest_name_for_service or '-'} · Ore calcolate: {edit_hours_worked:.2f} · Totale calcolato: € {edit_total_cost:.2f}"
                    )

                    edit_cleaning_notes = st.text_area(
                        "Note",
                        value=str(selected_cleaning_row.get("notes", "") or ""),
                        height=90,
                        key=f"edit_cleaning_notes_{int(selected_cleaning_id)}",
                    )

                    edit_action1, edit_action2 = st.columns(2)
                    with edit_action1:
                        if st.button("Aggiorna servizio pulizia", use_container_width=True, key=f"update_cleaning_service_button_{int(selected_cleaning_id)}"):
                            updated = update_cleaning_service(
                                st.session_state.utente["id"],
                                int(selected_cleaning_id),
                                {
                                    "service_date": edit_service_date.isoformat(),
                                    "booking_ref": edit_booking_ref_for_service,
                                    "guest_name": edit_guest_name_for_service,
                                    "service_type": edit_service_type,
                                    "cleaner_name": edit_cleaner_name,
                                    "start_time": edit_start_time,
                                    "end_time": edit_end_time,
                                    "hours_worked": edit_hours_worked,
                                    "hourly_rate": edit_hourly_rate,
                                    "extra_cost": edit_extra_cost,
                                    "custom_total_override": (edit_custom_total_override if edit_use_custom_total else None),
                                    "total_cost": edit_total_cost,
                                    "payment_status": edit_payment_status,
                                    "notes": edit_cleaning_notes,
                                },
                            )
                            if updated:
                                st.success("Servizio pulizia aggiornato.")
                                st.rerun()
                    with edit_action2:
                        if st.button("Elimina servizio selezionato", use_container_width=True, key=f"delete_cleaning_service_button_{int(selected_cleaning_id)}"):
                            deleted = delete_cleaning_service(st.session_state.utente["id"], int(selected_cleaning_id))
                            if deleted:
                                st.success("Servizio pulizia eliminato.")
                                st.rerun()

            

                with st.expander("Riepilogo mensile pulizie", expanded=False):
                    summary_df = cleaning_df.copy()
                    summary_df["month"] = pd.to_datetime(summary_df["service_date"], errors="coerce").dt.strftime("%Y-%m")
                    monthly_summary = summary_df.groupby(["month", "cleaner_name"], dropna=False, as_index=False).agg(
                        ore_totali=("hours_worked", "sum"),
                        pulizie=("id", "count"),
                        totale=("total_cost", "sum"),
                    )
                    monthly_summary["ore_totali"] = monthly_summary["ore_totali"].map(lambda x: round(float(x), 2))
                    monthly_summary["totale"] = monthly_summary["totale"].map(lambda x: round(float(x), 2))
                    st.dataframe(monthly_summary.rename(columns={
                        "month": "Mese",
                        "cleaner_name": "Donna pulizie",
                        "ore_totali": "Ore totali",
                        "pulizie": "Pulizie",
                        "totale": "Totale da pagare",
                    }), width="stretch")

if "dati" in tab_map:
    with tab_map["dati"]:
        if df is None:
            st.info(TESTI["stato_senza_file_dati"])
        else:
            st.subheader(TESTI["dati_titolo"])
            st.download_button(TESTI["dati_bottone_scarica"], data=dataframe_download(df), file_name="hostflow_v5_export.csv", mime="text/csv")
            if SEZIONI["mostra_colonne_calcolate"]:
                st.subheader(TESTI["dati_colonne_titolo"])
                st.write(["nights", "city_tax", "vat_platform_services", "transaction_cost", "cleaning_allocated", "withholding_tax", "net_operating", "net_real", "adr"])
