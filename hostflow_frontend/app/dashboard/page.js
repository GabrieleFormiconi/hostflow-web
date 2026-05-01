"use client";

import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "https://hostflow-backend.onrender.com";
const MAIN_TABS = ["Dashboard", "Immobile", "Analisi mercato", "Pricing", "Messaggi", "Pulizie", "Dati"];
const TABLE_COLS = ["platform", "guest_name", "guest_phone", "check_in", "check_out", "total_price", "cleaning_cost", "platform_fee", "transaction_cost", "status", "guests", "notes", "nights", "city_tax", "vat_platform_services", "cleaning_allocated", "withholding_tax", "net_operating", "net_real", "adr"];
const STORAGE = {
  settings: "hostflow_dashboard_settings_v3",
  sidebar: "hostflow_sidebar_collapsed_v1",
  sidebarWidth: "hostflow_sidebar_width_v1",
  tableCols: "hostflow_table_cols_v1",
  property: "hostflow_property_profile_v1",
  templates: "hostflow_message_templates_v1",
  schedules: "hostflow_message_schedules_v1",
  cleaningServices: "hostflow_cleaning_services_v1",
};

const DEFAULT_SETTINGS = {
  importMode: "Auto",
  cleaningMode: "Per prenotazione",
  cleaningCost: "0",
  monthlyCleaning: "0",
  cleaningHourlyRate: "0",
  cleaningHours: "0",
  cityTax: true,
  cityTaxRate: "4",
  transactionMode: "Percentuale",
  transactionPct: "1.5",
  vatPct: "22",
  withholding: true,
  withholdingPct: "21",
  periodMode: "Mensile",
  year: new Date().getFullYear(),
  month: new Date().getMonth() + 1,
  quarter: "1",
  semester: "1",
  customStart: new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10),
  customEnd: new Date().toISOString().slice(0, 10),
};
const DEFAULT_PROPERTY = {
  nome_immobile: "",
  indirizzo_completo: "",
  citta: "",
  cap: "",
  tipologia_immobile: "Appartamento intero",
  ospiti_massimi: 2,
  camere: 1,
  bagni: 1,
  fascia_qualita: "Standard",
  raggio_competitor: "1.0",
  nome_host: "",
  numero_whatsapp: "",
  checkin_da: "15:00",
  checkin_fino: "21:00",
  checkout_entro: "10:00",
  wifi_nome: "",
  wifi_password: "",
  animali_ammessi: false,
  fumatori_ammessi: false,
  parcheggio_disponibile: false,
  tassa_soggiorno: "4.0",
};
const DEFAULT_TEMPLATES = {
  booking_confirmation: `Ciao {nome_ospite}, grazie per aver prenotato {nome_struttura}.

Il tuo soggiorno è confermato dal {data_checkin} al {data_checkout}.

Check-in dalle {checkin_da} alle {checkin_fino}
Check-out entro le {checkout_entro}

Per qualsiasi necessità puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.`,
  checkin_reminder: `Ciao {nome_ospite}, ti ricordiamo che il check-in presso {nome_struttura} sarà il {data_checkin}.

L’orario di arrivo è previsto dalle {checkin_da} alle {checkin_fino}.

Se hai bisogno di assistenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.`,
  checkin_instructions: `Ciao {nome_ospite}, benvenuto a {nome_struttura}.

Ti ricordiamo che oggi il check-in è disponibile dalle {checkin_da} alle {checkin_fino}.

Dati Wi‑Fi:
Nome rete: {wifi_nome}
Password: {wifi_password}

Host di riferimento: {nome_host}
Contatto WhatsApp: {numero_whatsapp}{testo_parcheggio}{testo_tassa_soggiorno}`,
  checkout_reminder: `Ciao {nome_ospite}, ti ricordiamo che il check-out di {nome_struttura} è previsto il {data_checkout} entro le {checkout_entro}.

Per qualsiasi necessità prima della partenza puoi contattare {nome_host} su WhatsApp: {numero_whatsapp}.`,
  review_request: `Ciao {nome_ospite}, grazie per aver scelto {nome_struttura}.

Speriamo che il soggiorno sia stato piacevole. Se ti sei trovato bene, ci farebbe molto piacere ricevere una tua recensione.

Un saluto da {nome_host}.`,
};
const DEFAULT_SCHEDULES = {
  booking_confirmation: { label: "Conferma prenotazione", offsetDays: 0, time: "10:00" },
  checkin_reminder: { label: "Reminder check-in", offsetDays: -1, time: "18:00" },
  checkin_instructions: { label: "Istruzioni check-in", offsetDays: 0, time: "15:00" },
  checkout_reminder: { label: "Reminder check-out", offsetDays: -1, time: "18:00" },
  review_request: { label: "Richiesta recensione", offsetDays: 1, time: "12:00" },
};

function readJson(key, fallback) { try { const v = localStorage.getItem(key); return v ? { ...fallback, ...JSON.parse(v) } : fallback; } catch { return fallback; } }
function readArray(key) { try { const v = localStorage.getItem(key); const parsed = v ? JSON.parse(v) : []; return Array.isArray(parsed) ? parsed : []; } catch { return []; } }
function writeJson(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); } catch {} }
function fmtDate(v) { return v ? String(v).slice(0, 10) : ""; }
function toNum(v) { const n = Number(String(v ?? 0).replace(",", ".")); return Number.isFinite(n) ? n : 0; }
function euro(v) { return `€ ${toNum(v).toLocaleString("it-IT", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`; }
function nights(a, b) { const s = new Date(a), e = new Date(b); if (Number.isNaN(s.getTime()) || Number.isNaN(e.getTime())) return 0; return Math.max(1, Math.round((e - s) / 86400000)); }
function parseDateOnly(value) {
  if (value instanceof Date) return new Date(value.getFullYear(), value.getMonth(), value.getDate());
  const raw = String(value || "").slice(0, 10);
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? new Date() : new Date(d.getFullYear(), d.getMonth(), d.getDate());
}
function isoDate(d) {
  const x = parseDateOnly(d);
  const y = x.getFullYear();
  const m = String(x.getMonth() + 1).padStart(2, "0");
  const day = String(x.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function todayIso() {
  return isoDate(new Date());
}
function tomorrowIso() {
  const d = parseDateOnly(new Date());
  d.setDate(d.getDate() + 1);
  return isoDate(d);
}

function addDays(dateStr, days) { const d = parseDateOnly(dateStr); d.setDate(d.getDate() + Number(days || 0)); return isoDate(d); }
function isCancelled(row) { return String(row.status || "").toLowerCase() === "cancelled"; }
function periodRange(settings) {
  const y = Number(settings.year || new Date().getFullYear());
  const m = Number(settings.month || 1);
  if (settings.periodMode === "Trimestrale") { const q = Number(settings.quarter || 1); return [new Date(y, (q - 1) * 3, 1), new Date(y, q * 3, 0), `Q${q} ${y}`]; }
  if (settings.periodMode === "Semestrale") { const s = Number(settings.semester || 1); return [new Date(y, s === 1 ? 0 : 6, 1), new Date(y, s === 1 ? 6 : 12, 0), `${s === 1 ? "H1" : "H2"} ${y}`]; }
  if (settings.periodMode === "Annuale") return [new Date(y, 0, 1), new Date(y, 11, 31), `${y}`];
  if (settings.periodMode === "Personalizzato") return [parseDateOnly(settings.customStart), parseDateOnly(settings.customEnd), `${settings.customStart} → ${settings.customEnd}`];
  const label = new Date(y, m - 1, 1).toLocaleDateString("it-IT", { month: "long", year: "numeric" });
  return [new Date(y, m - 1, 1), new Date(y, m, 0), label.charAt(0).toUpperCase() + label.slice(1)];
}
function rowInPeriod(row, settings) { const [start, end] = periodRange(settings); const d = parseDateOnly(row.check_in); return !Number.isNaN(d.getTime()) && d >= parseDateOnly(start) && d <= parseDateOnly(end); }
function defaultCustomDatesForSettings(settings) {
  return { check_in: todayIso(), check_out: tomorrowIso() };
}
function bookingKeyForRow(row) { return `${row.platform || "row"}-${row.id || row.guest_name || row.check_in}-${row.check_in}-${row.check_out}`; }
function cleaningTotalForRow(row, cleaningServices = []) {
  const key = bookingKeyForRow(row);
  const guest = String(row.guest_name || "").trim().toLowerCase();
  const checkIn = fmtDate(row.check_in);
  const checkOut = fmtDate(row.check_out);
  return cleaningServices.reduce((sum, item) => {
    const itemKey = String(item.booking_key || "");
    const itemGuest = String(item.guest_name || "").trim().toLowerCase();
    const itemCheckIn = fmtDate(item.booking_check_in);
    const itemCheckOut = fmtDate(item.booking_check_out);
    const directMatch = itemKey && itemKey === key;
    const fallbackMatch = !itemKey && guest && itemGuest === guest && (!itemCheckIn || itemCheckIn === checkIn) && (!itemCheckOut || itemCheckOut === checkOut);
    return directMatch || fallbackMatch ? sum + toNum(item.total) : sum;
  }, 0);
}
function cleaningForRow(row, settings, allRows, cleaningServices = []) {
  if (isCancelled(row)) return 0;
  const registeredCleaning = cleaningTotalForRow(row, cleaningServices);
  if (registeredCleaning > 0) return registeredCleaning;
  if (settings.cleaningMode === "Mensile") {
    const activeNights = allRows.filter(r => !isCancelled(r)).reduce((s, r) => s + toNum(r.nights), 0) || 1;
    return (toNum(settings.monthlyCleaning) / activeNights) * toNum(row.nights);
  }
  if (settings.cleaningMode === "Ad ore") return toNum(settings.cleaningHourlyRate) * toNum(settings.cleaningHours);
  return toNum(row.cleaning_cost || settings.cleaningCost);
}
function enrichRow(row, settings, allRows, cleaningServices = []) {
  const n = toNum(row.nights || nights(row.check_in, row.check_out));
  const cancelled = isCancelled(row);
  const total = cancelled ? 0 : toNum(row.total_price);
  const cleaning = cleaningForRow({ ...row, nights: n }, settings, allRows || [], cleaningServices);
  const platformFee = cancelled ? 0 : toNum(row.platform_fee);
  const trx = cancelled ? 0 : (settings.transactionMode === "Percentuale" ? total * toNum(settings.transactionPct) / 100 : toNum(row.transaction_cost));
  const cityTax = cancelled || !settings.cityTax ? 0 : toNum(row.guests || 1) * n * toNum(settings.cityTaxRate);
  const vat = cancelled ? 0 : platformFee * toNum(settings.vatPct) / 100;
  const netOperating = cancelled ? 0 : total - cleaning - platformFee - trx - vat - cityTax;
  const withholding = cancelled || !settings.withholding ? 0 : Math.max(0, total - cityTax) * toNum(settings.withholdingPct) / 100;
  const netReal = netOperating - withholding;
  return { ...row, nights: n, total_price: total, cleaning_cost: cleaning, transaction_cost: trx, city_tax: cityTax, vat_platform_services: vat, cleaning_allocated: cleaning, withholding_tax: withholding, net_operating: netOperating, net_real: netReal, adr: n ? total / n : 0 };
}
function normalize(row, platformOverride) {
  return {
    id: row.id,
    platform: platformOverride || row.platform || "Booking",
    guest_name: row.guest_name || "",
    guest_phone: row.guest_phone || "",
    check_in: fmtDate(row.check_in),
    check_out: fmtDate(row.check_out),
    total_price: toNum(row.total_price),
    cleaning_cost: toNum(row.cleaning_cost),
    platform_fee: toNum(row.platform_fee),
    transaction_cost: toNum(row.transaction_cost),
    status: row.status || row.raw_booking_status || "confirmed",
    guests: toNum(row.guests || 1),
    notes: row.notes || "",
    nights: toNum(row.nights || nights(row.check_in, row.check_out)),
  };
}

function customRowSignature(row) {
  return [row.platform || "Custom", row.id || "", row.guest_name || "", row.check_in || "", row.check_out || "", row.total_price || ""].join("|");
}
function mergeCustomRows(serverRows, optimisticRows) {
  const out = [];
  const seen = new Set();
  [...(serverRows || []), ...(optimisticRows || [])].forEach((row) => {
    const key = row.id && !String(row.id).startsWith("local-") ? `${row.platform || "Custom"}-${row.id}` : customRowSignature(row);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(row);
    }
  });
  return out;
}

function extractReservationArray(data) {
  if (!data || typeof data !== "object") return [];
  const keys = ["reservations", "custom_bookings", "custom_reservations", "bookings", "items", "data", "rows"];
  for (const key of keys) {
    if (Array.isArray(data[key])) return data[key];
  }
  if (Array.isArray(data)) return data;
  return [];
}


function Metric({ label, value }) { return <div className="hf-metric"><div>{label}</div><strong>{value}</strong></div>; }
function Expander({ title, children, defaultOpen = false }) { const [open, setOpen] = useState(defaultOpen); return <div className={`hf-expander ${open ? "open" : ""}`}><button type="button" className="hf-expander-title" onClick={() => setOpen(!open)}><span>{open ? "−" : "+"}</span>{title}</button>{open && <div className="hf-expander-content">{children}</div>}</div>; }
function SidebarSection({ title, children }) { const [open, setOpen] = useState(false); return <div className={`hf-sidebar-section ${open ? "open" : ""}`}><button type="button" className="hf-sidebar-section-title" onClick={() => setOpen(v => !v)}><span>{open ? "−" : "+"}</span>{title}</button>{open && <div className="hf-sidebar-section-content">{children}</div>}</div>; }

function Sidebar({ token, onUploaded, settings, setSettings, collapsed, setCollapsed, sidebarWidth, setSidebarWidth, onLogout }) {
  const fileInput = useRef(null); const [uploadText, setUploadText] = useState(""); const [uploading, setUploading] = useState(false);
  function patch(next) { setSettings(prev => ({ ...prev, ...next })); }
  function startResize(e) {
    e.preventDefault();
    const onMove = (ev) => {
      const nextWidth = Math.min(620, Math.max(260, ev.clientX));
      setSidebarWidth(nextWidth);
      try { localStorage.setItem(STORAGE.sidebarWidth, String(nextWidth)); } catch {}
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }
  async function uploadFile(file) {
    if (!file) return; setUploading(true); setUploadText("");
    try { const form = new FormData(); form.append("file", file); const res = await fetch(`${API_URL}/reservations/upload`, { method: "POST", headers: { Authorization: `Bearer ${token}` }, body: form }); const data = await res.json(); if (!res.ok || data.status !== "ok") throw new Error(data.message || data.error || "Upload non riuscito."); setUploadText(`${data.inserted || 0} prenotazioni importate correttamente.`); onUploaded(); }
    catch (e) { setUploadText(e.message || "Errore upload."); } finally { setUploading(false); }
  }
  return <aside className={`hf-sidebar ${collapsed ? "collapsed" : ""}`} style={!collapsed ? { width: `${sidebarWidth}px` } : undefined}>
    <button type="button" className="hf-sidebar-toggle" onClick={() => setCollapsed(!collapsed)}>{collapsed ? "☰" : "×"}</button>
    {!collapsed && <>
      <h2>Import</h2><label>Tipo import</label>{["Auto", "Booking export", "CSV standard"].map(m => <label className="hf-radio" key={m}><input type="radio" checked={settings.importMode === m} onChange={() => patch({ importMode: m })} />{m}</label>)}
      <label>Carica file prenotazioni</label><div className="hf-upload"><input ref={fileInput} hidden type="file" accept=".csv,.xls,.xlsx" onChange={e => uploadFile(e.target.files?.[0])} /><button type="button" onClick={() => fileInput.current?.click()} disabled={uploading}>↥ {uploading ? "Upload..." : "Upload"}</button><p>200MB per file • CSV, XLS, XLSX</p></div>{uploadText && <div className="hf-success">{uploadText}</div>}
      <SidebarSection title="Pulizie"><label>Modalità pulizie</label>{["Per prenotazione", "Mensile", "Ad ore"].map(m => <label className="hf-radio" key={m}><input type="radio" checked={settings.cleaningMode === m} onChange={() => patch({ cleaningMode: m })} />{m}</label>)}<label>Pulizie per prenotazione (€)</label><input value={settings.cleaningCost} onChange={e => patch({ cleaningCost: e.target.value })} /><label>Costo pulizie mensile (€)</label><input value={settings.monthlyCleaning} onChange={e => patch({ monthlyCleaning: e.target.value })} /><label>Tariffa oraria pulizie (€)</label><input value={settings.cleaningHourlyRate} onChange={e => patch({ cleaningHourlyRate: e.target.value })} /><label>Ore medie per prenotazione</label><input value={settings.cleaningHours} onChange={e => patch({ cleaningHours: e.target.value })} /></SidebarSection>
      <SidebarSection title="Impostazioni finanziarie"><label className="hf-check"><input type="checkbox" checked={settings.cityTax} onChange={e => patch({ cityTax: e.target.checked })} />Includi tassa di soggiorno nel netto</label><label>Tassa soggiorno (€ per persona/notte)</label><input value={settings.cityTaxRate} onChange={e => patch({ cityTaxRate: e.target.value })} /><label>Costo transazione</label>{["Percentuale", "Dal file"].map(m => <label className="hf-radio" key={m}><input type="radio" checked={settings.transactionMode === m} onChange={() => patch({ transactionMode: m })} />{m}</label>)}<label>Costo transazione (%)</label><input value={settings.transactionPct} onChange={e => patch({ transactionPct: e.target.value })} /><label>VAT servizi piattaforma (%)</label><input value={settings.vatPct} onChange={e => patch({ vatPct: e.target.value })} /><label className="hf-check"><input type="checkbox" checked={settings.withholding} onChange={e => patch({ withholding: e.target.checked })} />Applica ritenuta locazione breve</label><label>Ritenuta locazione breve (%)</label><input value={settings.withholdingPct} onChange={e => patch({ withholdingPct: e.target.value })} /></SidebarSection>
      <SidebarSection title="Periodo"><label>Vista periodo</label><select value={settings.periodMode} onChange={e => patch({ periodMode: e.target.value })}>{["Mensile", "Trimestrale", "Semestrale", "Annuale", "Personalizzato"].map(x => <option key={x}>{x}</option>)}</select><label>Anno dashboard</label><input value={settings.year} onChange={e => patch({ year: e.target.value })} />{settings.periodMode === "Mensile" && <><label>Mese dashboard</label><select value={settings.month} onChange={e => patch({ month: e.target.value })}>{Array.from({ length: 12 }, (_, i) => <option key={i + 1}>{i + 1}</option>)}</select></>}{settings.periodMode === "Trimestrale" && <><label>Trimestre</label><select value={settings.quarter} onChange={e => patch({ quarter: e.target.value })}><option value="1">Q1</option><option value="2">Q2</option><option value="3">Q3</option><option value="4">Q4</option></select></>}{settings.periodMode === "Semestrale" && <><label>Semestre</label><select value={settings.semester} onChange={e => patch({ semester: e.target.value })}><option value="1">H1</option><option value="2">H2</option></select></>}{settings.periodMode === "Personalizzato" && <><label>Da</label><input type="date" value={settings.customStart} onChange={e => patch({ customStart: e.target.value })} /><label>A</label><input type="date" value={settings.customEnd} onChange={e => patch({ customEnd: e.target.value })} /></>}</SidebarSection>
      <button type="button" className="hf-sidebar-logout" onClick={onLogout}>Esci</button>
    </>}
    {!collapsed && <div className="hf-sidebar-resizer" onMouseDown={startResize} title="Trascina per ridimensionare" />}
  </aside>;
}

function DashboardTab({ token, rows, allRows, customRows, refresh, settings, periodLabel, onCustomCreated }) {
  const [visibleCols, setVisibleCols] = useState(TABLE_COLS); const [layoutMsg, setLayoutMsg] = useState(""); const [createMsg, setCreateMsg] = useState(""); const [editMsg, setEditMsg] = useState(""); const [dashView, setDashView] = useState("Dashboard");
  useEffect(() => { try { const saved = JSON.parse(localStorage.getItem(STORAGE.tableCols) || "null"); if (Array.isArray(saved) && saved.length) setVisibleCols(saved); } catch {} }, []);
  const periodDefaultDates = useMemo(() => defaultCustomDatesForSettings(settings), [settings.periodMode, settings.year, settings.month, settings.quarter, settings.semester, settings.customStart, settings.customEnd]);
  const [form, setForm] = useState({ guest_name: "", guest_phone: "", check_in: periodDefaultDates.check_in, check_out: periodDefaultDates.check_out, guests: 2, total_price: 0, cleaning_cost: 0, status: "confirmed", notes: "" });
  useEffect(() => { setForm(prev => (!prev.guest_name && !toNum(prev.total_price) ? { ...prev, check_in: periodDefaultDates.check_in, check_out: periodDefaultDates.check_out } : prev)); }, [periodDefaultDates.check_in, periodDefaultDates.check_out]);
  const [editId, setEditId] = useState(""); const selected = customRows.find(x => String(x.id) === String(editId)) || customRows[0]; const [edit, setEdit] = useState(null);
  useEffect(() => { if (selected) { setEditId(String(selected.id)); setEdit({ ...selected }); } }, [selected?.id]);
  const active = rows.filter(r => !isCancelled(r)); const revenue = active.reduce((s, r) => s + toNum(r.total_price), 0); const netOperating = active.reduce((s, r) => s + toNum(r.net_operating), 0); const netReal = active.reduce((s, r) => s + toNum(r.net_real), 0); const nightsSum = active.reduce((s, r) => s + toNum(r.nights), 0); const displayRows = dashView === "Storico" ? (allRows || rows) : rows;
  async function createCustom(e) {
    e.preventDefault();
    setCreateMsg("");
    setEditMsg("");
    try {
      const payload = { ...form, platform_fee: 0, transaction_cost: 0, raw_booking_status: form.status || "confirmed" };
      const res = await fetch(`${API_URL}/reservations/custom`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.message || "Errore salvataggio.");

      const returned = data.custom_booking || data.custom_reservation || data.booking || data.reservation || data.custom || data.item || data.data || null;
      const created = normalize({
        ...payload,
        ...(returned && typeof returned === "object" ? returned : {}),
        id: returned?.id || data.id || data.booking_id || data.reservation_id || data.custom_booking_id || data.custom_reservation_id || `local-${Date.now()}`,
        status: returned?.status || returned?.raw_booking_status || payload.status || "confirmed",
      }, "Custom");
      if (onCustomCreated) onCustomCreated(created);
      setEditId(String(created.id));
      setEdit(created);

      setCreateMsg("Prenotazione custom salvata correttamente.");
      setForm({ guest_name: "", guest_phone: "", check_in: todayIso(), check_out: tomorrowIso(), guests: 2, total_price: 0, cleaning_cost: 0, status: "confirmed", notes: "" });
      refresh();
    } catch (e) {
      setCreateMsg(e.message);
    }
  }
  async function updateCustom() { if (!edit?.id) return; setCreateMsg(""); setEditMsg(""); try { const res = await fetch(`${API_URL}/reservations/custom/${edit.id}`, { method: "PUT", headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` }, body: JSON.stringify({ ...edit, platform_fee: edit.platform_fee || 0, transaction_cost: edit.transaction_cost || 0, raw_booking_status: edit.raw_booking_status || edit.status || "confirmed" }) }); const data = await res.json(); if (!res.ok) throw new Error(data.error || data.message || "Errore aggiornamento."); setEditMsg("Prenotazione custom aggiornata."); refresh(); } catch (e) { setEditMsg(e.message); } }
  async function deleteCustom() { if (!edit?.id) return; setCreateMsg(""); setEditMsg(""); try { const res = await fetch(`${API_URL}/reservations/custom/${edit.id}`, { method: "DELETE", headers: { Authorization: `Bearer ${token}` } }); const data = await res.json(); if (!res.ok) throw new Error(data.error || data.message || "Errore eliminazione."); setEditMsg("Prenotazione custom eliminata."); setEdit(null); setEditId(""); refresh(); } catch (e) { setEditMsg(e.message); } }
  return <><p className="hf-period">Periodo selezionato: {periodLabel}</p><div className="hf-metrics"><Metric label="Prenotazioni" value={active.length} /><Metric label="Occupazione" value={`${Math.min(100, Math.round((nightsSum / 30) * 100)).toFixed(1)}%`} /><Metric label="Fatturato" value={euro(revenue)} /><Metric label="Netto operativo" value={euro(netOperating)} /><Metric label="Netto reale" value={euro(netReal)} /><Metric label="ADR medio" value={euro(nightsSum ? revenue / nightsSum : 0)} /></div><div className="hf-subtabs"><button type="button" className={dashView === "Dashboard" ? "active" : ""} onClick={() => setDashView("Dashboard")}>Dashboard</button><button type="button" className={dashView === "Storico" ? "active" : ""} onClick={() => setDashView("Storico")}>Storico</button></div><h2>{dashView === "Storico" ? "Storico prenotazioni" : `Prenotazioni elaborate · ${periodLabel}`}</h2><Expander title="Gestisci colonne"><div className="hf-columns">{TABLE_COLS.map(c => <label key={c} className="hf-check"><input type="checkbox" checked={visibleCols.includes(c)} onChange={e => setVisibleCols(e.target.checked ? [...visibleCols, c] : visibleCols.filter(x => x !== c))} />{c}</label>)}</div><div className="hf-actions"><button type="button" onClick={() => { localStorage.setItem(STORAGE.tableCols, JSON.stringify(visibleCols)); setLayoutMsg("Layout tabella salvato."); }}>Salva layout tabella</button><button type="button" onClick={() => { localStorage.removeItem(STORAGE.tableCols); setVisibleCols(TABLE_COLS); setLayoutMsg("Layout default ripristinato."); }}>Ripristina layout default</button></div>{layoutMsg && <div className="hf-success">{layoutMsg}</div>}</Expander><Expander title="Aggiungi prenotazione custom"><p className="hf-muted">Usa questa sezione per aggiungere prenotazioni non arrivate da Booking. Verranno integrate nella dashboard e nelle altre analisi.</p><form onSubmit={createCustom} className="hf-form-box"><div className="hf-grid-2"><div><label>Nome ospite</label><input value={form.guest_name} onChange={e => setForm({ ...form, guest_name: e.target.value })} required /><label>Telefono ospite</label><input value={form.guest_phone} onChange={e => setForm({ ...form, guest_phone: e.target.value })} /><label>Check-in</label><input type="date" value={form.check_in} onChange={e => setForm({ ...form, check_in: e.target.value })} /><label>Check-out</label><input type="date" value={form.check_out} onChange={e => setForm({ ...form, check_out: e.target.value })} /><label>Numero ospiti</label><input type="number" min="1" value={form.guests} onChange={e => setForm({ ...form, guests: Number(e.target.value) })} /></div><div><label>Prezzo totale netto (€)</label><input type="number" value={form.total_price} onChange={e => setForm({ ...form, total_price: Number(e.target.value) })} /><p className="hf-muted">Per le prenotazioni custom questo importo viene considerato già netto: non applichiamo commissioni piattaforma.</p><label>Pulizie (€)</label><input type="number" value={form.cleaning_cost} onChange={e => setForm({ ...form, cleaning_cost: Number(e.target.value) })} /><label>Stato prenotazione</label><select value={form.status} onChange={e => setForm({ ...form, status: e.target.value })}><option>confirmed</option><option>cancelled</option></select></div></div><label>Note</label><textarea value={form.notes} onChange={e => setForm({ ...form, notes: e.target.value })} /><button className="hf-primary-full">Salva prenotazione custom</button>{createMsg && <div className="hf-success hf-inline-success">{createMsg}</div>}</form><h3>Prenotazioni custom inserite</h3>{!customRows.length ? <p className="hf-muted">Non hai ancora inserito prenotazioni custom.</p> : <><label>Seleziona prenotazione custom da modificare</label><select value={editId} onChange={e => { setEditId(e.target.value); setEdit(customRows.find(x => String(x.id) === e.target.value)); }}>{customRows.map(r => <option key={r.id} value={r.id}>#{r.id} · {r.guest_name} · {r.check_in} → {r.check_out}</option>)}</select>{edit && <div className="hf-form-box"><div className="hf-grid-2"><div><label>Nome ospite</label><input value={edit.guest_name || ""} onChange={e => setEdit({ ...edit, guest_name: e.target.value })} /><label>Telefono ospite</label><input value={edit.guest_phone || ""} onChange={e => setEdit({ ...edit, guest_phone: e.target.value })} /><label>Check-in</label><input type="date" value={edit.check_in || ""} onChange={e => setEdit({ ...edit, check_in: e.target.value })} /><label>Check-out</label><input type="date" value={edit.check_out || ""} onChange={e => setEdit({ ...edit, check_out: e.target.value })} /><label>Numero ospiti</label><input type="number" value={edit.guests || 1} onChange={e => setEdit({ ...edit, guests: Number(e.target.value) })} /></div><div><label>Prezzo totale netto (€)</label><input type="number" value={edit.total_price || 0} onChange={e => setEdit({ ...edit, total_price: Number(e.target.value) })} /><label>Pulizie (€)</label><input type="number" value={edit.cleaning_cost || 0} onChange={e => setEdit({ ...edit, cleaning_cost: Number(e.target.value) })} /><label>Stato prenotazione</label><select value={edit.status || "confirmed"} onChange={e => setEdit({ ...edit, status: e.target.value })}><option>confirmed</option><option>cancelled</option></select></div></div><label>Note</label><textarea value={edit.notes || ""} onChange={e => setEdit({ ...edit, notes: e.target.value })} /><div className="hf-actions"><button type="button" onClick={updateCustom}>Aggiorna prenotazione custom</button><button type="button" onClick={deleteCustom}>Elimina prenotazione custom</button></div>{editMsg && <div className="hf-success hf-inline-success">{editMsg}</div>}</div>}</>}</Expander><div className="hf-table-wrap"><table><thead><tr><th></th>{visibleCols.map(c => <th key={c}>{c}</th>)}</tr></thead><tbody>{displayRows.length ? displayRows.map((r, i) => <tr key={`${r.platform}-${r.id}-${i}`}><td>{i}</td>{visibleCols.map(c => <td key={c}>{typeof r[c] === "number" ? r[c].toFixed(2) : String(r[c] ?? "")}</td>)}</tr>) : <tr><td colSpan={visibleCols.length + 1}>Nessuna prenotazione caricata.</td></tr>}</tbody></table></div></>;
}

function ImmobileTab({ property, setProperty, onboarding, setOnboarding }) {
  const [msg, setMsg] = useState(""); function patch(k, v) { setProperty(p => ({ ...p, [k]: v })); }
  function save() { writeJson(STORAGE.property, property); localStorage.removeItem("hostflow_needs_onboarding"); setOnboarding(false); setMsg("Dati immobile salvati. Ora verranno usati anche in Analisi mercato, Pricing e Messaggi."); }
  return <><h2>Scheda immobile</h2>{onboarding && <div className="hf-info">Prima configurazione: compila i dati dell’immobile. Servono per mercato, pricing e messaggi automatici.</div>}<div className="hf-form-box"><h3>Dati immobile</h3><div className="hf-grid-2"><div><label>Nome immobile<input value={property.nome_immobile} onChange={e => patch("nome_immobile", e.target.value)} /></label><label>Indirizzo completo<input value={property.indirizzo_completo} onChange={e => patch("indirizzo_completo", e.target.value)} /></label><label>Città<input value={property.citta} onChange={e => patch("citta", e.target.value)} /></label><label>CAP<input value={property.cap} onChange={e => patch("cap", e.target.value)} /></label><label>Tipologia immobile<select value={property.tipologia_immobile} onChange={e => patch("tipologia_immobile", e.target.value)}><option>Appartamento intero</option><option>Stanza privata</option><option>Casa vacanze</option></select></label></div><div><label>Ospiti massimi<input type="number" value={property.ospiti_massimi} onChange={e => patch("ospiti_massimi", e.target.value)} /></label><label>Camere<input type="number" value={property.camere} onChange={e => patch("camere", e.target.value)} /></label><label>Bagni<input type="number" value={property.bagni} onChange={e => patch("bagni", e.target.value)} /></label><label>Fascia qualità<select value={property.fascia_qualita} onChange={e => patch("fascia_qualita", e.target.value)}><option>Basic</option><option>Standard</option><option>Premium</option><option>Luxury</option></select></label><label>Raggio competitor (km)<input value={property.raggio_competitor} onChange={e => patch("raggio_competitor", e.target.value)} /></label></div></div><h3>Dati per messaggi automatici</h3><div className="hf-grid-2"><div><label>Nome host<input value={property.nome_host} onChange={e => patch("nome_host", e.target.value)} /></label><label>Numero WhatsApp<input value={property.numero_whatsapp} onChange={e => patch("numero_whatsapp", e.target.value)} /></label><label>Check-in dalle<input value={property.checkin_da} onChange={e => patch("checkin_da", e.target.value)} /></label><label>Check-in fino alle<input value={property.checkin_fino} onChange={e => patch("checkin_fino", e.target.value)} /></label><label>Check-out entro<input value={property.checkout_entro} onChange={e => patch("checkout_entro", e.target.value)} /></label><label>Nome Wi‑Fi<input value={property.wifi_nome} onChange={e => patch("wifi_nome", e.target.value)} /></label><label>Password Wi‑Fi<input value={property.wifi_password} onChange={e => patch("wifi_password", e.target.value)} /></label></div><div><label className="hf-check"><input type="checkbox" checked={property.animali_ammessi} onChange={e => patch("animali_ammessi", e.target.checked)} />Animali ammessi</label><label className="hf-check"><input type="checkbox" checked={property.fumatori_ammessi} onChange={e => patch("fumatori_ammessi", e.target.checked)} />Fumatori ammessi</label><label className="hf-check"><input type="checkbox" checked={property.parcheggio_disponibile} onChange={e => patch("parcheggio_disponibile", e.target.checked)} />Parcheggio disponibile</label><label>Tassa di soggiorno (€ per persona/notte)<input value={property.tassa_soggiorno} onChange={e => patch("tassa_soggiorno", e.target.value)} /></label></div></div><button type="button" className="hf-primary-full" onClick={save}>Salva dati immobile</button>{msg && <div className="hf-success">{msg}</div>}</div><h2>Riepilogo dati salvati</h2><div className="hf-table-wrap"><table><tbody>{Object.entries(property).map(([k, v]) => <tr key={k}><td>{k}</td><td>{String(v)}</td></tr>)}</tbody></table></div></>;
}
function MarketTab({ property }) { return <><h2>Analisi mercato</h2><div className="hf-form-box"><div className="hf-grid-2"><label>Check-in analisi<input type="date" /></label><label>Check-out analisi<input type="date" /></label><label>Raggio analisi (km)<input value={property.raggio_competitor} readOnly /></label><label>Ospiti analisi<input value={property.ospiti_massimi} readOnly /></label></div><label>Tipologia da confrontare<select value={property.tipologia_immobile} readOnly><option>Appartamento intero</option><option>Stanza privata</option><option>Casa vacanze</option></select></label><h2>Analisi mercato e pricing</h2><label>Indirizzo immobile<input value={property.indirizzo_completo} readOnly placeholder="Compila prima la scheda immobile" /></label><button type="button" className="hf-primary-full">Analizza mercato e pricing</button></div><div className="hf-metrics"><Metric label="Indirizzo" value={property.indirizzo_completo || "-"} /><Metric label="Città" value={property.citta || "-"} /><Metric label="Competitor trovati" value="0" /><Metric label="Prezzo suggerito" value="€ 0,00" /></div><h2>Competitor trovati</h2><div className="hf-info">Nessun competitor trovato nel raggio selezionato.</div></>; }
function PricingTab({ rows, settings }) { const active = rows.filter(r => !isCancelled(r)); const revenue = active.reduce((s, r) => s + toNum(r.total_price), 0); const n = active.reduce((s, r) => s + toNum(r.nights), 0); const adr = n ? revenue / n : 0; return <><h2>Pricing engine</h2><div className="hf-form-box"><h3>Base price</h3><label>Fonte base price<select><option>ADR periodo dashboard</option><option>Manuale</option></select></label><label>Prezzo manuale notte (€)<input defaultValue="100" /></label><label>Data da prezzare<input type="date" /></label><label className="hf-check"><input type="checkbox" />C'è un evento/ponte/fiera?</label><h3>Regole pricing</h3><div className="hf-grid-2">{["Weekend markup (%)", "Evento markup (%)", "Last minute sconto (%)", "Last minute entro giorni", "Early booking markup (%)", "Early booking oltre giorni", "Occupazione alta da (%)", "Markup occupazione alta (%)", "Occupazione bassa fino a (%)", "Sconto occupazione bassa (%)"].map(l => <label key={l}>{l}<input defaultValue="0" /></label>)}</div></div><h2>Risultato pricing</h2><div className="hf-metrics"><Metric label="Prezzo base usato" value={euro(adr)} /><Metric label="Fonte base price" value={settings.periodMode} /><Metric label="Prezzo suggerito" value={euro(adr)} /></div></>; }
function MessagesTab({ token, property, rows }) {
  const [showTemplates, setShowTemplates] = useState(false);
  const [templates, setTemplates] = useState(DEFAULT_TEMPLATES);
  const [schedules, setSchedules] = useState(DEFAULT_SCHEDULES);
  const [msg, setMsg] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [cancelledIds, setCancelledIds] = useState([]);
  const [sending, setSending] = useState(false);
  const [actionMsg, setActionMsg] = useState("");
  const [statusOverrides, setStatusOverrides] = useState({});
  const [chatLoading, setChatLoading] = useState(false);
  const [conversations, setConversations] = useState([]);
  const [selectedChatKey, setSelectedChatKey] = useState("");
  const [chatReply, setChatReply] = useState("");
  const [chatSending, setChatSending] = useState(false);
  const [chatMsg, setChatMsg] = useState("");
  const [chatSearch, setChatSearch] = useState("");
  const chatFeedRef = useRef(null);

  useEffect(() => {
    setTemplates(readJson(STORAGE.templates, DEFAULT_TEMPLATES));
    setSchedules(readJson(STORAGE.schedules, DEFAULT_SCHEDULES));
    try { setCancelledIds(JSON.parse(localStorage.getItem("hostflow_cancelled_message_ids_v1") || "[]")); } catch { setCancelledIds([]); }
    try { setStatusOverrides(JSON.parse(localStorage.getItem("hostflow_message_status_overrides_v1") || "{}")); } catch { setStatusOverrides({}); }
  }, []);

  useEffect(() => {
    if (!token) return;
    loadChat();
    const interval = setInterval(() => loadChat(), 3000);
    return () => clearInterval(interval);
  }, [token]);

  function updateTemplate(key, value) { setTemplates(prev => ({ ...prev, [key]: value })); }
  function saveTemplates() { writeJson(STORAGE.templates, templates); writeJson(STORAGE.schedules, schedules); setMsg("Template messaggi e orari salvati."); }
  function formatItalianDate(dateStr) { if (!dateStr) return "-"; const d = new Date(dateStr); if (Number.isNaN(d.getTime())) return String(dateStr); return d.toLocaleDateString("it-IT"); }
  function typeBaseDate(type, row) { if (type === "checkout_reminder" || type === "review_request") return row.check_out; return row.check_in; }
  function bookingRefFor(row) { return `${row.platform || "Booking"}|${row.guest_name || "Ospite"}|${fmtDate(row.check_in)}|${fmtDate(row.check_out)}`; }
  function saveStatusOverrides(next) { setStatusOverrides(next); try { localStorage.setItem("hostflow_message_status_overrides_v1", JSON.stringify(next)); } catch {} }

  function renderTemplate(template, row) {
    const ctx = {
      nome_ospite: row.guest_name || "Ospite",
      data_checkin: formatItalianDate(row.check_in),
      data_checkout: formatItalianDate(row.check_out),
      nome_struttura: property.nome_immobile || "-",
      nome_host: property.nome_host || "-",
      numero_whatsapp: property.numero_whatsapp || "-",
      checkin_da: property.checkin_da || "-",
      checkin_fino: property.checkin_fino || "-",
      checkout_entro: property.checkout_entro || "-",
      wifi_nome: property.wifi_nome || "-",
      wifi_password: property.wifi_password || "-",
      parcheggio_disponibile: property.parcheggio_disponibile ? "sì" : "no",
      tassa_soggiorno: property.tassa_soggiorno || "-",
      testo_parcheggio: property.parcheggio_disponibile ? "\n\nParcheggio disponibile in struttura." : "",
      testo_tassa_soggiorno: property.tassa_soggiorno ? `\n\nTassa di soggiorno: € ${property.tassa_soggiorno} per persona/notte.` : "",
    };
    return String(template || "").replace(/\{([^}]+)\}/g, (_, key) => ctx[key] ?? `{${key}}`);
  }

  const scheduledMessages = useMemo(() => {
    const today = new Date().toISOString().slice(0, 10);
    const activeRows = rows.filter((r) => {
      if (isCancelled(r)) return false;
      if (!String(r.guest_phone || "").trim()) return false;
      const checkout = fmtDate(r.check_out);
      return !checkout || checkout >= today;
    });

    const list = [];
    activeRows.forEach((row) => {
      Object.entries(DEFAULT_SCHEDULES).forEach(([type, defaultRule]) => {
        const rule = schedules[type] || defaultRule;
        const baseDate = typeBaseDate(type, row);
        const sendDate = addDays(baseDate, rule.offsetDays || 0);
        if (sendDate < today) return;
        const sendTime = rule.time || "10:00";
        const id = `${row.platform || "row"}-${row.id || row.guest_name || row.check_in}-${type}`;
        const cancelled = cancelledIds.includes(id);
        const override = statusOverrides[id] || {};

        list.push({
          id,
          row,
          type,
          label: rule.label || defaultRule.label,
          sendDate,
          sendTime,
          sendAt: `${sendDate} ${sendTime}`,
          status: cancelled ? "cancelled" : override.status || "pending",
          error_message: override.error_message || "",
          whatsapp_message_id: override.whatsapp_message_id || "",
          text: renderTemplate(templates[type], row),
          booking_ref: bookingRefFor(row),
        });
      });
    });

    return list.sort((a, b) => String(a.sendAt).localeCompare(String(b.sendAt)));
  }, [rows, schedules, templates, property, cancelledIds, statusOverrides]);

  useEffect(() => {
    if (!scheduledMessages.length) {
      setSelectedId("");
      return;
    }
    if (!selectedId || !scheduledMessages.some(m => m.id === selectedId)) setSelectedId(scheduledMessages[0].id);
  }, [scheduledMessages, selectedId]);

  const selected = scheduledMessages.find(m => m.id === selectedId) || scheduledMessages[0];
  const pendingCount = scheduledMessages.filter(m => m.status === "pending").length;
  const sentCount = scheduledMessages.filter(m => m.status === "sent").length;
  const failedCount = scheduledMessages.filter(m => m.status === "failed").length;
  const cancelledCount = scheduledMessages.filter(m => m.status === "cancelled").length;



  function normalizePhoneForMatch(value) {
    return String(value || "").replace(/\D/g, "");
  }

  const reservationPhoneSet = useMemo(() => {
    const phones = rows
      .map(r => normalizePhoneForMatch(r.guest_phone))
      .filter(Boolean);

    return new Set(phones);
  }, [rows]);

  function conversationKey(c, index = 0) {
    return [
      String(c?.guest_phone || "").trim(),
      String(c?.booking_ref || "").trim(),
      String(c?.guest_name || "").trim(),
      String(c?.last_at || "").trim(),
      String(index),
    ].join("|");
  }

  async function loadChat() {
    if (!token) return;
    setChatLoading(true);
    try {
      const res = await fetch(`${API_URL}/messages/chat`, { headers: { Authorization: `Bearer ${token}` } });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.message || "Errore caricamento chat.");
      const convs = Array.isArray(data.conversations) ? data.conversations : [];
      setConversations(convs);
    } catch (e) {
      setActionMsg(e.message || "Errore caricamento chat.");
    } finally {
      setChatLoading(false);
    }
  }

  async function sendSelected() {
    if (!selected || sending) return;
    setSending(true);
    setActionMsg("");
    try {
      const res = await fetch(`${API_URL}/messages/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          guest_phone: selected.row.guest_phone,
          guest_name: selected.row.guest_name,
          booking_ref: selected.booking_ref,
          message_type: selected.type,
          message_text: selected.text,
          scheduled_message_id: selected.id,
          send_at: `${selected.sendDate}T${selected.sendTime}:00`,
        }),
      });
      const data = await res.json();
      const status = data.message_status || (res.ok && data.status === "ok" ? "sent" : "failed");
      const next = { ...statusOverrides, [selected.id]: { status, error_message: data.error_message || data.error || data.message || "", whatsapp_message_id: data.whatsapp_message_id || "" } };
      saveStatusOverrides(next);
      setActionMsg(status === "sent" ? "Messaggio WhatsApp inviato correttamente." : `Invio fallito: ${data.error || data.message || data.error_message || "controlla token/numero Meta."}`);
      loadChat();
    } catch (e) {
      const next = { ...statusOverrides, [selected.id]: { status: "failed", error_message: e.message || "Errore invio WhatsApp." } };
      saveStatusOverrides(next);
      setActionMsg(e.message || "Errore invio WhatsApp.");
    } finally {
      setSending(false);
    }
  }

  function cancelSelected() {
    if (!selected) return;
    const nextCancelled = Array.from(new Set([...cancelledIds, selected.id]));
    setCancelledIds(nextCancelled);
    localStorage.setItem("hostflow_cancelled_message_ids_v1", JSON.stringify(nextCancelled));
    const nextStatus = { ...statusOverrides, [selected.id]: { ...(statusOverrides[selected.id] || {}), status: "cancelled" } };
    saveStatusOverrides(nextStatus);
  }

  function restoreSelected() {
    if (!selected) return;
    const nextCancelled = cancelledIds.filter(id => id !== selected.id);
    setCancelledIds(nextCancelled);
    localStorage.setItem("hostflow_cancelled_message_ids_v1", JSON.stringify(nextCancelled));
    const nextStatus = { ...statusOverrides, [selected.id]: { ...(statusOverrides[selected.id] || {}), status: "pending", error_message: "" } };
    saveStatusOverrides(nextStatus);
  }

  function visibleChatMessages(conversation) {
    const list = Array.isArray(conversation?.messages) ? conversation.messages : [];
    return list.filter((m) => {
      const direction = String(m.direction || "").toLowerCase();
      const status = String(m.status || "").toLowerCase();
      if (direction === "outbound" && status === "failed") return false;
      return true;
    });
  }

  async function sendChatReply() {
    const text = String(chatReply || "").trim();
    const phone = selectedConversation?.guest_phone;
    if (!token || !selectedConversation || !phone || !text || chatSending) return;
    setChatSending(true);
    setChatMsg("");
    setActionMsg("");
    try {
      const res = await fetch(`${API_URL}/messages/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          guest_phone: phone,
          guest_name: selectedConversation.guest_name || "Ospite",
          booking_ref: selectedConversation.booking_ref || `chat|${phone}`,
          message_type: "chat_reply",
          message_text: text,
          scheduled_message_id: `chat-reply-${Date.now()}`,
          send_at: new Date().toISOString(),
        }),
      });
      const data = await res.json();
      if (!res.ok || data.status !== "ok") throw new Error(data.error || data.message || data.error_message || "Errore invio risposta WhatsApp.");
      setChatReply("");
      setChatMsg("Risposta WhatsApp inviata correttamente.");
      await loadChat();
    } catch (e) {
      setChatMsg(e.message || "Errore invio risposta WhatsApp.");
    } finally {
      setChatSending(false);
    }
  }

  const visibleConversations = useMemo(() => {
    return conversations
      .map((c, index) => ({ ...c, __chatKey: conversationKey(c, index) }))
      .filter((c) => {
        const phone = normalizePhoneForMatch(c.guest_phone);
        return phone && reservationPhoneSet.has(phone);
      });
  }, [conversations, reservationPhoneSet]);

  const selectedConversation =
    visibleConversations.find(c => c.__chatKey === selectedChatKey) ||
    visibleConversations[0];

  const selectedConversationMessages = visibleChatMessages(selectedConversation);

  const filteredConversations = visibleConversations.filter((c) => {
    const q = String(chatSearch || "").trim().toLowerCase();
    if (!q) return true;
    return String(c.guest_name || "").toLowerCase().includes(q) || String(c.guest_phone || "").toLowerCase().includes(q);
  });

  useEffect(() => {
    if (!visibleConversations.length) {
      setSelectedChatKey("");
      return;
    }

    if (!selectedChatKey || !visibleConversations.some(c => c.__chatKey === selectedChatKey)) {
      setSelectedChatKey(visibleConversations[0].__chatKey);
    }
  }, [visibleConversations, selectedChatKey]);

  useEffect(() => {
    if (!chatFeedRef.current) return;
    chatFeedRef.current.scrollTop = chatFeedRef.current.scrollHeight;
  }, [selectedChatKey, selectedConversationMessages.length]);

  return <>
    <h2>Messaggi automatici</h2>
    <p className="hf-muted">I messaggi vengono generati dalle prenotazioni del periodo selezionato e dai dati salvati nella Scheda immobile.</p>

    <label className="hf-check hf-template-toggle">
      <input type="checkbox" checked={showTemplates} onChange={e => setShowTemplates(e.target.checked)} />
      Mostra template
    </label>

    {showTemplates && <div className="hf-form-box">
      <h3>Template messaggi</h3>
      {Object.entries(DEFAULT_SCHEDULES).map(([key, rule]) => <div key={key}>
        <label>{rule.label}</label>
        <textarea value={templates[key] || ""} onChange={e => updateTemplate(key, e.target.value)} />
        <div className="hf-grid-2">
          <label>Offset giorni<input type="number" value={schedules[key]?.offsetDays ?? 0} onChange={e => setSchedules(prev => ({ ...prev, [key]: { ...prev[key], offsetDays: Number(e.target.value) } }))} /></label>
          <label>Ora invio<input value={schedules[key]?.time || ""} onChange={e => setSchedules(prev => ({ ...prev, [key]: { ...prev[key], time: e.target.value } }))} /></label>
        </div>
      </div>)}
      <button type="button" className="hf-primary-full" onClick={saveTemplates}>Salva template messaggi</button>
      {msg && <div className="hf-success">{msg}</div>}
    </div>}

    <h2>Messaggi programmati</h2>
    <p className="hf-muted">I messaggi programmati vengono aggiornati automaticamente dalle prenotazioni e dai template salvati.</p>

    <div className="hf-metrics">
      <Metric label="Da inviare" value={pendingCount} />
      <Metric label="Inviati" value={sentCount} />
      <Metric label="Falliti" value={failedCount} />
      <Metric label="Annullati" value={cancelledCount} />
    </div>

    <div className="hf-messages-layout">
      <div>
        <h2>Prossimi messaggi</h2>
        <p className="hf-muted">Lista essenziale, ordinata per soggiorno e data di invio.</p>
        {scheduledMessages.length ? <div className="hf-message-list">{scheduledMessages.map((m, index) => (
          <button type="button" key={m.id} className={`hf-message-item status-${m.status} ${selected?.id === m.id ? "active" : ""} ${m.status === "cancelled" ? "cancelled" : ""}`} onClick={() => setSelectedId(m.id)}>
            <span className="hf-message-dot">{selected?.id === m.id ? "●" : "○"}</span>
            <span>#{index + 1} · {m.row.guest_name || "Ospite"} · {m.row.guest_phone} · {m.label} · {formatItalianDate(m.sendDate)}</span>
            <small>{m.sendTime}</small>
          </button>
        ))}</div> : <div className="hf-info">Nessun messaggio programmato. Verranno mostrati solo ospiti con telefono e prenotazioni correnti o future.</div>}
      </div>

      <div>
        <h2>Dettaglio messaggio</h2>
        {selected ? <>
          <div className={`hf-message-card ${selected.status === "cancelled" ? "cancelled" : ""}`}>
            <div className={`hf-status-pill status-${selected.status}`}>{selected.status === "cancelled" ? "Annullato" : selected.status === "sent" ? "Inviato" : selected.status === "failed" ? "Fallito" : "In attesa di invio"}</div>
            <h3>{selected.row.guest_name || "Ospite"}</h3>
            <p>{selected.label} · whatsapp · Invio previsto: {formatItalianDate(selected.sendDate)} {selected.sendTime}</p>
          </div>
          <div className="hf-grid-2">
            <label>Telefono<input value={selected.row.guest_phone || ""} readOnly /></label>
            <label>Piattaforma<input value={selected.row.platform || ""} readOnly /></label>
          </div>
          <label>Anteprima messaggio</label>
          <textarea value={selected.text} readOnly className="hf-message-preview" />
          {selected.error_message && <div className="hf-info">Errore ultimo invio: {selected.error_message}</div>}
          <div className="hf-info">WhatsApp Cloud API configurata: puoi inviare subito questo messaggio dal numero business automatico.</div>
          <div className="hf-actions">
            <button type="button" disabled={sending} onClick={sendSelected}>{sending ? "Invio..." : "Invia WhatsApp ora"}</button>
            {selected.status === "cancelled" ? <button type="button" onClick={restoreSelected}>Ripristina</button> : <button type="button" onClick={cancelSelected}>Annulla</button>}
          </div>
          {actionMsg && <div className={actionMsg.toLowerCase().includes("fall") || actionMsg.toLowerCase().includes("erro") ? "hf-info" : "hf-success"}>{actionMsg}</div>}
        </> : <div className="hf-info">Seleziona un messaggio dalla lista.</div>}
      </div>
    </div>

    <h2>Inbox WhatsApp</h2>
    <p className="hf-muted">Seleziona un cliente a sinistra e gestisci la conversazione a destra.</p>

    <div className="hf-wa-shell">
      <aside className="hf-wa-left">
        <div className="hf-wa-left-head">
          <div>
            <h2>Chat</h2>
            <span>{chatLoading ? "Aggiornamento..." : "Online"}</span>
          </div>
          <span className="hf-wa-menu">⋮</span>
        </div>

        <div className="hf-wa-search">
          <span>⌕</span>
          <input value={chatSearch} onChange={e => setChatSearch(e.target.value)} placeholder="Cerca cliente" />
        </div>

        <div className="hf-wa-list">
          {filteredConversations.length ? filteredConversations.map((c) => {
            const messages = visibleChatMessages(c);
            const last = messages[messages.length - 1] || {};
            const lastText = last.message_text || c.last_message || "";
            const lastAt = last.created_at || c.last_at || "";
            const active = selectedChatKey === c.__chatKey;

            return (
              <button type="button" key={c.__chatKey} className={`hf-wa-client ${active ? "active" : ""}`} onClick={() => setSelectedChatKey(c.__chatKey)}>
                <div className="hf-wa-avatar">{String(c.guest_name || "O").slice(0, 1).toUpperCase()}</div>
                <div className="hf-wa-client-body">
                  <div className="hf-wa-client-top">
                    <strong>{c.guest_name || "Ospite"}</strong>
                    <small>{lastAt ? new Date(lastAt).toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit" }) : ""}</small>
                  </div>
                  <div className="hf-wa-client-last">
                    {String(last.direction || "").toLowerCase() === "outbound" ? "✓✓ " : ""}
                    {lastText || "Nessun messaggio"}
                  </div>
                </div>
              </button>
            );
          }) : <div className="hf-wa-empty">Nessuna conversazione collegata alle prenotazioni visibili in Dashboard.</div>}
        </div>
      </aside>

      <section className="hf-wa-chatbox">
        {selectedConversation ? <>
          <header className="hf-wa-chat-head">
            <div className="hf-wa-avatar">{String(selectedConversation.guest_name || "O").slice(0, 1).toUpperCase()}</div>
            <div className="hf-wa-title">
              <strong>{selectedConversation.guest_name || "Ospite"}</strong>
              <span>{selectedConversation.guest_phone}</span>
            </div>
            <span className="hf-wa-menu">⋮</span>
          </header>

          <div className="hf-wa-feed" ref={chatFeedRef}>
            {selectedConversationMessages.length ? selectedConversationMessages.map((m) => {
              const inbound = String(m.direction || "").toLowerCase() !== "outbound";
              return (
                <div key={m.id || `${m.created_at}-${m.message_text}`} className={`hf-wa-bubble ${inbound ? "inbound" : "outbound"}`}>
                  <div className="hf-wa-text">{m.message_text}</div>
                  <div className="hf-wa-time">
                    {m.created_at ? new Date(m.created_at).toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit" }) : ""}
                    {!inbound && <span> ✓✓</span>}
                  </div>
                </div>
              );
            }) : <div className="hf-wa-empty-chat">Nessun messaggio in questa conversazione.</div>}
          </div>

          <footer className="hf-wa-compose">
            <button type="button" className="hf-wa-plus">＋</button>
            <textarea
              value={chatReply}
              onChange={e => setChatReply(e.target.value)}
              placeholder="Scrivi un messaggio"
              onKeyDown={e => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  sendChatReply();
                }
              }}
            />
            <button type="button" className="hf-wa-send" onClick={sendChatReply} disabled={chatSending || !chatReply.trim()}>
              {chatSending ? "…" : "➤"}
            </button>
          </footer>
          {chatMsg && <div className={chatMsg.toLowerCase().includes("erro") || chatMsg.toLowerCase().includes("fall") ? "hf-info" : "hf-success"}>{chatMsg}</div>}
        </> : <div className="hf-wa-empty-chat">Seleziona una conversazione.</div>}
      </section>
    </div>
  </>;
}
function PulizieTab({ settings, rows, services, setServices }) {
  const today = new Date().toISOString().slice(0, 10);
  const firstBooking = rows.find(r => !isCancelled(r)) || rows[0] || null;
  const [selectedBookingKey, setSelectedBookingKey] = useState("");
  const [selectedServiceId, setSelectedServiceId] = useState("");
  const [saveMsg, setSaveMsg] = useState("");
  const [form, setForm] = useState({
    date: today,
    service_type: "Pulizia check-out",
    cleaner: "",
    start_time: "11:00",
    end_time: "12:30",
    hourly_rate: "0",
    extra_costs: "0",
    use_custom_total: false,
    custom_total: "0",
    payment_status: "Da pagare",
    notes: "",
  });


  useEffect(() => {
    if (!selectedBookingKey && firstBooking) setSelectedBookingKey(bookingKey(firstBooking));
  }, [firstBooking, selectedBookingKey]);

  function bookingKey(row) {
    return `${row.platform || "row"}-${row.id || row.guest_name || row.check_in}-${row.check_in}-${row.check_out}`;
  }

  function selectedBooking() {
    return rows.find(r => bookingKey(r) === selectedBookingKey) || firstBooking;
  }

  function saveServices(next) {
    setServices(next);
    try { localStorage.setItem(STORAGE.cleaningServices, JSON.stringify(next)); } catch {}
  }

  function updateForm(next) {
    setForm(prev => ({ ...prev, ...next }));
    setSaveMsg("");
  }

  function hoursBetween(start, end) {
    const [sh, sm] = String(start || "00:00").split(":").map(Number);
    const [eh, em] = String(end || "00:00").split(":").map(Number);
    const startMin = (sh || 0) * 60 + (sm || 0);
    const endMin = (eh || 0) * 60 + (em || 0);
    return Math.max(0, (endMin - startMin) / 60);
  }

  const booking = selectedBooking();
  const calculatedHours = hoursBetween(form.start_time, form.end_time);
  const calculatedTotal = form.use_custom_total ? toNum(form.custom_total) : calculatedHours * toNum(form.hourly_rate) + toNum(form.extra_costs);
  const totalServices = services.reduce((s, x) => s + toNum(x.total), 0);
  const totalHours = services.reduce((s, x) => s + toNum(x.hours), 0);
  const averageCleaning = services.length ? totalServices / services.length : 0;
  const selectedService = services.find(x => String(x.id) === String(selectedServiceId)) || services[0];
  const monthlySummary = services.reduce((acc, item) => {
    const month = String(item.date || "").slice(0, 7) || "Senza mese";
    if (!acc[month]) acc[month] = { month, count: 0, hours: 0, total: 0 };
    acc[month].count += 1;
    acc[month].hours += toNum(item.hours);
    acc[month].total += toNum(item.total);
    return acc;
  }, {});

  function submitCleaning(e) {
    e.preventDefault();
    const b = selectedBooking();
    const item = {
      id: Date.now(),
      booking_key: b ? bookingKey(b) : "",
      booking_check_in: b?.check_in || "",
      booking_check_out: b?.check_out || "",
      date: form.date,
      guest_name: b?.guest_name || "",
      booking_label: b ? `${b.guest_name || "Ospite"} · check-out ${formatDateIt(b.check_out)} · check-in ${formatDateIt(b.check_in)}` : "Manuale",
      service_type: form.service_type,
      cleaner: form.cleaner,
      start_time: form.start_time,
      end_time: form.end_time,
      hours: calculatedHours,
      hourly_rate: toNum(form.hourly_rate),
      extra_costs: toNum(form.extra_costs),
      total: calculatedTotal,
      payment_status: form.payment_status,
      notes: form.notes,
    };
    const next = [item, ...services];
    saveServices(next);
    setSelectedServiceId(String(item.id));
    setSaveMsg("Servizio pulizia salvato correttamente.");
  }

  function deleteAllCleaning() {
    saveServices([]);
    setSelectedServiceId("");
    setSaveMsg("Registro pulizie azzerato.");
  }

  function formatDateIt(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleDateString("it-IT");
  }

  const bookingOptions = rows.filter(r => !isCancelled(r));

  return <>
    <h2>Servizi di pulizia</h2>
    <div className="hf-metrics">
      <Metric label="Ore totali periodo" value={totalHours.toFixed(2)} />
      <Metric label="Costo totale periodo" value={euro(totalServices)} />
      <Metric label="Pulizie registrate" value={services.length} />
      <Metric label="Costo medio pulizia" value={euro(averageCleaning)} />
    </div>

    <div className="hf-table-wrap">
      <table>
        <thead><tr><th>ID</th><th>Data</th><th>Cliente</th><th>Tipo servizio</th><th>Donna pulizie</th><th>Ora inizio</th><th>Ora fine</th><th>Ore</th><th>Tariffa oraria</th><th>Extra</th><th>Totale</th><th>Stato pagamento</th><th>Note</th></tr></thead>
        <tbody>{services.length ? services.map(item => <tr key={item.id}><td>{item.id}</td><td>{item.date}</td><td>{item.guest_name}</td><td>{item.service_type}</td><td>{item.cleaner}</td><td>{item.start_time}</td><td>{item.end_time}</td><td>{toNum(item.hours).toFixed(2)}</td><td>{toNum(item.hourly_rate).toFixed(2)}</td><td>{toNum(item.extra_costs).toFixed(2)}</td><td>{toNum(item.total).toFixed(2)}</td><td>{item.payment_status}</td><td>{item.notes}</td></tr>) : <tr><td colSpan="13">Nessun servizio pulizia.</td></tr>}</tbody>
      </table>
    </div>

    <h2>Inserimento servizio pulizia</h2>
    <form onSubmit={submitCleaning} className="hf-form-box">
      <label>Prenotazione associata</label>
      <select value={selectedBookingKey} onChange={e => setSelectedBookingKey(e.target.value)}>
        {bookingOptions.length ? bookingOptions.map(r => <option key={bookingKey(r)} value={bookingKey(r)}>{r.guest_name || "Ospite"} · check-out {formatDateIt(r.check_out)} · check-in {formatDateIt(r.check_in)}</option>) : <option value="">Nessuna prenotazione disponibile</option>}
      </select>
      <div className="hf-grid-2">
        <div>
          <label>Data pulizia</label><input type="date" value={form.date} onChange={e => updateForm({ date: e.target.value })} />
          <label>Tipo servizio</label><select value={form.service_type} onChange={e => updateForm({ service_type: e.target.value })}><option>Pulizia check-out</option><option>Pulizia intermedia</option><option>Cambio biancheria</option><option>Extra</option></select>
          <label>Donna delle pulizie</label><input value={form.cleaner} onChange={e => updateForm({ cleaner: e.target.value })} />
          <label>Ora inizio</label><input value={form.start_time} onChange={e => updateForm({ start_time: e.target.value })} placeholder="11:00" />
          <label>Ora fine</label><input value={form.end_time} onChange={e => updateForm({ end_time: e.target.value })} placeholder="12:30" />
        </div>
        <div>
          <label>Tariffa oraria (€)</label><input type="number" step="0.01" value={form.hourly_rate} onChange={e => updateForm({ hourly_rate: e.target.value })} />
          <label>Extra / materiali (€)</label><input type="number" step="0.01" value={form.extra_costs} onChange={e => updateForm({ extra_costs: e.target.value })} />
          <label className="hf-check"><input type="checkbox" checked={form.use_custom_total} onChange={e => updateForm({ use_custom_total: e.target.checked })} />Totale custom</label>
          <label>Totale personalizzato (€)</label><input type="number" step="0.01" value={form.custom_total} disabled={!form.use_custom_total} onChange={e => updateForm({ custom_total: e.target.value })} />
          <label>Stato pagamento</label><select value={form.payment_status} onChange={e => updateForm({ payment_status: e.target.value })}><option>Da pagare</option><option>Pagato</option><option>Non dovuto</option></select>
        </div>
      </div>
      <p className="hf-muted">Cliente associato: {booking?.guest_name || "-"} · Ore calcolate: {calculatedHours.toFixed(2)} · Totale calcolato: {euro(calculatedTotal)}</p>
      <label>Note</label><textarea value={form.notes} onChange={e => updateForm({ notes: e.target.value })} />
      <button type="submit" className="hf-primary-full">Salva servizio pulizia</button>
      {saveMsg && <div className="hf-success hf-inline-success">{saveMsg}</div>}
    </form>

    <h2>Modifica pulizie</h2>
    <div className="hf-grid-2">
      <div>
        <label>Seleziona servizio da modificare</label>
        <select value={selectedServiceId} onChange={e => setSelectedServiceId(e.target.value)}>
          {services.length ? services.map(item => <option key={item.id} value={item.id}>#{item.id} · {item.guest_name || "Manuale"} · {formatDateIt(item.date)}</option>) : <option value="">Nessun servizio</option>}
        </select>
      </div>
      <div>
        <button type="button" className="hf-primary-full" onClick={deleteAllCleaning}>Azzera tutto il registro pulizie</button>
      </div>
    </div>

    <Expander title="Modifica servizio pulizia selezionato">
      {selectedService ? <div className="hf-table-wrap"><table><tbody>{Object.entries(selectedService).map(([k, v]) => <tr key={k}><td>{k}</td><td>{String(v)}</td></tr>)}</tbody></table></div> : <p className="hf-muted">Nessun servizio selezionato.</p>}
    </Expander>

    <Expander title="Riepilogo mensile pulizie">
      <div className="hf-table-wrap"><table><thead><tr><th>Mese</th><th>Servizi</th><th>Ore</th><th>Totale</th></tr></thead><tbody>{Object.values(monthlySummary).length ? Object.values(monthlySummary).map(row => <tr key={row.month}><td>{row.month}</td><td>{row.count}</td><td>{row.hours.toFixed(2)}</td><td>{euro(row.total)}</td></tr>) : <tr><td colSpan="4">Nessun dato mensile.</td></tr>}</tbody></table></div>
    </Expander>
  </>;
}
function DatiTab({ rows }) { function downloadCsv() { const cols = TABLE_COLS; const csv = [cols.join(","), ...rows.map(r => cols.map(c => JSON.stringify(r[c] ?? "")).join(","))].join("\n"); const blob = new Blob([csv], { type: "text/csv;charset=utf-8" }); const url = URL.createObjectURL(blob); const a = document.createElement("a"); a.href = url; a.download = "hostflow_export.csv"; a.click(); URL.revokeObjectURL(url); } return <><h2>Scarica CSV elaborato</h2><button type="button" className="hf-primary-full" onClick={downloadCsv}>Scarica CSV</button><h2>Colonne calcolate</h2><div className="hf-table-wrap"><table><tbody>{TABLE_COLS.map(c => <tr key={c}><td>{c}</td></tr>)}</tbody></table></div></>; }

function DashboardPageContent() {
  const router = useRouter(); const searchParams = useSearchParams(); const [token, setToken] = useState(""); const [user, setUser] = useState(null); const [tab, setTab] = useState("Dashboard"); const [refreshKey, setRefreshKey] = useState(0); const [sidebarWidth, setSidebarWidth] = useState(370); const [baseRows, setBaseRows] = useState([]); const [customRows, setCustomRows] = useState([]); const [optimisticCustomRows, setOptimisticCustomRows] = useState([]); const [settings, setSettings] = useState(DEFAULT_SETTINGS); const [property, setProperty] = useState(DEFAULT_PROPERTY); const [cleaningServices, setCleaningServices] = useState([]); const [sidebarCollapsed, setSidebarCollapsed] = useState(false); const [onboarding, setOnboarding] = useState(false);
  useEffect(() => { const t = localStorage.getItem("hostflow_token"); const u = localStorage.getItem("hostflow_user"); if (!t) { router.push("/"); return; } setToken(t); if (u) setUser(JSON.parse(u)); setSettings({
      ...readJson(STORAGE.settings, DEFAULT_SETTINGS),
      periodMode: "Mensile",
      year: new Date().getFullYear(),
      month: new Date().getMonth() + 1,
    }); setProperty(readJson(STORAGE.property, DEFAULT_PROPERTY)); setCleaningServices(readArray(STORAGE.cleaningServices)); setSidebarCollapsed(localStorage.getItem(STORAGE.sidebar) === "1"); const needs = localStorage.getItem("hostflow_needs_onboarding") === "1" || searchParams.get("onboarding") === "1"; setOnboarding(needs); if (needs) setTab("Immobile"); }, [router, searchParams]);
  useEffect(() => { if (token) writeJson(STORAGE.settings, settings); }, [settings, token]);
  useEffect(() => { localStorage.setItem(STORAGE.sidebar, sidebarCollapsed ? "1" : "0"); }, [sidebarCollapsed]);
  useEffect(() => {
    if (!token) return;
    Promise.all([
      fetch(`${API_URL}/reservations`, { headers: { Authorization: `Bearer ${token}` } }).then(r => r.json()).catch(() => ({})),
      fetch(`${API_URL}/reservations/custom`, { headers: { Authorization: `Bearer ${token}` } }).then(r => r.json()).catch(() => ({})),
    ]).then(([a, b]) => {
      setBaseRows(extractReservationArray(a).map(r => normalize(r)));
      setCustomRows(extractReservationArray(b).map(r => normalize(r, "Custom")));
    });
  }, [token, refreshKey]);
  function logout() { localStorage.removeItem("hostflow_token"); localStorage.removeItem("hostflow_user"); router.push("/"); }
  const mergedCustomRows = useMemo(() => mergeCustomRows(customRows, optimisticCustomRows), [customRows, optimisticCustomRows]); const rawRows = useMemo(() => [...baseRows, ...mergedCustomRows], [baseRows, mergedCustomRows]); const enrichedRows = useMemo(() => rawRows.map(r => enrichRow(r, settings, rawRows, cleaningServices)), [rawRows, settings, cleaningServices]); const filteredRows = useMemo(() => enrichedRows.filter(r => rowInPeriod(r, settings)), [enrichedRows, settings]); const [, , periodLabel] = periodRange(settings);
  if (!token) return <main className="hf-loading">Caricamento...</main>;
  return <main className={`hf-app ${sidebarCollapsed ? "sidebar-collapsed" : ""}`}><Sidebar token={token} settings={settings} setSettings={setSettings} collapsed={sidebarCollapsed} setCollapsed={setSidebarCollapsed} sidebarWidth={sidebarWidth} setSidebarWidth={setSidebarWidth} onLogout={logout} onUploaded={() => setRefreshKey(x => x + 1)} /><section className="hf-main"><header><div><h1>HostFlow v5</h1><p>Dashboard host con import Booking, netto reale, immobile, analisi mercato e pricing regolabile</p></div></header><nav className="hf-tabs">{MAIN_TABS.map(t => <button type="button" key={t} className={tab === t ? "active" : ""} onClick={() => setTab(t)}>{t}</button>)}</nav>{tab === "Dashboard" && <DashboardTab token={token} rows={filteredRows} allRows={enrichedRows} customRows={mergedCustomRows} refresh={() => setRefreshKey(x => x + 1)} settings={settings} periodLabel={periodLabel} onCustomCreated={(row) => setOptimisticCustomRows(prev => mergeCustomRows(prev, [row]))} />}{tab === "Immobile" && <ImmobileTab property={property} setProperty={setProperty} onboarding={onboarding} setOnboarding={setOnboarding} />}{tab === "Analisi mercato" && <MarketTab property={property} />}{tab === "Pricing" && <PricingTab rows={filteredRows} settings={settings} />}{tab === "Messaggi" && <MessagesTab token={token} property={property} rows={filteredRows} />}{tab === "Pulizie" && <PulizieTab settings={settings} rows={filteredRows} services={cleaningServices} setServices={setCleaningServices} />}{tab === "Dati" && <DatiTab rows={filteredRows} />}</section></main>;
}

export default function DashboardPage() {
  return (
    <Suspense fallback={<main className="hf-loading">Caricamento...</main>}>
      <DashboardPageContent />
    </Suspense>
  );
}
