"use client";

import { useState } from "react";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "https://hostflow-backend.onrender.com";

export default function Home() {
  const [mode, setMode] = useState("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [password2, setPassword2] = useState("");
  const [resetEmail, setResetEmail] = useState("");
  const [resetCode, setResetCode] = useState("");
  const [resetPassword, setResetPassword] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);

  async function submitAuth(e) {
    e.preventDefault();
    setMessage("");

    if (mode === "reset") {
      setMessage("Recupero password da collegare al prossimo endpoint backend. Usa per ora login o registrazione.");
      return;
    }

    if (mode === "register" && password !== password2) {
      setMessage("Le password non coincidono.");
      return;
    }

    setLoading(true);
    try {
      const endpoint = mode === "login" ? "/auth/login" : "/auth/register";
      const res = await fetch(`${API_URL}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();

      if (!res.ok) {
        setMessage(data.error || data.detail || "Errore durante l'operazione.");
        return;
      }

      if (mode === "login") {
        localStorage.setItem("hostflow_user", JSON.stringify(data.user || { email }));
        localStorage.setItem("hostflow_token", data.access_token || "");
        window.location.href = "/dashboard";
      } else {
        localStorage.setItem("hostflow_user", JSON.stringify(data.user || { email }));
        localStorage.setItem("hostflow_token", data.access_token || "");
        localStorage.setItem("hostflow_needs_onboarding", "1");
        window.location.href = "/dashboard?onboarding=1";
      }
    } catch {
      setMessage("Backend non raggiungibile.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="hf-auth-page">
      <section className="hf-auth-hero">
        <h1>Accedi a HostFlow</h1>
        <p>Entra o crea un account per salvare il profilo immobile e ritrovare i dati già compilati.</p>
      </section>

      <section className="hf-auth-card">
        <div className="hf-auth-tabs">
          <button type="button" onClick={() => { setMode("login"); setMessage(""); }} className={mode === "login" ? "active" : ""}>Accedi</button>
          <button type="button" onClick={() => { setMode("register"); setMessage(""); }} className={mode === "register" ? "active" : ""}>Registrati</button>
          <button type="button" onClick={() => { setMode("reset"); setMessage(""); }} className={mode === "reset" ? "active" : ""}>Password dimenticata</button>
        </div>

        <form onSubmit={submitAuth} className="hf-form-box">
          {mode !== "reset" ? (
            <>
              <label>Email</label>
              <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
              <label>Password</label>
              <input type="password" minLength={6} value={password} onChange={(e) => setPassword(e.target.value)} required />
              {mode === "register" && (
                <>
                  <label>Conferma password</label>
                  <input type="password" minLength={6} value={password2} onChange={(e) => setPassword2(e.target.value)} required />
                  <div className="hf-info">Dopo la registrazione compilerai subito la scheda immobile. Quei dati verranno poi usati in Immobile, Analisi mercato, Pricing e Messaggi.</div>
                </>
              )}
              <button className="hf-primary-full" disabled={loading}>{loading ? "Attendere..." : mode === "login" ? "Accedi" : "Crea account e compila immobile"}</button>
            </>
          ) : (
            <>
              <p className="hf-muted">Inserisci la tua email e riceverai un codice di recupero valido 15 minuti.</p>
              <label>Email</label>
              <input value={resetEmail} onChange={(e) => setResetEmail(e.target.value)} />
              <button type="button" className="hf-primary-full" onClick={() => setMessage("Endpoint recupero password da collegare.")}>Invia codice di recupero</button>
              <label>Codice di recupero</label>
              <input value={resetCode} onChange={(e) => setResetCode(e.target.value)} />
              <label>Nuova password</label>
              <input type="password" value={resetPassword} onChange={(e) => setResetPassword(e.target.value)} />
              <button className="hf-primary-full">Reimposta password</button>
            </>
          )}
        </form>

        {message && <div className="hf-alert">{message}</div>}
      </section>
    </main>
  );
}
