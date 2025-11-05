# app.py — Dashboard CSAT (XLSX/CSV) — GitHub via ZIP + Upload por arquivo
# ---------------------------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl requests
#
# Secrets (.streamlit/secrets.toml):
#   GITHUB_DATA_TOKEN   = "ghp_xxx"                         # opcional
#   GITHUB_DATA_REPO    = "grupoperfil-glitch/csat-dashboard-data"
#   GITHUB_DATA_BRANCH  = "main"
#   GITHUB_DATA_PATH    = "data"

from __future__ import annotations
import os
import re
from io import BytesIO
from datetime import date
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

import requests
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ======================
# Config & helpers
# ======================

def _get_secret(name: str, default: str = "") -> str:
    try:
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

GH_REPO   = _get_secret("GITHUB_DATA_REPO",   "grupoperfil-glitch/csat-dashboard-data")
GH_BRANCH = _get_secret("GITHUB_DATA_BRANCH", "main")
GH_PATH   = _get_secret("GITHUB_DATA_PATH",   "data").strip("/")
GH_TOKEN  = _get_secret("GITHUB_DATA_TOKEN",  "")

LOCAL_STORE_DIR = "data_store"
TODAY_MK        = f"{date.today().year:04d}-{date.today().month:02d}"

LAST_GH_STATUS: List[str] = []

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

# ======================
# Leitura de arquivos
# ======================

def load_xlsx(file: BytesIO | str) -> pd.DataFrame:
    """Carrega Excel; tenta aba 'Resultado da consulta'; senão, 1ª aba."""
    try:
        xl = pd.ExcelFile(file, engine="openpyxl")
        sheet = "Resultado da consulta" if "Resultado da consulta" in xl.sheet_names else xl.sheet_names[0]
        return xl.parse(sheet)
    except Exception:
        try:
            return pd.read_excel(file, engine="openpyxl")
        except Exception as e:
            LAST_GH_STATUS.append(f"Erro ao ler Excel: {e}")
            return pd.DataFrame()

def load_xlsx_from_bytes(b: bytes) -> pd.DataFrame:
    return load_xlsx(BytesIO(b))

# ======================
# Normalização de dados
# ======================

def normalize_canal_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Canal" in df.columns:
        return df
    lower = {str(c).strip().lower(): c for c in df.columns}
    for alias in ["canal", "channel", "categoria/canal"]:
        if alias in lower:
            return df.rename(columns={lower[alias]: "Canal"})
    return df

def find_best_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        k = c.strip().lower()
        if k in lower:
            return lower[k]
    return None

def to_hours_strict(series: pd.Series) -> pd.Series:
    """Conversão ESTRITA:
       - string com ':' => HH:MM:SS => horas
       - numérico => SEMPRE segundos => horas
    """
    s_str = series.astype(str)
    has_colon = s_str.str.contains(":", regex=False)
    out = pd.Series(index=series.index, dtype="float64")
    # HH:MM:SS
    td = pd.to_timedelta(s_str.where(has_colon, None), errors="coerce")
    out.loc[has_colon] = td.dt.total_seconds() / 3600.0
    # Numérico (segundos)
    s_num = pd.to_numeric(s_str.where(~has_colon, None), errors="coerce")
    out.loc[~has_colon] = s_num / 3600.0
    return out

def sum_numeric_safe(df: pd.DataFrame) -> Optional[int]:
    """Soma segura de todas as células numéricas de um DataFrame."""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    num_only = df.select_dtypes(include=[np.number])
    if not num_only.empty:
        total = pd.to_numeric(num_only.stack(), errors="coerce").sum()
        return int(total) if not pd.isna(total) else None
    coerced = df.apply(pd.to_numeric, errors="coerce")
    num_only2 = coerced.select_dtypes(include=[np.number])
    if num_only2.empty:
        return None
    total2 = num_only2.stack().sum()
    return int(total2) if not pd.isna(total2) else None

# ======================
# KPIs e SLAs
# ======================

SLA = {
    "COMPLETION_RATE_MIN": 90.0,
    "FIRST_RESPONSE_MAX_H": 24.0,
    "CSAT_MIN": 4.0,
    "EVAL_COVERAGE_MIN": 75.0
}

CSAT_ORDER = [
    "Muito Insatisfeito",
    "Insatisfeito",
    "Neutro",
    "Satisfeito",
    "Muito Satisfeito",
]

def _sum_col_if_exists(df: pd.DataFrame, colnames: List[str]) -> Optional[float]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in colnames:
        key = name.lower()
        if key in lower:
            v = pd.to_numeric(df[lower[key]], errors="coerce").sum()
            return float(v) if not pd.isna(v) else None
    s = sum_numeric_safe(df)
    return float(s) if s is not None else None

def _mean_hours_from_df(df: pd.DataFrame, candidates: List[str]) -> Optional[float]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    col = find_best_column(df, candidates)
    if not col:
        return None
    try:
        h = to_hours_strict(df[col]).mean()
        return float(h) if not pd.isna(h) else None
    except Exception:
        return None

def compute_kpis_from_payload(payload: dict) -> dict:
    """Computa KPIs principais a partir do payload de um mês."""
    k = {
        "total": None,
        "completed": None,
        "completion_rate": None,
        "first_response_h": None,
        "csat_avg": None,
        "evaluated": None,
        "eval_coverage": None,
    }
    
    # Totais
    if "total_atendimentos" in payload:
        k["total"] = _sum_col_if_exists(payload["total_atendimentos"], ["total_tickets"]) or _sum_col_if_exists(payload["total_atendimentos"], [])
    if "total_atendimentos_conc" in payload:
        k["completed"] = _sum_col_if_exists(payload["total_atendimentos_conc"], ["total_tickets"]) or _sum_col_if_exists(payload["total_atendimentos_conc"], [])
    
    # Fallback a partir de by_channel
    byc = payload.get("by_channel")
    if (k["total"] is None) and isinstance(byc, pd.DataFrame):
        k["total"] = _sum_col_if_exists(byc, ["Total de atendimentos"]) or _sum_col_if_exists(byc, [])
    if (k["completed"] is None) and isinstance(byc, pd.DataFrame):
        k["completed"] = _sum_col_if_exists(byc, ["Total de atendimentos concluídos"]) or _sum_col_if_exists(byc, [])

    # Completion rate
    if k["total"] not in (None, 0) and k["completed"] is not None:
        try:
            k["completion_rate"] = (k["completed"] / k["total"]) * 100.0
        except Exception:
            k["completion_rate"] = None

    # First response
    if "tme_geral" in payload:
        k["first_response_h"] = _mean_hours_from_df(payload["tme_geral"], [
            "mean_total HH:MM:SS","mean_total","Tempo médio de espera","tempo medio de espera","wait_seconds","mean_wait_seconds"
        ])
    if k["first_response_h"] is None and isinstance(byc, pd.DataFrame):
        k["first_response_h"] = _mean_hours_from_df(byc, [
            "mean_wait HH:MM:SS","mean_wait","Tempo médio de espera","wait_seconds","mean_wait_seconds","Tempo médio de espera (s)"
        ])

    # CSAT médio
    if "media_csat" in payload:
        col = find_best_column(payload["media_csat"], ["avg","Média CSAT","media"])
        if col:
            try:
                k["csat_avg"] = float(pd.to_numeric(payload["media_csat"][col], errors="coerce").dropna().iloc[0])
            except Exception:
                pass
    if k["csat_avg"] is None and isinstance(byc, pd.DataFrame):
        col = find_best_column(byc, ["Média CSAT","media csat","avg","media"])
        if col:
            v = pd.to_numeric(byc[col], errors="coerce").mean()
            k["csat_avg"] = float(v) if not pd.isna(v) else None

    # Avaliadas
    if "csat" in payload:
        k["evaluated"] = _sum_col_if_exists(payload["csat"], ["score_total"])
    if (k["evaluated"] is None) and isinstance(byc, pd.DataFrame):
        k["evaluated"] = _sum_col_if_exists(byc, ["Respostas CSAT"])

    if k["evaluated"] is not None and k["completed"] not in (None, 0):
        try:
            k["eval_coverage"] = (k["evaluated"] / k["completed"]) * 100.0
        except Exception:
            k["eval_coverage"] = None

    return k

def sla_flags(k: dict) -> dict:
    flags = {}
    cr = k.get("completion_rate")
    if cr is not None:
        ok = cr > SLA["COMPLETION_RATE_MIN"]
        warn = not ok and (cr >= SLA["COMPLETION_RATE_MIN"] * 0.95)
        flags["completion"] = (ok, warn)
    fr = k.get("first_response_h")
    if fr is not None:
        ok = fr < SLA["FIRST_RESPONSE_MAX_H"]
        warn = not ok and (fr <= SLA["FIRST_RESPONSE_MAX_H"] * 1.05)
        flags["first_response"] = (ok, warn)
    cs = k.get("csat_avg")
    if cs is not None:
        ok = cs >= SLA["CSAT_MIN"]
        warn = not ok and (cs >= SLA["CSAT_MIN"] * 0.95)
        flags["csat"] = (ok, warn)
    cov = k.get("eval_coverage")
    if cov is not None:
        ok = cov >= SLA["EVAL_COVERAGE_MIN"]
        warn = not ok and (cov >= SLA["EVAL_COVERAGE_MIN"] * 0.95)
        flags["coverage"] = (ok, warn)
    return flags

# ======================
# Mapeamento de arquivos
# ======================

KEYS = {
    "csat": ["data_product__csat"],
    "media_csat": ["data_product__media_csat", "media_csat"],
    "tma_por_canal": ["tempo_medio_de_atendimento_por_canal"],
    "tma_geral": ["tempo_medio_de_atendimento"],
    "tme_geral": ["tempo_medio_de_espera"],
    "total_atendimentos": ["total_de_atendimentos"],
    "total_atendimentos_conc": ["total_de_atendimentos_concluidos", "total_de_atendimentos_concluídos"],
}

def detect_kind(filename: str) -> Optional[str]:
    low = filename.lower()
    for kind, tokens in KEYS.items():
        for t in tokens:
            if t in low and (low.endswith(".xlsx") or low.endswith(".csv")):
                return kind
    return None

def extract_month_from_any(s: str) -> Optional[str]:
    m = re.search(r"(20\d{2})[-_./ ](0[1-9]|1[0-2])", s)
    return f"{m.group(1)}-{m.group(2)}" if m else None

def build_by_channel(payload: dict) -> dict:
    """Cria payload['by_channel'] unificando qualquer DF com coluna 'Canal'."""
    dfs = []
    for _, df in payload.items():
        if isinstance(df, pd.DataFrame):
            ndf = normalize_canal_column(df)
            if "Canal" in ndf.columns:
                dfs.append(ndf.copy())
    merged = None
    for df in dfs:
        merged = df.copy() if merged is None else merged.merge(df, on="Canal", how="outer")
    if isinstance(merged, pd.DataFrame):
        mcol = find_best_column(merged, ["Média CSAT","media csat","avg","media"])
        if mcol and mcol != "Média CSAT":
            merged = merged.rename(columns={mcol: "Média CSAT"})
        ccol = find_best_column(merged, [
            "Respostas CSAT","Quantidade de respostas CSAT","score_total","ratings",
            "Total de avaliações","avaliacoes","avaliações","qtd","qtde"
        ])
        if ccol and ccol != "Respostas CSAT":
            merged = merged.rename(columns={ccol: "Respostas CSAT"})
        payload["by_channel"] = merged
    return payload

# ======================
# GitHub (ZIP)
# ======================

def fetch_repo_zip_bytes() -> Optional[bytes]:
    """Baixa o repositório como ZIP."""
    headers = {"User-Agent": "streamlit-csat-dashboard"}
    if GH_TOKEN:
        headers["Authorization"] = f"token {GH_TOKEN}"
    
    # Tenta API zipball primeiro
    try:
        api_zip_url = f"https://api.github.com/repos/{GH_REPO}/zipball/{GH_BRANCH}"
        r = requests.get(api_zip_url, headers=headers, timeout=120, allow_redirects=True)
        LAST_GH_STATUS.append(f"GET {api_zip_url} -> {r.status_code}")
        if r.status_code == 200 and r.content:
            return r.content
    except Exception as e:
        LAST_GH_STATUS.append(f"ERR API ZIP: {e}")
    
    # Fallback: codeload
    try:
        raw_zip_url = f"https://codeload.github.com/{GH_REPO}/zip/refs/heads/{GH_BRANCH}"
        r = requests.get(raw_zip_url, headers=headers, timeout=120, allow_redirects=True)
        LAST_GH_STATUS.append(f"GET {raw_zip_url} -> {r.status_code}")
        if r.status_code == 200 and r.content:
            return r.content
    except Exception as e:
        LAST_GH_STATUS.append(f"ERR CODELOAD: {e}")
    
    return None

def group_zip_files_by_month(zf: ZipFile) -> Dict[str, List[str]]:
    """Agrupa arquivos do ZIP por mês."""
    names = zf.namelist()
    months: Dict[str, List[str]] = {}
    
    if not names:
        return months
    
    root = names[0].split("/")[0]
    
    # Procura por arquivos .xlsx e .csv
    for n in names:
        low = n.lower()
        if not (low.endswith(".xlsx") or low.endswith(".csv")):
            continue
        
        # Tenta extrair mês do caminho
        month = None
        for seg in n.split("/"):
            m = extract_month_from_any(seg)
            if m:
                month = m
                break
        
        if not month:
            month = extract_month_from_any(os.path.basename(n)) or TODAY_MK
        
        months.setdefault(month, []).append(n)
    
    files_count = sum(len(v) for v in months.values())
    LAST_GH_STATUS.append(f"ZIP scan -> meses: {len(months)} | arquivos: {files_count}")
    
    return months

def gh_read_file_from_zip(zf: ZipFile, path: str) -> Optional[pd.DataFrame]:
    """Lê um arquivo do ZIP."""
    try:
        b = zf.read(path)
    except Exception as e:
        LAST_GH_STATUS.append(f"Erro ao ler {path}: {e}")
        return None
    
    if path.lower().endswith(".csv"):
        try:
            return pd.read_csv(BytesIO(b))
        except Exception as e:
            LAST_GH_STATUS.append(f"Erro CSV {path}: {e}")
            return None
    else:
        return load_xlsx_from_bytes(b)

def gh_read_month_payload_from_zip(zf: ZipFile, paths: List[str]) -> dict:
    """Lê todos os arquivos de um mês do ZIP."""
    payload: dict = {}
    by_kind: Dict[str, List[str]] = {}
    
    for p in paths:
        kind = detect_kind(os.path.basename(p))
        if kind:
            by_kind.setdefault(kind, []).append(p)
    
    for kind, lst in by_kind.items():
        sel = sorted(lst)[-1]
        df = gh_read_file_from_zip(zf, sel)
        if isinstance(df, pd.DataFrame) and not df.empty:
            payload[kind] = df
        else:
            LAST_GH_STATUS.append(f"Falha ao ler: {sel}")
    
    return build_by_channel(payload)

def load_all_github_months_via_zip(force: bool = False) -> Tuple[int, int]:
    """Carrega todos os meses do GitHub."""
    b = fetch_repo_zip_bytes()
    if not b:
        LAST_GH_STATUS.append("Falha ao baixar ZIP do GitHub")
        return (0, 0)
    
    months_loaded = 0
    files_count = 0
    
    try:
        with ZipFile(BytesIO(b)) as zf:
            grouped = group_zip_files_by_month(zf)
            files_count = sum(len(v) for v in grouped.values())
            
            for m, paths in sorted(grouped.items()):
                if not force and m in st.session_state["months"]:
                    continue
                
                payload = gh_read_month_payload_from_zip(zf, paths)
                if payload:
                    st.session_state["months"][m] = payload
                    months_loaded += 1
                    LAST_GH_STATUS.append(f"Mês {m} carregado com {len(payload)} arquivos")
    except Exception as e:
        LAST_GH_STATUS.append(f"Erro ao processar ZIP: {e}")
        return (0, 0)
    
    return (months_loaded, files_count)

# ======================
# Local (fallback)
# ======================

def read_local_month_payload(mk: str) -> dict:
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    payload: dict = {}
    if not os.path.isdir(folder):
        return payload
    
    for f in os.listdir(folder):
        low = f.lower()
        if not (low.endswith(".xlsx") or low.endswith(".csv")):
            continue
        
        kind = detect_kind(f)
        if not kind:
            continue
        
        path = os.path.join(folder, f)
        df: Optional[pd.DataFrame] = None
        
        if low.endswith(".csv"):
            try:
                df = pd.read_csv(path)
            except Exception:
                df = None
        else:
            try:
                df = load_xlsx(path)
            except Exception:
                df = None
        
        if isinstance(df, pd.DataFrame):
            payload[kind] = df
    
    return build_by_channel(payload)

def load_all_local_months_into_state() -> int:
    if not os.path.isdir(LOCAL_STORE_DIR):
        return 0
    
    loaded = 0
    for name in sorted(os.listdir(LOCAL_STORE_DIR)):
        p = os.path.join(LOCAL_STORE_DIR, name)
        if os.path.isdir(p) and re.fullmatch(r"\d{4}-\d{2}", name):
            payload = read_local_month_payload(name)
            if payload and name not in st.session_state["months"]:
                st.session_state["months"][name] = payload
                loaded += 1
    
    return loaded

# ======================
# Upload por arquivo
# ======================

def ingest_single_file(file, expected_kind: str) -> Optional[pd.DataFrame]:
    if not file:
        return None
    try:
        if file.name.lower().endswith(".csv"):
            return pd.read_csv(file)
        return load_xlsx(file)
    except Exception:
        return None

# ======================
# Streamlit App
# ======================

st.set_page_config(page_title="Dashboard CSAT", layout="wide")
st.title("Dashboard CSAT")
st.caption(f"Fonte GitHub: **{GH_REPO} / {GH_BRANCH} / {GH_PATH}**")

# Estado
if "months" not in st.session_state:
    st.session_state["months"] = {}

# Carrega dados
gh_loaded, gh_files = load_all_github_months_via_zip(force=False)
local_loaded = load_all_local_months_into_state()

# Sidebar
with st.sidebar:
    st.header("Parâmetros do Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, value=today.month, step=1)
    year  = st.number_input("Ano", 2000, 2100, value=today.year, step=1)
    mk = month_key(int(year), int(month))

    st.write("---")
    st.markdown("**Fonte dos dados**")
    st.write(f"Repo: `{GH_REPO}`")
    st.write(f"Branch: `{GH_BRANCH}`")
    st.write(f"Path: `{GH_PATH}`")
    
    if GH_TOKEN:
        st.success("✓ Token configurado")
    else:
        st.info("Sem token (repo público)")

    if st.button("🔄 Recarregar do GitHub"):
        LAST_GH_STATUS.clear()
        loaded, files_cnt = load_all_github_months_via_zip(force=True)
        st.success(f"Recarregados: {loaded} mês(es) — {files_cnt} arquivo(s)")

    with st.expander("📊 Diagnóstico"):
        st.write(f"Meses GitHub: **{gh_loaded}**")
        st.write(f"Arquivos: **{gh_files}**")
        st.write(f"Local: **{local_loaded}**")
        st.write(f"Total meses: **{len(st.session_state['months'])}**")
        
        if LAST_GH_STATUS:
            st.code("\n".join(LAST_GH_STATUS[-15:]))

    st.write("---")
    st.subheader("📤 Upload de Arquivos")
    st.caption(f"Mês: **{mk}**")
    
    u_csat = st.file_uploader("1) CSAT (score_total)", type=["xlsx","csv"], key="u_csat")
    u_media = st.file_uploader("2) Média CSAT (avg)", type=["xlsx","csv"], key="u_media")
    u_tma = st.file_uploader("3) TMA (mean_total)", type=["xlsx","csv"], key="u_tma")
    u_tme = st.file_uploader("4) TME (mean_total)", type=["xlsx","csv"], key="u_tme")
    u_total = st.file_uploader("5) Total atendimentos", type=["xlsx","csv"], key="u_total")
    u_total_c = st.file_uploader("6) Total concluídos", type=["xlsx","csv"], key="u_totalc")
    u_ch = st.file_uploader("7) Por canal", type=["xlsx","csv"], key="u_ch")

    if st.button("💾 Salvar arquivos"):
        payload = st.session_state["months"].get(mk, {})
        
        if u_csat:
            df = ingest_single_file(u_csat, "csat")
            if isinstance(df, pd.DataFrame):
                payload["csat"] = df
        
        if u_media:
            df = ingest_single_file(u_media, "media_csat")
            if isinstance(df, pd.DataFrame):
                payload["media_csat"] = df
        
        if u_tma:
            df = ingest_single_file(u_tma, "tma_geral")
            if isinstance(df, pd.DataFrame):
                payload["tma_geral"] = df
        
        if u_tme:
            df = ingest_single_file(u_tme, "tme_geral")
            if isinstance(df, pd.DataFrame):
                payload["tme_geral"] = df
        
        if u_total:
            df = ingest_single_file(u_total, "total_atendimentos")
            if isinstance(df, pd.DataFrame):
                payload["total_atendimentos"] = df
        
        if u_total_c:
            df = ingest_single_file(u_total_c, "total_atendimentos_conc")
            if isinstance(df, pd.DataFrame):
                payload["total_atendimentos_conc"] = df
        
        if u_ch:
            df = ingest_single_file(u_ch, "tma_por_canal")
            if isinstance(df, pd.DataFrame):
                payload["tma_por_canal"] = df
        
        payload = build_by_channel(payload)
        st.session_state["months"][mk] = payload
        
        # Salva localmente
        folder = os.path.join(LOCAL_STORE_DIR, mk)
        ensure_dir(folder)
        for kind, df in payload.items():
            if isinstance(df, pd.DataFrame):
                out = os.path.join(folder, f"{kind}.csv")
                try:
                    df.to_csv(out, index=False)
                except Exception:
                    pass
        
        st.success(f"✓ Arquivos salvos para {mk}")

# Helper
def get_current_by_channel(mk: str) -> Optional[pd.DataFrame]:
    payload = st.session_state["months"].get(mk, {})
    df = payload.get("by_channel")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.copy()
    for v in payload.values():
        if isinstance(v, pd.DataFrame) and "Canal" in normalize_canal_column(v).columns:
            return normalize_canal_column(v.copy())
    return None

# Abas
tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo Mensal", "Dicionário"])

# 1) Visão Geral
with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    payload = st.session_state["months"].get(mk, {})
    
    if not payload:
        st.info("📁 Nenhum dado carregado para este mês. Use o GitHub ou faça upload.")
    else:
        k = compute_kpis_from_payload(payload)
        flags = sla_flags(k)
        icon = lambda ok, warn: ("✅" if ok else ("⚠️" if warn else "❌"))

        # Indicadores
        c1, c2, c3, c4 = st.columns(4)
        
        cr = k.get("completion_rate")
        ok, warn = flags.get("completion", (False, False))
        c1.metric("Taxa de conclusão", 
                  f"{cr:.1f}%" if cr is not None else "-", 
                  help=f"SLA > {SLA['COMPLETION_RATE_MIN']}% {icon(ok, warn)}")
        
        fr = k.get("first_response_h")
        ok, warn = flags.get("first_response", (False, False))
        c2.metric("Tempo 1º atendimento", 
                  f"{fr:.2f}h" if fr is not None else "-", 
                  help=f"SLA
