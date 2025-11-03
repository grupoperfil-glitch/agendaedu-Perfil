# app.py — Dashboard CSAT (XLSX/CSV) — GitHub via ZIP + Upload por arquivo (compatível com v2)
# ---------------------------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl requests
#
# Secrets (opcionais) — .streamlit/secrets.toml:
#   GITHUB_DATA_TOKEN   = "ghp_xxx"                         # opcional (evita alguns limites)
#   GITHUB_DATA_REPO    = "owner/repo"                     # ex.: "grupoperfil-glitch/csat-dashboard-data"
#   GITHUB_DATA_BRANCH  = "main"
#   GITHUB_DATA_PATH    = "data"                            # subpasta contendo os .xlsx/.csv (ex.: data/2025-09/...)
#
# O app:
#  - Baixa o repositório como ZIP (via API zipball com token ou codeload) e lê TODOS os .xlsx/.csv dentro de GITHUB_DATA_PATH,
#    agrupando por mês (YYYY[-_./ ]MM) encontrado no caminho ou no nome do arquivo.
#  - **Upload por arquivo** (como no v2): campos separados para cada tipo esperado do mês atual.
#  - Converte “Tempo médio de atendimento/espera” **para HORAS** (regra estrita) na aba Por Canal.
#  - Aba “Análise dos Canais”: (a) piores por **menor quantidade de respostas CSAT**; (b) **menores notas** (CSAT <= 3.0).
#  - **Autotestes/Diagnóstico** na barra lateral para verificar conexão GitHub e mapeamento de meses.

from __future__ import annotations
import os, re
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
RAW_ZIP_URL     = f"https://codeload.github.com/{GH_REPO}/zip/refs/heads/{GH_BRANCH}"
# Mês atual (fallback quando não há AAAA-MM nos caminhos/nomes)
TODAY_MK        = f"{date.today().year:04d}-{date.today().month:02d}"

LAST_GH_STATUS: List[str] = []  # diagnóstico simples


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


# ======================
# Leitura de Excel
# ======================

def load_xlsx(file: BytesIO | str) -> pd.DataFrame:
    """Carrega Excel; tenta aba 'Resultado da consulta'; senão, 1ª aba."""
    try:
        xl = pd.ExcelFile(file)
        sheet = "Resultado da consulta" if "Resultado da consulta" in xl.sheet_names else xl.sheet_names[0]
        return xl.parse(sheet)
    except Exception:
        return pd.read_excel(file)


def load_xlsx_from_bytes(b: bytes) -> pd.DataFrame:
    return load_xlsx(BytesIO(b))


# ======================
# Normalização de dados
# ======================

def normalize_canal_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Canal" in df.columns:
        return df
    lower = {str(c).strip().lower(): c for c in df.columns}
    for alias in ["categoria", "canal", "channel", "categoria/canal"]:
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
    """Soma segura de todas as células numéricas de um DataFrame.
    - Usa apenas colunas numéricas quando disponíveis;
    - Caso não haja, tenta coerção numérica por coluna (erros->NaN) e soma;
    - Retorna None se nada numérico existir.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    num_only = df.select_dtypes(include=[np.number])
    if not num_only.empty:
        total = pd.to_numeric(num_only.stack(), errors="coerce").sum()
        return int(total) if not pd.isna(total) else None
    # tenta coerção ampla
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
    "COMPLETION_RATE_MIN": 90.0,        # > 90%
    "FIRST_RESPONSE_MAX_H": 24.0,       # < 24h
    "CSAT_MIN": 4.0,                    # >= 4.0
    "EVAL_COVERAGE_MIN": 75.0           # >= 75%
}

def _sum_col_if_exists(df: pd.DataFrame, colnames: List[str]) -> Optional[float]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in colnames:
        key = name.lower()
        if key in lower:
            v = pd.to_numeric(df[lower[key]], errors="coerce").sum()
            return float(v) if not pd.isna(v) else None
    # fallback: soma numérica geral
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
    """Computa KPIs principais a partir do payload de um mês.
    Retorna dict com chaves: total, completed, completion_rate, first_response_h, csat_avg, evaluated, eval_coverage.
    Usa fallbacks quando possível (ex.: por canal).
    """
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

    # First response (tempo médio de espera)
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

    # Avaliadas (avaliadas = soma de score_total no csat por categoria; fallback: Respostas CSAT por canal)
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
    """Retorna flags (ok, warn) por KPI de acordo com limites SLA."""
    flags = {}
    # Completion
    cr = k.get("completion_rate")
    if cr is not None:
        ok = cr > SLA["COMPLETION_RATE_MIN"]
        warn = not ok and (cr >= SLA["COMPLETION_RATE_MIN"] * 0.95)
        flags["completion"] = (ok, warn)
    # First response (horas)
    fr = k.get("first_response_h")
    if fr is not None:
        ok = fr < SLA["FIRST_RESPONSE_MAX_H"]
        warn = not ok and (fr <= SLA["FIRST_RESPONSE_MAX_H"] * 1.05)
        flags["first_response"] = (ok, warn)
    # CSAT
    cs = k.get("csat_avg")
    if cs is not None:
        ok = cs >= SLA["CSAT_MIN"]
        warn = not ok and (cs >= SLA["CSAT_MIN"] * 0.95)
        flags["csat"] = (ok, warn)
    # Cobertura
    cov = k.get("eval_coverage")
    if cov is not None:
        ok = cov >= SLA["EVAL_COVERAGE_MIN"]
        warn = not ok and (cov >= SLA["EVAL_COVERAGE_MIN"] * 0.95)
        flags["coverage"] = (ok, warn)
    return flags


# ======================
# Mapeamento de arquivos
# ======================
# Palavras-chave para identificar cada arquivo (em qualquer parte do nome)
KEYS = {
    "csat": ["data_product__csat"],
    "media_csat": ["data_product__media_csat", "media_csat"],
    "tma_por_canal": ["tempo_medio_de_atendimento_por_canal"],
    "tma_geral": ["tempo_medio_de_atendimento"],
    "tme_geral": ["tempo_medio_de_espera"],
    "total_atendimentos": ["total_de_atendimentos"],
    "total_atendimentos_conc": ["total_de_atendimentos_concluidos", "total_de_atendimentos_concluídos"],
}

# Mapas para CSVs gravados pela versão 2 (GH_PATH/YYYY-MM/*.csv)
CSV_TYPE_TO_KIND = {
    "csat_by_cat": "csat",
    "csat_avg": "media_csat",
    "handle_avg": "tma_geral",
    "wait_avg": "tme_geral",
    "total": "total_atendimentos",
    "completed": "total_atendimentos_conc",
    "by_channel": "by_channel",
}


def detect_kind(filename: str) -> Optional[str]:
    low = filename.lower()
    # 1) CSVs do app v2 (nomes fixos): by_channel.csv, csat_avg.csv, etc.
    if low.endswith('.csv'):
        base = os.path.splitext(os.path.basename(low))[0]
        return CSV_TYPE_TO_KIND.get(base)
    # 2) XLSX por palavras-chave
    for kind, tokens in KEYS.items():
        for t in tokens:
            if t in low and low.endswith(".xlsx"):
                return kind
    return None


def extract_month_from_any(s: str) -> Optional[str]:
    """Extrai AAAA-MM de forma flexível (YYYY[-_./ ]MM)."""
    m = re.search(r"(?P<y>20[0-9]{2})[-_./ ](?P<m>0[1-9]|1[0-2])", s)
    if m:
        return f"{m.group('y')}-{m.group('m')}"
    return None


# ======================
# Build payloads
# ======================

def build_by_channel(payload: dict) -> dict:
    """Monta/atualiza payload['by_channel'] unificando qualquer DF com coluna 'Canal'."""
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
        # Padroniza nomes mais comuns
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
# GitHub (ZIP): leitura robusta
# ======================

def fetch_repo_zip_bytes() -> Optional[bytes]:
    """Baixa o ZIP do repo.
    1) Tenta a API zipball com token (permite repositórios privados)
    2) Fallback para codeload (público)
    """
    # 1) API zipball
    headers = {}
    if GH_TOKEN:
        headers["Authorization"] = f"Bearer {GH_TOKEN}"
    try:
        api_zip_url = f"https://api.github.com/repos/{GH_REPO}/zipball/{GH_BRANCH}"
        r = requests.get(api_zip_url, headers=headers, timeout=120, allow_redirects=True)
        LAST_GH_STATUS.append(f"GET {api_zip_url} -> {r.status_code}")
        if r.status_code == 200 and r.content:
            return r.content
    except Exception as e:
        LAST_GH_STATUS.append(f"ERR API ZIP: {e}")
    # 2) Fallback codeload
    try:
        headers2 = {}
        if GH_TOKEN:
            headers2["Authorization"] = f"token {GH_TOKEN}"
        r2 = requests.get(RAW_ZIP_URL, headers=headers2, timeout=120)
        LAST_GH_STATUS.append(f"GET {RAW_ZIP_URL} -> {r2.status_code}")
        if r2.status_code == 200:
            return r2.content
    except Exception as e2:
        LAST_GH_STATUS.append(f"ERR ZIP (codeload): {e2}")
    return None


def group_zip_files_by_month(zf: ZipFile) -> Dict[str, List[str]]:
    """
    Dentro do ZIP, encontra todos os .xlsx/.csv sob a pasta GH_PATH (recursivo) e agrupa por mês.
    Se não encontrar AAAA-MM, usa o mês atual (TODAY_MK) como fallback.
    """
    names = zf.namelist()
    months: Dict[str, List[str]] = {}
    root = names[0].split("/")[0] if names else ""
    base_prefix = f"{root}/{GH_PATH.strip('/')}/" if GH_PATH else f"{root}/"

    for n in names:
        low = n.lower()
        if not (low.endswith(".xlsx") or low.endswith(".csv")):
            continue
        if base_prefix and not n.startswith(base_prefix):
            continue
        month = None
        for seg in n.split("/"):
            month = extract_month_from_any(seg)
            if month:
                break
        if not month:
            month = extract_month_from_any(os.path.basename(n))
        if not month:
            month = TODAY_MK
            LAST_GH_STATUS.append(f"[WARN] Sem AAAA-MM em: {n} -> usando {month}")
        months.setdefault(month, []).append(n)
    return months


def gh_read_month_payload_from_zip(zf: ZipFile, paths: List[str]) -> dict:
    payload: dict = {}
    by_kind: Dict[str, List[str]] = {}
    for p in paths:
        kind = detect_kind(os.path.basename(p))
        if kind:
            by_kind.setdefault(kind, []).append(p)
    for kind, lst in by_kind.items():
        sel = sorted(lst)[-1]  # pega o "mais novo" pelo nome
        try:
            b = zf.read(sel)
            low = sel.lower()
            if low.endswith('.xlsx'):
                df = load_xlsx_from_bytes(b)
            elif low.endswith('.csv'):
                df = pd.read_csv(BytesIO(b))
            else:
                continue
            payload[kind] = df
        except Exception as e:
            LAST_GH_STATUS.append(f"Falha ao ler arquivo do ZIP: {sel} -> {e}")
    return build_by_channel(payload)


def load_all_github_months_via_zip(force: bool = False) -> Tuple[int, int]:
    """Carrega todos os meses lendo o repositório via ZIP. Retorna (#meses, #arquivos)."""
    b = fetch_repo_zip_bytes()
    if not b:
        return (0, 0)
    with ZipFile(BytesIO(b)) as zf:
        grouped = group_zip_files_by_month(zf)
        months_loaded = 0
        files_count = sum(len(v) for v in grouped.values())
        for m, paths in sorted(grouped.items()):
            if not force and m in st.session_state["months"]:
                continue
            payload = gh_read_month_payload_from_zip(zf, paths)
            if payload:
                st.session_state["months"][m] = payload
                months_loaded += 1
        return (months_loaded, files_count)


# ======================
# Local (fallback)
# ======================

def read_local_month_payload(y: int, m: int) -> dict:
    mk = f"{y:04d}-{m:02d}"
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    payload: dict = {}
    if not os.path.isdir(folder):
        return payload
    for f in os.listdir(folder):
        low = f.lower()
        path = os.path.join(folder, f)
        try:
            if low.endswith(".xlsx"):
                kind = detect_kind(f) or os.path.splitext(f)[0]
                df = load_xlsx(path)
            elif low.endswith(".csv"):
                kind = detect_kind(f) or os.path.splitext(f)[0]
                df = pd.read_csv(path)
            else:
                continue
            if kind:
                payload[kind] = df
        except Exception:
            pass
    return build_by_channel(payload)


def load_all_local_months_into_state() -> int:
    if not os.path.isdir(LOCAL_STORE_DIR):
        return 0
    loaded = 0
    for name in sorted(os.listdir(LOCAL_STORE_DIR)):
        p = os.path.join(LOCAL_STORE_DIR, name)
        if os.path.isdir(p) and re.fullmatch(r"\d{4}-\d{2}", name):
            y, m = map(int, name.split("-"))
            payload = read_local_month_payload(y, m)
            if payload and name not in st.session_state["months"]:
                st.session_state["months"][name] = payload
                loaded += 1
    return loaded


# ======================
# Upload mensal (por arquivo — estilo v2)
# ======================

def ingest_single_file(kind: str, fl) -> Optional[pd.DataFrame]:
    if fl is None:
        return None
    try:
        df = load_xlsx(fl)
        return df
    except Exception:
        return None


# ======================
# Streamlit App
# ======================

st.set_page_config(page_title="Dashboard CSAT — GitHub (ZIP) + Upload por arquivo", layout="wide")
st.title("Dashboard CSAT (XLSX/CSV) — Fonte GitHub (ZIP) + Upload por arquivo")
st.caption(f"Fonte GitHub: **{GH_REPO} / {GH_BRANCH} / {GH_PATH}** — leitura via ZIP.")

# Estado
if "months" not in st.session_state:
    st.session_state["months"] = {}

# Carrega do GitHub via ZIP na inicialização (com diagnóstico)
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
    st.markdown("**Fonte dos dados (GitHub via ZIP)**")
    st.write(f"Repo: `{GH_REPO}` / Branch: `{GH_BRANCH}` / Path: `{GH_PATH}`")
    if GH_TOKEN:
        st.success("Token configurado (pode ajudar com limites).")
    else:
        st.info("Sem token: ZIP público (funciona para repositórios públicos).")

    if st.button("Recarregar do GitHub (ZIP) — todos os meses"):
        LAST_GH_STATUS.clear()
        loaded, files_cnt = load_all_github_months_via_zip(force=True)
        st.success(f"Recarregados do GitHub (ZIP): {loaded} mês(es) — {files_cnt} arquivo(s) analisado(s).")

    with st.expander("Diagnóstico GitHub"):
        st.write(f"Meses carregados agora: **{gh_loaded}** | Arquivos vistoriados: **{gh_files}** | Fallback local: **{local_loaded}**")
        if LAST_GH_STATUS:
            st.code("\n".join(LAST_GH_STATUS[-10:]))

    # ======================
    # Testes / Autodiagnóstico
    # ======================
    with st.expander("Testes (autodiagnóstico)"):
        if st.button("Rodar autoteste de conexão GitHub"):
            b = fetch_repo_zip_bytes()
            if not b:
                st.error("Falha ao baixar ZIP do GitHub. Verifique token, repo/branch e GH_PATH.")
            else:
                st.success(f"ZIP baixado com sucesso ({len(b)} bytes).")
                with ZipFile(BytesIO(b)) as zf:
                    names = zf.namelist()
                    st.write("Primeiros caminhos no ZIP:")
                    st.code("\n".join(names[:20]))
                    # filtro por GH_PATH
                    root = names[0].split("/")[0] if names else ""
                    base_prefix = f"{root}/{GH_PATH.strip('/')}/" if GH_PATH else f"{root}/"
                    in_path = [n for n in names if n.startswith(base_prefix) and (n.lower().endswith('.xlsx') or n.lower().endswith('.csv'))]
                    st.write(f"Arquivos (.xlsx/.csv) encontrados sob GH_PATH: **{len(in_path)}**")
                    grouped = group_zip_files_by_month(zf)
                    st.write(f"Meses mapeados: {sorted(grouped.keys())}")
                    # --- Testes rápidos da função sum_numeric_safe ---
                    df1 = pd.DataFrame({"a":[1,2], "b":[3,4]})
                    df2 = pd.DataFrame({"a":["1","x"], "b":["2","3"]})
                    df3 = pd.DataFrame({"a":["x","y"]})
                    st.write("Testes sum_numeric_safe:", {
                        "df1 (esperado 10)": sum_numeric_safe(df1),
                        "df2 (esperado 6)": sum_numeric_safe(df2),
                        "df3 (esperado None)": sum_numeric_safe(df3),
                    })

    st.write("---")
    st.subheader("Upload mensal (.xlsx)")
    st.caption("Envie **cada arquivo esperado** do mês selecionado. Todos devem conter a aba 'Resultado da consulta'.")

    # Upload por arquivo (iguais ao v2, mapeando para as chaves deste app)
    u_csat = st.file_uploader("1) _data_product__csat_*.xlsx  (Categoria, score_total)", type=["xlsx"], key="u_csat")
    u_media = st.file_uploader("2) _data_product__media_csat_*.xlsx  (avg)", type=["xlsx"], key="u_media")
    u_tma = st.file_uploader("3) tempo_medio_de_atendimento_*.xlsx  (mean_total HH:MM:SS)", type=["xlsx"], key="u_tma")
    u_tme = st.file_uploader("4) tempo_medio_de_espera_*.xlsx  (mean_total HH:MM:SS)", type=["xlsx"], key="u_tme")
    u_total = st.file_uploader("5) total_de_atendimentos_*.xlsx  (total_tickets)", type=["xlsx"], key="u_total")
    u_conc = st.file_uploader("6) total_de_atendimentos_concluidos_*.xlsx  (total_tickets)", type=["xlsx"], key="u_conc")
    u_canais = st.file_uploader("7) tempo_medio_de_atendimento_por_canal_*.xlsx  (opcional)", type=["xlsx"], key="u_canais")

    if st.button("Salvar arquivos do mês atual"):
        partial: Dict[str, pd.DataFrame] = {}
        mapping = {
            "csat": u_csat,
            "media_csat": u_media,
            "tma_geral": u_tma,
            "tme_geral": u_tme,
            "total_atendimentos": u_total,
            "total_atendimentos_conc": u_conc,
            "tma_por_canal": u_canais,
        }
        for kind, up in mapping.items():
            df = ingest_single_file(kind, up)
            if df is not None:
                partial[kind] = df
        if not partial:
            st.warning("Nenhum arquivo válido enviado.")
        else:
            payload = st.session_state["months"].get(mk, {})
            payload.update(partial)
            payload = build_by_channel(payload)
            st.session_state["months"][mk] = payload
            # salva local em .xlsx
            folder = os.path.join(LOCAL_STORE_DIR, mk)
            ensure_dir(folder)
            for kind, df in partial.items():
                df.to_excel(os.path.join(folder, f"{kind}.xlsx"), index=False)
            st.success(f"{len(partial)} arquivo(s) anexado(s) ao mês {mk} e salvo(s) em disco.")


# Helper

def get_current_by_channel() -> Optional[pd.DataFrame]:
    payload = st.session_state["months"].get(mk, {})
    df = payload.get("by_channel")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.copy()
    # fallback: primeiro DF com coluna 'Canal'
    for v in payload.values():
        if isinstance(v, pd.DataFrame) and "Canal" in normalize_canal_column(v).columns:
            return normalize_canal_column(v.copy())
    return None


# Abas

tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo Mensal", "Dicionário de Dados", "Análise dos Canais"]) 

# 1) Visão Geral
with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    if st.session_state["months"]:
        st.write(f"Meses carregados: `{', '.join(sorted(st.session_state['months'].keys()))}`")
    payload = st.session_state["months"].get(mk, {})
    if not payload:
        st.info("Selecione um mês com dados carregados (GitHub ou Upload).")
    else:
        # --- KPIs principais ---
        k = compute_kpis_from_payload(payload)
        flags = sla_flags(k)
        icon = lambda ok, warn: ("✅" if ok else ("⚠️" if warn else "❌"))

        c1, c2, c3, c4 = st.columns(4)
        # Taxa de conclusão
        cr = k.get("completion_rate")
        ok, warn = flags.get("completion", (False, False))
        c1.metric("Taxa de conclusão (%)", f"{cr:.1f}%" if cr is not None else "-",
                  help=f"SLA > {SLA['COMPLETION_RATE_MIN']}% {icon(ok, warn)}")
        # Tempo do 1º atendimento (horas)
        fr = k.get("first_response_h")
        ok, warn = flags.get("first_response", (False, False))
        c2.metric("Tempo do 1º atendimento (h)", f"{fr:.2f}" if fr is not None else "-",
                  help=f"SLA < {SLA['FIRST_RESPONSE_MAX_H']}h {icon(ok, warn)}")
        # CSAT médio
        cs = k.get("csat_avg")
        ok, warn = flags.get("csat", (False, False))
        c3.metric("CSAT médio (1–5)", f"{cs:.2f}" if cs is not None else "-",
                  help=f"SLA ≥ {SLA['CSAT_MIN']} {icon(ok, warn)}")
        # Cobertura de avaliação (%)
        cov = k.get("eval_coverage")
        ok, warn = flags.get("coverage", (False, False))
        c4.metric("Cobertura de avaliação (%)", f"{cov:.1f}%" if cov is not None else "-",
                  help=f"SLA ≥ {SLA['EVAL_COVERAGE_MIN']}% {icon(ok, warn)}")

        st.markdown("---")
        st.write("### Tabelas disponíveis no mês")
        for kname, vdf in payload.items():
            if isinstance(vdf, pd.DataFrame):
                st.markdown(f"**{kname}**")
                st.dataframe(vdf.head(50), use_container_width=True)

# 2) Por Canal
with tabs[1]:
    st.subheader(f"Por Canal — {mk}")
    dfc = get_current_by_channel()
    if dfc is None:
        st.info("Sem dados por canal para o mês atual.")
    else:
        dfc = normalize_canal_column(dfc)
        canais = sorted(dfc["Canal"].astype(str).unique())
        sel = st.multiselect("Filtrar canais", canais, default=canais)
        if sel:
            dfc = dfc[dfc["Canal"].astype(str).isin(sel)]

        col3, col4 = st.columns(2)

        # Tempo médio de atendimento (HORAS) — ESTRITO
        with col3:
            cand_tma = [
                "mean_total HH:MM:SS","mean_total","Tempo médio de atendimento",
                "Tempo medio de atendimento","_handle_seconds","handle_seconds",
                "mean_total_seconds","Tempo médio de atendimento (s)","tempo em segundos"
            ]
            tcol = find_best_column(dfc, cand_tma)
            if tcol is None:
                st.warning("Não encontrei a coluna de tempo de atendimento (ex.: 'mean_total HH:MM:SS').")
            else:
                dft = dfc.copy()
                dft["Tempo médio de atendimento (horas)"] = to_hours_strict(dft[tcol])
                st.plotly_chart(
                    px.bar(dft, x="Canal", y="Tempo médio de atendimento (horas)", title="Tempo médio de atendimento (horas)"),
                    use_container_width=True
                )

        # Tempo médio de espera (HORAS) — ESTRITO (se existir por canal)
        with col4:
            cand_wait = [
                "mean_wait HH:MM:SS","mean_wait","Tempo médio de espera",
                "Tempo medio de espera","wait_seconds","mean_wait_seconds",
                "Tempo médio de espera (s)","espera em segundos"
            ]
            wcol = find_best_column(dfc, cand_wait)
            if wcol is None:
                st.info("Coluna de tempo de espera por canal não localizada neste mês.")
            else:
                dfw = dfc.copy()
                dfw["Tempo médio de espera (horas)"] = to_hours_strict(dfw[wcol])
                st.plotly_chart(
                    px.bar(dfw, x="Canal", y="Tempo médio de espera (horas)", title="Tempo médio de espera (horas)"),
                    use_container_width=True
                )

        st.write("---")
        st.markdown("#### Tabela por Canal (mês atual)")
        st.dataframe(dfc, use_container_width=True)

# 3) Comparativo Mensal
with tabs[2]:
    st.subheader("Comparativo Mensal — indicadores principais")
    months_dict = st.session_state["months"]
    if not months_dict:
        st.info("Nenhum mês carregado.")
    else:
        # Filtra pelo ano selecionado na sidebar
        year_prefix = f"{int(year):04d}-"
        rows = []
        for mkey, payload in sorted(months_dict.items()):
            if not mkey.startswith(year_prefix):
                continue
            k = compute_kpis_from_payload(payload)
            rows.append({
                "mes": mkey,
                "taxa_conclusao": k.get("completion_rate"),
                "tempo_primeiro_atendimento_h": k.get("first_response_h"),
                "csat_medio": k.get("csat_avg"),
                "cobertura_avaliacao": k.get("eval_coverage"),
            })
        if not rows:
            st.info("Não há meses deste ano com dados carregados.")
        else:
            comp = pd.DataFrame(rows).sort_values("mes")
            st.dataframe(comp, use_container_width=True)
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(
                    px.line(comp, x="mes", y="taxa_conclusao", markers=True, title="Taxa de conclusão (%)"),
                    use_container_width=True
                )
            with c2:
                st.plotly_chart(
                    px.line(comp, x="mes", y="tempo_primeiro_atendimento_h", markers=True, title="Tempo do 1º atendimento (h)"),
                    use_container_width=True
                )
            c3, c4 = st.columns(2)
            with c3:
                st.plotly_chart(
                    px.line(comp, x="mes", y="csat_medio", markers=True, title="CSAT médio (1–5)"),
                    use_container_width=True
                )
            with c4:
                st.plotly_chart(
                    px.line(comp, x="mes", y="cobertura_avaliacao", markers=True, title="Cobertura de avaliação (%)"),
                    use_container_width=True
                )

# 4) Dicionário de Dados
with tabs[3]:
    st.subheader("Dicionário de Dados (colunas reconhecidas)")
    st.markdown("""
**Tempo de atendimento (por canal)**: `mean_total HH:MM:SS`, `mean_total`, `Tempo médio de atendimento`, `_handle_seconds`, `handle_seconds`, `mean_total_seconds`, `Tempo médio de atendimento (s)`.  
**Tempo de espera (por canal)**: `mean_wait HH:MM:SS`, `mean_wait`, `Tempo médio de espera`, `wait_seconds`, `mean_wait_seconds`, `Tempo médio de espera (s)`.  
**CSAT Médio**: `Média CSAT`, `avg`, `media`.  
**Respostas CSAT (contagem)**: `Respostas CSAT`, `score_total`, `ratings`, `Avaliações`, `Total de avaliações`, `qtd`, `qtde`.  
**Nome do Canal**: `Canal` (ou `Categoria`, `canal`, `channel` → renomeado para `Canal`).  
    """)


# 5) Análise dos Canais — novos requisitos
with tabs[4]:
    st.subheader("Análise dos Canais")
    st.caption("Por mês: (a) canais com MENOR quantidade de respostas do CSAT; (b) canais com **menores notas** (CSAT ≤ 3.0).")

    months_dict = st.session_state["months"]
    if not months_dict:
        st.info("Nenhum mês carregado.")
    else:
        count_candidates = [
            "Respostas CSAT","Quantidade de respostas CSAT","qtd respostas csat","qtd csat",
            "Respostas","Avaliadas","Avaliações","Total de avaliações",
            "Ratings","score_total","qtde","qtd"
        ]
        csat_candidates = ["Média CSAT","media csat","avg","media","CSAT","csat","CSAT Médio","csat médio"]

        rec_counts, rec_scores = [], []

        for mkey, payload in sorted(months_dict.items()):
            df = payload.get("by_channel")
            if not isinstance(df, pd.DataFrame) or df.empty:
                for v in payload.values():
                    if isinstance(v, pd.DataFrame) and "Canal" in normalize_canal_column(v).columns:
                        df = normalize_canal_column(v)
                        break
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            df = normalize_canal_column(df.copy())
            colmap = {str(c).strip().lower(): c for c in df.columns}

            # contagem de respostas CSAT
            ccol = None
            for c in count_candidates:
                if c.lower() in colmap: ccol = colmap[c.lower()]; break
            if ccol is not None:
                tmp = df[["Canal", ccol]].copy()
                tmp[ccol] = pd.to_numeric(tmp[ccol], errors="coerce")
                tmp = tmp.dropna()
                if not tmp.empty:
                    tmp = tmp.rename(columns={ccol: "Respostas CSAT"})
                    tmp["mes"] = mkey
                    rec_counts.append(tmp)

            # média csat (filtrar <= 3.0)
            scol = None
            for c in csat_candidates:
                if c.lower() in colmap: scol = colmap[c.lower()]; break
            if scol is not None:
                tmp2 = df[["Canal", scol]].copy()
                tmp2[scol] = pd.to_numeric(tmp2[scol], errors="coerce")
                tmp2 = tmp2.dropna()
                if not tmp2.empty:
                    tmp2 = tmp2.rename(columns={scol: "Média CSAT"})
                    tmp2 = tmp2[tmp2["Média CSAT"] <= 3.0]  # <= 3.0 (notas 1,2,3)
                    tmp2["mes"] = mkey
                    rec_scores.append(tmp2)

        colA, colB = st.columns(2)

        with colA:
            st.markdown("**Menor quantidade de respostas do CSAT por mês**")
            n_counts = st.number_input("Quantos canais exibir (menores quantidades)?", 1, 10, 3, 1, key="n_counts_new")
            if not rec_counts:
                st.warning("Não encontrei coluna de contagem de respostas por canal nos dados persistidos.")
            else:
                dd = pd.concat(rec_counts, ignore_index=True)
                tops = [g.sort_values("Respostas CSAT", ascending=True).head(int(n_counts)) for _, g in dd.groupby("mes", as_index=False)]
                dd_top = pd.concat(tops, ignore_index=True)
                st.plotly_chart(px.bar(dd_top, x="mes", y="Respostas CSAT", color="Canal",
                                       barmode="group", title="Menores quantidades de respostas (CSAT) por mês"),
                                use_container_width=True)
                st.dataframe(dd_top.sort_values(["mes","Respostas CSAT","Canal"]), use_container_width=True)

        with colB:
            st.markdown("**Canais com menores notas de CSAT (≤ 3.0) por mês**")
            n_scores = st.number_input("Quantos canais exibir (piores notas)?", 1, 10, 3, 1, key="n_scores_new")
            if not rec_scores:
                st.info("Não encontrei coluna de 'Média CSAT' por canal, ou não há notas ≤ 3.0.")
            else:
                dd2 = pd.concat(rec_scores, ignore_index=True)
                tops2 = [g.sort_values("Média CSAT", ascending=True).head(int(n_scores)) for _, g in dd2.groupby("mes", as_index=False)]
                dd2_top = pd.concat(tops2, ignore_index=True)
                st.plotly_chart(px.bar(dd2_top, x="mes", y="Média CSAT", color="Canal",
                                       barmode="group", title="Menores notas de CSAT (≤ 3.0) por mês"),
                                use_container_width=True)
                st.dataframe(dd2_top.sort_values(["mes","Média CSAT","Canal"]), use_container_width=True)
