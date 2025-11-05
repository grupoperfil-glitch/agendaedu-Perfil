# app.py — Dashboard CSAT (CSV) — GitHub + Upload com Nomes Reais
# Compatível com: _data_product__csat_*.csv, tempo_medio_de_*.csv, etc.
# Requisitos: pip install streamlit plotly pandas numpy requests

from __future__ import annotations
import os, re
from io import BytesIO, StringIO
from datetime import date
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile
import requests
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ====================== Config & Secrets ======================
def _get_secret(name: str, default: str = "") -> str:
    try:
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

GH_REPO = _get_secret("GITHUB_DATA_REPO", "grupoperfil-glitch/csat-dashboard-data")
GH_BRANCH = _get_secret("GITHUB_DATA_BRANCH", "main")
GH_PATH = _get_secret("GITHUB_DATA_PATH", "data").strip("/")
GH_TOKEN = _get_secret("GITHUB_DATA_TOKEN", "")
RAW_ZIP_URL = f"https://codeload.github.com/{GH_REPO}/zip/refs/heads/{GH_BRANCH}"

LOCAL_STORE_DIR = "data_store"
LAST_GH_STATUS: List[str] = []

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

# ====================== Leitura de CSV ======================
def load_csv(file: BytesIO | str) -> pd.DataFrame:
    try:
        content = file.read().decode('utf-8') if isinstance(file, BytesIO) else file
        return pd.read_csv(StringIO(content))
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return pd.DataFrame()

def load_csv_from_bytes(b: bytes) -> pd.DataFrame:
    return load_csv(BytesIO(b))

def normalize_canal_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Canal" in df.columns:
        return df
    lower = {str(c).strip().lower(): c for c in df.columns}
    for alias in ["categoria", "canal", "channel", "categoria/canal"]:
        if alias in lower:
            return df.rename(columns={lower[alias]: "Canal"})
    return df

def find(df: pd.DataFrame, cols: List[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in cols:
        if c.strip().lower() in lower:
            return lower[c.strip().lower()]
    return None

def to_hours(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    has_colon = s.str.contains(":", na=False)
    out = pd.Series(0.0, index=series.index)
    td = pd.to_timedelta(s.where(has_colon), errors="coerce")
    out.loc[has_colon] = td.dt.total_seconds() / 3600.0
    num = pd.to_numeric(s.where(~has_colon), errors="coerce")
    out.loc[~has_colon] = num / 3600.0
    return out

# ====================== Mapeamento de Arquivos (SEUS NOMES REAIS) ======================
KEYS = {
    "csat": ["data_product__csat", "csat"],
    "media_csat": ["data_product__media_csat", "media_csat"],
    "tma_por_canal": ["tempo_medio_de_atendimento_por_canal"],
    "tma_geral": ["tempo_medio_de_atendimento"],
    "tme_geral": ["tempo_medio_de_espera"],
    "total_atendimentos": ["total_de_atendimentos"],
    "total_atendimentos_conc": ["total_de_atendimentos_concluidos", "total_de_atendimentos_concluídos"],
}

def detect_kind(filename: str) -> Optional[str]:
    low = filename.lower().replace('.csv', '')
    for kind, tokens in KEYS.items():
        for t in tokens:
            if t in low:
                return kind
    return None

def extract_month(s: str) -> Optional[str]:
    m = re.search(r"\d{4}-\d{2}", s)
    return m.group(0) if m else None

# ====================== GitHub ZIP ======================
def fetch_zip() -> Optional[bytes]:
    headers = {"Authorization": f"token {GH_TOKEN}"} if GH_TOKEN else {}
    try:
        r = requests.get(RAW_ZIP_URL, headers=headers, timeout=120)
        LAST_GH_STATUS.append(f"ZIP → {r.status_code}")
        return r.content if r.status_code == 200 else None
    except Exception as e:
        LAST_GH_STATUS.append(f"ERR: {e}")
        return None

def group_by_month(zf: ZipFile) -> Dict[str, List[str]]:
    names = zf.namelist()
    root = names[0].split("/")[0] if names else ""
    prefix = f"{root}/{GH_PATH}/" if GH_PATH else f"{root}/"
    months: Dict[str, List[str]] = {}
    for n in names:
        if not n.lower().endswith(".csv") or not n.startswith(prefix):
            continue
        parts = n.split("/")
        month = next((extract_month(p) for p in parts if extract_month(p)), None)
        if not month:
            month = extract_month(os.path.basename(n))
        if month:
            months.setdefault(month, []).append(n)
    return months

def read_month(zf: ZipFile, paths: List[str]) -> dict:
    payload: dict = {}
    by_kind: Dict[str, List[str]] = {}
    for p in paths:
        kind = detect_kind(os.path.basename(p))
        if kind:
            by_kind.setdefault(kind, []).append(p)
    for kind, lst in by_kind.items():
        latest = sorted(lst)[-1]
        try:
            df = load_csv_from_bytes(zf.read(latest))
            payload[kind] = df
        except Exception:
            LAST_GH_STATUS.append(f"CSV falhou: {latest}")
    return build_by_channel(payload)

def build_by_channel(payload: dict) -> dict:
    dfs = [normalize_canal_column(df) for df in payload.values() if isinstance(df, pd.DataFrame) and "Canal" in normalize_canal_column(df).columns]
    if not dfs:
        return payload
    merged = dfs[0].copy()
    for df in dfs[1:]:
        merged = merged.merge(df, on="Canal", how="outer")
    if (col := find(merged, ["Média CSAT", "media csat", "avg"])):
        merged.rename(columns={col: "Média CSAT"}, inplace=True)
    if (col := find(merged, ["Respostas CSAT", "score_total", "qtd", "qtde"])):
        merged.rename(columns={col: "Respostas CSAT"}, inplace=True)
    payload["by_channel"] = merged
    return payload

def load_github(force: bool = False) -> Tuple[int, int]:
    b = fetch_zip()
    if not b:
        return 0, 0
    with ZipFile(BytesIO(b)) as zf:
        grouped = group_by_month(zf)
        loaded = 0
        files = sum(len(v) for v in grouped.values())
        for m, paths in sorted(grouped.items()):
            if not force and m in st.session_state.get("months", {}):
                continue
            payload = read_month(zf, paths)
            if payload:
                st.session_state.setdefault("months", {})[m] = payload
                loaded += 1
        return loaded, files

# ====================== Local Fallback ======================
def load_local() -> int:
    if not os.path.isdir(LOCAL_STORE_DIR):
        return 0
    loaded = 0
    for name in sorted(os.listdir(LOCAL_STORE_DIR)):
        if re.fullmatch(r"\d{4}-\d{2}", name):
            path = os.path.join(LOCAL_STORE_DIR, name)
            if os.path.isdir(path):
                payload = {}
                for f in os.listdir(path):
                    if f.lower().endswith(".csv"):
                        kind = detect_kind(f)
                        if kind:
                            try:
                                df = pd.read_csv(os.path.join(path, f))
                                payload[kind] = df
                            except:
                                pass
                if payload:
                    st.session_state.setdefault("months", {})[name] = build_by_channel(payload)
                    loaded += 1
    return loaded

# ====================== Upload ======================
def upload_file(file, kind: str) -> Optional[pd.DataFrame]:
    if not file or not any(tok in file.name.lower() for tok in KEYS.get(kind, [])):
        return None
    try:
        return load_csv(file)
    except:
        return None

# ====================== App ======================
st.set_page_config(page_title="CSAT Dashboard", layout="wide")
st.title("Dashboard CSAT — GitHub + Upload")

if "months" not in st.session_state:
    st.session_state["months"] = {}

gh_m, gh_f = load_github()
local_m = load_local()

# Sidebar
with st.sidebar:
    st.header("Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, today.month)
    year = st.number_input("Ano", 2000, 2100, today.year)
    mk = month_key(year, month)

    st.markdown("**GitHub**")
    st.write(f"`{GH_REPO}` → `{GH_PATH}` (CSV)")
    if GH_TOKEN:
        st.success("Token OK")
    else:
        st.info("Público")

    if st.button("Recarregar GitHub"):
        LAST_GH_STATUS.clear()
        m, f = load_github(force=True)
        st.success(f"{m} meses, {f} arquivos CSV")

    with st.expander("Log"):
        st.write(f"GitHub: {gh_m} | Local: {local_m}")
        if LAST_GH_STATUS:
            st.code("\n".join(LAST_GH_STATUS[-10:]))
        if st.session_state["months"]:
            st.write("**Meses carregados:**")
            for m in sorted(st.session_state["months"].keys()):
                st.write(f"{m}")

    st.subheader("Upload CSV")
    uploads = {
        "csat": st.file_uploader(
            "CSAT por Categoria (_data_product__csat_*.csv)", 
            type="csv", key="u1"
        ),
        "media_csat": st.file_uploader(
            "Média CSAT (_data_product__media_csat_*.csv)", 
            type="csv", key="u2"
        ),
        "tma_por_canal": st.file_uploader(
            "TMA por Canal (tempo_medio_de_atendimento_por_canal_*.csv)", 
            type="csv", key="u3"
        ),
        "tma_geral": st.file_uploader(
            "TMA Geral (tempo_medio_de_atendimento_*.csv)", 
            type="csv", key="u4"
        ),
        "tme_geral": st.file_uploader(
            "TME Geral (tempo_medio_de_espera_*.csv)", 
            type="csv", key="u5"
        ),
        "total_atendimentos": st.file_uploader(
            "Total de Atendimentos (total_de_atendimentos_*.csv)", 
            type="csv", key="u6"
        ),
        "total_atendimentos_conc": st.file_uploader(
            "Atendimentos Concluídos (total_de_atendimentos_concluidos_*.csv)", 
            type="csv", key="u7"
        ),
    }

    if st.button("Salvar no mês"):
        partial = {k: upload_file(f, k) for k, f in uploads.items() if upload_file(f, k) is not None}
        if partial:
            payload = st.session_state["months"].get(mk, {})
            payload.update(partial)
            payload = build_by_channel(payload)
            st.session_state["months"][mk] = payload
            folder = os.path.join(LOCAL_STORE_DIR, mk)
            ensure_dir(folder)
            for k, df in partial.items():
                df.to_csv(os.path.join(folder, f"{k}.csv"), index=False)
            st.success(f"{len(partial)} arquivos CSV salvos")
        else:
            st.warning("Nenhum arquivo válido")

# Helpers
def get_payload() -> dict:
    return st.session_state["months"].get(mk, {})

def get_by_channel() -> Optional[pd.DataFrame]:
    p = get_payload()
    df = p.get("by_channel")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return normalize_canal_column(df.copy())
    for v in p.values():
        if isinstance(v, pd.DataFrame) and "Canal" in normalize_canal_column(v).columns:
            return normalize_canal_column(v.copy())
    return None

# Função segura para soma
def safe_sum(df: pd.DataFrame) -> float:
    if df.empty:
        return np.nan
    num = df.select_dtypes(include="number")
    return num.sum().sum() if not num.empty else np.nan

# SLAs
SLA = {"completion": 90, "wait_h": 24, "csat": 4.0, "coverage": 75}

# Tabs
tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo Mensal", "Dicionário"])

# 1) Visão Geral
with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    p = get_payload()
    if not p:
        st.info("Sem dados para este mês.")
    else:
        total = completed = csat = wait_h = evaluated = coverage = np.nan

        df = p.get("total_atendimentos")
        if isinstance(df, pd.DataFrame): 
            total = safe_sum(df)

        df = p.get("total_atendimentos_conc")
        if isinstance(df, pd.DataFrame): 
            completed = safe_sum(df)

        df = p.get("media_csat")
        if isinstance(df, pd.DataFrame) and (col := find(df, ["avg", "Média CSAT"])):
            csat = pd.to_numeric(df[col], errors="coerce").mean()

        df = p.get("tme_geral")
        if isinstance(df, pd.DataFrame) and (col := find(df, ["mean_total", "Tempo médio de espera"])):
            wait_h = to_hours(df[col]).mean()

        df = p.get("csat")
        if isinstance(df, pd.DataFrame) and (col := find(df, ["score_total", "Respostas CSAT"])):
            evaluated = pd.to_numeric(df[col], errors="coerce").sum()

        coverage = evaluated / completed * 100 if completed and completed > 0 else np.nan
        completion_pct = completed / total * 100 if total and total > 0 else np.nan

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Conclusão >90%", f"{completion_pct:.1f}%" if not pd.isna(completion_pct) else "-", 
                  delta="OK" if completion_pct >= SLA["completion"] else "BAIXO")
        c2.metric("1º Resp <24h", f"{wait_h:.2f}h" if not pd.isna(wait_h) else "-", 
                  delta="OK" if wait_h < SLA["wait_h"] else "ALTO")
        c3.metric("CSAT ≥4.0", f"{csat:.2f}" if not pd.isna(csat) else "-", 
                  delta="OK" if csat >= SLA["csat"] else "BAIXO")
        c4.metric("Cobertura ≥75%", f"{coverage:.1f}%" if not pd.isna(coverage) else "-", 
                  delta="OK" if coverage >= SLA["coverage"] else "BAIXO")

        df_dist = p.get("csat")
        if isinstance(df_dist, pd.DataFrame):
            cat_col = find(df_dist, ["Categoria"])
            val_col = find(df_dist, ["score_total", "Respostas CSAT"])
            if cat_col and val_col:
                dist = df_dist[[cat_col, val_col]].groupby(cat_col).sum().reset_index()
                dist.columns = ["Categoria", "Total"]
                order = ["Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"]
                dist["Categoria"] = pd.Categorical(dist["Categoria"], order, ordered=True)
                dist = dist.sort_values("Categoria")
                fig = px.bar(dist, x="Categoria", y="Total", title="Distribuição CSAT")
                st.plotly_chart(fig, use_container_width=True)

# 2) Por Canal
with tabs[1]:
    st.subheader(f"Por Canal — {mk}")
    dfc = get_by_channel()
    if dfc is None:
        st.info("Sem dados por canal")
    else:
        channels = sorted(dfc["Canal"].unique())
        sel = st.multiselect("Canais", channels, default=channels)
        dfc = dfc[dfc["Canal"].isin(sel)]

        col1, col2 = st.columns(2)
        with col1:
            if (col := find(dfc, ["Tempo médio de atendimento", "mean_total"])):
                dfc["TMA (h)"] = to_hours(dfc[col])
                st.plotly_chart(px.bar(dfc, x="Canal", y="TMA (h)", title="TMA por Canal (horas)"), use_container_width=True)
        with col2:
            if (col := find(dfc, ["Tempo médio de espera", "mean_wait"])):
                dfc["TME (h)"] = to_hours(dfc[col])
                st.plotly_chart(px.bar(dfc, x="Canal", y="TME (h)", title="TME por Canal (horas)"), use_container_width=True)

        if (col := find(dfc, ["Média CSAT"])):
            st.markdown("### CSAT Médio por Canal")
            st.plotly_chart(px.bar(dfc, x="Canal", y=col, title="CSAT Médio"), use_container_width=True)

# 3) Comparativo Mensal
with tabs[2]:
    st.subheader("Comparativo Mensal")
    months = sorted(st.session_state["months"].keys())
    if len(months) < 2:
        st.info("Mínimo 2 meses")
    else:
        rows = []
        for m in months:
            p = st.session_state["months"][m]
            total = completed = csat = wait_h = coverage = np.nan
            df = p.get("total_atendimentos")
            if isinstance(df, pd.DataFrame): total = safe_sum(df)
            df = p.get("total_atendimentos_conc")
            if isinstance(df, pd.DataFrame): completed = safe_sum(df)
            df = p.get("media_csat")
            if isinstance(df, pd.DataFrame) and (col := find(df, ["avg"])): 
                csat = pd.to_numeric(df[col], errors="coerce").mean()
            df = p.get("tme_geral")
            if isinstance(df, pd.DataFrame) and (col := find(df, ["mean_total"])): 
                wait_h = to_hours(df[col]).mean()
            evaluated = 0
            df = p.get("csat")
            if isinstance(df, pd.DataFrame) and (col := find(df, ["score_total"])): 
                evaluated = pd.to_numeric(df[col], errors="coerce").sum()
            coverage = evaluated / completed * 100 if completed and completed > 0 else np.nan
            rows.append({
                "Mês": m,
                "Conclusão (%)": completed/total*100 if total else np.nan,
                "TME (h)": wait_h,
                "CSAT": csat,
                "Cobertura (%)": coverage
            })
        comp = pd.DataFrame(rows)
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.line(comp, x="Mês", y="Conclusão (%)", title="Conclusão"), use_container_width=True)
            st.plotly_chart(px.line(comp, x="Mês", y="CSAT", title="CSAT Médio"), use_container_width=True)
        with c2:
            st.plotly_chart(px.line(comp, x="Mês", y="TME (h)", title="TME (h)"), use_container_width=True)
            st.plotly_chart(px.line(comp, x="Mês", y="Cobertura (%)", title="Cobertura"), use_container_width=True)

# 4) Dicionário
with tabs[3]:
    st.markdown("""
### Dicionário de Dados
- `_data_product__csat_*.csv` → CSAT por categoria  
- `_data_product__media_csat_*.csv` → Média CSAT  
- `tempo_medio_de_atendimento_por_canal_*.csv` → TMA por canal  
- `tempo_medio_de_atendimento_*.csv` → TMA geral  
- `tempo_medio_de_espera_*.csv` → TME geral  
- `total_de_atendimentos_*.csv` → Total de atendimentos  
- `total_de_atendimentos_concluidos_*.csv` → Atendimentos concluídos  
""")
