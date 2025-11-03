# app.py — Dashboard CSAT (XLSX) — GitHub + Upload mensal (nomes com timestamp)
# -----------------------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl requests
#
# Secrets recomendados (Streamlit Cloud / .streamlit/secrets.toml):
#   GITHUB_DATA_TOKEN   = "ghp_xxx"                         # opcional (evita rate limit)
#   GITHUB_DATA_REPO    = "grupoperfil-glitch/csat-dashboard-data"
#   GITHUB_DATA_BRANCH  = "main"
#   GITHUB_DATA_PATH    = "data"                            # onde estão os .xlsx
#
# O app:
#  - Lê do GitHub os .xlsx, com ou sem subpastas YYYY-MM, aceitando nomes com timestamps.
#  - Aceita upload múltiplo mensal (vários .xlsx de uma vez); detecta o tipo pelo nome.
#  - Converte “Tempo médio de atendimento” para HORAS de forma estrita.
#  - Traz a aba “Análise dos Canais”.

from __future__ import annotations
import os, re, json, base64
from io import BytesIO
from datetime import date
from typing import Dict, List, Optional

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
GH_API_BASE = "https://api.github.com"

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

def detect_kind(filename: str) -> Optional[str]:
    low = filename.lower()
    for kind, tokens in KEYS.items():
        for t in tokens:
            if t in low and low.endswith(".xlsx"):
                return kind
    return None

def extract_month_from_name(name: str) -> Optional[str]:
    # pega a primeira ocorrência de AAAA-MM
    m = re.search(r"\d{4}-\d{2}", name)
    return m.group(0) if m else None

# ======================
# Build payloads
# ======================
def build_by_channel(payload: dict) -> dict:
    """Monta/atualiza payload['by_channel'] unificando qualquer DF com coluna 'Canal'."""
    dfs = []
    for k, df in payload.items():
        if isinstance(df, pd.DataFrame) and "Canal" in normalize_canal_column(df).columns:
            dfs.append(normalize_canal_column(df.copy()))

    merged = None
    for df in dfs:
        merged = df.copy() if merged is None else merged.merge(df, on="Canal", how="outer")

    # Deriva 'Média CSAT' padronizada se existir 'avg'/'media' etc.
    if isinstance(merged, pd.DataFrame):
        mcol = find_best_column(merged, ["Média CSAT","media csat","avg","media"])
        if mcol and mcol != "Média CSAT":
            merged = merged.rename(columns={mcol: "Média CSAT"})
        # Se houverem contagens com nomes diferentes, tenta padronizar
        ccol = find_best_column(merged, [
            "Respostas CSAT","Quantidade de respostas CSAT","score_total","ratings",
            "Total de avaliações","avaliacoes","avaliações","qtd","qtde"
        ])
        if ccol and ccol != "Respostas CSAT":
            merged = merged.rename(columns={ccol: "Respostas CSAT"})
        payload["by_channel"] = merged

    return payload

# ======================
# GitHub fetch
# ======================
def gh_headers() -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json"}
    if GH_TOKEN:
        h["Authorization"] = f"token {GH_TOKEN}"
    return h

def gh_list_contents(path: str) -> List[dict]:
    url = f"{GH_API_BASE}/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}"
    r = requests.get(url, headers=gh_headers(), timeout=30)
    if r.status_code != 200:
        return []
    data = r.json()
    return data if isinstance(data, list) else []

def gh_download_bytes(download_url: str) -> Optional[bytes]:
    r = requests.get(download_url, headers=gh_headers(), timeout=60)
    return r.content if r.status_code == 200 else None

def group_repo_files_by_month() -> Dict[str, List[dict]]:
    """
    Pesquisa GH_PATH e agrupa arquivos por mês (YYYY-MM), aceitando:
      - data/YYYY-MM/<arquivos>.xlsx
      - data/<arquivos_com_YYYY-MM_no_nome>.xlsx
    """
    out: Dict[str, List[dict]] = {}
    # 1) lista raiz GH_PATH
    root_items = gh_list_contents(GH_PATH)
    for it in root_items:
        t = it.get("type")
        name = it.get("name","")
        if t == "dir":
            # subpasta; se for YYYY-MM, usa os .xlsx de dentro
            if re.fullmatch(r"\d{4}-\d{2}", name):
                month = name
                for sub in gh_list_contents(f"{GH_PATH}/{name}"):
                    if sub.get("type") == "file" and sub.get("name","").lower().endswith(".xlsx"):
                        out.setdefault(month, []).append(sub)
            else:
                # pasta livre; se tiver .xlsx com mês no nome, captura
                for sub in gh_list_contents(f"{GH_PATH}/{name}"):
                    if sub.get("type") == "file" and sub.get("name","").lower().endswith(".xlsx"):
                        m = extract_month_from_name(sub.get("name",""))
                        if m:
                            out.setdefault(m, []).append(sub)
        elif t == "file" and name.lower().endswith(".xlsx"):
            m = extract_month_from_name(name)
            if m:
                out.setdefault(m, []).append(it)
    return out

def gh_read_month_payload_from_items(items: List[dict]) -> dict:
    payload: dict = {}
    # seleciona o arquivo mais "recente" por tipo (heurística: nome mais longo/lexicograficamente maior)
    by_kind: Dict[str, List[dict]] = {}
    for it in items:
        k = detect_kind(it.get("name",""))
        if k:
            by_kind.setdefault(k, []).append(it)
    for kind, lst in by_kind.items():
        # pega o último por nome (geralmente tem timestamp mais à direita)
        sel = sorted(lst, key=lambda x: x.get("name",""))[-1]
        b = gh_download_bytes(sel.get("download_url",""))
        if b:
            df = load_xlsx_from_bytes(b)
            payload[kind] = df
    return build_by_channel(payload)

def load_all_github_months_into_state(force: bool = False) -> int:
    grouped = group_repo_files_by_month()
    loaded = 0
    for m, items in sorted(grouped.items()):
        if not force and m in st.session_state["months"]:
            continue
        payload = gh_read_month_payload_from_items(items)
        if payload:
            st.session_state["months"][m] = payload
            loaded += 1
    return loaded

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
        if not f.lower().endswith(".xlsx"):
            continue
        kind = detect_kind(f)
        if not kind:
            continue
        try:
            df = load_xlsx(os.path.join(folder, f))
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
# Upload mensal (multi-arquivos)
# ======================
def ingest_uploaded_files(files: List) -> Dict[str, pd.DataFrame]:
    """Recebe múltiplos uploads e retorna payload parcial (por tipo)."""
    payload: Dict[str, pd.DataFrame] = {}
    for fl in files:
        name = fl.name
        kind = detect_kind(name)
        if not kind:
            continue
        try:
            df = load_xlsx(fl)
            payload[kind] = df
        except Exception:
            pass
    return payload

# ======================
# Streamlit App
# ======================
st.set_page_config(page_title="Dashboard CSAT — GitHub + Upload", layout="wide")
st.title("Dashboard CSAT (XLSX) — Fonte GitHub + Upload mensal")
st.caption(f"Fonte GitHub padrão: **{GH_REPO} / {GH_BRANCH} / {GH_PATH}**. Aceita uploads com nomes contendo palavras-chave e timestamps.")

# Estado
if "months" not in st.session_state:
    st.session_state["months"] = {}

# Carrega do GitHub na inicialização
_gh_loaded = load_all_github_months_into_state(force=False)
_local_loaded = load_all_local_months_into_state()

# Sidebar
with st.sidebar:
    st.header("Parâmetros do Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, value=today.month, step=1)
    year  = st.number_input("Ano", 2000, 2100, value=today.year, step=1)
    mk = month_key(int(year), int(month))

    st.write("---")
    st.markdown("**Fonte dos dados**")
    st.write(f"GitHub: `{GH_REPO}` / `{GH_BRANCH}` / `{GH_PATH}`")
    if GH_TOKEN:
        st.success("Token GitHub detectado.")
    else:
        st.info("Sem token GitHub: usando requisições públicas.")

    if st.button("Recarregar do GitHub (todos os meses)"):
        loaded = load_all_github_months_into_state(force=True)
        st.success(f"Recarregados do GitHub: {loaded} mês(es).")

    st.write("---")
    st.subheader("Upload mensal (.xlsx)")
    st.caption("Solte **todos** os arquivos do mês (ex.: data_product__*, tempo_medio_*, total_*).")
    ups = st.file_uploader("Arraste/Selecione os arquivos", type=["xlsx"], accept_multiple_files=True)
    if ups:
        st.caption("Opcional: informe o **mês** desses arquivos (YYYY-MM). Se não informar, uso o mês atual.")
        up_month = st.text_input("Mês destino (YYYY-MM)", value=mk)

        if st.button("Carregar arquivos enviados neste mês"):
            partial = ingest_uploaded_files(ups)
            if not partial:
                st.warning("Não reconheci nenhum arquivo pelos nomes. Verifique se os nomes contêm as palavras-chave.")
            else:
                payload = st.session_state["months"].get(up_month, {})
                payload.update(partial)
                payload = build_by_channel(payload)
                st.session_state["months"][up_month] = payload
                # salva local
                folder = os.path.join(LOCAL_STORE_DIR, up_month)
                ensure_dir(folder)
                for kind, df in partial.items():
                    fname = f"{kind}.xlsx"
                    df.to_excel(os.path.join(folder, fname), index=False)
                st.success(f"{len(partial)} arquivo(s) anexado(s) ao mês {up_month} e salvo(s) em disco.")

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
        cols = st.columns(3)
        with cols[0]:
            df = payload.get("total_atendimentos")
            if isinstance(df, pd.DataFrame) and not df.empty:
                st.metric("Total de Atendimentos (arquivo)", int(pd.to_numeric(df.select_dtypes(include=[np.number]), errors="coerce").sum().sum()))
        with cols[1]:
            df = payload.get("total_atendimentos_conc")
            if isinstance(df, pd.DataFrame) and not df.empty:
                st.metric("Atendimentos Concluídos (arquivo)", int(pd.to_numeric(df.select_dtypes(include=[np.number]), errors="coerce").sum().sum()))
        with cols[2]:
            df = payload.get("tma_geral")
            if isinstance(df, pd.DataFrame) and not df.empty:
                # tenta uma coluna de tempo
                tcol = find_best_column(df, ["mean_total HH:MM:SS","mean_total","Tempo médio de atendimento","tempo medio de atendimento"])
                if tcol:
                    v = to_hours_strict(df[tcol]).mean()
                    st.metric("Tempo médio de atendimento (h) — geral", f"{v:.2f}")

        st.write("### Tabelas disponíveis no mês")
        for k, v in payload.items():
            if isinstance(v, pd.DataFrame):
                st.markdown(f"**{k}**")
                st.dataframe(v.head(50), use_container_width=True)

# 2) Por Canal
with tabs[1]:
    st.subheader(f"Por Canal — {mk}")
    dfc = get_current_by_channel()
    if dfc is None:
        st.info("Sem dados por canal para o mês atual.")
    else:
        dfc = normalize_canal_column(dfc)

        col3, col4 = st.columns(2)

        # Tempo médio de atendimento (horas) — ESTRITO
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

        # Tempo médio de espera (horas) — ESTRITO (se existir por canal)
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

# 3) Comparativo Mensal (exemplo simples)
with tabs[2]:
    st.subheader("Comparativo Mensal — resumo")
    months_dict = st.session_state["months"]
    if not months_dict:
        st.info("Nenhum mês carregado.")
    else:
        rows = []
        for mkey, payload in sorted(months_dict.items()):
            df = payload.get("by_channel")
            if isinstance(df, pd.DataFrame) and not df.empty:
                csat_col = find_best_column(df, ["Média CSAT","media csat","avg","media"])
                if csat_col:
                    v = pd.to_numeric(df[csat_col], errors="coerce").mean()
                    rows.append({"mes": mkey, "Média CSAT (global)": v})
        if rows:
            dd = pd.DataFrame(rows)
            st.plotly_chart(px.line(dd, x="mes", y="Média CSAT (global)", title="Média CSAT global por mês"), use_container_width=True)
            st.dataframe(dd, use_container_width=True)
        else:
            st.info("Não foi possível montar o comparativo (faltam colunas de CSAT).")

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

# 5) Análise dos Canais
with tabs[4]:
    st.subheader("Análise dos Canais")
    st.caption("Exibe, por mês, os canais com MENOR quantidade de respostas de CSAT (se disponível) e as MENORES notas de CSAT.")

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

            # contagem
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

            # média csat
            scol = None
            for c in csat_candidates:
                if c.lower() in colmap: scol = colmap[c.lower()]; break
            if scol is not None:
                tmp2 = df[["Canal", scol]].copy()
                tmp2[scol] = pd.to_numeric(tmp2[scol], errors="coerce")
                tmp2 = tmp2.dropna()
                if not tmp2.empty:
                    tmp2 = tmp2.rename(columns={scol: "Média CSAT"})
                    tmp2["mes"] = mkey
                    rec_scores.append(tmp2)

        colA, colB = st.columns(2)

        with colA:
            st.markdown("**Menor quantidade de respostas do CSAT por mês**")
            n_counts = st.number_input("Quantos canais exibir (menores quantidades)?", 1, 10, 3, 1, key="n_counts")
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
            st.markdown("**Menores notas de CSAT por mês**")
            n_scores = st.number_input("Quantos canais exibir (menores notas)?", 1, 10, 3, 1, key="n_scores")
            if not rec_scores:
                st.info("Não encontrei coluna de 'Média CSAT' nos dados por canal dos meses persistidos.")
            else:
                dd2 = pd.concat(rec_scores, ignore_index=True)
                tops2 = [g.sort_values("Média CSAT", ascending=True).head(int(n_scores)) for _, g in dd2.groupby("mes", as_index=False)]
                dd2_top = pd.concat(tops2, ignore_index=True)
                st.plotly_chart(px.bar(dd2_top, x="mes", y="Média CSAT", color="Canal",
                                       barmode="group", title="Menores notas de CSAT por mês"),
                                use_container_width=True)
                st.dataframe(dd2_top.sort_values(["mes","Média CSAT","Canal"]), use_container_width=True)
