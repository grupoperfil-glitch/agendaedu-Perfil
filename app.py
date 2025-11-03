# app.py — Dashboard CSAT (XLSX) — GitHub via ZIP + Upload mensal
# ---------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl requests
#
# Secrets (opcionais) — .streamlit/secrets.toml:
#   GITHUB_DATA_TOKEN   = "ghp_xxx"                         # opcional (evita alguns limites)
#   GITHUB_DATA_REPO    = "grupoperfil-glitch/csat-dashboard-data"
#   GITHUB_DATA_BRANCH  = "main"
#   GITHUB_DATA_PATH    = "data"                            # subpasta contendo os .xlsx
#
# O app:
#  - Baixa o repositório como ZIP (codeload.github.com) e lê TODOS os .xlsx dentro de GITHUB_DATA_PATH,
#    agrupando por mês (YYYY-MM) encontrado no caminho ou no nome do arquivo (mesmo com timestamp).
#  - Aceita upload múltiplo mensal; reconhece os arquivos por palavras-chave no nome.
#  - Converte “Tempo médio de atendimento” para HORAS (regra estrita).
#  - Inclui aba “Análise dos Canais” (piores por menor quantidade de respostas e por menor nota).

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

def extract_month_from_any(s: str) -> Optional[str]:
    """Extrai a primeira ocorrência de AAAA-MM em um caminho ou nome."""
    m = re.search(r"\d{4}-\d{2}", s)
    return m.group(0) if m else None

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
    headers = {}
    if GH_TOKEN:
        # codeload normalmente funciona sem token para públicos; deixo header caso precise
        headers["Authorization"] = f"token {GH_TOKEN}"
    try:
        r = requests.get(RAW_ZIP_URL, headers=headers, timeout=120)
        LAST_GH_STATUS.append(f"GET {RAW_ZIP_URL} -> {r.status_code}")
        if r.status_code != 200:
            return None
        return r.content
    except Exception as e:
        LAST_GH_STATUS.append(f"ERR ZIP: {e}")
        return None

def group_zip_files_by_month(zf: ZipFile) -> Dict[str, List[str]]:
    """
    Dentro do ZIP, encontra todos os .xlsx sob a pasta GH_PATH (recursivo) e agrupa por mês.
    """
    names = zf.namelist()
    months: Dict[str, List[str]] = {}
    # raiz do zip costuma ser "<repo>-<branch>/"
    root = names[0].split("/")[0] if names else ""
    base_prefix = f"{root}/{GH_PATH.strip('/')}/" if GH_PATH else f"{root}/"

    for n in names:
        if not n.lower().endswith(".xlsx"):
            continue
        if base_prefix and not n.startswith(base_prefix):
            continue
        month = None
        # tenta extrair AAAA-MM em qualquer segmento
        parts = n.split("/")
        for seg in parts:
            m = extract_month_from_any(seg)
            if m:
                month = m
                break
        if not month:
            # tenta no nome do arquivo
            month = extract_month_from_any(os.path.basename(n))
        if not month:
            continue
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
            df = load_xlsx_from_bytes(b)
            payload[kind] = df
        except Exception:
            LAST_GH_STATUS.append(f"Falha ao ler XLSX do ZIP: {sel}")
    return build_by_channel(payload)

def load_all_github_months_via_zip(force: bool = False) -> Tuple[int, int]:
    """
    Carrega todos os meses lendo o repositório via ZIP. Retorna (#meses, #arquivos).
    """
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
st.set_page_config(page_title="Dashboard CSAT — GitHub (ZIP) + Upload", layout="wide")
st.title("Dashboard CSAT (XLSX) — Fonte GitHub (ZIP) + Upload mensal")
st.caption(f"Fonte GitHub: **{GH_REPO} / {GH_BRANCH} / {GH_PATH}** — leitura via ZIP (sem /git/trees).")

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
                    df.to_excel(os.path.join(folder, f"{kind}.xlsx"), index=False)
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
                v = int(pd.to_numeric(df.select_dtypes(include=[np.number]), errors="coerce").sum().sum())
                st.metric("Total de Atendimentos (arquivo)", v)
        with cols[1]:
            df = payload.get("total_atendimentos_conc")
            if isinstance(df, pd.DataFrame) and not df.empty:
                v = int(pd.to_numeric(df.select_dtypes(include=[np.number]), errors="coerce").sum().sum())
                st.metric("Atendimentos Concluídos (arquivo)", v)
        with cols[2]:
            df = payload.get("tma_geral")
            if isinstance(df, pd.DataFrame) and not df.empty:
                tcol = find_best_column(df, ["mean_total HH:MM:SS","mean_total","Tempo médio de atendimento","tempo medio de atendimento"])
                if tcol:
                    v = to_hours_strict(df[tcol]).mean()
                    st.metric("Tempo médio de atendimento (h) — geral", f"{v:.2f}")

        st.write("### Tabelas disponíveis no mês")
        for k, vdf in payload.items():
            if isinstance(vdf, pd.DataFrame):
                st.markdown(f"**{k}**")
                st.dataframe(vdf.head(50), use_container_width=True)

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
    st.caption("Exibe, por mês, os canais com MENOR quantidade de respostas do CSAT (se disponível) e as MENORES notas de CSAT.")

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
