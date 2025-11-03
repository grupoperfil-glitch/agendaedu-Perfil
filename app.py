# app.py — Dashboard CSAT Mensal (XLSX) — Persistência GitHub/Local
# ---------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl requests
#
# Abas:
#   1) Visão Geral
#   2) Por Canal  ---> CONVERSÃO ESTRITA p/ horas em "Tempo médio de atendimento (horas)"
#   3) Comparativo Mensal
#   4) Dicionário de Dados
#   5) Análise dos Canais

from __future__ import annotations
import os
from io import BytesIO
from datetime import date
import base64
import json

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# =========================
# Persistência (local/GitHub)
# =========================
LOCAL_STORE_DIR = "data_store"

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

def load_xlsx(file: BytesIO | str) -> pd.DataFrame:
    """Carrega Excel. Tenta 'Resultado da consulta', senão 1ª aba."""
    try:
        xl = pd.ExcelFile(file)
        sheet = "Resultado da consulta" if "Resultado da consulta" in xl.sheet_names else xl.sheet_names[0]
        return xl.parse(sheet)
    except Exception:
        return pd.read_excel(file)

def read_local_month_payload(y: int, m: int) -> dict:
    mk = month_key(y, m)
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    payload = {}
    if not os.path.isdir(folder):
        return payload

    def try_read(startswith_list: list[str]) -> pd.DataFrame | None:
        for f in os.listdir(folder):
            low = f.lower()
            if any(low.startswith(p) and low.endswith(".xlsx") for p in startswith_list):
                try:
                    return load_xlsx(os.path.join(folder, f))
                except Exception:
                    pass
        return None

    df_csat  = try_read(["data_product__csat"])
    df_media = try_read(["data_product__media_csat"])
    df_tma   = try_read(["tempo_medio_de_atendimento", "tempo_medio_atendimento", "tma"])

    if df_csat  is not None: payload["csat"] = df_csat
    if df_media is not None: payload["media_csat"] = df_media
    if df_tma   is not None: payload["tma_por_canal"] = df_tma

    payload = build_by_channel(payload)
    return payload

def save_df_local(df: pd.DataFrame, path: str) -> None:
    ensure_dir(os.path.dirname(path))
    df.to_excel(path, index=False)

def write_local_month_payload(y: int, m: int, payload: dict, save_flags: dict):
    mk = month_key(y, m)
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    ensure_dir(folder)

    if save_flags.get("save_local", True):
        if "csat" in payload:        save_df_local(payload["csat"], os.path.join(folder, "data_product__csat.xlsx"))
        if "media_csat" in payload:  save_df_local(payload["media_csat"], os.path.join(folder, "data_product__media_csat.xlsx"))
        if "tma_por_canal" in payload: save_df_local(payload["tma_por_canal"], os.path.join(folder, "tempo_medio_de_atendimento.xlsx"))

    if save_flags.get("save_github"):
        gh_token = os.getenv("GITHUB_TOKEN")
        gh_repo  = os.getenv("GITHUB_REPO")  # ex.: "org/repo"
        gh_path  = os.getenv("GITHUB_PATH", "data_store").strip("/")
        if gh_token and gh_repo:
            import requests
            base_url = f"https://api.github.com/repos/{gh_repo}/contents"
            branch = os.getenv("GITHUB_BRANCH", "main")

            def push_df(df: pd.DataFrame, relname: str):
                path = f"{gh_path}/{mk}/{relname}"
                buf = BytesIO()
                df.to_excel(buf, index=False)
                content_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
                # get sha (if exists)
                r = requests.get(f"{base_url}/{path}", headers={"Authorization": f"token {gh_token}"})
                sha = r.json().get("sha") if r.status_code == 200 else None
                body = {"message": f"update {path}", "content": content_b64, "branch": branch}
                if sha: body["sha"] = sha
                r2 = requests.put(f"{base_url}/{path}", headers={"Authorization": f"token {gh_token}"}, data=json.dumps(body))
                if r2.status_code not in (200, 201):
                    st.warning(f"Falha ao enviar {relname} ao GitHub ({r2.status_code}).")

            if "csat" in payload:        push_df(payload["csat"], "data_product__csat.xlsx")
            if "media_csat" in payload:  push_df(payload["media_csat"], "data_product__media_csat.xlsx")
            if "tma_por_canal" in payload: push_df(payload["tma_por_canal"], "tempo_medio_de_atendimento.xlsx")
        else:
            st.info("Persistência GitHub não configurada (defina GITHUB_TOKEN e GITHUB_REPO).")

# =========================
# Funções de dados
# =========================
def normalize_canal_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Canal" in df.columns:
        return df
    lower = {str(c).strip().lower(): c for c in df.columns}
    for alias in ["categoria", "canal", "channel", "categoria/canal"]:
        if alias in lower:
            return df.rename(columns={lower[alias]: "Canal"})
    return df

def find_best_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        if c.strip().lower() in lower:
            return lower[c.strip().lower()]
    return None

def to_hours_strict(series: pd.Series) -> pd.Series:
    """Conversão ESTRITA:
       - valores com ':' -> HH:MM:SS -> horas
       - valores numéricos -> SEMPRE segundos -> horas
    """
    s_str = series.astype(str)
    has_colon = s_str.str.contains(":")
    out = pd.Series(index=series.index, dtype="float64")

    # casos HH:MM:SS
    td = pd.to_timedelta(s_str.where(has_colon, None), errors="coerce")
    out.loc[has_colon] = td.dt.total_seconds() / 3600.0

    # casos numéricos (segundos)
    s_num = pd.to_numeric(s_str.where(~has_colon, None), errors="coerce")
    out.loc[~has_colon] = s_num / 3600.0

    return out

def build_by_channel(payload: dict) -> dict:
    """Monta/atualiza payload['by_channel'] a partir de TMA, média CSAT e contagem."""
    df_tma   = payload.get("tma_por_canal")
    df_media = payload.get("media_csat")
    df_csat  = payload.get("csat")

    if isinstance(df_tma, pd.DataFrame) and not df_tma.empty:
        df_tma = normalize_canal_column(df_tma.copy())
    else:
        df_tma = None

    if isinstance(df_media, pd.DataFrame) and not df_media.empty:
        df_media = normalize_canal_column(df_media.copy())
        # normaliza nome da média
        mcol = find_best_column(df_media, ["Média CSAT","media csat","avg","media"])
        if mcol and mcol != "Média CSAT":
            df_media = df_media.rename(columns={mcol: "Média CSAT"})
    else:
        df_media = None

    # Deriva contagem de respostas a partir do csat (se existir)
    csat_counts = None
    if isinstance(df_csat, pd.DataFrame) and not df_csat.empty:
        df_csat = normalize_canal_column(df_csat.copy())
        ccol = find_best_column(df_csat, [
            "Respostas CSAT","Quantidade de respostas CSAT","score_total","ratings",
            "Total de avaliações","avaliacoes","avaliações","qtd","qtde"
        ])
        if ccol:
            csat_counts = df_csat.groupby("Canal", as_index=False)[ccol].sum().rename(columns={ccol:"Respostas CSAT"})

    merged = None
    for df in [df_tma, df_media, csat_counts]:
        if isinstance(df, pd.DataFrame):
            merged = df.copy() if merged is None else merged.merge(df, on="Canal", how="outer")

    if isinstance(merged, pd.DataFrame):
        payload["by_channel"] = merged

    return payload

# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="Dashboard CSAT Mensal (XLSX) — Persistência GitHub", layout="wide")
st.title("Dashboard CSAT Mensal (XLSX) — Persistência GitHub")
st.caption("Arquivos por mês ficam salvos no repositório GitHub configurado e em `data_store/` como fallback.")

# Estado
if "months" not in st.session_state:
    st.session_state["months"] = {}

# Sidebar
with st.sidebar:
    st.header("Parâmetros do Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, value=today.month, step=1)
    year  = st.number_input("Ano", 2000, 2100, value=today.year, step=1)
    mk = month_key(int(year), int(month))

    st.write("---")
    save_local  = st.checkbox("Salvar em disco (fallback local)", value=True)
    save_github = st.checkbox("Salvar no GitHub (persistência durável)", value=False)

    st.write("---")
    st.subheader("Upload dos arquivos (.xlsx)")
    st.caption('Cada arquivo deve conter a aba "Resultado da consulta".')

    up_csat  = st.file_uploader("data_product__csat*.xlsx (Categoria, score_total)", type=["xlsx"], key="up1")
    up_media = st.file_uploader("data_product__media_csat*.xlsx (avg)", type=["xlsx"], key="up2")
    up_tma   = st.file_uploader("tempo_medio_de_atendimento_*.xlsx (mean_total HH:MM:SS)", type=["xlsx"], key="up3")

    if st.button("Carregar e salvar este mês"):
        payload = st.session_state["months"].get(mk, {}).copy()
        if up_csat is not None:  payload["csat"] = load_xlsx(up_csat)
        if up_media is not None: payload["media_csat"] = load_xlsx(up_media)
        if up_tma is not None:   payload["tma_por_canal"] = load_xlsx(up_tma)

        payload = build_by_channel(payload)
        write_local_month_payload(int(year), int(month), payload, {"save_local": save_local, "save_github": save_github})
        st.session_state["months"][mk] = payload
        st.success(f"Arquivos do mês {mk} carregados e armazenados.")

    if st.button("Recarregar do disco este mês"):
        payload = read_local_month_payload(int(year), int(month))
        if payload:
            st.session_state["months"][mk] = payload
            st.success(f"Mês {mk} recarregado do disco.")
        else:
            st.info("Nada encontrado no disco para este mês.")

# Carrega todos os meses do disco (se existirem) ao iniciar
def load_all_local_months_into_state():
    if not os.path.isdir(LOCAL_STORE_DIR):
        return
    for name in sorted(os.listdir(LOCAL_STORE_DIR)):
        p = os.path.join(LOCAL_STORE_DIR, name)
        if os.path.isdir(p) and len(name) == 7 and name[4] == "-":
            try:
                y, m = map(int, name.split("-"))
                payload = read_local_month_payload(y, m)
                if payload and name not in st.session_state["months"]:
                    st.session_state["months"][name] = payload
            except Exception:
                pass
load_all_local_months_into_state()

# Helper
def get_current_by_channel() -> pd.DataFrame | None:
    payload = st.session_state["months"].get(mk, {})
    df = payload.get("by_channel")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.copy()
    for v in payload.values():
        if isinstance(v, pd.DataFrame) and "Canal" in v.columns:
            return v.copy()
    return None

# Abas
tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo Mensal", "Dicionário de Dados", "Análise dos Canais"])

# 1) Visão Geral
with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    if st.session_state["months"]:
        st.write(f"Meses carregados: `{', '.join(sorted(st.session_state['months'].keys()))}`")
    dfc = get_current_by_channel()
    if dfc is None:
        st.info("Carregue os arquivos do mês no menu lateral para visualizar os painéis.")
    else:
        st.dataframe(dfc.head(50), use_container_width=True)

# 2) Por Canal
with tabs[1]:
    st.subheader(f"Por Canal — {mk}")
    dfc = get_current_by_channel()
    if dfc is None:
        st.info("Sem dados por canal para o mês atual.")
    else:
        dfc = normalize_canal_column(dfc)

        col3, col4 = st.columns(2)

        # ---- Tempo médio de atendimento (HORAS) — CONVERSÃO ESTRITA ----
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
                    px.bar(
                        dft, x="Canal", y="Tempo médio de atendimento (horas)",
                        title="Tempo médio de atendimento (horas)"
                    ),
                    use_container_width=True
                )

        # ---- Tempo médio de espera (HORAS) — mesma regra ----
        with col4:
            cand_wait = [
                "mean_wait HH:MM:SS","mean_wait","Tempo médio de espera",
                "Tempo medio de espera","wait_seconds","mean_wait_seconds",
                "Tempo médio de espera (s)","espera em segundos"
            ]
            wcol = find_best_column(dfc, cand_wait)
            if wcol is None:
                st.info("Coluna de tempo de espera não encontrada para este mês.")
            else:
                dfw = dfc.copy()
                dfw["Tempo médio de espera (horas)"] = to_hours_strict(dfw[wcol])
                st.plotly_chart(
                    px.bar(
                        dfw, x="Canal", y="Tempo médio de espera (horas)",
                        title="Tempo médio de espera (horas)"
                    ),
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
                    if isinstance(v, pd.DataFrame) and "Canal" in v.columns:
                        df = v
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
                st.warning("Não encontrei uma coluna de contagem de respostas por canal nos dados persistidos.")
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
