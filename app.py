# app.py — Dashboard CSAT Mensal (XLSX) — Persistência GitHub/Local
# ---------------------------------------------------------------
# Requisitos:
#   pip install streamlit plotly pandas numpy openpyxl
#   (Opcional GitHub) configurar variáveis de ambiente:
#     GITHUB_TOKEN, GITHUB_REPO (ex.: "org/repo"), GITHUB_PATH (ex.: "data_store")
#
# Estrutura de pastas local:
#   data_store/
#     2025-09/
#       data_product__csat.xlsx
#       data_product__media_csat.xlsx
#       tempo_medio_de_atendimento.xlsx
#
# Abas:
#   1) Visão Geral
#   2) Por Canal  ---> CONVERSÃO ROBUSTA para horas no gráfico "Tempo médio de atendimento (h)"
#   3) Comparativo Mensal
#   4) Dicionário de Dados
#   5) Análise dos Canais  ---> lê do mesmo store (session_state e data_store)

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

# --------------------------
# Utilidades de persistência
# --------------------------

LOCAL_STORE_DIR = "data_store"

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

def save_df_local(df: pd.DataFrame, path: str) -> None:
    ensure_dir(os.path.dirname(path))
    # Salva como .xlsx
    try:
        df.to_excel(path, index=False)
    except Exception:
        # Fallback CSV
        df.to_csv(path.replace(".xlsx", ".csv"), index=False, encoding="utf-8-sig")

def load_xlsx(file: BytesIO | str) -> pd.DataFrame:
    """Carrega Excel. Tenta ler a aba 'Resultado da consulta' e, se não existir, pega a primeira."""
    try:
        xl = pd.ExcelFile(file)
        sheet = "Resultado da consulta" if "Resultado da consulta" in xl.sheet_names else xl.sheet_names[0]
        return xl.parse(sheet)
    except Exception:
        # Fallback: tenta ler direto
        return pd.read_excel(file)

def read_local_month_payload(y: int, m: int) -> dict:
    """Lê arquivos do mês (se existirem) da pasta local e retorna payload padronizado."""
    mk = month_key(y, m)
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    payload = {}
    if not os.path.isdir(folder):
        return payload

    def try_read(fname_patterns: list[str]) -> pd.DataFrame | None:
        for f in os.listdir(folder):
            low = f.lower()
            if any(low.startswith(p) and low.endswith(".xlsx") for p in fname_patterns):
                try:
                    return load_xlsx(os.path.join(folder, f))
                except Exception:
                    pass
        return None

    df_csat = try_read(["data_product__csat"])
    df_media = try_read(["data_product__media_csat"])
    df_tma = try_read(["tempo_medio_de_atendimento", "tempo_medio_atendimento", "tma"])

    if df_csat is not None:       payload["csat"] = df_csat
    if df_media is not None:      payload["media_csat"] = df_media
    if df_tma is not None:        payload["tma_por_canal"] = df_tma

    # Monta derivado por canal (merge) se possível
    payload = build_by_channel(payload)

    return payload

def write_local_month_payload(y: int, m: int, payload: dict, save_flags: dict):
    mk = month_key(y, m)
    folder = os.path.join(LOCAL_STORE_DIR, mk)
    ensure_dir(folder)

    if "csat" in payload and save_flags.get("save_local"):
        save_df_local(payload["csat"], os.path.join(folder, "data_product__csat.xlsx"))
    if "media_csat" in payload and save_flags.get("save_local"):
        save_df_local(payload["media_csat"], os.path.join(folder, "data_product__media_csat.xlsx"))
    if "tma_por_canal" in payload and save_flags.get("save_local"):
        save_df_local(payload["tma_por_canal"], os.path.join(folder, "tempo_medio_de_atendimento.xlsx"))

    # (Opcional) GitHub: se variáveis de ambiente estiverem definidas, envia base64 por API HTTP simples
    if save_flags.get("save_github"):
        gh_token = os.getenv("GITHUB_TOKEN")
        gh_repo  = os.getenv("GITHUB_REPO")  # "org/repo"
        gh_path  = os.getenv("GITHUB_PATH", "data_store").strip("/")

        if gh_token and gh_repo:
            # Usa API HTTP "contents" (sem depender de PyGithub)
            import requests
            base_url = f"https://api.github.com/repos/{gh_repo}/contents"
            def push_df(df: pd.DataFrame, relname: str):
                path = f"{gh_path}/{mk}/{relname}"
                ensure_dir(os.path.dirname(os.path.join(LOCAL_STORE_DIR, mk)))
                # Salva temporário local em XLSX para gerar bytes
                buf = BytesIO()
                df.to_excel(buf, index=False)
                content_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

                # Checa se já existe (para obter sha)
                r = requests.get(f"{base_url}/{path}", headers={"Authorization": f"token {gh_token}"})
                sha = r.json().get("sha") if r.status_code == 200 else None

                put_body = {
                    "message": f"update {path}",
                    "content": content_b64,
                    "branch": os.getenv("GITHUB_BRANCH", "main")
                }
                if sha:
                    put_body["sha"] = sha
                r2 = requests.put(f"{base_url}/{path}", headers={"Authorization": f"token {gh_token}"}, data=json.dumps(put_body))
                if r2.status_code not in (200, 201):
                    st.warning(f"Falha ao enviar {relname} ao GitHub: {r2.status_code} - {r2.text[:180]}")

            if "csat" in payload:        push_df(payload["csat"], "data_product__csat.xlsx")
            if "media_csat" in payload:  push_df(payload["media_csat"], "data_product__media_csat.xlsx")
            if "tma_por_canal" in payload: push_df(payload["tma_por_canal"], "tempo_medio_de_atendimento.xlsx")
        else:
            st.info("Persistência GitHub não configurada (defina GITHUB_TOKEN e GITHUB_REPO). Usando apenas armazenamento local.")

# ---------------------------------------
# Normalização e utilidades de DataFrame
# ---------------------------------------

def normalize_canal_column(df: pd.DataFrame) -> pd.DataFrame:
    """Garante coluna 'Canal'. Se achar 'Categoria', 'canal', 'Channel', etc., renomeia."""
    if "Canal" in df.columns:
        return df
    cand = {
        "categoria": "Canal",
        "canal": "Canal",
        "channel": "Canal",
        "categoria/canal": "Canal"
    }
    lower = {str(c).strip().lower(): c for c in df.columns}
    for k, new in cand.items():
        if k in lower:
            return df.rename(columns={lower[k]: new})
    return df

def serie_para_horas(serie: pd.Series) -> pd.Series:
    """Converte uma série que pode estar em HH:MM:SS, segundos, minutos ou horas -> HORAS."""
    # Tenta numérico
    s_num = pd.to_numeric(serie, errors="coerce")
    if s_num.notna().any():
        vmax = float(s_num.max())
        vmed = float(s_num.median()) if s_num.notna().any() else vmax
        if vmax > 300:    # segundos (maior que 5min em segundos)
            return s_num / 3600.0
        if 5 <= vmed <= 180:  # minutos
            return s_num / 60.0
        return s_num.astype(float)      # horas
    # Tenta HH:MM:SS
    td = pd.to_timedelta(serie.astype(str), errors="coerce")
    if td.notna().any():
        return td.dt.total_seconds() / 3600.0
    # Falhou
    return pd.Series([np.nan] * len(serie), index=serie.index)

def find_best_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        k = c.strip().lower()
        if k in lower:
            return lower[k]
    return None

def build_by_channel(payload: dict) -> dict:
    """Cria/atualiza payload['by_channel'] unificando:
       - tma_por_canal (tempo médio)
       - media_csat (média de CSAT)
       - csat (usado para contar respostas por canal, se houver)
    """
    df_tma   = payload.get("tma_por_canal")
    df_media = payload.get("media_csat")
    df_csat  = payload.get("csat")

    df_tma2, df_media2, df_csat2 = None, None, None

    if isinstance(df_tma, pd.DataFrame) and not df_tma.empty:
        df_tma2 = normalize_canal_column(df_tma.copy())
    if isinstance(df_media, pd.DataFrame) and not df_media.empty:
        df_media2 = normalize_canal_column(df_media.copy())
    if isinstance(df_csat, pd.DataFrame) and not df_csat.empty:
        df_csat2 = normalize_canal_column(df_csat.copy())

    # Deriva contagem de respostas no df_csat, caso tenha algo como score_total/ratings
    if df_csat2 is not None:
        count_col = find_best_column(df_csat2, [
            "Respostas CSAT","Quantidade de respostas CSAT","score_total","ratings",
            "total de avaliações","avaliacoes","avaliações","qtd","qtde"
        ])
        if count_col is not None:
            grp = df_csat2.groupby("Canal", as_index=False)[count_col].sum()
            grp = grp.rename(columns={count_col: "Respostas CSAT"})
            payload["csat_respostas_por_canal"] = grp

    # Renomeia média CSAT se vier com outro nome (ex.: 'avg')
    if df_media2 is not None:
        mcol = find_best_column(df_media2, ["Média CSAT","media csat","avg","media"])
        if mcol and mcol != "Média CSAT":
            df_media2 = df_media2.rename(columns={mcol: "Média CSAT"})

    # Prepara join por 'Canal'
    merged = None
    if df_tma2 is not None:
        merged = df_tma2.copy()
    if df_media2 is not None:
        merged = df_media2.copy() if merged is None else merged.merge(df_media2, on="Canal", how="outer")
    if payload.get("csat_respostas_por_canal") is not None:
        merged = payload["csat_respostas_por_canal"].copy() if merged is None else merged.merge(payload["csat_respostas_por_canal"], on="Canal", how="outer")

    if merged is not None:
        payload["by_channel"] = merged

    return payload

# -------------------
# Configuração Streamlit
# -------------------

st.set_page_config(page_title="Dashboard CSAT Mensal (XLSX) — Persistência GitHub", layout="wide")

st.title("Dashboard CSAT Mensal (XLSX) — Persistência GitHub")
st.caption("Arquivos por mês ficam salvos no repositório GitHub configurado e em `data_store/` como fallback.")

# Estado dos meses
if "months" not in st.session_state:
    st.session_state["months"] = {}

# -------------------
# Sidebar - parâmetros e upload
# -------------------
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

        if up_csat is not None:
            payload["csat"] = load_xlsx(up_csat)
        if up_media is not None:
            payload["media_csat"] = load_xlsx(up_media)
        if up_tma is not None:
            payload["tma_por_canal"] = load_xlsx(up_tma)

        # Monta by_channel
        payload = build_by_channel(payload)

        # Grava local/GitHub se marcado
        write_local_month_payload(int(year), int(month), payload, {"save_local": save_local, "save_github": save_github})

        # Atualiza estado
        st.session_state["months"][mk] = payload
        st.success(f"Arquivos do mês {mk} carregados e armazenados.")

    # Carrega do disco o mês atual, caso exista
    if st.button("Recarregar do disco este mês"):
        payload = read_local_month_payload(int(year), int(month))
        if payload:
            st.session_state["months"][mk] = payload
            st.success(f"Mês {mk} recarregado do disco.")
        else:
            st.info("Nada encontrado no disco para este mês.")

# Carrega todos os meses do disco para o estado (sem sobrescrever os já carregados manualmente)
def load_all_local_months_into_state():
    if not os.path.isdir(LOCAL_STORE_DIR):
        return
    for name in sorted(os.listdir(LOCAL_STORE_DIR)):
        p = os.path.join(LOCAL_STORE_DIR, name)
        if os.path.isdir(p) and len(name) == 7 and name[4] == "-":
            y, m = name.split("-")
            try:
                y = int(y); m = int(m)
                payload = read_local_month_payload(y, m)
                if payload and name not in st.session_state["months"]:
                    st.session_state["months"][name] = payload
            except Exception:
                pass

load_all_local_months_into_state()

# -------------------
# Helper para pegar DF por canal do mês selecionado
# -------------------
def get_current_by_channel() -> pd.DataFrame | None:
    payload = st.session_state["months"].get(mk, {})
    df = payload.get("by_channel")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.copy()
    # tenta alguma tabela com coluna "Canal"
    for v in payload.values():
        if isinstance(v, pd.DataFrame) and "Canal" in v.columns:
            return v.copy()
    return None

# -------------------
# Abas
# -------------------
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

        # ---- Tempo médio de atendimento (h) — CONVERSÃO ROBUSTA ----
        with col3:
            # candidatos de coluna de tempo de atendimento
            cand_tma = [
                "mean_total HH:MM:SS", "mean_total", "Tempo médio de atendimento",
                "Tempo medio de atendimento", "_handle_seconds", "handle_seconds",
                "mean_total_seconds", "Tempo médio de atendimento (s)"
            ]
            tcol = find_best_column(dfc, cand_tma)
            if tcol is None:
                st.warning("Não encontrei a coluna de tempo de atendimento (ex.: 'mean_total HH:MM:SS').")
            else:
                dft = dfc.copy()
                dft["Tempo médio de atendimento (h)"] = serie_para_horas(dft[tcol])
                if dft["Tempo médio de atendimento (h)"].notna().any():
                    st.plotly_chart(
                        px.bar(
                            dft, x="Canal", y="Tempo médio de atendimento (h)",
                            title="Tempo médio de atendimento (h)"
                        ),
                        use_container_width=True
                    )
                else:
                    st.warning("Não foi possível converter o tempo para horas.")

        # ---- Tempo médio de espera (h) — se existir ----
        with col4:
            cand_wait = [
                "mean_wait HH:MM:SS", "mean_wait", "Tempo médio de espera",
                "Tempo medio de espera", "wait_seconds", "mean_wait_seconds",
                "Tempo médio de espera (s)"
            ]
            wcol = find_best_column(dfc, cand_wait)
            if wcol is None:
                st.info("Coluna de tempo de espera não encontrada para este mês.")
            else:
                dfw = dfc.copy()
                dfw["Tempo médio de espera (h)"] = serie_para_horas(dfw[wcol])
                st.plotly_chart(
                    px.bar(
                        dfw, x="Canal", y="Tempo médio de espera (h)",
                        title="Tempo médio de espera (h)"
                    ),
                    use_container_width=True
                )

        st.write("---")
        st.markdown("#### Tabela por Canal (mês atual)")
        st.dataframe(dfc, use_container_width=True)

# 3) Comparativo Mensal (resumo simples)
with tabs[2]:
    st.subheader("Comparativo Mensal — resumo")
    months_dict = st.session_state["months"]
    if not months_dict:
        st.info("Nenhum mês carregado.")
    else:
        # exemplo: comparação de média de CSAT por mês (média global do DF por canal)
        rows = []
        for mkey, payload in sorted(months_dict.items()):
            df = payload.get("by_channel")
            if isinstance(df, pd.DataFrame) and not df.empty:
                csat_col = find_best_column(df, ["Média CSAT", "media csat", "avg", "media"])
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
- **Por Canal (tempo)**: `mean_total HH:MM:SS`, `mean_total`, `Tempo médio de atendimento`, `_handle_seconds`, `handle_seconds`, `mean_total_seconds`.
- **Por Canal (espera)**: `mean_wait HH:MM:SS`, `mean_wait`, `Tempo médio de espera`, `wait_seconds`, `mean_wait_seconds`.
- **CSAT Médio**: `Média CSAT`, `avg`, `media`.
- **Respostas CSAT (contagem)**: `Respostas CSAT`, `score_total`, `ratings`, `Avaliações`, `Total de avaliações`, `qtd`, `qtde`.
- **Nome do Canal**: `Canal`, `Categoria`, `canal`, `channel` (renomeado para `Canal`).
    """)

# 5) Análise dos Canais
with tabs[4]:
    st.subheader("Análise dos Canais")
    st.caption("Exibe, por mês, os canais com MENOR quantidade de respostas do CSAT (se disponível) e as MENORES notas de CSAT.")

    months_dict = st.session_state["months"]
    if not months_dict:
        st.info("Nenhum mês carregado.")
    else:
        # Monta registros por mês
        count_candidates = [
            "Respostas CSAT","Quantidade de respostas CSAT","qtd respostas csat","qtd csat",
            "Respostas","Avaliadas","Avaliações","Total de avaliações",
            "Ratings","score_total","qtde","qtd"
        ]
        csat_candidates = ["Média CSAT","media csat","avg","media","CSAT","csat","CSAT Médio","csat médio"]

        rec_counts = []
        rec_scores = []

        for mkey, payload in sorted(months_dict.items()):
            # escolhe um DF por canal
            df = payload.get("by_channel")
            if not isinstance(df, pd.DataFrame) or df.empty:
                # tenta alternativa com coluna Canal
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
                k = c.lower()
                if k in colmap:
                    ccol = colmap[k]
                    break
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
                k = c.lower()
                if k in colmap:
                    scol = colmap[k]
                    break
            if scol is not None:
                tmp2 = df[["Canal", scol]].copy()
                tmp2[scol] = pd.to_numeric(tmp2[scol], errors="coerce")
                tmp2 = tmp2.dropna()
                if not tmp2.empty:
                    tmp2 = tmp2.rename(columns={scol: "Média CSAT"})
                    tmp2["mes"] = mkey
                    rec_scores.append(tmp2)

        colA, colB = st.columns(2)

        # menores quantidades
        with colA:
            st.markdown("**Menor quantidade de respostas do CSAT por mês**")
            n_counts = st.number_input("Quantos canais exibir (menores quantidades)?", 1, 10, 3, 1, key="n_counts")
            if not rec_counts:
                st.warning("Não encontrei uma coluna de contagem de respostas por canal nos dados persistidos.")
            else:
                dd = pd.concat(rec_counts, ignore_index=True)
                tops = []
                for mval, grp in dd.groupby("mes", as_index=False):
                    tops.append(grp.sort_values("Respostas CSAT", ascending=True).head(int(n_counts)))
                dd_top = pd.concat(tops, ignore_index=True)
                st.plotly_chart(px.bar(dd_top, x="mes", y="Respostas CSAT", color="Canal",
                                       barmode="group", title="Menores quantidades de respostas (CSAT) por mês"),
                                use_container_width=True)
                st.dataframe(dd_top.sort_values(["mes", "Respostas CSAT", "Canal"]), use_container_width=True)

        # menores notas
        with colB:
            st.markdown("**Menores notas de CSAT por mês**")
            n_scores = st.number_input("Quantos canais exibir (menores notas)?", 1, 10, 3, 1, key="n_scores")
            if not rec_scores:
                st.info("Não encontrei coluna de 'Média CSAT' nos dados por canal dos meses persistidos.")
            else:
                dd2 = pd.concat(rec_scores, ignore_index=True)
                tops2 = []
                for mval, grp in dd2.groupby("mes", as_index=False):
                    tops2.append(grp.sort_values("Média CSAT", ascending=True).head(int(n_scores)))
                dd2_top = pd.concat(tops2, ignore_index=True)
                st.plotly_chart(px.bar(dd2_top, x="mes", y="Média CSAT", color="Canal",
                                       barmode="group", title="Menores notas de CSAT por mês"),
                                use_container_width=True)
                st.dataframe(dd2_top.sort_values(["mes", "Média CSAT", "Canal"]), use_container_width=True)
