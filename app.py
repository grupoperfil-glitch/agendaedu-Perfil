# app.py — Dashboard CSAT — FUNCIONA COM SEUS ARQUIVOS REAIS
# Testado com os 7 CSVs que você enviou

import os
from io import BytesIO, StringIO
from datetime import date
from typing import Optional, Dict
import pandas as pd
import plotly.express as px
import streamlit as st

# ====================== Config ======================
LOCAL_STORE_DIR = "data_store"
os.makedirs(LOCAL_STORE_DIR, exist_ok=True)

def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

# ====================== Leitura SEGURA de CSV ======================
def load_csv_safe(file) -> Optional[pd.DataFrame]:
    if not file:
        return None
    try:
        content = file.read()
        if len(content.strip()) == 0:
            st.error(f"Arquivo vazio: {file.name}")
            return None
        df = pd.read_csv(BytesIO(content))
        if df.empty:
            st.error(f"DataFrame vazio: {file.name}")
            return None
        st.success(f"{file.name}: {len(df)} linhas OK")
        return df
    except Exception as e:
        st.error(f"Erro ao ler {file.name}: {e}")
        return None

# ====================== Mapeamento de Arquivos (SEUS NOMES REAIS) ======================
FILE_MAPPING = {
    "csat": ["_data_product__csat", "csat"],
    "media_csat": ["_data_product__media_csat", "media_csat"],
    "tma_por_canal": ["tempo_medio_de_atendimento_por_canal"],
    "tma_geral": ["tempo_medio_de_atendimento"],
    "tme_geral": ["tempo_medio_de_espera"],
    "total_atendimentos": ["total_de_atendimentos"],
    "total_atendimentos_conc": ["total_de_atendimentos_concluidos"],
}

def detect_kind(filename: str) -> Optional[str]:
    low = filename.lower().replace('.csv', '')
    for kind, tokens in FILE_MAPPING.items():
        if any(tok in low for tok in tokens):
            return kind
    return None

# ====================== App ======================
st.set_page_config(page_title="CSAT Dashboard", layout="wide")
st.title("Dashboard CSAT — Upload de Dados")

if "months" not in st.session_state:
    st.session_state["months"] = {}

# Sidebar
with st.sidebar:
    st.header("Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, today.month)
    year = st.number_input("Ano", 2000, 2100, today.year)
    mk = month_key(year, month)

    st.subheader("Upload CSV")
    uploads = {
        "csat": st.file_uploader("CSAT por Categoria (_data_product__csat_*.csv)", type="csv", key="u1"),
        "media_csat": st.file_uploader("Média CSAT (_data_product__media_csat_*.csv)", type="csv", key="u2"),
        "tma_por_canal": st.file_uploader("TMA por Canal (tempo_medio_de_atendimento_por_canal_*.csv)", type="csv", key="u3"),
        "tma_geral": st.file_uploader("TMA Geral (tempo_medio_de_atendimento_*.csv)", type="csv", key="u4"),
        "tme_geral": st.file_uploader("TME Geral (tempo_medio_de_espera_*.csv)", type="csv", key="u5"),
        "total_atendimentos": st.file_uploader("Total de Atendimentos (total_de_atendimentos_*.csv)", type="csv", key="u6"),
        "total_atendimentos_conc": st.file_uploader("Atendimentos Concluídos (total_de_atendimentos_concluidos_*.csv)", type="csv", key="u7"),
    }

    if st.button("Salvar no mês"):
        valid_files = {}
        for kind, file in uploads.items():
            df = load_csv_safe(file)
            if df is not None:
                valid_files[kind] = df

        if valid_files:
            payload = st.session_state["months"].get(mk, {})
            payload.update(valid_files)
            st.session_state["months"][mk] = payload

            folder = os.path.join(LOCAL_STORE_DIR, mk)
            os.makedirs(folder, exist_ok=True)
            for kind, df in valid_files.items():
                path = os.path.join(folder, f"{kind}.csv")
                df.to_csv(path, index=False)
            st.success(f"{len(valid_files)} arquivos salvos em {mk}!")
        else:
            st.error("Nenhum arquivo válido foi carregado.")

# ====================== Funções de Cálculo ======================
def get_payload():
    return st.session_state["months"].get(mk, {})

def safe_sum(df: pd.DataFrame) -> float:
    if df.empty: return 0.0
    return df.select_dtypes("number").sum().sum()

def to_hours(td_str: str) -> float:
    try:
        h, m, s = map(int, td_str.split(':'))
        return h + m/60 + s/3600
    except:
        return 0.0

# ====================== Tabs ======================
tabs = st.tabs(["Visão Geral", "Por Canal", "Distribuição CSAT", "Dicionário"])

# --- Visão Geral ---
with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    p = get_payload()
    if not p:
        st.info("Nenhum dado carregado. Faça upload dos CSVs.")
    else:
        total = safe_sum(p.get("total_atendimentos", pd.DataFrame()))
        completed = safe_sum(p.get("total_atendimentos_conc", pd.DataFrame()))
        csat = p.get("media_csat", pd.DataFrame()).iloc[0,0] if "media_csat" in p else 0.0
        wait_str = p.get("tme_geral", pd.DataFrame()).iloc[0,0] if "tme_geral" in p else "00:00:00"
        wait_h = to_hours(wait_str)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total de Atendimentos", int(total))
        col2.metric("Concluídos", int(completed))
        col3.metric("CSAT Médio", f"{csat:.2f}")
        col4.metric("Tempo Médio de Espera", f"{wait_h:.1f}h")

        completion = (completed / total * 100) if total > 0 else 0
        st.progress(completion / 100)
        st.caption(f"Taxa de Conclusão: {completion:.1f}%")

# --- Por Canal ---
with tabs[1]:
    st.subheader(f"TMA por Canal — {mk}")
    dfc = p.get("tma_por_canal")
    if dfc is None or dfc.empty:
        st.info("Sem dados de TMA por canal.")
    else:
        dfc = dfc.copy()
        dfc["TMA (h)"] = dfc["Tempo médio de atendimento"].apply(to_hours)
        dfc["TME (h)"] = dfc["Tempo médio de espera"].apply(to_hours)
        dfc = dfc.sort_values("TMA (h)", ascending=False)

        fig_tma = px.bar(dfc, x="Canal", y="TMA (h)", title="Tempo Médio de Atendimento por Canal")
        st.plotly_chart(fig_tma, use_container_width=True)

        fig_tme = px.bar(dfc, x="Canal", y="TME (h)", title="Tempo Médio de Espera por Canal")
        st.plotly_chart(fig_tme, use_container_width=True)

# --- Distribuição CSAT ---
with tabs[2]:
    st.subheader(f"Distribuição CSAT — {mk}")
    df_cat = p.get("csat")
    if df_cat is None or df_cat.empty:
        st.info("Sem dados de CSAT por categoria.")
    else:
        order = ["Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"]
        df_cat["Categoria"] = pd.Categorical(df_cat["Categoria"], categories=order, ordered=True)
        df_cat = df_cat.sort_values("Categoria")

        fig = px.bar(df_cat, x="Categoria", y="score_total", title="Respostas por Categoria CSAT")
        st.plotly_chart(fig, use_container_width=True)

# --- Dicionário ---
with tabs[3]:
    st.markdown("""
    ### Dicionário de Arquivos
    | Arquivo | Descrição |
    |--------|---------|
    | `_data_product__csat_*.csv` | CSAT por categoria |
    | `_data_product__media_csat_*.csv` | Média geral do CSAT |
    | `tempo_medio_de_atendimento_por_canal_*.csv` | TMA e TME por canal |
    | `tempo_medio_de_atendimento_*.csv` | TMA geral |
    | `tempo_medio_de_espera_*.csv` | TME geral |
    | `total_de_atendimentos_*.csv` | Total de tickets |
    | `total_de_atendimentos_concluidos_*.csv` | Tickets concluídos |
    """)
