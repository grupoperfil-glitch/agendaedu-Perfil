# app.py — Dashboard CSAT Mensal (CSV) — Persistência Local
# Adaptado para CSV com esquema fixo por arquivo
# Baseado no código original, mas para CSV e seus arquivos

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import re
import os
import shutil
import base64
from io import BytesIO
from zipfile import ZipFile
from datetime import date

# ====================== CONFIG ======================
DATA_DIR = "data_store"
os.makedirs(DATA_DIR, exist_ok=True)

SLA = {
    "WAITING_TIME_MAX_SECONDS": 24 * 3600,  # < 24 horas
    "CSAT_MIN": 4.0,  # >= 4.0
    "COMPLETION_RATE_MIN": 90.0,  # > 90%
    "EVAL_COVERAGE_MIN": 75.0,  # >= 75%
    "NEAR_RATIO": 0.05  # margem ±5% (amarelo)
}

CSAT_ORDER = [
    "Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"
]

FILE_PATTERNS = {
    "csat_by_cat": r"^_data_product__csat_.*\.csv$",
    "csat_avg": r"^_data_product__media_csat_.*\.csv$",
    "handle_avg": r"^tempo_medio_de_atendimento_.*\.csv$",
    "wait_avg": r"^tempo_medio_de_espera_.*\.csv$",
    "total": r"^total_de_atendimentos_.*\.csv$",
    "completed": r"^total_de_atendimentos_concluidos_.*\.csv$",
    "by_channel": r"^tempo_medio_de_atendimento_por_canal_.*\.csv$",
}

REQUIRED_TYPES = ["csat_by_cat", "csat_avg", "handle_avg", "wait_avg", "total", "completed"]
OPTIONAL_TYPES = ["by_channel"]

EXPECTED_SCHEMAS = {
    "csat_by_cat": {"Categoria", "score_total"},
    "csat_avg": {"avg"},
    "handle_avg": {"mean_total"},
    "wait_avg": {"mean_total"},
    "total": {"total_tickets"},
    "completed": {"total_tickets"},
    "by_channel": {
        "Canal", "Tempo médio de atendimento", "Tempo médio de espera",
        "Total de atendimentos", "Total de atendimentos concluídos", "Média CSAT"
    },
}

# ====================== HELPERS ======================
def init_state():
    if "data" not in st.session_state:
        st.session_state.data = {}  # {"YYYY-MM": {"csat_by_cat": df, ...}}
    if "autosave_local" not in st.session_state:
        st.session_state.autosave_local = True

def month_key(year, month):
    return f"{int(year):04d}-{int(month):02d}"

def hhmmss_to_seconds(s: str) -> int:
    if pd.isna(s):
        return 0
    s = str(s).strip()
    if not s or s.lower() in ["nan", "none"]:
        return 0
    parts = s.split(":")
    if len(parts) != 3:
        return 0
    try:
        h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
        return h*3600 + m*60 + sec
    except Exception:
        return 0

def seconds_to_hhmmss(total: int) -> str:
    if total is None or pd.isna(total):
        return "00:00:00"
    total = int(total)
    h = total // 3600
    rem = total % 3600
    m = rem // 60
    s = rem % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def classify_filename(filename: str) -> str:
    for ftype, pattern in FILE_PATTERNS.items():
        if re.match(pattern, filename, flags=re.IGNORECASE):
            return ftype
    return "unknown"

def load_csv_result_sheet(uploaded_file) -> pd.DataFrame:
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return pd.DataFrame()

def ensure_schema(df: pd.DataFrame, expected_cols: set, file_label: str) -> pd.DataFrame:
    if df.empty:
        st.warning(f"{file_label}: arquivo vazio.")
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    cols = set(df.columns)
    if not expected_cols.issubset(cols):
        st.warning(f"{file_label}: colunas inesperadas. Esperado: {sorted(expected_cols)} | Encontrado: {sorted(cols)}")
        return pd.DataFrame()
    return df

def ensure_csat_order(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    present = set(df["Categoria"].astype(str).str.strip())
    rows = []
    for cat in CSAT_ORDER:
        val = 0
        if cat in present:
            val = int(pd.to_numeric(df.loc[df["Categoria"].str.strip() == cat, "score_total"], errors="coerce").sum())
        rows.append({"Categoria": cat, "score_total": val})
    return pd.DataFrame(rows)

def validate_and_clean(month_data: dict) -> dict:
    cleaned = {}
    # 1) CSAT por categoria
    if "csat_by_cat" in month_data:
        df = ensure_schema(month_data["csat_by_cat"], EXPECTED_SCHEMAS["csat_by_cat"], "CSAT por categoria")
        if not df.empty:
            df["Categoria"] = df["Categoria"].astype(str).str.strip()
            df["score_total"] = pd.to_numeric(df["score_total"], errors="coerce").fillna(0).astype(int)
            cleaned["csat_by_cat"] = ensure_csat_order(df)
    # 2) CSAT médio
    if "csat_avg" in month_data:
        df = ensure_schema(month_data["csat_avg"], EXPECTED_SCHEMAS["csat_avg"], "CSAT médio")
        if not df.empty:
            try:
                avg_val = float(pd.to_numeric(df["avg"], errors="coerce").dropna().iloc[0])
            except Exception:
                avg_val = np.nan
            cleaned["csat_avg"] = pd.DataFrame({"avg": [avg_val]})
    # 3) Tempo médio de atendimento
    if "handle_avg" in month_data:
        df = ensure_schema(month_data["handle_avg"], EXPECTED_SCHEMAS["handle_avg"], "Tempo médio de atendimento")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["handle_avg"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
    # 4) Tempo médio de espera
    if "wait_avg" in month_data:
        df = ensure_schema(month_data["wait_avg"], EXPECTED_SCHEMAS["wait_avg"], "Tempo médio de espera")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["wait_avg"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
    # 5) Totais
    if "total" in month_data:
        df = ensure_schema(month_data["total"], EXPECTED_SCHEMAS["total"], "Total de atendimentos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["total"] = pd.DataFrame({"total_tickets": [total]})
    if "completed" in month_data:
        df = ensure_schema(month_data["completed"], EXPECTED_SCHEMAS["completed"], "Atendimentos concluídos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["completed"] = pd.DataFrame({"total_tickets": [total]})
    # 6) Por canal (opcional)
    if "by_channel" in month_data:
        df = ensure_schema(month_data["by_channel"], EXPECTED_SCHEMAS["by_channel"], "Por canal")
        if not df.empty:
            df["Canal"] = df["Canal"].astype(str).str.strip()
            df["Total de atendimentos"] = pd.to_numeric(df["Total de atendimentos"], errors="coerce").fillna(0).astype(int)
            df["Total de atendimentos concluídos"] = pd.to_numeric(df["Total de atendimentos concluídos"], errors="coerce").fillna(0).astype(int)
            df["Média CSAT"] = pd.to_numeric(df["Média CSAT"], errors="coerce")
            df["_handle_seconds"] = df["Tempo médio de atendimento"].astype(str).apply(hhmmss_to_seconds)
            df["_wait_seconds"] = df["Tempo médio de espera"].astype(str).apply(hhmmss_to_seconds)
            cleaned["by_channel"] = df
    return cleaned

def compute_kpis(cleaned: dict) -> dict:
    kpis = {
        "total": np.nan, "completed": np.nan, "completion_rate": np.nan,
        "handle_avg_sec": np.nan, "wait_avg_sec": np.nan,
        "csat_avg": np.nan, "evaluated": np.nan, "eval_coverage": np.nan
    }
    if "total" in cleaned:
        kpis["total"] = int(cleaned["total"]["total_tickets"].iloc[0])
    if "completed" in cleaned:
        kpis["completed"] = int(cleaned["completed"]["total_tickets"].iloc[0])
    if not pd.isna(kpis["total"]) and kpis["total"] > 0 and not pd.isna(kpis["completed"]):
        kpis["completion_rate"] = kpis["completed"] / kpis["total"] * 100.0
    if "handle_avg" in cleaned:
        kpis["handle_avg_sec"] = int(cleaned["handle_avg"]["seconds"].iloc[0])
    if "wait_avg" in cleaned:
        kpis["wait_avg_sec"] = int(cleaned["wait_avg"]["seconds"].iloc[0])
    if "csat_avg" in cleaned:
        kpis["csat_avg"] = float(cleaned["csat_avg"]["avg"].iloc[0])
    if "csat_by_cat" in cleaned:
        kpis["evaluated"] = int(pd.to_numeric(cleaned["csat_by_cat"]["score_total"], errors="coerce").sum())
    if not pd.isna(kpis["evaluated"]) and not pd.isna(kpis["completed"]) and kpis["completed"] > 0:
        kpis["eval_coverage"] = kpis["evaluated"] / kpis["completed"] * 100.0
    return kpis

def near_threshold(actual, target, greater_is_better=True, near_ratio=0.05):
    if target == 0 or pd.isna(actual):
        return False
    if greater_is_better:
        return (actual < target) and (actual >= target*(1 - near_ratio))
    else:
        return (actual > target) and (actual <= target*(1 + near_ratio))

def color_flag(ok: bool, warn: bool = False):
    if ok:
        return "✅"
    if warn:
        return "⚠️"
    return "❌"

def sla_flags(kpis: dict):
    flags = {}
    wt = kpis.get("wait_avg_sec", np.nan)
    if not pd.isna(wt):
        ok = wt < SLA["WAITING_TIME_MAX_SECONDS"]
        warn = near_threshold(wt, SLA["WAITING_TIME_MAX_SECONDS"], greater_is_better=False, near_ratio=SLA["NEAR_RATIO"])
        flags["wait"] = (ok, warn)
    cs = kpis.get("csat_avg", np.nan)
    if not pd.isna(cs):
        ok = cs >= SLA["CSAT_MIN"]
        warn = near_threshold(cs, SLA["CSAT_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["csat"] = (ok, warn)
    cr = kpis.get("completion_rate", np.nan)
    if not pd.isna(cr):
        ok = cr > SLA["COMPLETION_RATE_MIN"]
        warn = near_threshold(cr, SLA["COMPLETION_RATE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["completion"] = (ok, warn)
    ev = kpis.get("eval_coverage", np.nan)
    if not pd.isna(ev):
        ok = ev >= SLA["EVAL_COVERAGE_MIN"]
        warn = near_threshold(ev, SLA["EVAL_COVERAGE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["coverage"] = (ok, warn)
    return flags

# ====================== PERSISTÊNCIA LOCAL ======================
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def month_dir(mkey: str) -> str:
    return os.path.join(DATA_DIR, mkey)

def save_month_to_disk(mkey: str, raw_month_data: dict):
    ensure_data_dir()
    os.makedirs(month_dir(mkey), exist_ok=True)
    cleaned = validate_and_clean(raw_month_data)
    for t, df in cleaned.items():
        path = os.path.join(month_dir(mkey), f"{t}.csv")
        df.to_csv(path, index=False)

def delete_month_from_disk(mkey: str):
    p = month_dir(mkey)
    if os.path.isdir(p):
        shutil.rmtree(p)

def load_all_from_disk() -> dict:
    ensure_data_dir()
    result = {}
    for mkey in sorted(os.listdir(DATA_DIR)):
        p = month_dir(mkey)
        if not os.path.isdir(p):
            continue
        result[mkey] = {}
        for fname in os.listdir(p):
            if fname.endswith(".csv"):
                ftype = fname[:-4]
                fpath = os.path.join(p, fname)
                try:
                    df = pd.read_csv(fpath)
                    result[mkey][ftype] = df
                except Exception as e:
                    st.warning(f"Falha ao ler {fpath}: {e}")
    return result

# ====================== APP ======================
st.set_page_config(page_title="CSAT Dashboard Mensal (CSV) — Persistência Local", layout="wide")

init_state()

# Carregar dados locais
disk_data = load_all_from_disk()
for mk, payload in disk_data.items():
    st.session_state.data[mk] = payload

st.sidebar.title("Parâmetros do Mês")
col_m, col_y = st.sidebar.columns(2)
month = col_m.selectbox("Mês", list(range(1, 13)), format_func=lambda x: f"{x:02d}")
year = col_y.selectbox("Ano", list(range(2025, 2031)))
current_month_key = month_key(year, month)

st.sidebar.checkbox("Salvar em disco local", value=True, key="autosave_local")

st.sidebar.markdown("### Upload dos arquivos (.csv)")
st.sidebar.caption('Cada arquivo deve ter o formato correto das colunas.')

u_csat_by_cat = st.sidebar.file_uploader("1) _data_product__csat_*.csv (Categoria, score_total)", type=["csv"], key="u_csat_by_cat")
u_csat_avg = st.sidebar.file_uploader("2) _data_product__media_csat_*.csv (avg)", type=["csv"], key="u_csat_avg")
u_handle_avg = st.sidebar.file_uploader("3) tempo_medio_de_atendimento_*.csv (mean_total HH:MM:SS)", type=["csv"], key="u_handle_avg")
u_wait_avg = st.sidebar.file_uploader("4) tempo_medio_de_espera_*.csv (mean_total HH:MM:SS)", type=["csv"], key="u_wait_avg")
u_total = st.sidebar.file_uploader("5) total_de_atendimentos_*.csv (total_tickets)", type=["csv"], key="u_total")
u_completed = st.sidebar.file_uploader("6) total_de_atendimentos_concluidos_*.csv (total_tickets)", type=["csv"], key="u_completed")
u_by_channel = st.sidebar.file_uploader("7) tempo_medio_de_atendimento_por_canal_*.csv (opcional)", type=["csv"], key="u_by_channel")

st.sidebar.markdown("— ou arraste vários de uma vez —")
multi = st.sidebar.file_uploader("Upload múltiplo (classificação automática por nome)", type=["csv"], accept_multiple_files=True, key="multi_all")

def _ingest_to_session(mkey: str, fileobj, expected_type: str):
    if not fileobj:
        return
    if not re.match(FILE_PATTERNS[expected_type], fileobj.name, flags=re.IGNORECASE):
        st.sidebar.warning(f"Nome não bate com o padrão para {expected_type}: {fileobj.name}")
    df = load_csv_safe(fileobj)
    if mkey not in st.session_state.data:
        st.session_state.data[mkey] = {}
    st.session_state.data[mkey][expected_type] = df

if st.sidebar.button("Salvar arquivos do mês atual"):
    _ingest_to_session(current_month_key, u_csat_by_cat, "csat_by_cat")
    _ingest_to_session(current_month_key, u_csat_avg, "csat_avg")
    _ingest_to_session(current_month_key, u_handle_avg, "handle_avg")
    _ingest_to_session(current_month_key, u_wait_avg, "wait_avg")
    _ingest_to_session(current_month_key, u_total, "total")
    _ingest_to_session(current_month_key, u_completed, "completed")
    if u_by_channel:
        _ingest_to_session(current_month_key, u_by_channel, "by_channel")
    for f in multi or []:
        ftype = classify_filename(f.name)
        if ftype == "unknown":
            st.sidebar.warning(f"Arquivo ignorado: {f.name}")
            continue
        df = load_csv_safe(f)
        if current_month_key not in st.session_state.data:
            st.session_state.data[current_month_key] = {}
        st.session_state.data[current_month_key][ftype] = df
    st.sidebar.success(f"Arquivos anexados para {current_month_key}.")

    if st.session_state.autosave_local:
        save_month_to_disk(current_month_key, st.session_state.data[current_month_key])
        st.sidebar.success(f"[DISCO] Dados salvos em data_store/{current_month_key}/")

# ====================== CONTEÚDO PRINCIPAL ======================
st.title("Dashboard CSAT Mensal (CSV) — Persistência Local")
st.caption("Arquivos por mês ficam salvos localmente em data_store/.")

tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo Mensal", "Dicionário de Dados"])

# 1) Visão Geral
with tabs[0]:
    st.subheader(f"Mês selecionado: {current_month_key}")
    if current_month_key not in st.session_state.data:
        st.info("Nenhum dado carregado para este mês. Faça upload na barra lateral e clique em 'Salvar arquivos do mês atual'.")
    else:
        raw = st.session_state.data[current_month_key]
        cleaned = validate_and_clean(raw)
        for req in REQUIRED_TYPES:
            if req not in cleaned:
                st.warning(f"Arquivo obrigatório ausente em {current_month_key}: {req}")
        kpis = compute_kpis(cleaned)
        flags = sla_flags(kpis)
        c1, c2, c3, c4 = st.columns(4)
        c5, c6, c7 = st.columns(3)
        total = kpis.get("total")
        completed = kpis.get("completed")
        cr = kpis.get("completion_rate")
        ht = kpis.get("handle_avg_sec")
        wt = kpis.get("wait_avg_sec")
        cs = kpis.get("csat_avg")
        ev = kpis.get("evaluated")
        cov = kpis.get("eval_coverage")
        c1.metric("Total de atendimentos", f"{int(total) if not pd.isna(total) else '-'}")
        c2.metric("Concluídos", f"{int(completed) if not pd.isna(completed) else '-'}")
        comp_icon = color_flag(*(flags.get("completion",(False,False))))
        c3.metric("Taxa de conclusão", f"{(f'{cr:.1f}%' if not pd.isna(cr) else '-')}", help=f"SLA > {SLA['COMPLETION_RATE_MIN']}% {comp_icon}")
        c4.metric("Tempo médio de atendimento", seconds_to_hhmmss(ht))
        w_ok, w_warn = flags.get("wait",(False,False)) if "wait" in flags else (False,False)
        c5.metric("Tempo médio de espera", seconds_to_hhmmss(wt), help="SLA < 24:00:00 " + ("✅" if w_ok else "⚠️" if w_warn else "❌"))
        cs_ok, cs_warn = flags.get("csat",(False,False)) if "csat" in flags else (False,False)
        c6.metric("CSAT médio (1–5)", f"{cs:.2f}" if not pd.isna(cs) else "-", help=f"SLA ≥ {SLA['CSAT_MIN']} " + ("✅" if cs_ok else "⚠️" if cs_warn else "❌"))
        cov_ok, cov_warn = flags.get("coverage",(False,False)) if "coverage" in flags else (False,False)
        c7.metric("Cobertura de avaliação", f"{(f'{cov:.1f}%' if not pd.isna(cov) else '-')}", help=f"SLA ≥ {SLA['EVAL_COVERAGE_MIN']}% " + ("✅" if cov_ok else "⚠️" if cov_warn else "❌"))
        if not pd.isna(ev) and not pd.isna(completed) and ev > completed:
            st.warning("Inconsistência: chamadas avaliadas > concluídas.")
        st.markdown("---")
        if "csat_by_cat" in cleaned:
            dist = cleaned["csat_by_cat"].copy()
            dist["percent"] = dist["score_total"] / dist["score_total"].sum() * 100 if dist["score_total"].sum() > 0 else 0
            left, right = st.columns([2,1])
            with left:
                fig = px.bar(dist, x="Categoria", y="percent", title="CSAT por Categoria (%)", text=dist["percent"].round(1))
                fig.update_layout(xaxis_title="", yaxis_title="%")
                st.plotly_chart(fig, use_container_width=True)
            with right:
                st.dataframe(dist, use_container_width=True)
                st.download_button("Baixar tabela (CSV)", data=dist.to_csv(index=False).encode("utf-8"), file_name=f"csat_{current_month_key}.csv")

# 2) Por Canal
with tabs[1]:
    st.subheader("Indicadores por Canal")
    if current_month_key not in st.session_state.data or "by_channel" not in st.session_state.data[current_month_key]:
        st.info("Arquivo por canal não disponível para o mês selecionado.")
    else:
        dfc = st.session_state.data[current_month_key]["by_channel"].copy()
        channels_available = sorted(dfc["Canal"].astype(str).unique())
        selected_channels = st.multiselect("Filtrar canais", channels_available, default=channels_available)
        if selected_channels:
            dfc = dfc[dfc["Canal"].astype(str).isin(selected_channels)]
        st.dataframe(dfc, use_container_width=True)
        st.download_button("Baixar por canal (CSV)", data=dfc.to_csv(index=False).encode("utf-8"), file_name=f"por_canal_{current_month_key}.csv")
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.bar(dfc, x="Canal", y="Total de atendimentos", title="Total de atendimentos por canal"), use_container_width=True)
        with col2:
            st.plotly_chart(px.bar(dfc, x="Canal", y="Total de atendimentos concluídos", title="Concluídos por canal"), use_container_width=True)
        col3, col4 = st.columns(2)
        with col3:
            if "_handle_seconds" in dfc.columns:
                dft = dfc.copy()
                dft["Tempo médio de atendimento (s)"] = dft["_handle_seconds"]
                st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de atendimento (s)", title="Tempo médio de atendimento (s)"), use_container_width=True)
        with col4:
            if "_wait_seconds" in dfc.columns:
                dft = dfc.copy()
                dft["Tempo médio de espera (h)"] = dft["_wait_seconds"] / 3600
                st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de espera (h)", title="Tempo médio de espera (horas)"), use_container_width=True)
        st.markdown("---")
        st.plotly_chart(px.bar(dfc, x="Canal", y="Média CSAT", title="CSAT médio por canal"), use_container_width=True)

# 3) Comparativo Mensal
with tabs[2]:
    st.subheader("Comparativo Mensal (KPIs)")
    if len(st.session_state.data) < 2:
        st.info("Carregue pelo menos dois meses para habilitar o comparativo.")
    else:
        rows = []
        for mkey, month_data in sorted(st.session_state.data.items()):
            cleaned = validate_and_clean(month_data)
            k = compute_kpis(cleaned)
            rows.append({
                "mes": mkey,
                "total": k.get("total"),
                "concluidos": k.get("completed"),
                "taxa_conclusao": k.get("completion_rate"),
                "tempo_espera_s": k.get("wait_avg_sec"),
                "csat_medio": k.get("csat_avg"),
                "cobertura_%": k.get("eval_coverage"),
            })
        comp = pd.DataFrame(rows).sort_values("mes")
        if "tempo_espera_s" in comp.columns:
            comp["tempo_espera_h"] = comp["tempo_espera_s"] / 3600
        st.dataframe(comp, use_container_width=True)
        st.download_button("Baixar comparativo (CSV)", data=comp.to_csv(index=False).encode("utf-8"), file_name="comparativo_mensal.csv")
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.line(comp, x="mes", y="total", markers=True, title="Total de atendimentos (mensal)"), use_container_width=True)
            st.plotly_chart(px.line(comp, x="mes", y="taxa_conclusao", markers=True, title="Taxa de conclusão (%)"), use_container_width=True)
        with c2:
            st.plotly_chart(px.line(comp, x="mes", y="csat_medio", markers=True, title="CSAT médio (1–5)"), use_container_width=True)
            y_col = "tempo_espera_h" if "tempo_espera_h" in comp.columns else "tempo_espera_s"
            y_title = "Tempo médio de espera (h)" if y_col == "tempo_espera_h" else "Tempo médio de espera (s)"
            st.plotly_chart(px.line(comp, x="mes", y=y_col, markers=True, title=y_title), use_container_width=True)

# 4) Dicionário de Dados
with tabs[3]:
    st.subheader("Dicionário de Dados")
    st.markdown(f"""
**Arquivos .csv por mês:**
- `_data_product__csat_*.csv` — `Categoria`, `score_total`
- `_data_product__media_csat_*.csv` — `avg` (CSAT 1–5)
- `tempo_medio_de_atendimento_*.csv` — `mean_total` (HH:MM:SS, pode exceder 24h)
- `tempo_medio_de_espera_*.csv` — `mean_total` (HH:MM:SS)
- `total_de_atendimentos_*.csv` → `total_tickets` (int)
- `total_de_atendimentos_concluidos_*.csv` → `total_tickets` (int)
- `tempo_medio_de_atendimento_por_canal_*.csv` (opcional): `Canal`, `Tempo médio de atendimento`, `Tempo médio de espera`, `Total de atendimentos`, `Total de atendimentos concluídos`, `Média CSAT`

**Métricas e fórmulas:**
- Total: soma de `total_tickets`
- Concluídos: soma de `total_tickets` (concluídos)
- Taxa de conclusão (%) = `concluídos/total*100`
- Tempo médio de atendimento/espera: `mean_total` → segundos → formatação HH:MM:SS
- CSAT médio (1–5): `avg`
- Cobertura de avaliação (%) = `avaliadas/concluídos*100`, avaliadas = soma(score_total)
- Ordem CSAT: {", ".join(CSAT_ORDER)}

**SLAs:**
- Espera < 24h; CSAT ≥ 4.0; Conclusão > 90%; Cobertura ≥ 75%

**Persistência:**
- Fallback local em `data_store/AAAA-MM/*.csv`
""")
