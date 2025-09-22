# app.py
# Dashboard CSAT Mensal — Streamlit + Plotly (XLSX com esquema fixo por arquivo)
# Persistência durável no GitHub (via PyGithub) + fallback local em data_store/
#
# Arquivos esperados por mês (aba "Resultado da consulta"):
#  - _data_product__csat_*.xlsx                       (Categoria, score_total)
#  - _data_product__media_csat_*.xlsx                 (avg)
#  - tempo_medio_de_atendimento_*.xlsx                (mean_total HH:MM:SS; pode exceder 24h)
#  - tempo_medio_de_espera_*.xlsx                     (mean_total HH:MM:SS)
#  - total_de_atendimentos_*.xlsx                     (total_tickets)
#  - total_de_atendimentos_concluidos_*.xlsx          (total_tickets)
#  - tempo_medio_de_atendimento_por_canal_*.xlsx      (opcional: Canal, ..., Média CSAT)

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
from datetime import datetime

# ---------- GitHub (persistência durável) ----------
try:
    from github import Github, GithubException, InputGitAuthor
    GITHUB_AVAILABLE = True
except Exception:
    GITHUB_AVAILABLE = False

def _github_client():
    if not GITHUB_AVAILABLE:
        return None
    token = st.secrets.get("GH_TOKEN")
    if not token:
        return None
    try:
        return Github(token)
    except Exception:
        return None

def _github_repo():
    gh = _github_client()
    if not gh:
        return None
    repo_name = st.secrets.get("GH_REPO")
    if not repo_name:
        return None
    try:
        return gh.get_repo(repo_name)
    except Exception:
        return None

def _git_author():
    author_str = st.secrets.get("GH_COMMITS_AUTHOR", "")
    name = st.secrets.get("GH_COMMITS_NAME", "")
    email = st.secrets.get("GH_COMMITS_EMAIL", "")

    if (not name or not email) and author_str and "<" in author_str and ">" in author_str:
        try:
            n, e = author_str.split("<", 1)
            name = n.strip()
            email = e.replace(">", "").strip()
        except Exception:
            pass

    if not name:
        name = "Streamlit Bot"
    if not email:
        email = "bot@example.com"

    return InputGitAuthor(name, email)

def gh_path_for(mkey: str, ftype: str) -> str:
    base = st.secrets.get("GH_PATH", "data")
    return f"{base}/{mkey}/{ftype}.csv"

def save_month_to_github(mkey: str, raw_month_data: dict) -> bool:
    repo = _github_repo()
    if not repo:
        return False
    cleaned = validate_and_clean(raw_month_data)
    branch = st.secrets.get("GH_BRANCH", "main")
    author = _git_author()

    for t, df in cleaned.items():
        path = gh_path_for(mkey, t)
        content = df.to_csv(index=False)
        message_update = f"update {path}"
        message_create = f"create {path}"

        try:
            file = repo.get_contents(path, ref=branch)
            repo.update_file(
                path, message_update, content, file.sha,
                branch=branch, author=author, committer=author
            )
        except GithubException as ge:
            if ge.status == 404:
                repo.create_file(
                    path, message_create, content,
                    branch=branch, author=author, committer=author
                )
            else:
                st.error(f"[GitHub] Falha ao salvar {path}: {ge}")
                return False
        except Exception as e:
            st.error(f"[GitHub] Erro ao salvar {path}: {e}")
            return False
    return True

def load_all_from_github() -> dict:
    repo = _github_repo()
    if not repo:
        return {}
    branch = st.secrets.get("GH_BRANCH", "main")
    base = st.secrets.get("GH_PATH", "data")
    result = {}
    try:
        months = repo.get_contents(base, ref=branch)
    except GithubException as ge:
        if ge.status == 404:
            return {}
        st.warning(f"[GitHub] Não consegui listar {base}: {ge}")
        return {}
    except Exception as e:
        st.warning(f"[GitHub] Erro ao listar {base}: {e}")
        return {}

    for mdir in months:
        if getattr(mdir, "type", None) != "dir":
            continue
        mkey = mdir.name
        result[mkey] = {}
        try:
            files = repo.get_contents(mdir.path, ref=branch)
        except Exception as e:
            st.warning(f"[GitHub] Falha ao listar {mdir.path}: {e}")
            continue
        for f in files:
            if getattr(f, "type", None) != "file" or not f.name.endswith(".csv"):
                continue
            ftype = f.name[:-4]
            try:
                blob = repo.get_git_blob(f.sha)
                csv_bytes = base64.b64decode(blob.content)
                df = pd.read_csv(BytesIO(csv_bytes))
                result[mkey][ftype] = df
            except Exception as e:
                st.warning(f"[GitHub] Falha ao ler {f.path}: {e}")
    return result

# ---------- Configurações gerais ----------
st.set_page_config(page_title="CSAT Dashboard (Mensal XLSX) — GitHub Persist", layout="wide")

DATA_DIR = "data_store"
SLA = {
    "WAITING_TIME_MAX_SECONDS": 24 * 3600,
    "CSAT_MIN": 4.0,
    "COMPLETION_RATE_MIN": 90.0,
    "EVAL_COVERAGE_MIN": 75.0,
    "NEAR_RATIO": 0.05
}
CSAT_ORDER = [
    "Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"
]

FILE_PATTERNS = {
    "csat_by_cat": r"^_data_product__csat_.*\.xlsx$",
    "csat_avg": r"^_data_product__media_csat_.*\.xlsx$",
    "handle_avg": r"^tempo_medio_de_atendimento_.*\.xlsx$",
    "wait_avg": r"^tempo_medio_de_espera_.*\.xlsx$",
    "total": r"^total_de_atendimentos_.*\.xlsx$",
    "completed": r"^total_de_atendimentos_concluidos_.*\.xlsx$",
    "by_channel": r"^tempo_medio_de_atendimento_por_canal_.*\.xlsx$",
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
    "by_channel": {"Canal","Tempo médio de atendimento","Tempo médio de espera",
                   "Total de atendimentos","Total de atendimentos concluídos","Média CSAT"},
}

RESULT_SHEET = "Resultado da consulta"

# ... [mantém todas as funções auxiliares, persistência local, UI, abas, etc. SEM MUDANÇAS] ...

# ---------- UI ----------
init_state()

gh_data = load_all_from_github()
if gh_data:
    for mk, payload in gh_data.items():
        st.session_state.data[mk] = payload
else:
    disk_data = load_all_from_disk()
    for mk, payload in disk_data.items():
        st.session_state.data[mk] = payload

st.sidebar.title("Parâmetros do Mês")

col_m, col_y = st.sidebar.columns(2)
month = col_m.selectbox("Mês", list(range(1, 13)), format_func=lambda x: f"{x:02d}")
# 🔥 Alterado: anos fixos de 2025 até 2030
year = col_y.selectbox("Ano", list(range(2025, 2031)))
current_month_key = month_key(year, month)

# ... [restante do código segue igual: upload, salvar, tabs, comparativo, dicionário de dados] ...
