# app.py — Dashboard CSAT Mensal — Persistência GitHub
# Este app lê a configuração de 'config.json' e usa a API do GitHub
# para ler os arquivos de dados e fazer upload de novos arquivos.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import re
import os
import requests # <- Necessário para a API do GitHub
import base64
import json     # <- Necessário para carregar o config.json
from io import BytesIO
from datetime import date

# ====================== CONFIG ======================
# Carrega a configuração do arquivo JSON
# @st.cache_resource garante que o arquivo seja lido apenas uma vez.
@st.cache_resource
def load_config():
    """Carrega o arquivo de configuração 'config.json'."""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)['dashboard_config']
    except FileNotFoundError:
        st.error("ERRO: Arquivo 'config.json' não encontrado. Por favor, crie o arquivo.")
        return None
    except Exception as e:
        st.error(f"ERRO ao ler 'config.json': {e}")
        return None

CONFIG = load_config()

# Se a configuração falhar, para o app
if not CONFIG:
    st.stop()

# Mapeia as configurações do JSON para as constantes que o código original usava
# Isso nos permite reutilizar 90% das funções de helper sem modificá-las.
try:
    # Mapeia os nomes de arquivos esperados
    FILE_PATTERNS = {
        key: f"^{re.escape(name).replace('_*.', '.*')}$"
        for key, name in CONFIG['data_source_files'].items()
    }
    
    # Mapeia as metas de SLA
    SLA = {
        "WAITING_TIME_MAX_SECONDS": 24 * 3600, # < 24h
        "CSAT_MIN": CONFIG['tabs'][0]['indicators'][5]['goal'], # Média CSAT goal
        "COMPLETION_RATE_MIN": CONFIG['tabs'][0]['indicators'][2]['goal'], # % concluídos goal
        "EVAL_COVERAGE_MIN": CONFIG['tabs'][0]['indicators'][6]['goal'], # % avaliados goal
        "NEAR_RATIO": 0.05 # margem ±5% (amarelo)
    }

    # Ordem do CSAT (pode ser movido para o config.json se desejado)
    CSAT_ORDER = [
        "Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"
    ]
    
    # Define os tipos de arquivos
    REQUIRED_TYPES = [
        "total_atendimentos", "concluidos", "tempo_atendimento", 
        "tempo_espera", "media_csat", "dist_csat"
    ]
    OPTIONAL_TYPES = ["por_canal"]
    
    # Define os esquemas esperados (pode ser movido para o config.json)
    EXPECTED_SCHEMAS = {
        "dist_csat": {"Categoria", "score_total"},
        "media_csat": {"avg"},
        "tempo_atendimento": {"mean_total"},
        "tempo_espera": {"mean_total"},
        "total_atendimentos": {"total_tickets"},
        "concluidos": {"total_tickets"},
        "por_canal": {
            "Canal", "Tempo médio de atendimento", "Tempo médio de espera",
            "Total de atendimentos", "Total de atendimentos concluídos", "Média CSAT"
        },
    }
except KeyError as e:
    st.error(f"ERRO: 'config.json' está incompleto. Chave ausente: {e}")
    st.stop()
except Exception as e:
    st.error(f"ERRO ao inicializar configuração: {e}")
    st.stop()


# ====================== HELPERS (LÓGICA DE NEGÓCIO) ======================
# Estas funções são do seu app.py original e foram preservadas,
# pois contêm a lógica de negócio central.

def init_state():
    """Inicializa o session_state se necessário."""
    # O st.session_state.data não é mais usado para persistência,
    # mas pode ser usado para caching de sessão se desejado.
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = {}

def month_key(year, month):
    """Formata a chave do mês como YYYY-MM."""
    return f"{int(year):04d}-{int(month):02d}"

def hhmmss_to_seconds(s: str) -> int:
    """Converte 'HH:MM:SS' para segundos inteiros."""
    if pd.isna(s):
        return 0
    s = str(s).strip()
    if not s or s.lower() in ["nan", "none"]:
        return 0
    
    # Trata horas que podem passar de 24h (ex: 63:12:12)
    h, m, sec = 0, 0, 0
    parts = s.split(":")
    if len(parts) == 3:
        try:
            h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
        except Exception:
            return 0
    elif len(parts) == 2: # Caso HH:MM
        try:
            h = int(parts[0]); m = int(parts[1])
        except Exception:
            return 0
    elif len(parts) == 1: # Caso Segundos
        try:
            sec = int(parts[0])
        except Exception:
            return 0
            
    return h*3600 + m*60 + sec

def seconds_to_hhmmss(total: int) -> str:
    """Converte segundos inteiros para 'HH:MM:SS'."""
    if total is None or pd.isna(total) or not isinstance(total, (int, float)):
        return "00:00:00"
    total = int(total)
    h = total // 3600
    rem = total % 3600
    m = rem // 60
    s = rem % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def classify_filename(filename: str) -> str:
    """Classifica um nome de arquivo com base nos padrões do config.json."""
    for ftype, pattern in FILE_PATTERNS.items():
        if re.match(pattern, filename, flags=re.IGNORECASE):
            return ftype
    return "unknown"

def load_csv_result_sheet(uploaded_file) -> pd.DataFrame:
    """Lê um arquivo CSV (de upload ou URL)."""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return pd.DataFrame()

def ensure_schema(df: pd.DataFrame, expected_cols: set, file_label: str) -> pd.DataFrame:
    """Valida se o DataFrame contém as colunas esperadas."""
    if df.empty:
        # st.warning(f"{file_label}: arquivo vazio.") # (Silenciado para não poluir a UI)
        return pd.DataFrame()
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    cols = set(df.columns)
    if not expected_cols.issubset(cols):
        st.warning(f"{file_label}: colunas inesperadas. Esperado: {sorted(expected_cols)} | Encontrado: {sorted(cols)}")
        return pd.DataFrame()
    return df

def ensure_csat_order(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que as categorias de CSAT estejam na ordem correta."""
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
    """
    Recebe um dicionário de DataFrames brutos (como lido do GitHub)
    e retorna um dicionário de DataFrames limpos e validados.
    (Função original do app.py)
    """
    cleaned = {}
    
    # 1) CSAT por categoria
    if "dist_csat" in month_data:
        df = ensure_schema(month_data["dist_csat"], EXPECTED_SCHEMAS["dist_csat"], "CSAT por categoria")
        if not df.empty:
            df["Categoria"] = df["Categoria"].astype(str).str.strip()
            df["score_total"] = pd.to_numeric(df["score_total"], errors="coerce").fillna(0).astype(int)
            cleaned["dist_csat"] = ensure_csat_order(df)
            
    # 2) CSAT médio
    if "media_csat" in month_data:
        df = ensure_schema(month_data["media_csat"], EXPECTED_SCHEMAS["media_csat"], "CSAT médio")
        if not df.empty:
            try:
                avg_val = float(pd.to_numeric(df["avg"], errors="coerce").dropna().iloc[0])
            except Exception:
                avg_val = np.nan
            cleaned["media_csat"] = pd.DataFrame({"avg": [avg_val]})
            
    # 3) Tempo médio de atendimento
    if "tempo_atendimento" in month_data:
        df = ensure_schema(month_data["tempo_atendimento"], EXPECTED_SCHEMAS["tempo_atendimento"], "Tempo médio de atendimento")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["tempo_atendimento"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
            
    # 4) Tempo médio de espera
    if "tempo_espera" in month_data:
        df = ensure_schema(month_data["tempo_espera"], EXPECTED_SCHEMAS["tempo_espera"], "Tempo médio de espera")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["tempo_espera"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
            
    # 5) Totais
    if "total_atendimentos" in month_data:
        df = ensure_schema(month_data["total_atendimentos"], EXPECTED_SCHEMAS["total_atendimentos"], "Total de atendimentos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["total_atendimentos"] = pd.DataFrame({"total_tickets": [total]})
            
    if "concluidos" in month_data:
        df = ensure_schema(month_data["concluidos"], EXPECTED_SCHEMAS["concluidos"], "Atendimentos concluídos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["concluidos"] = pd.DataFrame({"total_tickets": [total]})
            
    # 6) Por canal (opcional)
    if "por_canal" in month_data:
        df = ensure_schema(month_data["por_canal"], EXPECTED_SCHEMAS["por_canal"], "Por canal")
        if not df.empty:
            df["Canal"] = df["Canal"].astype(str).str.strip()
            df["Total de atendimentos"] = pd.to_numeric(df["Total de atendimentos"], errors="coerce").fillna(0).astype(int)
            df["Total de atendimentos concluídos"] = pd.to_numeric(df["Total de atendimentos concluídos"], errors="coerce").fillna(0).astype(int)
            df["Média CSAT"] = pd.to_numeric(df["Média CSAT"], errors="coerce")
            df["_handle_seconds"] = df["Tempo médio de atendimento"].astype(str).apply(hhmmss_to_seconds)
            df["_wait_seconds"] = df["Tempo médio de espera"].astype(str).apply(hhmmss_to_seconds)
            cleaned["por_canal"] = df
            
    return cleaned

def compute_kpis(cleaned: dict) -> dict:
    """Calcula os KPIs principais a partir dos dados limpos."""
    kpis = {
        "total": np.nan, "completed": np.nan, "completion_rate": np.nan,
        "handle_avg_sec": np.nan, "wait_avg_sec": np.nan,
        "csat_avg": np.nan, "evaluated": np.nan, "eval_coverage": np.nan
    }
    
    if "total_atendimentos" in cleaned:
        kpis["total"] = int(cleaned["total_atendimentos"]["total_tickets"].iloc[0])
    if "concluidos" in cleaned:
        kpis["completed"] = int(cleaned["concluidos"]["total_tickets"].iloc[0])
    if not pd.isna(kpis["total"]) and kpis["total"] > 0 and not pd.isna(kpis["completed"]):
        kpis["completion_rate"] = kpis["completed"] / kpis["total"] * 100.0
        
    if "tempo_atendimento" in cleaned:
        kpis["handle_avg_sec"] = int(cleaned["tempo_atendimento"]["seconds"].iloc[0])
    if "tempo_espera" in cleaned:
        kpis["wait_avg_sec"] = int(cleaned["tempo_espera"]["seconds"].iloc[0])
        
    if "media_csat" in cleaned:
        kpis["csat_avg"] = float(cleaned["media_csat"]["avg"].iloc[0])
    if "dist_csat" in cleaned:
        kpis["evaluated"] = int(pd.to_numeric(cleaned["dist_csat"]["score_total"], errors="coerce").sum())
    if not pd.isna(kpis["evaluated"]) and not pd.isna(kpis["completed"]) and kpis["completed"] > 0:
        # Garante que avaliados não seja maior que concluídos
        if kpis["evaluated"] > kpis["completed"]:
            kpis["eval_coverage"] = 100.0
            st.warning(f"Inconsistência: N° de Avaliados ({kpis['evaluated']}) > N° de Concluídos ({kpis['completed']}). Cobertura definida como 100%.")
        else:
            kpis["eval_coverage"] = kpis["evaluated"] / kpis["completed"] * 100.0
            
    return kpis

def near_threshold(actual, target, greater_is_better=True, near_ratio=0.05):
    """Verifica se um valor está próximo da meta (para status 'Alerta')."""
    if target == 0 or pd.isna(actual):
        return False
    if greater_is_better:
        return (actual < target) and (actual >= target*(1 - near_ratio))
    else:
        return (actual > target) and (actual <= target*(1 + near_ratio))

def color_flag(ok: bool, warn: bool = False):
    """Retorna um ícone de status."""
    if ok:
        return "✅"
    if warn:
        return "⚠️"
    return "❌"

def sla_flags(kpis: dict):
    """Gera flags de status (OK/Alerta/Falha) para os KPIs."""
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
        ok = cr > SLA["COMPLETION_RATE_MIN"] # > 90%, não >=
        warn = near_threshold(cr, SLA["COMPLETION_RATE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["completion"] = (ok, warn)
        
    ev = kpis.get("eval_coverage", np.nan)
    if not pd.isna(ev):
        ok = ev >= SLA["EVAL_COVERAGE_MIN"]
        warn = near_threshold(ev, SLA["EVAL_COVERAGE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["coverage"] = (ok, warn)
        
    return flags

# ====================== PERSISTÊNCIA GITHUB (NOVO) ======================

@st.cache_data(ttl=300) # Cache de 5 minutos
def load_data_from_github(empresa_path: str, mes_key: str) -> dict:
    """
    Baixa todos os arquivos de dados de um mês/empresa específico do GitHub.
    Retorna um dicionário de DataFrames brutos, pronto para 'validate_and_clean'.
    """
    try:
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
    except KeyError as e:
        st.error(f"ERRO: Segredo do Streamlit não encontrado: {e}. Por favor, configure os segredos do GitHub.")
        return {}
        
    base_url = f"https://raw.githubusercontent.com/{repo_name}/{branch}/{empresa_path}/{mes_key}"
    
    month_data_raw = {}
    data_files_map = CONFIG['data_source_files']
    
    # Paralelizar downloads (se houver muitos arquivos, considere 'threading')
    # Para 7 arquivos, o sequencial é rápido o suficiente.
    for file_type, file_name in data_files_map.items():
        url = f"{base_url}/{file_name}"
        try:
            df = pd.read_csv(url)
            month_data_raw[file_type] = df
        except Exception as e:
            # Não é um erro fatal, o arquivo pode ser opcional ou ainda não existir
            if file_type not in OPTIONAL_TYPES:
                # Silencioso, a validação principal tratará disso
                pass
            
    return month_data_raw

@st.cache_data(ttl=3600) # Cache de 1 hora
def get_all_kpis(empresa_path: str) -> pd.DataFrame:
    """
    Busca KPIs de TODOS os meses para a aba 'Comparativo'.
    """
    try:
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
    except KeyError as e:
        st.error(f"ERRO: Segredo do Streamlit não encontrado: {e}.")
        return pd.DataFrame()

    # 1. Listar diretórios (meses) na pasta da empresa
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{empresa_path}?ref={branch}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status() # Lança erro se a requisição falhar
        content = response.json()
        
        # Filtra apenas diretórios que parecem ser meses (YYYY-MM)
        month_keys = sorted([
            item['name'] for item in content 
            if item['type'] == 'dir' and re.match(r'^\d{4}-\d{2}$', item['name'])
        ], reverse=True) # Mais novos primeiro
        
        if not month_keys:
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Falha ao listar meses no repositório: {e}")
        return pd.DataFrame()

    # 2. Para cada mês, carregar dados e calcular KPIs
    rows = []
    
    # Limita a 12 meses por padrão para performance
    for mkey in month_keys[:12]:
        try:
            # Usa a função de cache
            raw_data = load_data_from_github(empresa_path, mkey)
            if not raw_data:
                continue
            
            cleaned = validate_and_clean(raw_data)
            k = compute_kpis(cleaned)
            
            # Adiciona os KPIs à lista
            rows.append({
                "mes": mkey,
                "total": k.get("total"),
                "concluidos": k.get("completed"),
                "taxa_conclusao": k.get("completion_rate"),
                "tempo_espera_s": k.get("wait_avg_sec"),
                "tempo_atendimento_s": k.get("handle_avg_sec"),
                "csat_medio": k.get("csat_avg"),
                "cobertura_%": k.get("eval_coverage"),
            })
        except Exception as e:
            st.warning(f"Falha ao processar dados para o mês {mkey}: {e}")
            
    comp = pd.DataFrame(rows).sort_values("mes")
    return comp


def upload_to_github(file_content: bytes, empresa_path: str, mes_key: str, target_filename: str):
    """
    Faz upload (ou atualização) de um único arquivo para o repositório GitHub.
    """
    try:
        # Pega os segredos
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
        author = {
            "name": st.secrets["GH_COMMITS_AUTHOR_NAME"],
            "email": st.secrets["GH_COMMITS_AUTHOR_EMAIL"],
        }
    except KeyError as e:
        st.sidebar.error(f"ERRO: Segredo não configurado: {e}")
        return False

    # Constrói o path no repositório
    path_no_repo = f"{empresa_path}/{mes_key}/{target_filename}"
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{path_no_repo}"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    # 1. Verifica se o arquivo já existe para obter o 'sha' (necessário para update)
    sha = None
    try:
        response_get = requests.get(api_url, headers=headers)
        if response_get.status_code == 200:
            sha = response_get.json()['sha']
    except Exception as e:
        st.sidebar.warning(f"Não foi possível verificar {target_filename}: {e}")

    # 2. Prepara os dados para o PUT (upload)
    content_b64 = base64.b64encode(file_content).decode('utf-8')
    
    data = {
        "message": f"Upload de dados: {path_no_repo}",
        "author": author,
        "content": content_b64,
        "branch": branch,
    }
    
    # Adiciona o 'sha' se estivermos atualizando um arquivo existente
    if sha:
        data["sha"] = sha

    # 3. Faz o PUT (cria ou atualiza o arquivo)
    try:
        response_put = requests.put(api_url, headers=headers, json=data)
        
        if response_put.status_code == 201: # Criado
            st.sidebar.success(f"Arquivo '{target_filename}' criado.")
            return True
        elif response_put.status_code == 200: # Atualizado
            st.sidebar.success(f"Arquivo '{target_filename}' atualizado.")
            return True
        else:
            st.sidebar.error(f"Erro ({response_put.status_code}) ao enviar '{target_filename}': {response_put.json().get('message')}")
            return False
            
    except Exception as e:
        st.sidebar.error(f"Exceção ao enviar '{target_filename}': {e}")
        return False

# ====================== APP (UI) ======================
st.set_page_config(page_title="CSAT Dashboard Mensal (GitHub)", layout="wide")
init_state()

# --- SIDEBAR (Seleção e Upload) ---

# --- 1. Seleção de Análise ---
st.sidebar.title("Filtros de Análise")

# Mapeia as empresas do config.json
empresa_opcoes = {comp['name']: comp['data_path_segment'] for comp in CONFIG['companies']}
empresa_selecionada_nome = st.sidebar.selectbox(
    "Empresa (Análise)", 
    options=empresa_opcoes.keys(),
    key="analise_empresa"
)
# Path da empresa selecionada (ex: "data" ou "datavilla")
empresa_path_selecionada = empresa_opcoes[empresa_selecionada_nome]

# Seletores de Mês/Ano (como no original)
col_m, col_y = st.sidebar.columns(2)
# Define o mês e ano atuais como padrão
today = date.today()
# Se for início do mês (ex: dia 1-5), o padrão é o mês anterior
if today.day <= 5:
    today = today.replace(day=1) - pd.DateOffset(months=1)
    
month = col_m.selectbox("Mês", list(range(1, 13)), index=today.month - 1)
year = col_y.selectbox("Ano", list(range(2024, today.year + 2)), index=today.year - 2024)
current_month_key = month_key(year, month)


# --- 2. Seção de Upload (em um expander) ---
with st.sidebar.expander("Upload de Novos Dados"):
    st.markdown("### Enviar arquivos para o GitHub")
    
    # Seletores de MÊS/ANO para UPLOAD
    st.caption("Selecione o destino do upload:")
    
    # Empresa para Upload
    empresa_upload_nome = st.selectbox(
        "Empresa (Destino)", 
        options=empresa_opcoes.keys(),
        key="upload_empresa"
    )
    empresa_path_upload = empresa_opcoes[empresa_upload_nome]
    
    # Mês/Ano para Upload
    col_um, col_uy = st.columns(2)
    upload_month = col_um.selectbox("Mês (Destino)", list(range(1, 13)), index=today.month - 1, key="upload_mes")
    upload_year = col_uy.selectbox("Ano (Destino)", list(range(2024, today.year + 2)), index=today.year - 2024, key="upload_ano")
    upload_month_key = month_key(upload_year, upload_month)

    st.markdown("---")
    st.caption(f"Os arquivos serão enviados para: `{empresa_path_upload}/{upload_month_key}/`")

    # Gera os uploaders dinamicamente a partir do config.json
    upload_files_map = {} # Armazena {UploadedFile: target_filename}
    
    for file_config in CONFIG['upload_config']['required_files']:
        file_id = file_config['id']
        label = file_config['label']
        # Pega o nome do arquivo de destino (ex: "total_de_atendimentos.csv")
        target_name_key = file_config['target_filename_ref'].split('.')[-1]
        target_name = CONFIG['data_source_files'][target_name_key]
        
        uploaded_file = st.file_uploader(label, type=["csv"], key=file_id)
        
        if uploaded_file:
            # Verifica se o nome do arquivo bate com o padrão
            if not re.match(FILE_PATTERNS[target_name_key], uploaded_file.name, flags=re.IGNORECASE):
                st.warning(f"O nome '{uploaded_file.name}' não parece ser um arquivo de '{label}'. Verifique o arquivo.")
            
            upload_files_map[uploaded_file] = target_name

    # Botão de Envio
    if st.button("Enviar Arquivos para o GitHub"):
        if not upload_files_map:
            st.sidebar.warning("Nenhum arquivo selecionado para envio.")
        else:
            # Verifica se todos os segredos necessários existem
            try:
                st.secrets["GH_TOKEN"]
                st.secrets["GH_REPO"]
                st.secrets["GH_BRANCH"]
                st.secrets["GH_COMMITS_AUTHOR_NAME"]
                st.secrets["GH_COMMITS_AUTHOR_EMAIL"]
            except KeyError as e:
                st.sidebar.error(f"ERRO: Segredo não configurado: {e}. Não é possível fazer upload.")
                st.stop()

            # Processa o upload
            with st.spinner(f"Enviando {len(upload_files_map)} arquivo(s) para {upload_month_key}..."):
                success_count = 0
                for uploaded_file, target_filename in upload_files_map.items():
                    file_content = uploaded_file.getvalue()
                    if upload_to_github(file_content, empresa_path_upload, upload_month_key, target_filename):
                        success_count += 1
                
                if success_count == len(upload_files_map):
                    st.sidebar.success("Todos os arquivos foram enviados!")
                    # Limpa o cache de dados para forçar o recarregamento
                    st.cache_data.clear()
                else:
                    st.sidebar.error("Alguns arquivos falharam no envio. Verifique os logs.")


# ====================== CONTEÚDO PRINCIPAL ======================
st.title("Dashboard CSAT Mensal (GitHub)")
st.caption(f"Exibindo dados de: **{empresa_selecionada_nome}** | Mês: **{current_month_key}**")

# Carrega os dados do mês selecionado no sidebar
try:
    raw_month_data = load_data_from_github(empresa_path_selecionada, current_month_key)
except Exception as e:
    st.error(f"Falha crítica ao carregar dados do GitHub: {e}")
    raw_month_data = {}

# Cria as abas dinamicamente a partir do config.json
tab_configs = CONFIG['tabs']
tab_names = [tab['name'] for tab in tab_configs]
tab_names.append("Dicionário de Dados") # Adiciona a aba estática
tabs = st.tabs(tab_names)


# --- 1) Visão Geral ---
with tabs[0]:
    config_tab1 = tab_configs[0]
    
    if not raw_month_data:
        st.info(f"Nenhum dado encontrado para '{empresa_selecionada_nome}' em '{current_month_key}' no GitHub.")
    else:
        # Reutiliza toda a lógica de validação e KPI do app original
        cleaned = validate_and_clean(raw_month_data)
        
        # Verifica se os arquivos obrigatórios estão presentes
        missing_files = []
        for req in REQUIRED_TYPES:
            if req not in cleaned:
                missing_files.append(CONFIG['data_source_files'].get(req, req))
        if missing_files:
            st.warning(f"Arquivos obrigatórios ausentes em {current_month_key}: {', '.join(missing_files)}")
        
        kpis = compute_kpis(cleaned)
        flags = sla_flags(kpis)
        
        # Pega os títulos e metas do config.json
        ind_conf = {ind['title']: ind for ind in config_tab1['indicators']}
        
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

        # Indicadores (lendo títulos do config)
        c1.metric(ind_conf['Total de atendimentos']['title'], f"{int(total) if not pd.isna(total) else '-'}")
        c2.metric(ind_conf['Atendimentos concluídos']['title'], f"{int(completed) if not pd.isna(completed) else '-'}")
        
        comp_icon = color_flag(*(flags.get("completion",(False,False))))
        c3.metric(ind_conf['Porcentagem de atendimentos concluídos']['title'], 
                  f"{(f'{cr:.1f}%' if not pd.isna(cr) else '-')}", 
                  help=f"Meta > {SLA['COMPLETION_RATE_MIN']}% {comp_icon}")
                  
        c4.metric(ind_conf['Tempo médio de atendimento']['title'], seconds_to_hhmmss(ht))
        
        w_ok, w_warn = flags.get("wait",(False,False))
        c5.metric(ind_conf['Tempo médio de espera']['title'], 
                  seconds_to_hhmmss(wt), 
                  help=f"Meta < 24:00:00 {color_flag(w_ok, w_warn)}")
                  
        cs_ok, cs_warn = flags.get("csat",(False,False))
        c6.metric(ind_conf['Média CSAT']['title'], 
                  f"{cs:.2f}" if not pd.isna(cs) else "-", 
                  help=f"Meta > {SLA['CSAT_MIN']} {color_flag(cs_ok, cs_warn)}")
                  
        cov_ok, cov_warn = flags.get("coverage",(False,False))
        c7.metric(ind_conf['Porcentagem de atendimentos avaliados']['title'], 
                  f"{(f'{cov:.1f}%' if not pd.isna(cov) else '-')}", 
                  help=f"Meta ≥ {SLA['EVAL_COVERAGE_MIN']}% {color_flag(cov_ok, cov_warn)}")

        st.markdown("---")
        
        # Gráfico (lendo do config)
        chart_conf_1 = config_tab1['charts'][0]
        if "dist_csat" in cleaned:
            dist = cleaned["dist_csat"].copy()
            dist["percent"] = dist["score_total"] / dist["score_total"].sum() * 100 if dist["score_total"].sum() > 0 else 0
            
            left, right = st.columns([2,1])
            with left:
                fig = px.bar(dist, x=chart_conf_1['x_axis'], y="percent", title=chart_conf_1['title'], text=dist["percent"].round(1))
                fig.update_layout(xaxis_title="", yaxis_title="%")
                st.plotly_chart(fig, use_container_width=True)
            with right:
                st.dataframe(dist, use_container_width=True)
                st.download_button("Baixar tabela (CSV)", data=dist.to_csv(index=False).encode("utf-8"), file_name=f"csat_{current_month_key}.csv")
        else:
            st.warning(f"Arquivo '{CONFIG['data_source_files']['dist_csat']}' não encontrado para o gráfico de distribuição.")

# --- 2) Por Canal ---
with tabs[1]:
    st.subheader("Indicadores por Canal")
    
    if "por_canal" not in cleaned:
        st.info("Arquivo 'por canal' não disponível para o mês selecionado.")
    else:
        # A lógica original do app.py é mantida
        dfc = cleaned["por_canal"].copy()
        channels_available = sorted(dfc["Canal"].astype(str).unique())
        selected_channels = st.multiselect("Filtrar canais", channels_available, default=channels_available, key="channel_filter")
        
        if selected_channels:
            dfc = dfc[dfc["Canal"].astype(str).isin(selected_channels)]
            
        st.dataframe(dfc, use_container_width=True)
        st.download_button("Baixar por canal (CSV)", data=dfc.to_csv(index=False).encode("utf-8"), file_name=f"por_canal_{current_month_key}.csv")
        
        st.markdown("---")
        
        # Gráficos (lendo títulos do config)
        chart_configs_t2 = {c['title']: c for c in tab_configs[1]['charts']}
        
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.bar(dfc, x="Canal", y="Média CSAT", title=chart_configs_t2['Média CSAT por canal']['title']), use_container_width=True)
        with col2:
            dft = dfc.copy()
            dft["Tempo médio de espera (h)"] = dft["_wait_seconds"] / 3600
            st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de espera (h)", title=chart_configs_t2['Tempo médio de espera por canal (horas)']['title']), use_container_width=True)
        
        col3, col4 = st.columns(2)
        with col3:
            dft = dfc.copy()
            dft["Tempo médio de atendimento (h)"] = dft["_handle_seconds"] / 3600
            st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de atendimento (h)", title=chart_configs_t2['Tempo médio de atendimento por canal (horas)']['title']), use_container_width=True)
        with col4:
            dfc["% Concluídos"] = (dfc["Total de atendimentos concluídos"] / dfc["Total de atendimentos"] * 100).fillna(0)
            st.plotly_chart(px.bar(dfc, x="Canal", y="% Concluídos", title=chart_configs_t2['% de atendimentos concluídos por canal']['title']), use_container_width=True)

# --- 3) Comparativo Mensal ---
with tabs[2]:
    st.subheader("Comparativo Mensal (KPIs)")
    
    with st.spinner(f"Carregando histórico de KPIs para {empresa_selecionada_nome}..."):
        comp_df = get_all_kpis(empresa_path_selecionada)
    
    if comp_df.empty:
        st.info(f"Nenhum dado histórico encontrado para '{empresa_selecionada_nome}'. Carregue dados de pelo menos um mês.")
    elif len(comp_df) < 2:
        st.info("Carregue dados de pelo menos dois meses para habilitar o comparativo.")
        st.dataframe(comp_df, use_container_width=True)
    else:
        # Lógica original mantida
        if "tempo_espera_s" in comp_df.columns:
            comp_df["tempo_espera_h"] = comp_df["tempo_espera_s"] / 3600
        if "tempo_atendimento_s" in comp_df.columns:
            comp_df["tempo_atendimento_h"] = comp_df["tempo_atendimento_s"] / 3600
            
        st.dataframe(comp_df, use_container_width=True)
        st.download_button("Baixar comparativo (CSV)", data=comp_df.to_csv(index=False).encode("utf-8"), file_name=f"comparativo_mensal_{empresa_path_selecionada}.csv")
        
        # Gráficos (lendo títulos do config)
        chart_configs_t3 = {c['title']: c for c in tab_configs[2]['charts']}
        
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.line(comp_df, x="mes", y="csat_medio", markers=True, title=chart_configs_t3['Média CSAT geral (Mensal)']['title']), use_container_width=True)
            st.plotly_chart(px.line(comp_df, x="mes", y="tempo_espera_h", markers=True, title=chart_configs_t3['Média do tempo de espera geral (Mensal)']['title']), use_container_width=True)
        with c2:
            st.plotly_chart(px.line(comp_df, x="mes", y="taxa_conclusao", markers=True, title=chart_configs_t3['Porcentagem de atendimentos concluídos (Mensal)']['title']), use_container_width=True)
            st.plotly_chart(px.bar(comp_df, x="mes", y="total", title=chart_configs_t3['Total de atendimentos recebidos (Mensal)']['title']), use_container_width=True)

# --- 4) Dicionário de Dados ---
with tabs[3]:
    st.subheader("Dicionário de Dados e SLAs")
    st.markdown("**Arquivos .csv por mês (mapeados em `config.json`)**")
    
    # Gera o dicionário dinamicamente
    rows = "<ul>"
    for key, name in CONFIG['data_source_files'].items():
        schema = EXPECTED_SCHEMAS.get(key)
        cols = f" (Colunas: `{', '.join(schema)}`)" if schema else ""
        rows += f"<li><code>{name}</code> — Mapeado para <b>{key}</b>{cols}</li>"
    rows += "</ul>"
    st.markdown(rows, unsafe_allow_html=True)

    st.markdown(f"""
**Métricas e fórmulas:**
- Total: soma de `total_tickets` (de `total_atendimentos`)
- Concluídos: soma de `total_tickets` (de `concluidos`)
- Taxa de conclusão (%) = `concluídos / total * 100`
- Tempo médio de atendimento/espera: `mean_total` (HH:MM:SS)
- CSAT médio (1–5): `avg` (de `media_csat`)
- Cobertura de avaliação (%) = `avaliadas / concluídos * 100`, (avaliadas = soma(`score_total`) de `dist_csat`)
- Ordem CSAT: {", ".join(CSAT_ORDER)}

**SLAs (Metas):**
- **Tempo de Espera:** < 24:00:00 (calculado de `SLA['WAITING_TIME_MAX_SECONDS']`)
- **CSAT Médio:** ≥ {SLA['CSAT_MIN']} (lido do `config.json`)
- **Taxa de Conclusão:** > {SLA['COMPLETION_RATE_MIN']}% (lido do `config.json`)
- **Cobertura de Avaliação:** ≥ {SLA['EVAL_COVERAGE_MIN']}% (lido do `config.json`)

**Persistência:**
- Os dados são lidos e escritos no repositório GitHub: `{CONFIG['secrets_config']['GH_REPO']}`
- As credenciais são lidas de `st.secrets`.
""")
