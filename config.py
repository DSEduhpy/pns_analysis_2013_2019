"""
Configurações gerais do projeto PNS Analysis.

Centraliza todas as constantes: IDs do BigQuery, caminhos de arquivo,
filtros padrão e parametrizações.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# --- BigQuery / Base dos Dados ---
BILLING_PROJECT_ID = os.getenv("BILLING_PROJECT_ID")
BIGQUERY_DATASET = "basedosdados.br_ms_pns"

# Tabelas por origem (ano)
PNS_TABLES = {
    "2013": "microdados_2013",
    "2019": "microdados_2019",
}

# --- SQLite Cache ---
# Caminho relativo ao diretório raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQLITE_PATH = PROJECT_ROOT / "data" / "pns_cache.sqlite"

# Nome da tabela principal no SQLite
PNS_TABLE_NAME = "pns_respostas"

# --- Filtros Padrão ---
# Filtros aplicados por padrão em cada origem
# Valores são definidos em termos semânticos e serão traduzidos para códigos físicos
# via mapping.py durante a construção das queries
PNS_FILTERS = {
    "2013": {
        "sexo": {"semantico": "sexo", "valor": "2"},  # Mulheres
        "idade_min": 25,
    },
    "2019": {
        "sexo": {"semantico": "sexo", "valor": "2"},  # Mulheres
        "idade_min": 25,
    },
}

# --- Configurações de Log ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

