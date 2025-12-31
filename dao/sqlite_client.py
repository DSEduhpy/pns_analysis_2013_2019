"""
Cliente SQLite de baixo nível para gerenciamento do repositório local.

Este módulo fornece funções básicas para:
- Gerenciamento de conexões
- Criação e evolução de schema
- Operações de upsert

O SQLite é usado como repositório permanente de dados processados do BigQuery,
não como cache temporário.
"""
import sqlite3
import logging
import json
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
import pandas as pd

from config import SQLITE_PATH, PNS_TABLE_NAME, PRIMARY_KEY_COLUMNS

# Configurar logger
log = logging.getLogger(__name__)

@contextmanager
def get_connection():
    """
    Context manager para conexão SQLite.
    
    Garante que a conexão seja fechada corretamente e cria o diretório
    do banco se não existir.
    
    Yields:
        sqlite3.Connection: Conexão com o banco de dados
    """
    # Garantir que o diretório existe
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(SQLITE_PATH))
    conn.row_factory = sqlite3.Row  # Permite acesso por nome de coluna
    
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"Erro na transação SQLite: {e}")
        raise
    finally:
        conn.close()


def table_exists(table_name: str = PNS_TABLE_NAME) -> bool:
    """
    Verifica se uma tabela existe no banco de dados.
    
    Args:
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    
    Returns:
        True se a tabela existe, False caso contrário
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (table_name,))
        return cursor.fetchone() is not None


def get_table_columns(table_name: str = PNS_TABLE_NAME) -> List[str]:
    """
    Retorna lista de colunas existentes em uma tabela.
    
    Args:
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    
    Returns:
        Lista de nomes de colunas
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        return columns


def ensure_table_exists(table_name: str = PNS_TABLE_NAME) -> None:
    """
    Cria a tabela principal se ela não existir.
    
    A tabela é criada com:
    - Chave primária composta usando PRIMARY_KEY_COLUMNS do config.py
      Esta PK identifica unicamente cada morador (indivíduo) na pesquisa
    - Colunas de controle: created_at, updated_at
    - Sem colunas de dados semânticos (serão adicionadas dinamicamente)
    
    Args:
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    """
    if table_exists(table_name):
        log.debug(f"Tabela {table_name} já existe")
        return
    
    # Construir definições de colunas da PK
    pk_columns_def = ", ".join([f"{col} TEXT NOT NULL" for col in PRIMARY_KEY_COLUMNS])
    pk_constraint = ", ".join(PRIMARY_KEY_COLUMNS)
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {pk_columns_def},
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY ({pk_constraint})
            )
        """)
        log.info(f"Tabela {table_name} criada com sucesso")


def add_column_if_not_exists(
    column_name: str,
    column_type: str,
    table_name: str = PNS_TABLE_NAME
) -> None:
    """
    Adiciona uma coluna à tabela se ela não existir.
    
    Verifica via PRAGMA table_info se a coluna já existe antes de adicionar.
    
    Args:
        column_name: Nome da coluna a ser adicionada
        column_type: Tipo SQLite (TEXT, INTEGER, REAL, etc.)
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    """
    # Garantir que a tabela existe antes de consultar suas colunas
    ensure_table_exists(table_name)
    
    existing_columns = get_table_columns(table_name)
    
    if column_name in existing_columns:
        log.debug(f"Coluna {column_name} já existe em {table_name}")
        return
    
    with get_connection() as conn:
        cursor = conn.cursor()
        # SQLite não suporta IF NOT EXISTS em ALTER TABLE, por isso verificamos antes
        cursor.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN {column_name} {column_type}
        """)
        log.info(f"Coluna {column_name} ({column_type}) adicionada à tabela {table_name}")


def ensure_columns_exist(
    column_names: List[str],
    table_name: str = PNS_TABLE_NAME
) -> None:
    """
    Garante que múltiplas colunas existam na tabela.
    
    Para cada coluna, infere o tipo apropriado baseado no nome ou usa TEXT como padrão.
    Esta função é útil quando se recebe um DataFrame e precisa garantir que
    todas as colunas existam antes de fazer upsert.
    
    Args:
        column_names: Lista de nomes de colunas
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    """
    # Garantir que a tabela existe antes de consultar suas colunas
    ensure_table_exists(table_name)
    
    existing_columns = get_table_columns(table_name)
    
    for col_name in column_names:
        if col_name in existing_columns:
            continue
        
        # Inferir tipo baseado no nome da coluna
        # Colunas de PK e controle já existem, então não precisamos tratá-las aqui
        if col_name in PRIMARY_KEY_COLUMNS or col_name in ['created_at', 'updated_at']:
            continue
        
        # Tipos padrão baseados em convenções de nome
        if col_name.endswith('_at') or col_name in ['created_at', 'updated_at']:
            col_type = 'DATETIME'
        elif any(keyword in col_name.lower() for keyword in ['idade', 'anos', 'filhos', 'count']):
            col_type = 'INTEGER'
        elif any(keyword in col_name.lower() for keyword in ['peso', 'renda', 'per_capita']):
            col_type = 'REAL'
        else:
            col_type = 'TEXT'
        
        add_column_if_not_exists(col_name, col_type, table_name)


def upsert_rows(
    df: pd.DataFrame,
    table_name: str = PNS_TABLE_NAME,
    pk_columns: Optional[List[str]] = None
) -> None:
    """
    Insere ou atualiza linhas na tabela baseado na chave primária.
    
    Usa INSERT ... ON CONFLICT DO UPDATE para fazer upsert, preservando
    created_at quando a linha já existe (não atualiza created_at em updates).
    
    Args:
        df: DataFrame com os dados a serem inseridos/atualizados
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
        pk_columns: Lista de colunas que formam a chave primária
                   (padrão: PRIMARY_KEY_COLUMNS do config.py)
    
    Raises:
        ValueError: Se o DataFrame estiver vazio ou não contiver as colunas da PK
    """
    if df.empty:
        log.warning("DataFrame vazio, nada a inserir")
        return
    
    if pk_columns is None:
        pk_columns = PRIMARY_KEY_COLUMNS
    
    # Verificar se as colunas da PK estão presentes
    missing_pk = [col for col in pk_columns if col not in df.columns]
    if missing_pk:
        raise ValueError(f"DataFrame não contém colunas da PK: {missing_pk}")
    
    # Garantir que todas as colunas do DataFrame existem na tabela
    ensure_columns_exist(df.columns.tolist(), table_name)
    
    # Colunas para inserção (excluir created_at e updated_at se não estiverem no DataFrame)
    # created_at será preenchido automaticamente pelo DEFAULT na primeira inserção
    # updated_at será atualizado sempre
    columns_to_insert = [col for col in df.columns.tolist() 
                         if col not in ['created_at', 'updated_at']]
    
    # Construir query com ON CONFLICT DO UPDATE
    # SQLite 3.24+ suporta esta sintaxe
    placeholders = ', '.join(['?' for _ in columns_to_insert])
    columns_str = ', '.join(columns_to_insert)
    
    # Colunas para atualizar (todas exceto PK, created_at e updated_at)
    # created_at nunca é atualizado (preserva o valor original)
    # updated_at sempre é atualizado para CURRENT_TIMESTAMP
    update_columns = [col for col in columns_to_insert if col not in pk_columns]
    update_parts = [f'{col} = excluded.{col}' for col in update_columns]
    update_parts.append('updated_at = CURRENT_TIMESTAMP')
    update_set = ', '.join(update_parts)
    
    # Valores para inserção
    # Converter DataFrame para lista de tuplas, substituindo NaN por None
    # Isso é necessário porque SQLite não suporta NAType do pandas
    df_subset = df[columns_to_insert].copy()
    
    # Converter para lista de tuplas, substituindo todos os tipos de NaN por None
    # SQLite aceita None como NULL, mas não aceita NAType do pandas
    values = []
    for _, row in df_subset.iterrows():
        row_tuple = tuple(None if pd.isna(val) else val for val in row)
        values.append(row_tuple)
    
    # Construir cláusula ON CONFLICT usando PRIMARY_KEY_COLUMNS
    pk_constraint = ", ".join(PRIMARY_KEY_COLUMNS)
    
    query = f"""
        INSERT INTO {table_name} ({columns_str}, created_at, updated_at)
        VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT({pk_constraint}) DO UPDATE SET
            {update_set}
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(query, values)
        rows_affected = cursor.rowcount
        log.info(f"Inseridas/atualizadas {rows_affected} linhas em {table_name}")


# --- Tabelas de Metadados ---

def ensure_metadata_tables() -> None:
    """
    Cria as tabelas de metadados se elas não existirem.
    
    Tabelas criadas:
    - metadata_variables: Catálogo de variáveis (físicas e derivadas)
    - metadata_mapping: Mapeamento de variáveis por origem (ano)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Tabela metadata_variables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata_variables (
                nome_semantico TEXT PRIMARY KEY,
                descricao TEXT,
                tipo_dado TEXT,
                categoria TEXT NOT NULL CHECK(categoria IN ('fisica', 'derivada')),
                regra_derivacao TEXT,
                depends_on TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabela metadata_mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata_mapping (
                nome_semantico TEXT NOT NULL,
                origem TEXT NOT NULL,
                codigo_original TEXT,
                labels_categorias TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (nome_semantico, origem),
                FOREIGN KEY (nome_semantico) REFERENCES metadata_variables(nome_semantico)
                    ON DELETE CASCADE
            )
        """)
        
        log.info("Tabelas de metadados criadas/verificadas com sucesso")


def upsert_metadata_variable(
    nome_semantico: str,
    descricao: Optional[str] = None,
    tipo_dado: Optional[str] = None,
    categoria: str = "fisica",
    regra_derivacao: Optional[str] = None,
    depends_on: Optional[List[str]] = None
) -> None:
    """
    Insere ou atualiza uma variável na tabela metadata_variables.
    
    Args:
        nome_semantico: Nome semântico da variável (PK)
        descricao: Descrição da variável
        tipo_dado: Tipo de dado (int, string, float, etc.)
        categoria: 'fisica' ou 'derivada'
        regra_derivacao: Regra de derivação (para variáveis derivadas)
        depends_on: Lista de variáveis das quais esta depende (para variáveis derivadas)
    """
    # Garantir que as tabelas de metadados existem antes de inserir
    ensure_metadata_tables()
    
    depends_on_json = json.dumps(depends_on) if depends_on else None
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metadata_variables 
                (nome_semantico, descricao, tipo_dado, categoria, regra_derivacao, depends_on, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(nome_semantico) DO UPDATE SET
                descricao = excluded.descricao,
                tipo_dado = excluded.tipo_dado,
                categoria = excluded.categoria,
                regra_derivacao = excluded.regra_derivacao,
                depends_on = excluded.depends_on,
                updated_at = CURRENT_TIMESTAMP
        """, (nome_semantico, descricao, tipo_dado, categoria, regra_derivacao, depends_on_json))
        
        log.debug(f"Variável '{nome_semantico}' sincronizada em metadata_variables")


def upsert_metadata_mapping(
    nome_semantico: str,
    origem: str,
    codigo_original: Optional[str] = None,
    labels_categorias: Optional[Dict[str, str]] = None
) -> None:
    """
    Insere ou atualiza um mapeamento na tabela metadata_mapping.
    
    Args:
        nome_semantico: Nome semântico da variável
        origem: Ano da pesquisa ("2013" ou "2019")
        codigo_original: Código físico original no BigQuery
        labels_categorias: Dicionário com labels das categorias (ex: {"1": "Sim", "2": "Não"})
    """
    labels_json = json.dumps(labels_categorias) if labels_categorias else None
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metadata_mapping 
                (nome_semantico, origem, codigo_original, labels_categorias, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(nome_semantico, origem) DO UPDATE SET
                codigo_original = excluded.codigo_original,
                labels_categorias = excluded.labels_categorias,
                updated_at = CURRENT_TIMESTAMP
        """, (nome_semantico, origem, codigo_original, labels_json))
        
        log.debug(f"Mapeamento '{nome_semantico}' -> '{origem}' sincronizado em metadata_mapping")


def get_metadata_variables() -> pd.DataFrame:
    """
    Retorna todas as variáveis do catálogo de metadados.
    
    Returns:
        DataFrame com todas as variáveis e seus metadados
    """
    # Garantir que as tabelas de metadados existem
    ensure_metadata_tables()
    
    with get_connection() as conn:
        df = pd.read_sql_query("""
            SELECT 
                nome_semantico,
                descricao,
                tipo_dado,
                categoria,
                regra_derivacao,
                depends_on
            FROM metadata_variables
            ORDER BY nome_semantico
        """, conn)
        
        # Converter depends_on de JSON para lista Python
        if 'depends_on' in df.columns:
            df['depends_on'] = df['depends_on'].apply(
                lambda x: json.loads(x) if x else None
            )
        
        return df


def get_metadata_mapping(nome_semantico: Optional[str] = None) -> pd.DataFrame:
    """
    Retorna os mapeamentos de variáveis por origem.
    
    Args:
        nome_semantico: Se fornecido, filtra por esta variável específica
    
    Returns:
        DataFrame com os mapeamentos
    """
    # Garantir que as tabelas de metadados existem
    ensure_metadata_tables()
    
    query = """
        SELECT 
            nome_semantico,
            origem,
            codigo_original,
            labels_categorias
        FROM metadata_mapping
    """
    
    params = []
    if nome_semantico:
        query += " WHERE nome_semantico = ?"
        params.append(nome_semantico)
    
    query += " ORDER BY nome_semantico, origem"
    
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params)
        
        # Converter labels_categorias de JSON para dicionário Python
        if 'labels_categorias' in df.columns:
            df['labels_categorias'] = df['labels_categorias'].apply(
                lambda x: json.loads(x) if x else None
            )
        
        return df


def variable_exists_in_metadata(nome_semantico: str) -> bool:
    """
    Verifica se uma variável existe no catálogo de metadados.
    
    Args:
        nome_semantico: Nome semântico da variável
    
    Returns:
        True se a variável existe, False caso contrário
    """
    # Garantir que as tabelas de metadados existem antes de consultar
    ensure_metadata_tables()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM metadata_variables WHERE nome_semantico = ?",
            (nome_semantico,)
        )
        return cursor.fetchone()[0] > 0

