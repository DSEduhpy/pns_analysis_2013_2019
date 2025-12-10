"""
Cliente SQLite de baixo nível para gerenciamento do cache local.

Este módulo fornece funções básicas para:
- Gerenciamento de conexões
- Criação e evolução de schema
- Operações de upsert
"""
import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, List
import pandas as pd

from config import SQLITE_PATH, PNS_TABLE_NAME

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
    - Chave primária composta: (origem, identificador_unidade)
    - Colunas de controle: created_at, updated_at
    - Sem colunas de dados semânticos (serão adicionadas dinamicamente)
    
    Args:
        table_name: Nome da tabela (padrão: PNS_TABLE_NAME)
    """
    if table_exists(table_name):
        log.debug(f"Tabela {table_name} já existe")
        return
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                origem TEXT NOT NULL,
                identificador_unidade TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (origem, identificador_unidade)
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
    existing_columns = get_table_columns(table_name)
    
    for col_name in column_names:
        if col_name in existing_columns:
            continue
        
        # Inferir tipo baseado no nome da coluna
        # Colunas de controle já existem, então não precisamos tratá-las aqui
        if col_name in ['origem', 'identificador_unidade', 'created_at', 'updated_at']:
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
                   (padrão: ['origem', 'identificador_unidade'])
    
    Raises:
        ValueError: Se o DataFrame estiver vazio ou não contiver as colunas da PK
    """
    if df.empty:
        log.warning("DataFrame vazio, nada a inserir")
        return
    
    if pk_columns is None:
        pk_columns = ['origem', 'identificador_unidade']
    
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
    values = df[columns_to_insert].values.tolist()
    
    query = f"""
        INSERT INTO {table_name} ({columns_str}, created_at, updated_at)
        VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(origem, identificador_unidade) DO UPDATE SET
            {update_set}
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(query, values)
        rows_affected = cursor.rowcount
        log.info(f"Inseridas/atualizadas {rows_affected} linhas em {table_name}")

