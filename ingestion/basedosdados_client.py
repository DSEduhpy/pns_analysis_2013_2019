"""
Cliente para execução de queries no BigQuery via basedosdados.

Wrapper simples em torno da biblioteca basedosdados para executar
queries SQL e retornar DataFrames pandas com dados brutos (colunas físicas).
"""
import logging
import pandas as pd
import basedosdados as bd

from config import BILLING_PROJECT_ID

log = logging.getLogger(__name__)


def run_query(sql: str) -> pd.DataFrame:
    """
    Executa uma query SQL no BigQuery via basedosdados e retorna DataFrame.
    
    Esta função é um wrapper simples em torno de `bd.read_sql()` que:
    - Executa a query no BigQuery usando o billing_project_id configurado
    - Retorna um DataFrame pandas com os dados brutos
    - As colunas do DataFrame terão nomes físicos (ex: "c006", "r001")
    
    Args:
        sql: Query SQL completa para execução no BigQuery
            Deve ser uma query SELECT válida
    
    Returns:
        DataFrame pandas com os dados retornados pela query
        Colunas terão nomes físicos (códigos originais do BigQuery)
    
    Raises:
        ValueError: Se BILLING_PROJECT_ID não estiver configurado
        Exception: Se houver erro na execução da query (erro do BigQuery, 
                  rede, autenticação, etc.)
    """
    if not BILLING_PROJECT_ID:
        raise ValueError(
            "BILLING_PROJECT_ID não configurado. "
            "Configure no arquivo .env ou variável de ambiente."
        )
    
    log.info(f"Executando query no BigQuery (projeto: {BILLING_PROJECT_ID})...")
    log.debug(f"Query SQL:\n{sql}")
    
    try:
        # Executar query via basedosdados
        df = bd.read_sql(sql, billing_project_id=BILLING_PROJECT_ID)
        
        log.info(f"Query executada com sucesso. Retornados {len(df)} registros.")
        log.debug(f"Colunas retornadas: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        log.error(f"Erro ao executar query no BigQuery: {e}")
        raise


def test_connection() -> bool:
    """
    Testa a conexão com o BigQuery.
    
    Executa uma query simples para verificar se:
    - As credenciais estão configuradas corretamente
    - O billing_project_id é válido
    - Há acesso ao dataset da PNS
    
    Returns:
        True se a conexão está funcionando, False caso contrário
    """
    try:
        # Query de teste simples (apenas conta linhas)
        test_query = """
            SELECT COUNT(*) as total
            FROM `basedosdados.br_ms_pns.microdados_2013`
            LIMIT 1
        """
        
        df = run_query(test_query)
        total = df.iloc[0]['total'] if not df.empty else 0
        
        log.info(f"Teste de conexão bem-sucedido. Total de registros em 2013: {total}")
        return True
    
    except Exception as e:
        log.error(f"Teste de conexão falhou: {e}")
        return False

