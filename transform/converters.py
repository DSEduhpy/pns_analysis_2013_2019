"""
Conversores de dados físicos para semânticos.

Este módulo converte DataFrames com colunas físicas (códigos do BigQuery)
para colunas semânticas (nomes legíveis), usando o mapping.py.
"""
import logging
import pandas as pd
from typing import Dict

from mapping import VAR_MAP, get_codigo_fisico

log = logging.getLogger(__name__)


def to_semantic(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Converte DataFrame físico (colunas com códigos) para semântico (nomes legíveis).
    
    Esta função:
    1. Renomeia colunas físicas para semânticas usando mapping.py
    2. Adiciona coluna de controle 'origem' com o ano da pesquisa
    3. Garante que colunas de identificação (id_upa, id_domicilio, id_morador) sejam mapeadas
    
    Args:
        df: DataFrame com colunas físicas (ex: c006, r001, v00291, upa_pns, v0006_pns, c00301)
        source: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        DataFrame com colunas semânticas (ex: sexo, preventivo, peso_amostral)
        e colunas de controle (origem, id_upa, id_domicilio, id_morador)
    """
    if df.empty:
        log.warning("DataFrame vazio recebido, retornando vazio")
        return df.copy()
    
    # Criar cópia para não modificar o original
    df_semantic = df.copy()
    
    # Construir dicionário de renomeação: código_fisico -> nome_semantico
    rename_dict: Dict[str, str] = {}
    
    # Iterar sobre o VAR_MAP para encontrar mapeamentos
    for semantic_name, var_info in VAR_MAP.items():
        # Ignorar a chave "descricao" se existir
        if source not in var_info:
            continue
        
        # Verificar se a chave é realmente uma origem (não "descricao")
        if not isinstance(var_info[source], dict):
            continue
        
        physical_code = var_info[source].get("codigo")
        if physical_code is None:
            continue
        
        # Se a coluna física existe no DataFrame, adicionar ao dicionário de renomeação
        if physical_code in df_semantic.columns:
            rename_dict[physical_code] = semantic_name
            log.debug(f"Mapeando '{physical_code}' -> '{semantic_name}' para origem {source}")
        else:
            log.debug(f"Coluna física '{physical_code}' não encontrada no DataFrame")
    
    # Renomear colunas físicas para semânticas
    df_semantic = df_semantic.rename(columns=rename_dict)
    
    # Adicionar coluna de controle 'origem'
    df_semantic['origem'] = source
    
    # Verificar se as colunas de identificação (PK) foram mapeadas corretamente
    required_id_vars = ["id_upa", "id_domicilio", "id_morador"]
    missing_ids = []
    for id_var in required_id_vars:
        if id_var not in df_semantic.columns:
            # Tentar encontrar o código físico e renomear manualmente se necessário
            id_code = get_codigo_fisico(id_var, source)
            if id_code and id_code in df_semantic.columns:
                df_semantic = df_semantic.rename(columns={id_code: id_var})
                log.debug(f"Coluna '{id_code}' renomeada para '{id_var}'")
            else:
                missing_ids.append(id_var)
    
    if missing_ids:
        log.warning(
            f"Colunas de identificação não encontradas: {missing_ids}. "
            f"Estas são necessárias para a chave primária."
        )
    
    # Manter colunas que não foram mapeadas (caso existam colunas físicas não mapeadas)
    # Essas serão mantidas com seus nomes originais
    
    log.info(
        f"Conversão concluída: {len(rename_dict)} colunas renomeadas, "
        f"DataFrame com {len(df_semantic)} linhas e {len(df_semantic.columns)} colunas"
    )
    
    return df_semantic

