"""
Construtor de queries SQL para BigQuery.

Traduz pedidos semânticos (variáveis e filtros) em queries SQL físicas
que podem ser executadas no BigQuery via basedosdados.
"""
import logging
from typing import List, Dict, Optional

from config import BIGQUERY_DATASET, PNS_TABLES, PNS_FILTERS, PRIMARY_KEY_COLUMNS
from mapping import get_codigo_fisico, variavel_existe

log = logging.getLogger(__name__)


def resolve_physical_codes(
    semantic_variables: List[str],
    source: str
) -> List[str]:
    """
    Resolve variáveis semânticas para códigos físicos, ignorando None.
    
    Args:
        semantic_variables: Lista de nomes semânticos (ex: ["sexo", "idade"])
        source: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        Lista de códigos físicos válidos (ex: ["c006", "c008"])
        Ignora variáveis que não existem nesta origem (codigo=None)
    """
    physical_codes = []
    
    for semantic_var in semantic_variables:
        code = get_codigo_fisico(semantic_var, source)
        if code is not None:
            physical_codes.append(code)
            log.debug(f"Variável '{semantic_var}' -> código físico '{code}' para {source}")
        else:
            log.warning(
                f"Variável semântica '{semantic_var}' não existe para origem {source}, "
                f"será ignorada na query"
            )
    
    return physical_codes


def translate_semantic_filter_to_physical(
    semantic_filter: Dict,
    source: str
) -> Optional[str]:
    """
    Traduz um filtro semântico para uma condição SQL física.
    
    Args:
        semantic_filter: Dicionário com filtro semântico
            Ex: {"semantico": "sexo", "valor": "2"}
            Ex: {"semantico": "idade", "operador": ">=", "valor": 25}
        source: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        String com condição SQL (ex: "c006 = '2'" ou "CAST(c008 AS INT64) >= 25")
        ou None se não puder traduzir
    """
    from mapping import get_tipo
    
    semantic_var = semantic_filter.get("semantico")
    if not semantic_var:
        return None
    
    physical_code = get_codigo_fisico(semantic_var, source)
    if physical_code is None:
        log.warning(f"Não é possível traduzir filtro para '{semantic_var}' em {source}")
        return None
    
    operator = semantic_filter.get("operador", "=")
    value = semantic_filter.get("valor")
    
    # Verificar se a variável é numérica (int ou float)
    # Se for, usar CAST para garantir comparação numérica no BigQuery
    var_type = get_tipo(semantic_var, source)
    use_cast = var_type in ["int", "integer", "float", "real"]
    
    # Determinar se o valor precisa de aspas (string) ou não (número)
    if isinstance(value, str) and not use_cast:
        value_str = f"'{value}'"
    else:
        value_str = str(value)
    
    # Aplicar CAST se necessário (para comparações numéricas)
    if use_cast and operator in [">", "<", ">=", "<=", "!=", "<>"]:
        # BigQuery usa INT64 para inteiros e FLOAT64 para floats
        cast_type = "INT64" if var_type in ["int", "integer"] else "FLOAT64"
        column_with_cast = f"CAST({physical_code} AS {cast_type})"
        return f"{column_with_cast} {operator} {value_str}"
    else:
        return f"{physical_code} {operator} {value_str}"


def build_where_clause(
    filters: Optional[Dict] = None,
    source: str = None,
    use_default_filters: bool = True
) -> str:
    """
    Constrói a cláusula WHERE da query SQL.
    
    Args:
        filters: Filtros adicionais em formato semântico (opcional)
        source: Ano da pesquisa ("2013" ou "2019")
        use_default_filters: Se True, aplica filtros padrão do config.py
    
    Returns:
        String com cláusula WHERE (ex: "WHERE c006 = '2' AND CAST(c008 AS INT64) >= 25")
        Retorna string vazia se não houver filtros
    """
    conditions = []
    
    # Aplicar filtros padrão se solicitado
    if use_default_filters and source and source in PNS_FILTERS:
        default_filters = PNS_FILTERS[source]
        
        # Filtro de sexo
        if "sexo" in default_filters:
            sex_filter = default_filters["sexo"]
            condition = translate_semantic_filter_to_physical(sex_filter, source)
            if condition:
                conditions.append(condition)
        
        # Filtro de idade mínima
        if "idade_min" in default_filters:
            min_age = default_filters["idade_min"]
            age_code = get_codigo_fisico("idade", source)
            if age_code:
                # BigQuery precisa de CAST para comparação numérica
                conditions.append(f"CAST({age_code} AS INT64) >= {min_age}")
    
    # Aplicar filtros adicionais fornecidos
    if filters:
        for semantic_var, value in filters.items():
            # Se o valor é um dicionário, é um filtro complexo
            if isinstance(value, dict):
                filter_dict = {"semantico": semantic_var, **value}
                condition = translate_semantic_filter_to_physical(filter_dict, source)
            else:
                # Filtro simples: variável = valor
                filter_dict = {"semantico": semantic_var, "valor": value}
                condition = translate_semantic_filter_to_physical(filter_dict, source)
            
            if condition:
                conditions.append(condition)
    
    if not conditions:
        return ""
    
    return "WHERE " + " AND ".join(conditions)


def build_select_query(
    semantic_variables: List[str],
    source: str,
    filters: Optional[Dict] = None,
    use_default_filters: bool = True
) -> str:
    """
    Constrói uma query SELECT completa para o BigQuery.
    
    Esta é a função principal do query_builder. Ela:
    1. Traduz variáveis semânticas para códigos físicos
    2. Ignora variáveis que não existem na origem (codigo=None)
    3. Traduz filtros semânticos para físicos
    4. Monta a query SQL completa
    
    Args:
        semantic_variables: Lista de variáveis semânticas solicitadas
            Ex: ["sexo", "idade", "preventivo", "mamografia"]
        source: Ano da pesquisa ("2013" ou "2019")
        filters: Filtros adicionais em formato semântico (opcional)
            Ex: {"idade": {"operador": ">=", "valor": 30}}
        use_default_filters: Se True, aplica filtros padrão do config.py
    
    Returns:
        String com query SQL completa pronta para execução no BigQuery
    
    Raises:
        ValueError: Se a origem não for válida ou não houver variáveis válidas
    """
    if source not in PNS_TABLES:
        raise ValueError(f"Origem inválida: {source}. Deve ser '2013' ou '2019'")
    
    # Colunas de identificação obrigatórias para PK (sempre incluídas)
    # Nota: 'origem' é adicionada automaticamente pelo transform/converters.py
    pk_vars = [col for col in PRIMARY_KEY_COLUMNS if col != 'origem']
    
    # Resolver códigos físicos (ignora None automaticamente)
    physical_codes = resolve_physical_codes(semantic_variables, source)
    
    # Garantir que colunas de identificação estejam sempre presentes
    for id_var in pk_vars:
        id_code = get_codigo_fisico(id_var, source)
        if id_code and id_code not in physical_codes:
            physical_codes.insert(0, id_code)  # Inserir no início para facilitar leitura
            log.debug(f"Coluna de identificação '{id_code}' (de '{id_var}') adicionada automaticamente")
    
    if not physical_codes:
        raise ValueError(
            f"Nenhuma variável válida encontrada para origem {source}. "
            f"Variáveis solicitadas: {semantic_variables}"
        )
    
    # Construir SELECT com colunas físicas
    columns_str = ", ".join(physical_codes)
    
    # Nome completo da tabela no BigQuery
    full_table_name = f"`{BIGQUERY_DATASET}.{PNS_TABLES[source]}`"
    
    # Construir WHERE clause
    where_clause = build_where_clause(filters, source, use_default_filters)
    
    # Montar query completa
    query = f"SELECT {columns_str}\nFROM {full_table_name}\n{where_clause}"
    
    log.info(
        f"Query construída para origem {source}: "
        f"{len(physical_codes)} colunas, {len(where_clause.split('AND')) if where_clause else 0} filtros"
    )
    log.debug(f"Query SQL:\n{query}")
    
    return query
