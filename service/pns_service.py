"""
Camada de serviço - Interface pública para o cientista de dados.

Expõe uma função de alto nível `get_dataframe` que abstrai completamente
o acesso ao BigQuery e ao repositório local em SQLite.
"""
from typing import List, Optional, Dict, Any

import logging
import pandas as pd

from config import PNS_TABLES, PRIMARY_KEY_COLUMNS
from dao.pns_dao import ensure_data as dao_ensure_data, load_from_storage as dao_load_from_storage
from mapping import VAR_MAP
from dao.sqlite_client import (
    get_metadata_variables,
    get_metadata_mapping,
    variable_exists_in_metadata,
    upsert_metadata_variable
)
from dao.pns_dao import get_dao

log = logging.getLogger(__name__)

# Registry em memória para funções de variáveis derivadas
# Chave: nome_semantico, Valor: função que recebe DataFrame e retorna Series/array
_DERIVED_VARIABLES_REGISTRY: Dict[str, Any] = {}


def get_dataframe(
    variables: Optional[List[str]] = None,
    sources: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Retorna um DataFrame consolidado da PNS pronto para análise.
    
    Esta é a interface oficial para o cientista de dados. Ela:
    - Aceita variáveis semânticas (ex.: "sexo", "idade", "fez_mamografia")
    - Garante que os dados estejam no cache (lazy loading)
    - Carrega do SQLite e devolve um DataFrame empilhado por `origem`
    
    Args:
        variables: Lista de variáveis semânticas desejadas
        sources: Lista de origens (anos) desejadas, ex.: ["2013", "2019"]
        filters: Dicionário de filtros semânticos adicionais
    
    Returns:
        DataFrame pandas com dados da PNS já limpos e prontos para análise.
    
    Raises:
        ValueError: Se a lista de variáveis estiver vazia.
    """
    # Garantir listas e copiar para evitar efeitos colaterais
    variables = list(variables) if variables is not None else []
    sources = list(sources) if sources is not None else []

    # Remover duplicatas preservando ordem
    variables = list(dict.fromkeys(variables))
    sources = list(dict.fromkeys(sources))

    # Validar variáveis semânticas: verificar primeiro nos metadados (permite derivadas),
    # depois no mapping.py (variáveis físicas), e por último se existe fisicamente no banco
    # Nota: 'origem' é uma coluna de controle adicionada automaticamente e não precisa estar no VAR_MAP
    from dao.sqlite_client import get_table_columns, PNS_TABLE_NAME
    
    unknown_variables = []
    # Carregar colunas do banco uma única vez (otimização)
    existing_columns = None
    
    for var in variables:
        # 'origem' é uma coluna especial adicionada automaticamente, permitir sempre
        if var == 'origem':
            continue
        # Verificar se existe nos metadados (inclui variáveis derivadas)
        if not variable_exists_in_metadata(var):
            # Verificar se existe no mapping.py (variáveis físicas)
            if var not in VAR_MAP:
                # Última verificação: se existe fisicamente no banco SQLite
                # Isso permite ler variáveis derivadas calculadas em sessões anteriores
                # Carregar colunas do banco apenas quando necessário (lazy loading)
                if existing_columns is None:
                    existing_columns = get_table_columns(PNS_TABLE_NAME)
                
                if var not in existing_columns:
                    unknown_variables.append(var)
                else:
                    log.debug(
                        f"Variável '{var}' não está no mapping nem nos metadados, "
                        "mas existe fisicamente no banco. Permitindo leitura."
                    )
    
    if unknown_variables:
        # Não devemos criar colunas nem seguir o fluxo para variáveis inexistentes
        raise ValueError(
            "Variáveis semânticas desconhecidas: "
            f"{unknown_variables}. "
            "Use service.pns_service.list_variables() para consultar as variáveis disponíveis."
        )

    # Garantir que as variáveis de identificação (PK) sempre estejam presentes internamente.
    # Isso é necessário para que o fluxo de ingestão/transformação consiga
    # popular a PK composta no SQLite.
    # Nota: 'origem' é adicionada automaticamente pelo transform/converters.py
    pk_vars = set(col for col in PRIMARY_KEY_COLUMNS if col != 'origem')
    variables = list(set(variables) | pk_vars)

    if not variables:
        raise ValueError("A lista de variáveis (variables) não pode ser vazia.")

    # Se não forem fornecidas origens, usar todas as disponíveis em PNS_TABLES
    if not sources:
        sources = list(PNS_TABLES.keys())

    log.info(
        "get_dataframe chamado com %d variáveis e %d origens",
        len(variables),
        len(sources),
    )
    log.debug("Variáveis: %s", variables)
    log.debug("Origens: %s", sources)
    log.debug("Filtros adicionais: %s", filters)

    # Separar variáveis físicas e derivadas
    physical_vars = [v for v in variables if not _is_derived_variable(v)]
    derived_vars = [v for v in variables if _is_derived_variable(v)]
    
    # 1. Garantir que os dados físicos estejam no cache (pode disparar ingestão)
    if physical_vars:
        dao_ensure_data(semantic_variables=physical_vars, sources=sources, filters=filters)
    
    # 2. Carregar dados físicos do repositório local
    # Incluir colunas dos filtros temporariamente para poder aplicá-los
    filter_columns = set(filters.keys()) if filters else set()
    physical_vars_with_filters = list(set(physical_vars) | filter_columns) if physical_vars else list(filter_columns)
    
    if physical_vars_with_filters:
        df = dao_load_from_storage(semantic_variables=physical_vars_with_filters, sources=sources, filters=filters)
    else:
        # Se só há variáveis derivadas, precisamos carregar pelo menos as PKs
        df = dao_load_from_storage(
            semantic_variables=list(PRIMARY_KEY_COLUMNS),
            sources=sources,
            filters=filters
        )
    
    # 3. Calcular e persistir variáveis derivadas se necessário
    if derived_vars:
        dao = get_dao()
        for derived_var in derived_vars:
            df = dao._ensure_derived_variable(df, derived_var, sources)
    
    # 4. Selecionar apenas as variáveis solicitadas (incluindo derivadas)
    # Remover colunas dos filtros se não foram solicitadas explicitamente
    if not df.empty:
        # Usar set para união e remover duplicatas automaticamente
        requested_cols = set(v for v in variables if v in df.columns)
        pk_cols = set(col for col in PRIMARY_KEY_COLUMNS if col in df.columns)
        columns_to_return = list(requested_cols | pk_cols)
        df = df[columns_to_return]

    # Garantir que sempre retorne um DataFrame mesmo se vazio
    if df is None:
        df = pd.DataFrame()

    return df


def register_derived_variable(
    name: str,
    description: str,
    depends_on: List[str],
    func: Any
) -> None:
    """
    Registra uma nova variável derivada no sistema.
    
    Uma variável derivada é calculada a partir de outras variáveis usando
    uma função Python. Uma vez registrada, ela pode ser usada como qualquer
    outra variável no `get_dataframe()`, e será automaticamente calculada
    e armazenada no cache SQLite.
    
    Args:
        name: Nome semântico da nova variável (ex: "imc")
        description: Descrição da variável para o catálogo de metadados
        depends_on: Lista de variáveis necessárias para o cálculo (ex: ["peso", "altura"])
        func: Função que recebe um DataFrame e retorna uma Series/array com os valores calculados.
              A função deve ser capaz de trabalhar com DataFrames que contêm as colunas
              especificadas em `depends_on`.
    
    Raises:
        ValueError: Se o nome já estiver em uso por uma variável física,
                    ou se as dependências não forem válidas
    
    Example:
        >>> register_derived_variable(
        ...     name="imc",
        ...     description="Índice de Massa Corporal calculado a partir de peso e altura",
        ...     depends_on=["peso", "altura"],
        ...     func=lambda df: df["peso"] / (df["altura"] ** 2)
        ... )
    """
    # Validar nome
    if name in VAR_MAP:
        raise ValueError(
            f"Nome '{name}' já está em uso por uma variável física. "
            "Escolha um nome diferente para a variável derivada."
        )
    
    # Validar dependências
    unknown_deps = []
    for dep in depends_on:
        if not variable_exists_in_metadata(dep) and dep not in VAR_MAP:
            unknown_deps.append(dep)
    
    if unknown_deps:
        raise ValueError(
            f"Dependências desconhecidas: {unknown_deps}. "
            "Todas as dependências devem ser variáveis válidas (físicas ou derivadas)."
        )
    
    # Validar função
    if not callable(func):
        raise ValueError("O parâmetro 'func' deve ser uma função callable")
    
    # Registrar função no registry em memória
    _DERIVED_VARIABLES_REGISTRY[name] = func
    log.info(f"Função para variável derivada '{name}' registrada em memória")
    
    # Salvar metadados no SQLite
    # Construir regra de derivação como string (descrição da função)
    regra_derivacao = f"Calculada a partir de: {', '.join(depends_on)}"
    
    try:
        upsert_metadata_variable(
            nome_semantico=name,
            descricao=description,
            tipo_dado="float",  # Padrão para variáveis derivadas (pode ser ajustado)
            categoria="derivada",
            regra_derivacao=regra_derivacao,
            depends_on=depends_on
        )
        log.info(
            f"Variável derivada '{name}' registrada com sucesso nos metadados. "
            f"Dependências: {depends_on}"
        )
    except Exception as e:
        log.error(
            f"Erro ao salvar metadados para variável derivada '{name}': {e}. "
            "A variável foi registrada em memória, mas pode não estar disponível em sessões futuras."
        )
        raise


def _get_derived_variable_func(name: str) -> Optional[Any]:
    """
    Retorna a função registrada para uma variável derivada.
    
    Args:
        name: Nome semântico da variável derivada
    
    Returns:
        Função registrada ou None se não existir
    """
    return _DERIVED_VARIABLES_REGISTRY.get(name)


def _is_derived_variable(name: str) -> bool:
    """
    Verifica se uma variável é derivada.
    
    Args:
        name: Nome semântico da variável
    
    Returns:
        True se for variável derivada, False caso contrário
    """
    # Verificar no registry em memória primeiro (mais rápido)
    if name in _DERIVED_VARIABLES_REGISTRY:
        return True
    
    # Verificar nos metadados do SQLite
    df_vars = get_metadata_variables()
    if not df_vars.empty:
        var_row = df_vars[df_vars['nome_semantico'] == name]
        if not var_row.empty:
            return var_row.iloc[0].get('categoria') == 'derivada'
    
    return False


def list_variables(source: Optional[str] = None) -> pd.DataFrame:
    """
    Lista as variáveis semânticas disponíveis consultando o catálogo de metadados.
    
    Retorna um DataFrame amigável ao cientista de dados, com informações
    sobre a existência de cada variável em cada origem (ano), bem como
    os códigos físicos, tipos, descrições e regras de derivação.
    
    Args:
        source: Origem específica para filtrar (ex.: "2013" ou "2019").
                Se None, lista variáveis considerando todas as origens.
    
    Returns:
        DataFrame com colunas:
            - variable: nome semântico
            - descricao: descrição da variável
            - categoria: 'fisica' ou 'derivada'
            - tipo_dado: tipo de dado (int, string, float, etc.)
            - regra_derivacao: regra de derivação (se aplicável)
            - depends_on: lista de dependências (se aplicável)
            - exists_2013 / exists_2019: se existe em cada origem
            - code_2013 / code_2019: código físico por origem
            - type_2013 / type_2019: tipo por origem
    """
    # Garantir que os metadados estejam sincronizados antes de consultar
    # Isso garante que as variáveis do mapping.py estejam disponíveis
    from dao.pns_dao import get_dao
    dao = get_dao()  # Isso garante que sync_metadata() foi chamado no __init__
    dao.sync_metadata()  # Garantir sincronização explícita (caso o DAO já exista)
    
    # Carregar metadados do SQLite
    df_vars = get_metadata_variables()
    df_mapping = get_metadata_mapping()
    
    if df_vars.empty:
        log.warning("Nenhuma variável encontrada no catálogo de metadados")
        return pd.DataFrame()
    
    # Fazer join com mapeamentos
    available_sources = sorted(PNS_TABLES.keys())
    
    # Criar DataFrame consolidado
    rows = []
    for _, var_row in df_vars.iterrows():
        nome_semantico = var_row['nome_semantico']
        
        # Buscar mapeamentos desta variável
        var_mappings = df_mapping[df_mapping['nome_semantico'] == nome_semantico]
        
        row = {
            'variable': nome_semantico,
            'descricao': var_row.get('descricao'),
            'categoria': var_row.get('categoria', 'fisica'),
            'tipo_dado': var_row.get('tipo_dado'),
            'regra_derivacao': var_row.get('regra_derivacao'),
            'depends_on': var_row.get('depends_on'),
        }
        
        # Adicionar informações por origem
        for src in available_sources:
            mapping_row = var_mappings[var_mappings['origem'] == src]
            if not mapping_row.empty:
                row[f'code_{src}'] = mapping_row.iloc[0].get('codigo_original')
                # Tipo pode vir do mapping ou do metadata_variables
                row[f'type_{src}'] = mapping_row.iloc[0].get('tipo', var_row.get('tipo_dado'))
                row[f'exists_{src}'] = mapping_row.iloc[0].get('codigo_original') is not None
            else:
                row[f'code_{src}'] = None
                row[f'type_{src}'] = None
                row[f'exists_{src}'] = False
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Ordenar por nome de variável para facilitar leitura
    if not df.empty:
        df = df.sort_values("variable").reset_index(drop=True)
    
    # Se uma origem específica foi fornecida, filtrar apenas variáveis que existem nela
    if source is not None:
        if source not in available_sources:
            raise ValueError(f"Origem inválida: {source}. Valores válidos: {available_sources}")
        exists_col = f"exists_{source}"
        if exists_col in df.columns:
            df = df[df[exists_col]].reset_index(drop=True)
    
    return df


def repopulate_all_data(
    sources: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    preserve_derived: bool = True
) -> None:
    """
    Repopula o repositório local com todos os dados físicos do mapping.py.
    
    Esta função é útil para:
    - Repopular o banco após problemas ou corrupção de dados
    - Atualizar todos os dados quando o mapping.py foi modificado
    - Garantir que todas as variáveis físicas estejam disponíveis
    
    **IMPORTANTE**: Esta função preserva as colunas derivadas por padrão.
    As colunas derivadas não são removidas nem recalculadas automaticamente.
    Se você quiser recalcular variáveis derivadas, faça isso manualmente
    após a repopulação.
    
    Args:
        sources: Lista de origens (anos) a repopular. Se None, usa todas as origens disponíveis.
        filters: Filtros adicionais a aplicar (opcional)
        preserve_derived: Se True (padrão), preserva colunas derivadas existentes.
                         Se False, permite que colunas derivadas sejam sobrescritas
                         (não recomendado, pois pode causar perda de dados).
    
    Exemplo:
        ```python
        from service.pns_service import repopulate_all_data
        
        # Repopular todos os dados para 2013 e 2019
        repopulate_all_data(sources=["2013", "2019"])
        
        # Repopular apenas 2019 com filtros
        repopulate_all_data(
            sources=["2019"],
            filters={"sexo": "2", "idade": {"operador": ">=", "valor": 25}}
        )
        ```
    
    Notas:
        - Esta operação pode levar vários minutos, dependendo da quantidade de dados
        - Os dados são atualizados via UPSERT, então registros existentes são atualizados
        - Colunas derivadas são preservadas automaticamente (não são removidas)
        - Variáveis físicas são atualizadas/inseridas conforme o mapping.py atual
    """
    log.info("Iniciando repopulação de todos os dados do mapping.py")
    
    # Se não foram fornecidas origens, usar todas as disponíveis
    if sources is None:
        sources = list(PNS_TABLES.keys())
    
    # Listar todas as variáveis físicas do VAR_MAP
    # Excluir PRIMARY_KEY_COLUMNS pois elas já são sempre incluídas
    all_physical_variables = [
        var_name for var_name in VAR_MAP.keys()
        if var_name not in PRIMARY_KEY_COLUMNS
    ]
    
    log.info(
        f"Repopulando {len(all_physical_variables)} variáveis físicas "
        f"para {len(sources)} origem(ns): {sources}"
    )
    
    # Garantir que todas as variáveis físicas estejam no repositório local
    # O DAO vai verificar se precisa buscar dados do BigQuery para cada variável/origem
    dao = get_dao()
    dao.ensure_data(
        semantic_variables=all_physical_variables,
        sources=sources,
        filters=filters
    )
    
    log.info(
        f"Repopulação concluída. {len(all_physical_variables)} variáveis físicas "
        f"garantidas para {len(sources)} origem(ns)."
    )
    
    if preserve_derived:
        # Verificar se há colunas derivadas no repositório
        from dao.sqlite_client import get_table_columns, get_metadata_variables
        
        existing_columns = set(get_table_columns())
        df_metadata = get_metadata_variables()
        
        if not df_metadata.empty:
            derived_vars = df_metadata[
                df_metadata.get('categoria', '') == 'derivada'
            ]['nome_semantico'].tolist()
            
            existing_derived = [v for v in derived_vars if v in existing_columns]
            
            if existing_derived:
                log.info(
                    f"Colunas derivadas preservadas ({len(existing_derived)}): "
                    f"{', '.join(existing_derived[:5])}"
                    + (f" e mais {len(existing_derived) - 5}" if len(existing_derived) > 5 else "")
                )
            else:
                log.debug("Nenhuma coluna derivada encontrada para preservar")
    
    log.info("Repopulação finalizada com sucesso")
