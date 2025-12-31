"""
DAO (Data Access Object) para orquestração de dados da PNS.

Este módulo é o cérebro que decide se vai ao repositório local (SQLite) ou ao BigQuery,
orquestrando todo o fluxo: verificação de repositório -> ingestão -> transformação -> persistência.
"""
import logging
import re
import os
from typing import List, Optional, Dict
import pandas as pd

from config import PNS_TABLES, PNS_TABLE_NAME, PRIMARY_KEY_COLUMNS
from dao.sqlite_client import (
    ensure_table_exists,
    get_table_columns,
    ensure_columns_exist,
    upsert_rows,
    get_connection,
    ensure_metadata_tables,
    upsert_metadata_variable,
    upsert_metadata_mapping
)
from ingestion.query_builder import build_select_query
from ingestion.basedosdados_client import run_query
from transform.converters import to_semantic
from transform.cleaning import DataCleaner
from mapping import VAR_MAP


log = logging.getLogger(__name__)


class PNSDAO:
    """
    DAO responsável por orquestrar acesso aos dados da PNS.
    
    Decide quando usar cache (SQLite) ou buscar do BigQuery,
    garantindo que os dados estejam sempre atualizados e completos.
    """
    
    def __init__(self):
        """Inicializa o DAO."""
        # Garantir que as tabelas existem
        ensure_table_exists()
        ensure_metadata_tables()
        
        # Sincronizar metadados do mapping.py
        self.sync_metadata()
    
    def ensure_data(
        self,
        semantic_variables: List[str],
        sources: List[str],
        filters: Optional[Dict] = None
    ) -> None:
        """
        Garante que os dados solicitados estão no repositório local.
        
        Verifica se as colunas e registros existem no SQLite.
        Se faltar algo, executa o fluxo completo:
        Ingestion -> Transform -> SQLite Upsert
        
        Args:
            semantic_variables: Lista de variáveis semânticas solicitadas
                Ex: ["sexo", "idade", "preventivo", "mamografia"]
            sources: Lista de origens (anos) solicitadas
                Ex: ["2013", "2019"]
            filters: Filtros adicionais em formato semântico (opcional)
        """
        log.info(
            f"Garantindo dados: {len(semantic_variables)} variáveis, "
            f"{len(sources)} origens"
        )
        
        # Incluir colunas usadas nos filtros (se houver)
        filter_columns = set()
        if filters:
            filter_columns = set(filters.keys())
        
        # Garantir que todas as colunas necessárias existem na tabela
        # Inclui: variáveis solicitadas + PK + colunas de filtros
        required_columns = set(semantic_variables) | set(PRIMARY_KEY_COLUMNS) | filter_columns
        ensure_columns_exist(list(required_columns))
        
        # Para cada origem, verificar se precisa buscar dados
        # Usar todas as colunas necessárias (variáveis + filtros) para garantir que os dados estejam completos
        all_required_vars = list(set(semantic_variables) | filter_columns)
        
        for source in sources:
            if source not in PNS_TABLES:
                log.warning(f"Origem inválida ignorada: {source}")
                continue
            
            # Filtrar variáveis que realmente existem nesta origem
            # Variáveis que não existem em uma origem (codigo: None) não devem ser buscadas
            from mapping import variavel_existe
            
            vars_for_source = [
                var for var in all_required_vars
                if var in PRIMARY_KEY_COLUMNS or var not in VAR_MAP or variavel_existe(var, source)
            ]
            
            if not vars_for_source:
                log.debug(f"Nenhuma variável válida para origem {source} (todas são None para este ano)")
                continue
            
            needs_data = self._check_if_needs_data(vars_for_source, source)
            
            if needs_data:
                log.info(f"Buscando dados para origem {source}...")
                # Buscar apenas com variáveis que existem nesta origem
                self._fetch_and_save_data(vars_for_source, source, filters)
            else:
                log.debug(f"Dados para origem {source} já estão no repositório local")
    
    def _check_if_needs_data(
        self,
        semantic_variables: List[str],
        source: str
    ) -> bool:
        """
        Verifica se precisa buscar dados do BigQuery para uma origem.
        
        Verifica se:
        1. Não há registros para esta origem no repositório local
        2. Faltam colunas físicas necessárias no repositório local
        3. Colunas físicas solicitadas estão vazias (todos NULL) para esta origem
        
        IMPORTANTE: Esta função verifica apenas variáveis FÍSICAS.
        Variáveis derivadas não devem ser verificadas aqui, pois podem
        ser calculadas a partir de variáveis físicas já existentes.
        
        Args:
            semantic_variables: Lista de variáveis semânticas (apenas físicas)
            source: Origem (ano) a verificar
        
        Returns:
            True se precisa buscar dados, False caso contrário
        """
        # Verificar se há registros para esta origem
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM {PNS_TABLE_NAME} WHERE origem = ?",
                (source,)
            )
            count = cursor.fetchone()[0]
            
            if count == 0:
                log.debug(f"Nenhum registro encontrado para origem {source}")
                return True
        
        # Verificar se todas as colunas necessárias existem
        existing_columns = set(get_table_columns())
        required_columns = set(semantic_variables) | set(PRIMARY_KEY_COLUMNS)
        
        missing_columns = required_columns - existing_columns
        
        if missing_columns:
            log.debug(
                f"Colunas faltantes para origem {source}: {missing_columns}"
            )
            return True
        
        # Filtrar apenas variáveis físicas (que estão no VAR_MAP)
        # Variáveis derivadas não precisam ser verificadas aqui
        # IMPORTANTE: Verificar se a variável existe nesta origem antes de verificar dados
        from mapping import variavel_existe
        
        physical_variables = [
            var for var in semantic_variables 
            if var in VAR_MAP 
            and var not in PRIMARY_KEY_COLUMNS
            and variavel_existe(var, source)  # Apenas variáveis que existem nesta origem
        ]
        
        # Verificar se as colunas físicas têm dados não-nulos para esta origem
        # Se uma coluna foi criada mas nunca populada, ela terá todos os valores NULL
        if physical_variables:
            with get_connection() as conn:
                cursor = conn.cursor()
                for var in physical_variables:
                    # Verificar se há pelo menos um valor não-nulo para esta variável e origem
                    cursor.execute(
                        f"""
                        SELECT COUNT(*) 
                        FROM {PNS_TABLE_NAME} 
                        WHERE origem = ? AND {var} IS NOT NULL
                        """,
                        (source,)
                    )
                    non_null_count = cursor.fetchone()[0]
                    
                    if non_null_count == 0:
                        log.debug(
                            f"Coluna '{var}' existe mas está vazia (todos NULL) "
                            f"para origem {source}. Precisa buscar dados."
                        )
                        return True
        
        # Se chegou aqui, há registros, as colunas existem e têm dados
        log.debug(
            f"Dados para origem {source} já estão no repositório local "
            f"({count} registros, todas as colunas necessárias existem e têm dados)"
        )
        return False
    
    def _fetch_and_save_data(
        self,
        semantic_variables: List[str],
        source: str,
        filters: Optional[Dict] = None
    ) -> None:
        """
        Busca dados do BigQuery e salva no SQLite.
        
        Executa o fluxo completo:
        1. Construir query SQL (query_builder)
        2. Executar no BigQuery (basedosdados_client)
        3. Converter para semântico (converters)
        4. Limpar dados (cleaning)
        5. Salvar no SQLite (sqlite_client)
        
        Args:
            semantic_variables: Lista de variáveis semânticas
            source: Origem (ano) a buscar
            filters: Filtros adicionais (opcional)
        """
        try:
            # 1. Construir query SQL
            log.debug(f"Construindo query para origem {source}...")
            query = build_select_query(
                semantic_variables=semantic_variables,
                source=source,
                filters=filters,
                use_default_filters=True
            )
            
            # 2. Executar no BigQuery
            log.info(f"Executando query no BigQuery para origem {source}...")
            df_physical = run_query(query)
            
            if df_physical.empty:
                log.warning(f"Nenhum dado retornado do BigQuery para origem {source}")
                return
            
            log.info(f"Retornados {len(df_physical)} registros do BigQuery")
            
            # 3. Converter para semântico
            log.debug("Convertendo colunas físicas para semânticas...")
            df_semantic = to_semantic(df_physical, source)
            
            # 4. Limpar dados
            log.debug("Aplicando limpeza e transformações...")
            cleaner = DataCleaner(source=source)
            df_clean = cleaner.clean(df_semantic)
            
            if df_clean.empty:
                log.warning(f"DataFrame vazio após limpeza para origem {source}")
                return
            
            # 5. Garantir que todas as colunas existem no SQLite
            ensure_columns_exist(df_clean.columns.tolist())
            
            # 6. Salvar no SQLite
            log.info(f"Salvando {len(df_clean)} registros no SQLite...")
            upsert_rows(df_clean)
            
            log.info(f"Dados salvos com sucesso para origem {source}")
            
        except Exception as e:
            log.error(f"Erro ao buscar e salvar dados para origem {source}: {e}")
            raise
    
    def load_from_storage(
        self,
        semantic_variables: List[str],
        sources: List[str],
        filters: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Carrega dados do repositório local SQLite.
        
        Retorna DataFrame com dados das origens solicitadas,
        contendo apenas as colunas semânticas solicitadas.
        
        Args:
            semantic_variables: Lista de variáveis semânticas a carregar
            sources: Lista de origens (anos) a carregar
            filters: Filtros adicionais (opcional, aplicados em memória)
        
        Returns:
            DataFrame consolidado com dados de todas as origens solicitadas
        """
        log.info(
            f"Carregando do repositório local: {len(semantic_variables)} variáveis, "
            f"{len(sources)} origens"
        )
        
        # Incluir colunas usadas nos filtros (se houver)
        filter_columns = set()
        if filters:
            filter_columns = set(filters.keys())
        
        # Colunas obrigatórias sempre incluídas: variáveis solicitadas + PK + colunas de filtros
        columns_to_select = set(semantic_variables) | set(PRIMARY_KEY_COLUMNS) | filter_columns
        
        # Verificar quais colunas existem na tabela
        existing_columns = set(get_table_columns())
        columns_to_select = [col for col in columns_to_select if col in existing_columns]
        
        if not columns_to_select:
            log.warning("Nenhuma coluna válida encontrada no repositório local")
            return pd.DataFrame()
        
        # Construir query SQL
        columns_str = ", ".join(columns_to_select)
        sources_str = ", ".join([f"'{s}'" for s in sources])
        
        query = f"""
            SELECT {columns_str}
            FROM {PNS_TABLE_NAME}
            WHERE origem IN ({sources_str})
        """
        
        # Executar query
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
        
        log.info(f"Carregados {len(df)} registros do repositório local")
        
        # Aplicar filtros adicionais em memória se fornecidos
        if filters and not df.empty:
            df = self._apply_filters(df, filters)
            log.debug(f"Após filtros: {len(df)} registros")
        
        return df
    
    def _apply_filters(
        self,
        df: pd.DataFrame,
        filters: Dict
    ) -> pd.DataFrame:
        """
        Aplica filtros adicionais em memória no DataFrame.
        
        Args:
            df: DataFrame a filtrar
            filters: Dicionário com filtros semânticos
        
        Returns:
            DataFrame filtrado
        """
        df_filtered = df.copy()
        
        for var_semantica, value in filters.items():
            if var_semantica not in df_filtered.columns:
                log.warning(f"Coluna '{var_semantica}' não encontrada para filtrar")
                continue
            
            # Se o valor é um dicionário, é um filtro complexo
            if isinstance(value, dict):
                operator = value.get("operador", "=")
                filter_value = value.get("valor")
                
                if operator == ">=":
                    df_filtered = df_filtered[df_filtered[var_semantica] >= filter_value]
                elif operator == "<=":
                    df_filtered = df_filtered[df_filtered[var_semantica] <= filter_value]
                elif operator == ">":
                    df_filtered = df_filtered[df_filtered[var_semantica] > filter_value]
                elif operator == "<":
                    df_filtered = df_filtered[df_filtered[var_semantica] < filter_value]
                elif operator == "!=" or operator == "<>":
                    df_filtered = df_filtered[df_filtered[var_semantica] != filter_value]
                else:  # "="
                    df_filtered = df_filtered[df_filtered[var_semantica] == filter_value]
            else:
                # Filtro simples: igualdade
                df_filtered = df_filtered[df_filtered[var_semantica] == value]
        
        return df_filtered
    
    def _ensure_derived_variable(
        self,
        df: pd.DataFrame,
        derived_var_name: str,
        sources: List[str]
    ) -> pd.DataFrame:
        """
        Garante que uma variável derivada está calculada e persistida no repositório local.
        
        Se a variável já existe no DataFrame e no repositório local, retorna o DataFrame.
        Caso contrário, calcula usando a função registrada e persiste no SQLite.
        
        Args:
            df: DataFrame com dados físicos (deve conter as dependências)
            derived_var_name: Nome da variável derivada
            sources: Lista de origens (anos)
        
        Returns:
            DataFrame com a variável derivada adicionada
        """
        from dao.sqlite_client import (
            get_metadata_variables,
            get_table_columns,
            ensure_columns_exist,
            upsert_rows
        )
        
        # Importar função do registry (evitar import circular)
        # A função _get_derived_variable_func está no módulo service.pns_service
        # mas precisamos acessá-la sem criar dependência circular
        import sys
        if 'service.pns_service' in sys.modules:
            from service.pns_service import _get_derived_variable_func
        else:
            # Se o módulo ainda não foi importado, importar agora
            from service import pns_service
            _get_derived_variable_func = pns_service._get_derived_variable_func
        
        # Verificar se já está no DataFrame
        if derived_var_name in df.columns:
            # Verificar se todos os valores estão preenchidos
            if df[derived_var_name].notna().all():
                log.debug(f"Variável derivada '{derived_var_name}' já está no DataFrame")
                return df
        
        # Buscar metadados da variável derivada
        df_vars = get_metadata_variables()
        var_row = df_vars[df_vars['nome_semantico'] == derived_var_name]
        
        if var_row.empty:
            log.warning(f"Metadados não encontrados para variável derivada '{derived_var_name}'")
            return df
        
        depends_on = var_row.iloc[0].get('depends_on')
        if not depends_on:
            log.warning(f"Nenhuma dependência encontrada para variável derivada '{derived_var_name}'")
            return df
        
        # Verificar se todas as dependências estão no DataFrame
        missing_deps = [dep for dep in depends_on if dep not in df.columns]
        if missing_deps:
            log.info(
                f"Dependências faltantes no DataFrame para '{derived_var_name}': {missing_deps}. "
                "Garantindo que estejam no repositório local (pode buscar do BigQuery se necessário)..."
            )
            # Garantir que as dependências estejam no repositório local (pode disparar ingestão do BigQuery)
            # Separar dependências físicas e derivadas
            # Importar função auxiliar para verificar se é derivada
            import sys
            if 'service.pns_service' in sys.modules:
                from service.pns_service import _is_derived_variable
            else:
                from service import pns_service
                _is_derived_variable = pns_service._is_derived_variable
            
            missing_physical = [d for d in missing_deps if not _is_derived_variable(d)]
            missing_derived = [d for d in missing_deps if _is_derived_variable(d)]
            
            # Primeiro garantir dados físicos (pode buscar do BigQuery)
            # Filtrar variáveis por origem: apenas buscar variáveis que existem em cada origem
            if missing_physical:
                from mapping import variavel_existe
                
                # Agrupar variáveis por origem (algumas podem não existir em certas origens)
                vars_by_source = {}
                for source in sources:
                    vars_for_source = [
                        var for var in missing_physical
                        if var in PRIMARY_KEY_COLUMNS or var not in VAR_MAP or variavel_existe(var, source)
                    ]
                    if vars_for_source:
                        vars_by_source[source] = vars_for_source
                
                # Para cada origem, garantir apenas as variáveis que existem nela
                for source, vars_for_source in vars_by_source.items():
                    self.ensure_data(
                        semantic_variables=vars_for_source,
                        sources=[source],  # Uma origem por vez
                        filters=None  # Não aplicar filtros adicionais nas dependências
                    )
            
            # Carregar dependências do repositório local (físicas e derivadas já calculadas)
            df_deps = self.load_from_storage(
                semantic_variables=missing_deps,
                sources=sources
            )
            
            if not df_deps.empty:
                # Fazer merge com o DataFrame principal usando a PK
                df = df.merge(
                    df_deps,
                    on=PRIMARY_KEY_COLUMNS,
                    how='left',
                    suffixes=('', '_dup')
                )
                # Remover colunas duplicadas (se houver)
                for col in df.columns:
                    if col.endswith('_dup'):
                        df = df.drop(columns=[col])
            else:
                log.error(
                    f"Não foi possível carregar dependências {missing_deps} do repositório local. "
                    f"Variável derivada '{derived_var_name}' não será calculada."
                )
                return df
        
        # Verificar novamente se todas as dependências estão presentes
        missing_deps = [dep for dep in depends_on if dep not in df.columns]
        if missing_deps:
            log.error(
                f"Ainda faltam dependências após tentativa de carregamento: {missing_deps}. "
                f"Variável derivada '{derived_var_name}' não será calculada."
            )
            return df
        
        # Verificar se a coluna já existe fisicamente no banco
        # Se existir, podemos carregar do repositório local mesmo sem a função no registry
        existing_columns = get_table_columns(PNS_TABLE_NAME)
        if derived_var_name in existing_columns:
            log.info(
                f"Variável derivada '{derived_var_name}' existe fisicamente no repositório local. "
                "Carregando do banco (função não necessária se já estiver calculada)."
            )
            # Tentar carregar do repositório local
            df_cached = self.load_from_storage(
                semantic_variables=[derived_var_name],
                sources=sources
            )
            if not df_cached.empty and derived_var_name in df_cached.columns:
                # Fazer merge com o DataFrame principal usando a PK
                df = df.merge(
                    df_cached[[*PRIMARY_KEY_COLUMNS, derived_var_name]],
                    on=PRIMARY_KEY_COLUMNS,
                    how='left',
                    suffixes=('', '_cached')
                )
                # Se a coluna foi mesclada com sufixo, renomear
                if f'{derived_var_name}_cached' in df.columns:
                    df[derived_var_name] = df[f'{derived_var_name}_cached']
                    df = df.drop(columns=[f'{derived_var_name}_cached'])
                
                # Verificar se todos os valores foram carregados
                if df[derived_var_name].notna().any():
                    log.info(f"Variável derivada '{derived_var_name}' carregada do repositório local com sucesso")
                    return df
                else:
                    log.warning(
                        f"Variável '{derived_var_name}' existe no banco mas não há dados para os registros solicitados. "
                        "Tentando calcular com função registrada..."
                    )
            else:
                log.warning(
                    f"Variável '{derived_var_name}' existe no banco mas não foi possível carregar. "
                    "Tentando calcular com função registrada..."
                )
        
        # Obter função registrada
        func = _get_derived_variable_func(derived_var_name)
        if func is None:
            log.warning(
                f"Função não encontrada no registry para variável derivada '{derived_var_name}'. "
                "A variável pode ter sido registrada em outra sessão. "
                "Tente registrar novamente com register_derived_variable()."
            )
            # Se a coluna existe no banco mas não conseguimos carregar, retornar DataFrame sem a variável
            return df
        
        # Calcular variável derivada
        try:
            log.info(f"Calculando variável derivada '{derived_var_name}'...")
            calculated_values = func(df)
            
            # Garantir que o resultado seja uma Series com o mesmo índice
            if not isinstance(calculated_values, pd.Series):
                calculated_values = pd.Series(calculated_values, index=df.index)
            
            # Adicionar ao DataFrame
            df[derived_var_name] = calculated_values
            
            log.info(f"Variável derivada '{derived_var_name}' calculada com sucesso")
            
            # Persistir no SQLite
            # Garantir que a coluna existe
            ensure_columns_exist([derived_var_name])
            
            # Preparar DataFrame para upsert (apenas PK + variável derivada)
            df_to_save = df[PRIMARY_KEY_COLUMNS + [derived_var_name]].copy()
            
            # Salvar no repositório local
            log.info(f"Persistindo variável derivada '{derived_var_name}' no repositório local...")
            upsert_rows(df_to_save)
            
            log.info(f"Variável derivada '{derived_var_name}' persistida com sucesso")
            
        except Exception as e:
            log.error(
                f"Erro ao calcular variável derivada '{derived_var_name}': {e}",
                exc_info=True
            )
            # Não falhar completamente, apenas retornar DataFrame sem a variável derivada
            if derived_var_name in df.columns:
                df = df.drop(columns=[derived_var_name])
        
        return df
    
    def sync_metadata(self) -> None:
        """
        Sincroniza metadados do mapping.py com as tabelas de metadados do SQLite.
        
        Este método é chamado na inicialização do DAO e garante que as tabelas
        metadata_variables e metadata_mapping reflitam exatamente o que está
        definido no VAR_MAP do mapping.py.
        
        Para variáveis derivadas (registradas via register_derived_variable),
        os metadados são preservados e não são sobrescritos por este método.
        
        As descrições são lidas diretamente do campo "descricao" no VAR_MAP,
        permitindo descrições multi-linha de forma segura.
        """
        log.info("Sincronizando metadados do mapping.py com SQLite...")
        
        # Sincronizar cada variável do VAR_MAP
        for nome_semantico, var_info in VAR_MAP.items():
            # Verificar se é variável derivada (não deve sobrescrever se já existir como derivada)
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT categoria FROM metadata_variables WHERE nome_semantico = ?",
                    (nome_semantico,)
                )
                result = cursor.fetchone()
                if result and result[0] == "derivada":
                    # Não sobrescrever variáveis derivadas
                    log.debug(f"Variável derivada '{nome_semantico}' preservada, pulando sincronização")
                    continue
            
            # Extrair descrição do VAR_MAP (campo "descricao" opcional)
            descricao = var_info.get("descricao")
            if not descricao:
                # Gerar descrição padrão baseada no nome
                descricao = self._generate_default_description(nome_semantico)
            elif isinstance(descricao, tuple):
                # Se for uma tupla (string multi-linha), juntar com espaços
                descricao = " ".join(descricao)
            
            # Determinar tipo mais comum (ou primeiro tipo encontrado)
            # Ignorar a chave "descricao" ao iterar
            tipos = []
            for key, value in var_info.items():
                if key == "descricao":
                    continue
                if isinstance(value, dict) and "tipo" in value:
                    tipos.append(value["tipo"])
            
            tipo_dado = tipos[0] if tipos else "string"
            
            # Inserir/atualizar na tabela metadata_variables
            upsert_metadata_variable(
                nome_semantico=nome_semantico,
                descricao=descricao,
                tipo_dado=tipo_dado,
                categoria="fisica"
            )
            
            # Sincronizar mapeamentos por origem
            # Ignorar a chave "descricao" ao iterar
            for key, meta in var_info.items():
                if key == "descricao":
                    continue
                if not isinstance(meta, dict):
                    continue
                
                # A chave deve ser uma origem (ano)
                origem = key
                codigo_original = meta.get("codigo")
                
                # Extrair labels de categorias se houver comentários especiais
                # (por enquanto, deixamos None - pode ser expandido depois)
                labels_categorias = None
                
                upsert_metadata_mapping(
                    nome_semantico=nome_semantico,
                    origem=origem,
                    codigo_original=codigo_original,
                    labels_categorias=labels_categorias
                )
        
        log.info("Sincronização de metadados concluída")
    
    def _generate_default_description(self, nome_semantico: str) -> str:
        """
        Gera uma descrição padrão baseada no nome semântico da variável.
        
        Args:
            nome_semantico: Nome semântico da variável
        
        Returns:
            Descrição padrão
        """
        # Mapeamento de nomes comuns para descrições
        default_descriptions = {
            "id_upa": "Identificador único da unidade primária de amostragem (UPA)",
            "id_domicilio": "Identificador do domicílio dentro da UPA",
            "id_morador": "Número de ordem do morador no domicílio",
            "sexo": "Sexo do entrevistado",
            "idade": "Idade do entrevistado em anos completos",
            "preventivo": "Quando fez o último exame preventivo (Papanicolau)",
            "mamografia": "Fez mamografia (Sim/Não)",
            "renda_per_capita": "Renda per capita da família em reais",
            "peso_amostral": "Peso amostral estatístico para expansão dos resultados",
            "estado_civil": "Estado civil do entrevistado",
        }
        
        if nome_semantico in default_descriptions:
            return default_descriptions[nome_semantico]
        
        # Gerar descrição genérica baseada no nome
        # Converter snake_case para texto legível
        desc = nome_semantico.replace("_", " ").title()
        return f"{desc} (variável da PNS)"


# Instância singleton para uso direto
_dao_instance: Optional[PNSDAO] = None


def get_dao() -> PNSDAO:
    """
    Retorna instância singleton do DAO.
    
    Returns:
        Instância do PNSDAO
    """
    global _dao_instance
    if _dao_instance is None:
        _dao_instance = PNSDAO()
    return _dao_instance


# Funções de conveniência para compatibilidade
def ensure_data(
    semantic_variables: List[str],
    sources: List[str],
    filters: Optional[Dict] = None
) -> None:
    """
    Função de conveniência para garantir dados no repositório local.
    
    Args:
        semantic_variables: Lista de variáveis semânticas
        sources: Lista de origens (anos)
        filters: Filtros adicionais (opcional)
    """
    dao = get_dao()
    dao.ensure_data(semantic_variables, sources, filters)


def load_from_storage(
    semantic_variables: List[str],
    sources: List[str],
    filters: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Função de conveniência para carregar dados do repositório local.
    
    Args:
        semantic_variables: Lista de variáveis semânticas
        sources: Lista de origens (anos)
        filters: Filtros adicionais (opcional)
    
    Returns:
        DataFrame com dados do repositório local
    """
    dao = get_dao()
    return dao.load_from_storage(semantic_variables, sources, filters)

