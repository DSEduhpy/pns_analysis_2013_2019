"""
Limpeza e tratamento de dados da PNS.

Este módulo aplica transformações de limpeza, conversão de tipos e criação
de variáveis derivadas (flags binárias) nos dados já convertidos para semânticos.
"""
import logging
import pandas as pd
import numpy as np
from typing import Optional

from config import PRIMARY_KEY_COLUMNS


log = logging.getLogger(__name__)


class DataCleaner:
    """
    Classe responsável por limpar e transformar dados da PNS.
    
    Cada passo de limpeza é implementado como um método separado,
    e o método clean() orquestra a execução de todos os passos.
    """
    
    def __init__(self, source: Optional[str] = None):
        """
        Inicializa o DataCleaner.
        
        Args:
            source: Ano da pesquisa ("2013" ou "2019") - opcional, usado apenas para logs
        """
        self.source = source
        self.original_rows = 0
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas as transformações de limpeza no DataFrame semântico.
        
        Este método orquestra a execução de todos os passos de limpeza:
        1. Conversão de tipos de dados (int, float)
        2. Tratamento de peso amostral
        3. Tratamento de renda per capita
        4. Criação de flags binárias (fez_mamografia, fez_preventivo, eh_branca)
        5. Remoção de linhas sem peso amostral
        
        Args:
            df: DataFrame com colunas semânticas (já convertido por converters.to_semantic)
        
        Returns:
            DataFrame limpo e tratado, pronto para ser salvo no SQLite
        """
        if df.empty:
            log.warning("DataFrame vazio recebido, retornando vazio")
            return df.copy()
        
        # Criar cópia para não modificar o original
        df_clean = df.copy()
        self.original_rows = len(df_clean)
        
        # Executar todos os passos de limpeza
        df_clean = self._convert_numeric_types(df_clean)
        df_clean = self._treat_sample_weight(df_clean)
        df_clean = self._treat_income(df_clean)
        df_clean = self._create_mammography_flag(df_clean)
        df_clean = self._create_preventive_flag(df_clean)
        df_clean = self._create_race_flag(df_clean)
        df_clean = self._ensure_control_columns(df_clean)
        
        final_rows = len(df_clean)
        log.info(
            f"Limpeza concluída: {self.original_rows} -> {final_rows} linhas, "
            f"{len(df_clean.columns)} colunas"
        )
        
        return df_clean
    
    def _convert_numeric_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte tipos numéricos conforme necessário.
        
        Converte idade, filhos_vivos e anos_estudo para inteiros.
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com tipos convertidos
        """
        # Idade deve ser inteiro
        columns_to_convert = ['idade', 'filhos_vivos', 'anos_estudo']
        
        for column in columns_to_convert:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                log.debug(f"Coluna `{column}` convertida para inteiro")
        
        return df
    
    def _treat_sample_weight(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Trata peso amostral: converte vírgula para ponto e remove linhas inválidas.
        
        Remove linhas sem peso amostral (não responderam módulo de saúde).
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com peso amostral tratado e linhas inválidas removidas
        """
        if 'peso_amostral' in df.columns:
            # Converter vírgula para ponto e converter para numérico
            df['peso_amostral'] = pd.to_numeric(
                df['peso_amostral'].astype(str).str.replace(',', '.'),
                errors='coerce'
            )
            log.debug("Peso amostral convertido para numérico")
            
            # Remover linhas sem peso amostral (não responderam módulo de saúde)
            rows_before = len(df)
            df = df.dropna(subset=['peso_amostral']).copy()
            rows_removed = rows_before - len(df)
            if rows_removed > 0:
                log.info(
                    f"Removidas {rows_removed} linhas sem peso amostral "
                    f"(não responderam módulo de saúde)"
                )
        
        return df
    
    def _treat_income(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Trata renda per capita: converte vírgula para ponto.
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com renda per capita tratada
        """
        if 'renda_per_capita' in df.columns:
            df['renda_per_capita'] = pd.to_numeric(
                df['renda_per_capita'].astype(str).str.replace(',', '.'),
                errors='coerce'
            )
            log.debug("Renda per capita convertida para numérico")
        
        return df
    
    def _create_mammography_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria flag binária fez_mamografia a partir da coluna mamografia.
        
        Mapeamento: '1' ou 'sim' -> 1, '2' ou 'não' -> 0.
        Se fez_mamografia é NaN mas médico não pediu, então vira 0.
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com flag fez_mamografia criada
        """
        if 'mamografia' in df.columns:
            # Mapeamento: '1' ou 'sim' -> 1, '2' ou 'não' -> 0
            mapping_mamo = {'1': 1, 'sim': 1, '2': 0, 'não': 0}
            df['fez_mamografia'] = (
                df['mamografia']
                .astype(str)
                .str.lower()
                .map(mapping_mamo)
            )
            
            # Se 'fez_mamografia' é NaN mas 'medico_pediu_mamografia' é 0 (Não),
            # então 'fez_mamografia' vira 0
            if 'medico_pediu_mamografia' in df.columns:
                medico_pediu_mapping = {'1': 1, 'sim': 1, '2': 0, 'não': 0}
                medico_pediu_series = (
                    df['medico_pediu_mamografia']
                    .astype(str)
                    .str.lower()
                    .map(medico_pediu_mapping)
                )
                
                df.loc[
                    (df['fez_mamografia'].isna()) & (medico_pediu_series == 0),
                    'fez_mamografia'
                ] = 0
            
            log.debug("Flag 'fez_mamografia' criada")
        
        return df
    
    def _create_preventive_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria flag binária fez_preventivo a partir da coluna preventivo.
        
        Mapeamento:
        - '5' ou 'nunca' -> 0
        - Valores válidos (1, 2, 3, 4, 'menos', 'entre') -> 1
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com flag fez_preventivo criada
        """
        if 'preventivo' in df.columns:
            def treat_preventive(value):
                """Trata valor de preventivo e retorna 0, 1 ou NaN."""
                if pd.isna(value):
                    return np.nan
                value_str = str(value).lower()
                # Se responder 5 ('Nunca fez') -> 0
                if 'nunca' in value_str or '5' in value_str:
                    return 0
                # Se responder qualquer tempo (1, 2, 3, 4) -> 1
                if 'menos' in value_str or 'entre' in value_str or value_str in ['1', '2', '3', '4']:
                    return 1
                return np.nan
            
            df['fez_preventivo'] = df['preventivo'].apply(treat_preventive)
            log.debug("Flag 'fez_preventivo' criada")
        
        return df
    
    def _create_race_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria flag binária eh_branca a partir da coluna raca.
        
        Mapeamento: '1' ou 'branca' -> 1, resto -> 0.
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com flag eh_branca criada
        """
        if 'raca' in df.columns:
            # 1 ou 'Branca' = 1, resto = 0
            df['eh_branca'] = np.where(
                df['raca'].astype(str).str.lower().isin(['1', 'branca']),
                1,
                0
            )
            log.debug("Flag 'eh_branca' criada")
        
        return df
    
    def _ensure_control_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Garante que colunas de controle (PRIMARY_KEY_COLUMNS) estão presentes.
        
        Args:
            df: DataFrame a ser processado
        
        Returns:
            DataFrame com colunas de controle garantidas
        """
        # Garantir que origem está presente
        if 'origem' not in df.columns:
            if self.source:
                df['origem'] = self.source
                log.debug(f"Coluna 'origem' adicionada com valor '{self.source}'")
            else:
                log.warning("Coluna 'origem' não encontrada e source não fornecido")
        
        # Verificar colunas de identificação (PK)
        required_id_cols = [col for col in PRIMARY_KEY_COLUMNS if col != 'origem']
        missing_id_cols = [col for col in required_id_cols if col not in df.columns]
        if missing_id_cols:
            log.warning(f"Colunas de identificação não encontradas: {missing_id_cols}")
        
        return df


def clean(df: pd.DataFrame, source: Optional[str] = None) -> pd.DataFrame:
    """
    Função de conveniência que cria um DataCleaner e executa a limpeza.
    
    Mantida para compatibilidade com código existente.
    
    Args:
        df: DataFrame com colunas semânticas (já convertido por converters.to_semantic)
        source: Ano da pesquisa ("2013" ou "2019") - opcional, usado apenas para logs
    
    Returns:
        DataFrame limpo e tratado, pronto para ser salvo no SQLite
    """
    cleaner = DataCleaner(source=source)
    return cleaner.clean(df)
