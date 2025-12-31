"""
Teste de persistência de variáveis derivadas.

Este teste verifica que variáveis derivadas podem ser:
1. Calculadas e salvas no repositório local
2. Recuperadas em uma nova sessão (sem a função no registry)
"""
import sys
from pathlib import Path
import pandas as pd

# Adiciona raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

from service.pns_service import get_dataframe, register_derived_variable
from dao.sqlite_client import get_connection, PNS_TABLE_NAME
from config import SQLITE_PATH
import os


def apagar_banco():
    """Remove o banco SQLite se existir."""
    if SQLITE_PATH.exists():
        print(f"🗑️  Removendo banco existente: {SQLITE_PATH}")
        os.remove(SQLITE_PATH)
        print("✅ Banco removido com sucesso")
    else:
        print("ℹ️  Banco não existe, nada a remover")


def limpar_registry():
    """Limpa o registry de variáveis derivadas (simula nova sessão)."""
    from service.pns_service import _DERIVED_VARIABLES_REGISTRY
    _DERIVED_VARIABLES_REGISTRY.clear()
    print("🧹 Registry de variáveis derivadas limpo (simulando nova sessão)")


def test_derived_variable_persistence():
    """Teste completo de persistência de variáveis derivadas."""
    
    print("="*80)
    print("TESTE DE PERSISTÊNCIA DE VARIÁVEIS DERIVADAS")
    print("="*80)
    print()
    
    # ========================================================================
    # PARTE 1: Preparação - Apagar banco e limpar estado
    # ========================================================================
    print("\n" + "="*80)
    print("PARTE 1: Preparação")
    print("="*80)
    
    apagar_banco()
    limpar_registry()
    
    # ========================================================================
    # PARTE 2: Registrar variável derivada e buscar dados
    # ========================================================================
    print("\n" + "="*80)
    print("PARTE 2: Registrar variável derivada e buscar dados")
    print("="*80)
    
    # Registrar uma variável derivada simples baseada em variáveis do mapping.py
    # Vamos criar "eh_branca" que é 1 se cor_raca == '1', 0 caso contrário
    print("\n📝 Registrando variável derivada 'eh_branca'...")
    register_derived_variable(
        name="eh_branca",
        description="1 se cor_raca é Branca (código 1), 0 caso contrário",
        depends_on=["cor_raca"],
        func=lambda df: (df["cor_raca"].astype(str).str.replace(r'\.0$', '', regex=True) == '1').astype(int)
    )
    print("✅ Variável derivada registrada")
    
    # Buscar dados incluindo variáveis físicas e a derivada
    print("\n📥 Buscando dados do BigQuery (primeira vez)...")
    print("   Variáveis físicas: sexo, idade, cor_raca")
    print("   Variável derivada: eh_branca")
    
    df1 = get_dataframe(
        variables=["sexo", "idade", "cor_raca", "eh_branca"],
        sources=["2013"],  # Apenas 2013 para ser mais rápido
        filters={"sexo": "2", "idade": {"operador": ">=", "valor": 25}}
    )
    
    print(f"✅ Dados carregados: {len(df1)} registros")
    print(f"   Colunas: {list(df1.columns)}")
    
    # Verificar se a variável derivada foi calculada
    if "eh_branca" in df1.columns:
        print(f"\n✅ Variável derivada 'eh_branca' presente no DataFrame")
        print(f"   Valores únicos: {df1['eh_branca'].unique()}")
        print(f"   Distribuição:\n{df1['eh_branca'].value_counts()}")
    else:
        print("❌ ERRO: Variável derivada 'eh_branca' não encontrada no DataFrame")
        return False
    
    # Verificar se foi salva no banco
    print("\n🔍 Verificando se 'eh_branca' foi salva no repositório local...")
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({PNS_TABLE_NAME})")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "eh_branca" in columns:
            print("✅ Coluna 'eh_branca' existe no repositório local")
            
            # Verificar se há dados
            cursor.execute(f"SELECT COUNT(*) as total FROM {PNS_TABLE_NAME} WHERE eh_branca IS NOT NULL")
            count = cursor.fetchone()[0]
            print(f"   Registros com 'eh_branca' preenchido: {count}")
        else:
            print("❌ ERRO: Coluna 'eh_branca' não encontrada no repositório local")
            return False
    
    # ========================================================================
    # PARTE 3: Simular nova sessão - Limpar registry e buscar novamente
    # ========================================================================
    print("\n" + "="*80)
    print("PARTE 3: Simular nova sessão (sem função no registry)")
    print("="*80)
    
    limpar_registry()
    print("\n📥 Buscando dados novamente (sem função no registry)...")
    print("   Deve carregar 'eh_branca' do repositório local")
    
    df2 = get_dataframe(
        variables=["sexo", "idade", "cor_raca", "eh_branca"],
        sources=["2013"],
        filters={"sexo": "2", "idade": {"operador": ">=", "valor": 25}}
    )
    
    print(f"✅ Dados carregados: {len(df2)} registros")
    # Remover colunas duplicadas para exibição
    df2_unique_cols = df2.columns[~df2.columns.duplicated()]
    print(f"   Colunas: {list(df2_unique_cols)}")
    
    # Verificar se a variável derivada foi carregada do repositório
    if "eh_branca" in df2.columns:
        print(f"\n✅ Variável derivada 'eh_branca' carregada do repositório local")
        print(f"   Valores únicos: {df2['eh_branca'].unique()}")
        print(f"   Distribuição:\n{df2['eh_branca'].value_counts()}")
        
        # Verificar se os dados são consistentes
        if len(df1) == len(df2):
            # Remover colunas duplicadas (se houver)
            df1 = df1.loc[:, ~df1.columns.duplicated()]
            df2 = df2.loc[:, ~df2.columns.duplicated()]
            
            # Comparar valores (apenas primeiras linhas para não ser muito lento)
            sample_size = min(100, len(df1))
            pk_cols = ['origem', 'id_upa', 'id_domicilio', 'id_morador']
            
            # Garantir que as colunas PK existem
            for col in pk_cols:
                if col not in df1.columns:
                    print(f"⚠️  Coluna '{col}' não encontrada em df1")
                    return False
                if col not in df2.columns:
                    print(f"⚠️  Coluna '{col}' não encontrada em df2")
                    return False
            
            df1_sample = df1[pk_cols + ['eh_branca']].head(sample_size).copy()
            df2_sample = df2[pk_cols + ['eh_branca']].head(sample_size).copy()
            
            # Merge para comparar
            merged = df1_sample.merge(
                df2_sample,
                on=pk_cols,
                suffixes=('_original', '_recuperado')
            )
            
            if 'eh_branca_original' in merged.columns and 'eh_branca_recuperado' in merged.columns:
                # Converter para mesmo tipo para comparação (pode ser int ou string)
                merged['eh_branca_original'] = merged['eh_branca_original'].astype(str)
                merged['eh_branca_recuperado'] = merged['eh_branca_recuperado'].astype(str)
                
                matches = (merged['eh_branca_original'] == merged['eh_branca_recuperado']).sum()
                total = len(merged)
                print(f"\n✅ Validação de consistência:")
                print(f"   {matches}/{total} registros coincidem na amostra de {sample_size}")
                
                if matches == total:
                    print("   ✅ Todos os valores coincidem - persistência funcionando corretamente!")
                    return True
                else:
                    # Mostrar alguns exemplos de diferenças
                    diff = merged[merged['eh_branca_original'] != merged['eh_branca_recuperado']]
                    if len(diff) > 0:
                        print(f"   ⚠️  Exemplos de diferenças (primeiros 5):")
                        print(diff[['origem', 'id_upa', 'eh_branca_original', 'eh_branca_recuperado']].head())
                    print(f"   ⚠️  {total - matches} valores diferentes encontrados")
                    return False
            else:
                print("⚠️  Não foi possível comparar valores (colunas não encontradas)")
                return True  # Pelo menos a coluna existe
        else:
            print(f"⚠️  Número de registros diferente: {len(df1)} vs {len(df2)}")
            return False
    else:
        print("❌ ERRO: Variável derivada 'eh_branca' não encontrada após limpar registry")
        return False


if __name__ == "__main__":
    try:
        sucesso = test_derived_variable_persistence()
        
        print("\n" + "="*80)
        if sucesso:
            print("✅ TESTE PASSOU: Persistência de variáveis derivadas funcionando!")
        else:
            print("❌ TESTE FALHOU: Verifique os erros acima")
        print("="*80)
        
        sys.exit(0 if sucesso else 1)
        
    except Exception as e:
        print(f"\n❌ ERRO NO TESTE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

