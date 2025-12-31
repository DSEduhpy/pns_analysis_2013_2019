"""
Script de teste para a camada de transformação.

Testa converters.to_semantic() e cleaning.clean() com dados simulados.
"""
import pandas as pd
import logging
from transform.converters import to_semantic
from transform.cleaning import clean

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("=" * 60)
print("TESTE DA CAMADA DE TRANSFORMAÇÃO")
print("=" * 60)

# 1. Criar DataFrame simulado com colunas físicas (como viria do BigQuery)
print("\n1. Criando DataFrame simulado com colunas físicas...")
df_physical = pd.DataFrame({
    'upa_pns': ['UPA001', 'UPA002', 'UPA003'],
    'c006': ['2', '2', '2'],  # Sexo (mulheres)
    'c008': ['30', '35', '28'],  # Idade
    'r001': ['1', '5', '2'],  # Preventivo
    'r015': ['1', '2', '1'],  # Mamografia
    'r014': ['1', '2', '1'],  # Médico pediu mamografia
    'v00291': ['1,5', '2,0', '1,8'],  # Peso amostral (com vírgula)
    'vdf003': ['1500,50', '2000,75', '1200,00'],  # Renda (com vírgula)
    'c009': ['1', '2', '1'],  # Raça
    'r045': ['2', '1', '0'],  # Filhos vivos
})

print(f"   DataFrame físico criado: {len(df_physical)} linhas, {len(df_physical.columns)} colunas")
print(f"   Colunas físicas: {list(df_physical.columns)}")
print("\n   Primeiras linhas:")
print(df_physical.head())

# 2. Converter para semântico
print("\n2. Convertendo para semântico (converters.to_semantic)...")
df_semantic = to_semantic(df_physical, "2013")
print(f"   DataFrame semântico: {len(df_semantic)} linhas, {len(df_semantic.columns)} colunas")
print(f"   Colunas semânticas: {list(df_semantic.columns)}")
print("\n   Primeiras linhas:")
print(df_semantic.head())

# Verificar se colunas foram renomeadas corretamente
print("\n   Verificações:")
print(f"   - Coluna 'origem' presente: {'origem' in df_semantic.columns}")
print(f"   - Coluna 'identificador_unidade' presente: {'identificador_unidade' in df_semantic.columns}")
print(f"   - Coluna 'sexo' presente: {'sexo' in df_semantic.columns}")
print(f"   - Coluna 'idade' presente: {'idade' in df_semantic.columns}")
print(f"   - Coluna 'preventivo' presente: {'preventivo' in df_semantic.columns}")
print(f"   - Coluna 'mamografia' presente: {'mamografia' in df_semantic.columns}")

# 3. Aplicar limpeza
print("\n3. Aplicando limpeza (cleaning.clean)...")
df_clean = clean(df_semantic, "2013")
print(f"   DataFrame limpo: {len(df_clean)} linhas, {len(df_clean.columns)} colunas")
print("\n   Primeiras linhas:")
print(df_clean.head())

# Verificar transformações
print("\n   Verificações de limpeza:")
print(f"   - Flag 'fez_mamografia' criada: {'fez_mamografia' in df_clean.columns}")
if 'fez_mamografia' in df_clean.columns:
    print(f"     Valores: {df_clean['fez_mamografia'].value_counts().to_dict()}")

print(f"   - Flag 'fez_preventivo' criada: {'fez_preventivo' in df_clean.columns}")
if 'fez_preventivo' in df_clean.columns:
    print(f"     Valores: {df_clean['fez_preventivo'].value_counts().to_dict()}")

print(f"   - Flag 'eh_branca' criada: {'eh_branca' in df_clean.columns}")
if 'eh_branca' in df_clean.columns:
    print(f"     Valores: {df_clean['eh_branca'].value_counts().to_dict()}")

print(f"   - Peso amostral convertido para numérico: {df_clean['peso_amostral'].dtype if 'peso_amostral' in df_clean.columns else 'N/A'}")
if 'peso_amostral' in df_clean.columns:
    print(f"     Valores: {df_clean['peso_amostral'].tolist()}")

print(f"   - Renda per capita convertida para numérico: {df_clean['renda_per_capita'].dtype if 'renda_per_capita' in df_clean.columns else 'N/A'}")
if 'renda_per_capita' in df_clean.columns:
    print(f"     Valores: {df_clean['renda_per_capita'].tolist()}")

print(f"   - Idade convertida para inteiro: {df_clean['idade'].dtype if 'idade' in df_clean.columns else 'N/A'}")
if 'idade' in df_clean.columns:
    print(f"     Valores: {df_clean['idade'].tolist()}")

# 4. Teste com DataFrame vazio
print("\n4. Testando com DataFrame vazio...")
df_empty = pd.DataFrame()
df_empty_semantic = to_semantic(df_empty, "2013")
df_empty_clean = clean(df_empty_semantic, "2013")
print(f"   DataFrame vazio processado: {len(df_empty_clean)} linhas")

print("\n" + "=" * 60)
print("✅ Testes de transformação concluídos!")
print("=" * 60)

