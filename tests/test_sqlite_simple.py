"""
Teste simples e rápido do cliente SQLite.

Execute este script para verificar se o cliente SQLite está funcionando.
"""
import pandas as pd
from dao.sqlite_client import (
    ensure_table_exists,
    ensure_columns_exist,
    upsert_rows,
    get_table_columns
)
from config import PNS_TABLE_NAME

print("🧪 Teste Rápido do Cliente SQLite\n")

# 1. Criar tabela
print("1. Criando tabela...")
ensure_table_exists()
print("   ✓ Tabela criada\n")

# 2. Criar dados de teste
print("2. Criando dados de teste...")
df = pd.DataFrame({
    'origem': ['2013', '2019'],
    'identificador_unidade': ['TEST001', 'TEST002'],
    'sexo': ['2', '2'],
    'idade': [30, 35],
    'preventivo': ['1', '2']
})
print(f"   ✓ DataFrame criado com {len(df)} linhas\n")

# 3. Garantir colunas
print("3. Garantindo colunas...")
ensure_columns_exist(df.columns.tolist())
print("   ✓ Colunas garantidas\n")

# 4. Inserir dados
print("4. Inserindo dados...")
upsert_rows(df)
print("   ✓ Dados inseridos\n")

# 5. Verificar resultado
print("5. Verificando resultado...")
columns = get_table_columns()
print(f"   ✓ Tabela tem {len(columns)} colunas")
print(f"   ✓ Colunas: {', '.join(columns[:5])}...\n")

print("✅ Teste concluído com sucesso!")
print(f"   Banco de dados: data/pns_cache.sqlite")

