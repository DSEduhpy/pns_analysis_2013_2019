"""
Script de teste para o cliente SQLite.

Este script testa todas as funcionalidades do dao/sqlite_client.py:
- Criação de tabela
- Adição de colunas dinamicamente
- Upsert de dados
- Leitura de dados
"""
import pandas as pd
import logging
from dao.sqlite_client import (
    get_connection,
    table_exists,
    get_table_columns,
    ensure_table_exists,
    add_column_if_not_exists,
    ensure_columns_exist,
    upsert_rows
)
from config import PNS_TABLE_NAME, SQLITE_PATH

# Configurar logging para ver o que está acontecendo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("=" * 60)
print("TESTE DO CLIENTE SQLITE")
print("=" * 60)

# 1. Verificar se a tabela existe (deve ser False inicialmente)
print("\n1. Verificando se a tabela existe...")
exists_before = table_exists()
print(f"   Tabela existe antes da criação: {exists_before}")

# 2. Criar a tabela
print("\n2. Criando a tabela...")
ensure_table_exists()
exists_after = table_exists()
print(f"   Tabela existe após criação: {exists_after}")

# 3. Verificar colunas iniciais
print("\n3. Colunas iniciais da tabela:")
columns_initial = get_table_columns()
for col in columns_initial:
    print(f"   - {col}")

# 4. Criar DataFrame de teste
print("\n4. Criando DataFrame de teste...")
df_test = pd.DataFrame({
    'origem': ['2013', '2013', '2019'],
    'identificador_unidade': ['UPA001', 'UPA002', 'UPA001'],
    'sexo': ['2', '2', '2'],  # Mulheres
    'idade': [30, 35, 28],
    'preventivo': ['1', '2', '1'],
    'mamografia': ['1', '1', '2'],
    'renda_per_capita': [1500.50, 2000.75, 1200.00],
    'peso_amostral': [1.5, 2.0, 1.8],
    'estado_civil': ['1', '5', '1']
})
print(f"   DataFrame criado com {len(df_test)} linhas")
print("\n   Primeiras linhas:")
print(df_test.head())

# 5. Garantir que todas as colunas existem
print("\n5. Garantindo que todas as colunas existem na tabela...")
ensure_columns_exist(df_test.columns.tolist())
columns_after = get_table_columns()
print(f"   Total de colunas após adição: {len(columns_after)}")
print("   Colunas adicionadas:")
for col in columns_after:
    if col not in columns_initial:
        print(f"   - {col} (NOVA)")

# 6. Inserir dados (primeira vez - INSERT)
print("\n6. Inserindo dados pela primeira vez (INSERT)...")
upsert_rows(df_test)
print("   Dados inseridos com sucesso!")

# 7. Ler dados do banco para verificar
print("\n7. Lendo dados do banco para verificar...")
with get_connection() as conn:
    df_read = pd.read_sql_query(
        f"SELECT * FROM {PNS_TABLE_NAME} ORDER BY origem, identificador_unidade",
        conn
    )
    print(f"   Total de linhas lidas: {len(df_read)}")
    print("\n   Dados lidos:")
    print(df_read.to_string(index=False))

# 8. Atualizar dados existentes (UPDATE)
print("\n8. Atualizando dados existentes (UPDATE)...")
df_update = pd.DataFrame({
    'origem': ['2013'],
    'identificador_unidade': ['UPA001'],
    'idade': [31],  # Idade atualizada
    'renda_per_capita': [1600.00],  # Renda atualizada
    'estado_civil': ['1']
})
upsert_rows(df_update)
print("   Dados atualizados!")

# 9. Ler novamente para ver as mudanças
print("\n9. Lendo dados novamente para verificar atualizações...")
with get_connection() as conn:
    df_read_after = pd.read_sql_query(
        f"SELECT origem, identificador_unidade, idade, renda_per_capita, "
        f"created_at, updated_at FROM {PNS_TABLE_NAME} "
        f"WHERE origem = '2013' AND identificador_unidade = 'UPA001'",
        conn
    )
    print("\n   Dados após atualização:")
    print(df_read_after.to_string(index=False))
    print("\n   Note que 'created_at' foi preservado e 'updated_at' foi atualizado!")

# 10. Adicionar uma nova coluna dinamicamente
print("\n10. Testando adição de coluna dinâmica...")
add_column_if_not_exists('nova_coluna_teste', 'TEXT')
columns_final = get_table_columns()
print(f"   Total de colunas: {len(columns_final)}")
if 'nova_coluna_teste' in columns_final:
    print("   ✓ Coluna 'nova_coluna_teste' adicionada com sucesso!")

# 11. Testar inserção com nova coluna
print("\n11. Testando inserção com a nova coluna...")
df_with_new_col = pd.DataFrame({
    'origem': ['2019'],
    'identificador_unidade': ['UPA003'],
    'sexo': ['2'],
    'idade': [40],
    'nova_coluna_teste': ['valor_teste']
})
ensure_columns_exist(df_with_new_col.columns.tolist())
upsert_rows(df_with_new_col)
print("   Dados inseridos com nova coluna!")

# 12. Resumo final
print("\n" + "=" * 60)
print("RESUMO FINAL")
print("=" * 60)
with get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {PNS_TABLE_NAME}")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(DISTINCT origem) FROM {PNS_TABLE_NAME}")
    total_origens = cursor.fetchone()[0]
    
    print(f"Total de linhas na tabela: {total_rows}")
    print(f"Total de origens distintas: {total_origens}")
    print(f"Total de colunas na tabela: {len(get_table_columns())}")
    print(f"\nCaminho do banco: {SQLITE_PATH}")
    print("\n✓ Todos os testes concluídos com sucesso!")

