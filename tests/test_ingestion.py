"""
Script de teste para a camada de ingestão.

Testa query_builder e basedosdados_client sem executar queries reais no BigQuery.
"""
import logging
from ingestion.query_builder import (
    resolve_physical_codes,
    translate_semantic_filter_to_physical,
    build_where_clause,
    build_select_query
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("=" * 60)
print("TESTE DA CAMADA DE INGESTÃO")
print("=" * 60)

# 1. Teste: Resolver códigos físicos
print("\n1. Testando resolve_physical_codes()...")
variables = ["sexo", "idade", "preventivo", "mamografia"]
codes_2013 = resolve_physical_codes(variables, "2013")
codes_2019 = resolve_physical_codes(variables, "2019")
print(f"   Variáveis semânticas: {variables}")
print(f"   Códigos físicos 2013: {codes_2013}")
print(f"   Códigos físicos 2019: {codes_2019}")

# 2. Teste: Traduzir filtro semântico
print("\n2. Testando translate_semantic_filter_to_physical()...")
sex_filter = {"semantico": "sexo", "valor": "2"}
sex_condition = translate_semantic_filter_to_physical(sex_filter, "2013")
print(f"   Filtro semântico: {sex_filter}")
print(f"   Condição SQL: {sex_condition}")

age_filter = {"semantico": "idade", "operador": ">=", "valor": 25}
age_condition = translate_semantic_filter_to_physical(age_filter, "2013")
print(f"   Filtro semântico: {age_filter}")
print(f"   Condição SQL: {age_condition}")

# 3. Teste: Construir WHERE clause
print("\n3. Testando build_where_clause()...")
where_2013 = build_where_clause(source="2013", use_default_filters=True)
print(f"   WHERE clause (2013, com filtros padrão):")
print(f"   {where_2013}")

# 4. Teste: Construir query completa
print("\n4. Testando build_select_query()...")
test_variables = ["sexo", "idade", "preventivo", "mamografia", "peso_amostral"]
query_2013 = build_select_query(test_variables, "2013")
print(f"   Query para 2013:")
print(query_2013)

query_2019 = build_select_query(test_variables, "2019")
print(f"\n   Query para 2019:")
print(query_2019)

# 5. Teste: Variável que não existe em um ano
print("\n5. Testando com variável inexistente...")
# Assumindo que "mamografia" pode não existir em 2019 (exemplo)
variables_with_missing = ["sexo", "idade", "mamografia"]
try:
    codes = resolve_physical_codes(variables_with_missing, "2019")
    print(f"   Variáveis: {variables_with_missing}")
    print(f"   Códigos válidos (None ignorados): {codes}")
except Exception as e:
    print(f"   Erro: {e}")

# 6. Teste: Query com filtros adicionais
print("\n6. Testando query com filtros adicionais...")
additional_filters = {"idade": {"operador": ">=", "valor": 30}}
query_with_filters = build_select_query(
    ["sexo", "idade", "preventivo"],
    "2013",
    filters=additional_filters,
    use_default_filters=True
)
print(f"   Query com filtros adicionais:")
print(query_with_filters)

print("\n" + "=" * 60)
print("✅ Testes de construção de queries concluídos!")
print("=" * 60)
print("\nNota: Para testar basedosdados_client.run_query(), você precisa:")
print("  1. Ter BILLING_PROJECT_ID configurado no .env")
print("  2. Ter credenciais do Google Cloud configuradas")
print("  3. Executar: python -c 'from ingestion.basedosdados_client import test_connection; test_connection()'")
