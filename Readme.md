PNS Analysis 2013–2019

Pipeline completo para ingestão, tratamento, transformação e análise dos microdados da Pesquisa Nacional de Saúde (PNS) — com foco em saúde da mulher, exames preventivos e variáveis sociodemográficas.

Este projeto implementa um fluxo estruturado utilizando:

BigQuery + Base dos Dados (BD+)

Cache local via SQLite

Pipelines modulares (ingestão → transformação → análise)

Testes unitários

Boas práticas de engenharia de dados e organização de código

1. Visão Geral

O objetivo principal é permitir que pesquisadores, estudantes e profissionais de dados realizem análises reprodutíveis, escaláveis e auditáveis sobre temas essenciais da saúde pública brasileira, tais como:

Cobertura de mamografia

Cobertura de exame preventivo

Indicadores de renda e desigualdade

Variáveis demográficas

Condições sociais e acesso à saúde

O projeto suporta os anos 2013 e 2019 da PNS.

2. Arquitetura do Projeto
pns_analysis/
│
├── config.py                 # Variáveis globais e configurações
├── mapping.py                # Dicionários e mapeamentos da PNS
├── requirements.txt          # Dependências do ambiente
│
├── dao/                      # Data Access Layer
│   ├── sqlite_client.py      # Cliente SQLite (cache local)
│   └── __init__.py
│
├── ingestion/                # Ingestão dos microdados
│   └── __init__.py
│
├── transform/                # Transformações e limpeza
│   └── __init__.py
│
├── service/                  # Lógica de negócio e agregações
│   └── __init__.py
│
├── data/
│   └── pns_cache.sqlite      # Cache local (ignorado no Git)
│
└── tests/                    # Testes unitários
    ├── test_sqlite_client.py
    └── test_sqlite_simple.py
3. Objetivos do Projeto
3.1 Técnicos

Criar um pipeline ETL modular, limpo e escalável.

Implementar boas práticas de engenharia de dados.

Centralizar ingestão da PNS via Base dos Dados / BigQuery.

Garantir reprodutibilidade por meio de cache local em SQLite.

Oferecer transformações consistentes e fáceis de auditar.

Disponibilizar testes unitários para estabilidade.

3.2 Analíticos

Estudar indicadores de saúde da mulher.

Comparar padrões entre 2013 vs 2019.

Explorar desigualdades por:

renda

escolaridade

região

raça/cor

Facilitar construção de dashboards, artigos científicos e análises exploratórias.

4. Como Executar o Projeto
4.1 Preparação do Ambiente

Crie um ambiente virtual:

python -m venv .venv


Ative:

# Windows
.venv\Scripts\activate

# Linux / Mac
source .venv/bin/activate


Instale dependências:

pip install -r requirements.txt

5. Configurações Necessárias

O projeto utiliza variáveis de ambiente configuradas via .env.

Arquivo config.py (trecho):
BILLING_PROJECT_ID = os.getenv("BILLING_PROJECT_ID")
BIGQUERY_DATASET = "basedosdados.br_ms_pns"

Crie um arquivo .env com:
BILLING_PROJECT_ID=seu-projeto-no-gcp
LOG_LEVEL=INFO


O arquivo .env.example serve como referência.

6. Principais Módulos
6.1 DAO — Data Access Layer

Responsável por:

Criar tabelas no SQLite

Garantir colunas dinâmicas

Fazer upsert (insert/update) de dados

Realizar leitura confiável do cache

Permite trabalhar sem depender da internet após ingesta inicial.

6.2 Ingestion

Coleta os microdados via Base dos Dados (BD+)

Executa consultas no BigQuery

Salva os resultados em cache SQLite

6.3 Transform

Aplicação de regras consistentes:

Normalização de códigos

Conversão de valores semânticos

Padronização de variáveis

6.4 Service

Camada intermediária:

Regras de negócio

Agregações

Preparação final de datasets analíticos

7. Testes Unitários

O projeto possui uma suíte inicial de testes:

pytest -q


Esses testes garantem que a camada DAO e o SQLite funcionam de forma confiável.

8. Status do Projeto e Próximos Passos
Concluído:

Estrutura completa (ETL modular)

Cache SQLite

Mapeamentos iniciais

Testes unitários

Configurações via .env

Organização em camadas (DAO, ingestion, transform, service)

Próximos passos recomendados:

Expandir mapeamentos da PNS

Criar notebooks exploratórios

Construir dashboards (Metabase / Power BI / Streamlit)

Adicionar pipelines CI/CD (GitHub Actions)

9. Licença

Projeto disponibilizado sob MIT License.
Dados oficiais seguem regras do IBGE e da Base dos Dados.

10. Contato

Autor: Eduardo de Castro
Especialidade: Engenharia, Data Science e IA
LinkedIn: https: //www.linkedin.com/in/eduardo-de-castro-vieira-5b061027b/