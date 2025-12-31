## pns-womens-health-analysis

Biblioteca Python para análise da **Pesquisa Nacional de Saúde (PNS)** focada em
saúde da mulher (2013 e 2019), com:

- **Abstração semântica**: você trabalha com variáveis como `sexo`, `fez_mamografia`,
  `renda_per_capita`, em vez de códigos físicos (`c006`, `r001`, `vdf003`).
- **Repositório local em SQLite**: primeira execução busca do BigQuery, execuções
  seguintes usam apenas o repositório local (persistente e eficiente).
- **Interface simples para notebooks**: uma única função pública de alto nível:

```python
from service.pns_service import get_dataframe
```

---

## 1. Visão geral da arquitetura

A biblioteca é organizada em camadas:

- `config.py`: configurações gerais (BigQuery, SQLite, filtros padrão, `PRIMARY_KEY_COLUMNS`).
- `mapping.py`: mapeamento semântico (`VAR_MAP`) de variáveis para códigos físicos.
- `ingestion/`: monta e executa queries no BigQuery (`basedosdados`).
- `transform/`: converte dados físicos → semânticos e faz limpeza/derivação.
- `dao/`: gerencia o repositório local SQLite (verifica, insere e lê dados, sincroniza metadados).
- `service/`: camada de serviço usada pelo cientista de dados (inclui registro de variáveis derivadas).
- `notebooks/`: exemplos de uso e análises.

Fluxo simplificado:

1. `get_dataframe` recebe variáveis semânticas e origens (anos).
2. O DAO verifica se os dados já estão no SQLite (repositório local).
3. Se faltar dado:
   - Monta SQL no BigQuery com base em `mapping.py` e `config.py`;
   - Baixa os dados via `basedosdados`;
   - Converte para semântico e limpa;
   - Faz UPSERT na tabela `pns_respostas` (SQLite).
4. Lê apenas do SQLite e retorna um `DataFrame` pronto para análise.

**Importante**: O repositório local é **persistente** - os dados ficam salvos entre sessões,
não é um cache temporário. Isso significa economia de custos e tempo em execuções subsequentes.

Para detalhes completos da arquitetura, consulte:

- `docs/arquitetura.md`
- `docs/diagrama.mmd`

---

## 2. Instalação e setup

### 2.1. Dependências Python

No seu ambiente virtual (`.venv`), instale:

```bash
pip install -r requirements.txt
```

O arquivo `requirements.txt` inclui, entre outros:

- `pandas`
- `basedosdados`
- `db-dtypes`
- `pandas-gbq`
- `python-dotenv`

### 2.2. Configuração do `.env`

Na raiz do projeto, copie `.env.example` para `.env` e ajuste:

```bash
cp .env.example .env
```

Edite o arquivo `.env`:

```bash
BILLING_PROJECT_ID=seu_projeto_de_billing_gcp
LOG_LEVEL=INFO
```

- `BILLING_PROJECT_ID`: projeto de billing do GCP que será usado para as queries
  no BigQuery via `basedosdados`.

### 2.3. Autenticação no Google Cloud

Certifique-se de que seu usuário está autenticado com permissões de leitura no
dataset da PNS:

```bash
gcloud auth application-default login
```

---

## 3. Uso básico no notebook

O uso esperado é em notebooks Jupyter ou semelhantes.

### 3.1. Importando o serviço

```python
from service.pns_service import (
    get_dataframe,
    list_variables,
    register_derived_variable,
    repopulate_all_data
)
```

### 3.2. Explorando variáveis disponíveis

```python
# Todas as variáveis conhecidas no mapping.py
df_vars = list_variables()
df_vars.head()

# Somente variáveis que existem em 2013
df_vars_2013 = list_variables(source="2013")

# Somente variáveis que existem em 2019
df_vars_2019 = list_variables(source="2019")
```

### 3.3. Obtendo um DataFrame consolidado

```python
variables = [
    "escolaridade_nivel", "trabalha", "renda_domiciliar_pc", "tem_filhos_nascidos_vivos"
]

sources = ["2013", "2019"]

df = get_dataframe(
    variables=variables,
    sources=sources,
    filters={"sexo": "2", "idade": {"operador": ">=", "valor": 25}}  # opcional
)
```

**Comportamento importante dos filtros:**

- Você **não precisa** incluir as colunas usadas nos filtros na lista de `variables`.
- O sistema **automaticamente** inclui essas colunas no carregamento para aplicar os filtros.
- As colunas dos filtros **não aparecem** no DataFrame retornado (a menos que você as solicite explicitamente).

No exemplo acima, mesmo que você não tenha incluído `sexo` e `idade` em `variables`, o sistema:
1. Carrega essas colunas do repositório local (ou busca do BigQuery se necessário).
2. Aplica os filtros (`sexo == "2"` e `idade >= 25`).
3. Retorna apenas as variáveis solicitadas (`escolaridade_nivel`, `trabalha`, etc.).

O `DataFrame` retornado:

- Inclui sempre `origem` (ano) e componentes da PK (`id_upa`, `id_domicilio`, `id_morador`);
- Tem colunas semânticas nas demais variáveis solicitadas;
- Já traz variáveis derivadas pré-definidas, como:
  - `fez_mamografia` (0/1)
  - `fez_preventivo` (0/1)
  - `eh_branca` (0/1)
- Pode incluir variáveis derivadas personalizadas registradas via `register_derived_variable()`

---

## 4. Comportamento do repositório local (SQLite)

O repositório local é um banco SQLite persistente (`data/pns_cache.sqlite`) que armazena
todos os dados já processados. Isso permite:

- **Economia de custos**: evita queries repetidas no BigQuery.
- **Performance**: leitura local é muito mais rápida que queries no BigQuery.
- **Persistência**: dados permanecem salvos entre sessões.

### 4.1. Primeira execução

Quando você chama `get_dataframe` pela primeira vez com um conjunto de variáveis/origens:

1. A biblioteca verifica se os dados já existem no repositório local.
2. Se faltar algo:
   - Monta SQL para o BigQuery (apenas as colunas necessárias, não `SELECT *`);
   - Baixa os dados via `basedosdados`;
   - Converte para semântico e limpa;
   - Salva no SQLite (`data/pns_cache.sqlite`).

### 4.2. Execuções subsequentes

Chamadas subsequentes com o mesmo conjunto (ou subconjunto) de variáveis:

- São muito mais rápidas (apenas leitura do SQLite);
- Não geram custo adicional no BigQuery;
- Usam dados já processados e limpos.

### 4.3. Estrutura do repositório local

A tabela principal é:

- **Nome**: `pns_respostas`
- **PK composta**: (`origem`, `id_upa`, `id_domicilio`, `id_morador`) - granularidade de indivíduo
- **Colunas semânticas**:
  - `sexo`, `idade`, `preventivo`, `preventivo_pagou`, `mamografia`,
  - `peso_amostral`, `renda_per_capita`, `eh_branca`, etc.
  - Variáveis derivadas registradas pelo usuário

O schema é **evolutivo**: se você pedir uma variável ainda não presente, a
aplicação faz `ALTER TABLE ... ADD COLUMN` conforme necessário.

**Tabelas de metadados:**

- `metadata_variables`: catálogo de todas as variáveis (físicas e derivadas)
- `metadata_mapping`: mapeamento de variáveis por origem (ano)

O catálogo de metadados é sincronizado automaticamente com `mapping.py` e
documenta variáveis derivadas registradas via `register_derived_variable()`.

---

## 5. Variáveis derivadas

O sistema permite criar **variáveis derivadas** calculadas a partir de outras variáveis:

```python
from service.pns_service import register_derived_variable, get_dataframe

# Registrar uma nova variável derivada
register_derived_variable(
    name="imc",
    description="Índice de Massa Corporal calculado a partir de peso e altura",
    depends_on=["peso", "altura"],
    func=lambda df: df["peso"] / (df["altura"] ** 2)
)

# Usar como variável normal
df = get_dataframe(variables=["sexo", "imc"], sources=["2013", "2019"])
```

**Características:**

- **Cálculo automático**: Calculada automaticamente quando solicitada
- **Persistência**: Resultados são salvos no SQLite para evitar recálculo
- **Documentação automática**: Aparece em `list_variables()` com descrição e dependências
- **Transparência**: Usa como qualquer outra variável, sem diferença no código
- **Persistência entre sessões**: Variáveis derivadas calculadas em uma sessão ficam
  disponíveis em sessões futuras (não precisa recalcular)

Para mais detalhes, consulte a seção 8 do [`docs/manual_cientista_dados.md`](docs/manual_cientista_dados.md).

---

## 6. Repopulando todos os dados

Se você precisar repopular o repositório local com todos os dados do `mapping.py` (por exemplo,
após problemas com dados ou atualizações no `mapping.py`), use a função `repopulate_all_data()`:

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

**Características:**

- **Preserva colunas derivadas**: Por padrão, as colunas derivadas não são removidas
- **Atualização via UPSERT**: Dados existentes são atualizados, novos são inseridos
- **Lazy loading inteligente**: Apenas busca dados do BigQuery se necessário
- **Todas as variáveis físicas**: Garante que todas as variáveis do `mapping.py` estejam disponíveis

**Notas importantes:**

- Esta operação pode levar vários minutos, dependendo da quantidade de dados
- Os dados são atualizados via UPSERT, então registros existentes são atualizados
- Colunas derivadas são preservadas automaticamente (não são removidas)
- Variáveis físicas são atualizadas/inseridas conforme o `mapping.py` atual

---

## 7. Comportamento com variáveis inexistentes

Se você tentar pedir uma variável que **não existe** no `mapping.py`, por exemplo:

```python
df = get_dataframe(variables=["cor_de_olhos"], sources=["2013"])
```

a função `get_dataframe` irá lançar:

```text
ValueError: Variáveis semânticas desconhecidas: ['cor_de_olhos'].
Use service.pns_service.list_variables() para consultar as variáveis disponíveis.
```

Ou seja:

- Nenhuma coluna extra é criada no SQLite;
- Nenhuma query ao BigQuery é executada para variáveis desconhecidas;
- A mensagem de erro orienta a usar `list_variables()` para descobrir os nomes
  corretos.

---

## 7. Manual para Cientistas de Dados

Para um guia mais detalhado, passo a passo, focado no uso em notebooks,
consulte o manual dedicado:

- [`docs/manual_cientista_dados.md`](docs/manual_cientista_dados.md)

Esse manual cobre:

- Como preparar o ambiente;
- Como descobrir variáveis disponíveis;
- Como montar consultas comuns;
- Como usar filtros (incluindo colunas não solicitadas);
- Como interpretar e trabalhar com o repositório local;
- Boas práticas de análise com peso amostral.

---

## 8. Desenvolvimento e testes

### 8.1. Estrutura do código

Arquivos principais:

- `config.py` – constantes e configurações globais.
- `mapping.py` – dicionário `VAR_MAP` (semântico → físico).
- `dao/sqlite_client.py` – operações de baixo nível no SQLite.
- `dao/pns_dao.py` – orquestração repositório local/ingestão/transformação.
- `ingestion/query_builder.py` – construção de SQL para BigQuery.
- `ingestion/basedosdados_client.py` – execução de SQL via `basedosdados`.
- `transform/converters.py` – conversão físico → semântico.
- `transform/cleaning.py` – limpeza e criação de variáveis derivadas.
- `service/pns_service.py` – interface pública (`get_dataframe`, `list_variables`, `register_derived_variable`).

### 8.2. Testes existentes

Na pasta `tests/` e scripts auxiliares, há testes para:

- Cliente SQLite (`dao/sqlite_client.py`);
- Ingestão e construção de queries;
- Transformação e limpeza de dados.

Você pode executá-los conforme sua ferramenta de testes preferida (por exemplo,
`pytest`) ou rodar os scripts de teste diretamente, se desejar validar comportamentos
específicos.

---

## 10. Contribuições e extensões

Para adicionar novas variáveis:

**Variáveis físicas (do BigQuery):**

1. Inclua a variável em `mapping.py` dentro de `VAR_MAP`, com:
   - Nome semântico;
   - Códigos físicos por ano (ou `None` se não existir em algum ano);
   - Tipo (`int`, `string`, `float`);
   - Descrição (opcional, mas recomendado).

2. (Opcional) Adapte `transform/cleaning.py` se a variável exigir tratamento
   especial ou criação de flags derivadas.

3. Use `get_dataframe` para regenerar e popular o repositório local com a nova variável.
   Os metadados serão sincronizados automaticamente.

**Variáveis derivadas (calculadas):**

Use `register_derived_variable()` diretamente no notebook ou script Python.
Não é necessário modificar o código base. A variável será documentada
automaticamente no catálogo de metadados.

**Outras configurações:**

- Se precisar alterar filtros padrão (por exemplo, mudar faixa etária ou foco em
  outro grupo populacional), ajuste o dicionário `PNS_FILTERS` em `config.py`.
- A chave primária é definida em `PRIMARY_KEY_COLUMNS` em `config.py` (não modifique
  sem entender as implicações).
