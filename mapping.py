"""
Mapeamento semântico de variáveis da PNS.

Este módulo é a fonte da verdade para tradução entre nomes semânticos
(usados pelo cientista de dados) e códigos físicos (colunas originais do BigQuery).

Estrutura:
    VAR_MAP[nome_semantico] = {
        "descricao": "Descrição da variável (opcional, pode ser multi-linha)",
        "2013": {"codigo": "codigo_fisico" ou None, "tipo": "int" | "string" | "float"},
        "2019": {"codigo": "codigo_fisico" ou None, "tipo": "int" | "string" | "float"},
    }

Regras:
    - Se uma variável não existe em um ano específico, codigo = None
    - O sistema deve lidar graciosamente com None, preenchendo com NULL/NaN
    - Componentes da PK (id_upa, id_domicilio, id_morador) e origem são sempre obrigatórios
    - O campo "descricao" é opcional, mas recomendado para documentação
"""
from typing import Optional

VAR_MAP = {
    # --- Identificação (Sempre obrigatórias para PK) ---
    "id_upa": {
        "descricao": "Identificador único da unidade primária de amostragem (UPA)",
        "2013": {"codigo": "upa_pns", "tipo": "string"},
        "2019": {"codigo": "upa_pns", "tipo": "string"},
    },
    "id_domicilio": {
        "descricao": "Identificador do domicílio dentro da UPA",
        "2013": {"codigo": "v0006_pns", "tipo": "string"},
        "2019": {"codigo": "v0006_pns", "tipo": "string"},
    },
    "id_morador": {
        "descricao": "Número de ordem do morador no domicílio",
        "2013": {"codigo": "c00301", "tipo": "string"},
        "2019": {"codigo": "c00301", "tipo": "string"},
    },
    # --- Identificação e Localização ---
    "uf": {
        "descricao": """
        Unidade da Federação (código IBGE).
        Código numérico da UF conforme padrão IBGE (11-53).
        Exemplos: 11=Rondônia, 12=Acre, 13=Amazonas, 21=Maranhão, 35=São Paulo, etc.
        Utilizada para derivar a 'Região' (Norte, Nordeste, etc.) conforme metodologia do estudo.
        """,
        "2013": {"codigo": "sigla_uf", "tipo": "string"},
        "2019": {"codigo": "sigla_uf", "tipo": "string"},
    },
    "situacao_censitaria": {
        "descricao": """
        Tipo de situação censitária (Urbano/Rural).
        1: Urbano
        2: Rural
        """,
        "2013": {"codigo": "V0026", "tipo": "string"},
        "2019": {"codigo": "V0026", "tipo": "string"},
    },

    # --- Variáveis Demográficas (Filtros e Controles) ---
    "sexo": {
        "descricao": "Sexo do morador (1=Masculino, 2=Feminino).",
        "2013": {"codigo": "C006", "tipo": "string"},
        "2019": {"codigo": "C006", "tipo": "string"},
    },
    "idade": {
        "descricao": "Idade do morador na data de referência (em anos).",
        "2013": {"codigo": "C008", "tipo": "int"},
        "2019": {"codigo": "C008", "tipo": "int"},
    },
    "cor_raca": {
        "descricao": """
        Cor ou raça.
        1: Branca, 2: Preta, 3: Amarela, 4: Parda, 5: Indígena
        O estudo agrupa em Branca vs Não-Branca.
        """,
        "2013": {"codigo": "C009", "tipo": "string"},
        "2019": {"codigo": "C009", "tipo": "string"},
    },
    "estado_civil": {
        "descricao": """
        Situação conjugal / Estado civil legal.
        1: Casado(a), 2: Separado(a), 3: Divorciado(a), 4: Viúvo(a), 5: Solteiro(a), "": Não aplicável
        """,
        "2013": {"codigo": "C011", "tipo": "string"},
        "2019": {"codigo": "C011", "tipo": "string"},
    },
    "vive_com_companheiro": {
        "descricao": "Vive com cônjuge ou companheiro(a)? (1=Sim, 2=Não)",
        "2013": {"codigo": "C010", "tipo": "string"},
        "2019": {"codigo": "C01001", "tipo": "string"},
    },

    # --- Variáveis Socioeconômicas ---
    "escolaridade_nivel": {
        "descricao": """
        Nível de instrução mais elevado alcançado (padronizado).
        Utilizado para criar a variável 'Anos de estudo' ou faixas de escolaridade.
        """,
        "2013": {"codigo": "VDD004A", "tipo": "string"},
        "2019": {"codigo": "VDD004A", "tipo": "string"},
    },
    "trabalha": {
        "descricao": """
        Condição de ocupação na semana de referência.
        1: Pessoas Ocupadas
        2: Pessoas desocupadas
        """,
        "2013": {"codigo": "VDE002", "tipo": "string"},
        "2019": {"codigo": "VDE002", "tipo": "string"},
    },
    "renda_domiciliar_pc": {
        "descricao": """
        Rendimento domiciliar per capita (Reais).
        Utilizado para calcular os decis de renda.
        """,
        "2013": {"codigo": "VDF003", "tipo": "float"},
        "2019": {"codigo": "VDF003", "tipo": "float"},
    },

    # --- Saúde Reprodutiva (Existência de Filhos) ---
    "tem_filhos_nascidos_vivos": {
        "descricao": """
        Variável proxy para 'Ter Filhos'.
        2013: Quantos filhos nasceram vivos (R045).
        2019: Quantos partos a Sra já teve (S066).
        Obs: Em 2019, a variável disponível no recorte é 'número de partos', 
        enquanto em 2013 é 'filhos nascidos vivos'.
        """,
        "2013": {"codigo": "R045", "tipo": "int"},
        "2019": {"codigo": "S066", "tipo": "int"},
    },

    # --- Variável Dependente 1: Exame Preventivo (Papanicolau) ---
    "preventivo_quando": {
        "descricao": """
        Quando foi a última vez que a Sra fez um exame preventivo para câncer de colo do útero?
        1: Menos de 1 ano atrás
        2: De 1 ano a menos de 2 anos
        3: De 2 anos a menos de 3 anos
        4: 3 anos ou mais atrás
        5: Nunca fez
        """,
        "2013": {"codigo": "R001", "tipo": "string"},
        "2019": {"codigo": "R00101", "tipo": "string"},
    },
    "preventivo_plano": {
        "descricao": "O último exame preventivo foi coberto por plano de saúde? (1=Sim, 2=Não)",
        "2013": {"codigo": "R003", "tipo": "string"},
        "2019": {"codigo": None, "tipo": None}, # Não consta no trecho do dicionário 2019 fornecido
    },
    "preventivo_pago": {
        "descricao": "A Sra pagou algum valor pelo último exame preventivo? (1=Sim, 2=Não)",
        "2013": {"codigo": "R004", "tipo": "string"},
        "2019": {"codigo": "R004", "tipo": "string"},
    },
    "preventivo_sus": {
        "descricao": "O último exame preventivo foi feito pelo SUS? (1=Sim, 2=Não)",
        "2013": {"codigo": "R005", "tipo": "string"},
        "2019": {"codigo": "R005", "tipo": "string"},
    },

    # --- Variável Dependente 2: Mamografia ---
    "mamografia_quando": {
        "descricao": """
        Quando foi a última vez que a Sra fez um exame de mamografia?
        1: Menos de 1 ano atrás
        2: De 1 ano a menos de 2 anos
        3: De 2 anos a menos de 3 anos
        4: 3 anos ou mais atrás
        (Categorias de 'Nunca fez' variam na origem, requer tratamento)
        """,
        "2013": {"codigo": "R017", "tipo": "string"},
        "2019": {"codigo": "R01701", "tipo": "string"},
    },
    "mamografia_plano": {
        "descricao": "A última mamografia foi coberta por plano de saúde? (1=Sim, 2=Não)",
        "2013": {"codigo": "R018", "tipo": "string"},
        "2019": {"codigo": None, "tipo": None}, # Não consta no trecho do dicionário 2019 fornecido
    },
    "mamografia_paga": {
        "descricao": "A Sra pagou algum valor pela última mamografia? (1=Sim, 2=Não)",
        "2013": {"codigo": "R019", "tipo": "string"},
        "2019": {"codigo": "R019", "tipo": "string"},
    },
    "mamografia_sus": {
        "descricao": "A última mamografia foi feita pelo SUS? (1=Sim, 2=Não)",
        "2013": {"codigo": "R020", "tipo": "string"},
        "2019": {"codigo": "R020", "tipo": "string"},
    },
    
    # --- Pesos Amostrais ---
    "peso_amostral": {
        "descricao": "Peso do morador selecionado com calibração.",
        "2013": {"codigo": "V00291", "tipo": "float"},
        "2019": {"codigo": "V00291", "tipo": "float"},
    },
}

# --- Funções auxiliares ---

def get_codigo_fisico(variavel_semantica: str, origem: str) -> Optional[str]:
    """
    Retorna o código físico de uma variável semântica para uma origem específica.
    
    Args:
        variavel_semantica: Nome semântico da variável (ex: "sexo")
        origem: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        Código físico (ex: "c006") ou None se não existir
    """
    if variavel_semantica not in VAR_MAP:
        return None
    
    if origem not in VAR_MAP[variavel_semantica]:
        return None
    
    return VAR_MAP[variavel_semantica][origem].get("codigo")


def get_tipo(variavel_semantica: str, origem: str) -> Optional[str]:
    """
    Retorna o tipo de uma variável semântica para uma origem específica.
    
    Args:
        variavel_semantica: Nome semântico da variável
        origem: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        Tipo da variável ("int", "string", "float") ou None
    """
    if variavel_semantica not in VAR_MAP:
        return None
    
    if origem not in VAR_MAP[variavel_semantica]:
        return None
    
    return VAR_MAP[variavel_semantica][origem].get("tipo")


def variavel_existe(variavel_semantica: str, origem: str) -> bool:
    """
    Verifica se uma variável semântica existe para uma origem específica.
    
    Args:
        variavel_semantica: Nome semântico da variável
        origem: Ano da pesquisa ("2013" ou "2019")
    
    Returns:
        True se a variável existe (codigo != None), False caso contrário
    """
    codigo = get_codigo_fisico(variavel_semantica, origem)
    return codigo is not None


def listar_variaveis_disponiveis(origem: str = None) -> list[str]:
    """
    Lista todas as variáveis semânticas disponíveis.
    
    Args:
        origem: Se fornecido, retorna apenas variáveis que existem nesta origem.
                Se None, retorna todas as variáveis.
    
    Returns:
        Lista de nomes semânticos
    """
    if origem is None:
        return list(VAR_MAP.keys())
    
    return [
        var for var in VAR_MAP.keys()
        if variavel_existe(var, origem)
    ]

