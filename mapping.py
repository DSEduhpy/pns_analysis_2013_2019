"""
Mapeamento semântico de variáveis da PNS.

Este módulo é a fonte da verdade para tradução entre nomes semânticos
(usados pelo cientista de dados) e códigos físicos (colunas originais do BigQuery).

Estrutura:
    VAR_MAP[nome_semantico][origem] = {
        "codigo": "codigo_fisico" ou None,
        "tipo": "int" | "string" | "float"
    }

Regras:
    - Se uma variável não existe em um ano específico, codigo = None
    - O sistema deve lidar graciosamente com None, preenchendo com NULL/NaN
    - identificador_unidade e origem são sempre obrigatórios
"""
from typing import Optional

VAR_MAP = {
    # --- Identificação (Sempre obrigatórias) ---
    "identificador_unidade": {  # Identificador único da unidade primária de amostragem (UPA)
        "2013": {"codigo": "upa_pns", "tipo": "string"},
        "2019": {"codigo": "upa_pns", "tipo": "string"},
    },
    "sigla_uf": {  # Sigla da Unidade Federativa (UF) do entrevistado
        "2013": {"codigo": "sigla_uf", "tipo": "string"},
        "2019": {"codigo": "sigla_uf", "tipo": "string"},
    },
    "v0024": {  # Identificador adicional da unidade de amostragem
        "2013": {"codigo": "v0024", "tipo": "string"},
        "2019": {"codigo": "v0024", "tipo": "string"},
    },
    
    # --- Variáveis Críticas (Conforme task_list.md) ---
    "sexo": { # Sexo do entrevistado
        "2013": {"codigo": "c006", "tipo": "string"},
        "2019": {"codigo": "c006", "tipo": "string"},
    },
    "idade": {  # Idade do entrevistado em anos completos
        "2013": {"codigo": "c008", "tipo": "int"},
        "2019": {"codigo": "c008", "tipo": "int"},
    },
    "preventivo": {  # Quando fez o último exame preventivo (Papanicolau)
        "2013": {"codigo": "r001", "tipo": "string"},
        "2019": {"codigo": "r001", "tipo": "string"},
    },
    "mamografia": {  # Fez mamografia (Sim/Não)
        "2013": {"codigo": "r015", "tipo": "string"},
        "2019": {"codigo": "r015", "tipo": "string"},
    },
    "renda_per_capita": {  # Renda per capita da família em reais
        "2013": {"codigo": "vdf003", "tipo": "float"},
        "2019": {"codigo": "vdf003", "tipo": "float"},
    },
    "peso_amostral": {  # Peso amostral estatístico para expansão dos resultados
        "2013": {"codigo": "v00291", "tipo": "float"},
        "2019": {"codigo": "v00291", "tipo": "float"},
    },
    
    # --- Variáveis Adicionais (Do notebook de estudo) ---
    "preventivo_pagou": {  # Quem pagou pelo exame preventivo
        "2013": {"codigo": "r012", "tipo": "string"},
        "2019": {"codigo": "r012", "tipo": "string"},
    },
    "medico_pediu_mamografia": {  # Médico pediu mamografia (Sim/Não)
        "2013": {"codigo": "r014", "tipo": "string"},
        "2019": {"codigo": "r014", "tipo": "string"},
    },
    "mamografia_pagou": {  # Quem pagou pela mamografia
        "2013": {"codigo": "r019", "tipo": "string"},
        "2019": {"codigo": "r019", "tipo": "string"},
    },
    "raca": {  # Raça/cor declarada pelo entrevistado
        "2013": {"codigo": "c009", "tipo": "string"},
        "2019": {"codigo": "c009", "tipo": "string"},
    },
    "anos_estudo": {  # Anos de estudo completos do entrevistado
        "2013": {"codigo": "vdd004a", "tipo": "int"},
        "2019": {"codigo": "vdd004a", "tipo": "int"},
    },
    "ja_engravidou": {  # Já engravidou alguma vez (Sim/Não)
        "2013": {"codigo": "r039", "tipo": "string"},
        "2019": {"codigo": "r039", "tipo": "string"},
    },
    "filhos_vivos": {  # Número de filhos vivos
        "2013": {"codigo": "r045", "tipo": "int"},
        "2019": {"codigo": "r045", "tipo": "int"},
    },
    "estado_civil": {  # Estado civil do entrevistado
        # Opções: 1=Casado(a), 2=Separado(a) ou desquitado(a) judicialmente,
        # 3=Divorciado(a), 4=Viúvo(a), 5=Solteiro(a), vazio/Não aplicável
        "2013": {"codigo": "c011", "tipo": "string"},
        "2019": {"codigo": "c011", "tipo": "string"},
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

