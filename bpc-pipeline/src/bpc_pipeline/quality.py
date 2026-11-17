"""
quality.py
----------
Suíte de Data Quality automatizada + relatório de auditoria (JSON e CSV).

Validações implementadas:
  * SCHEMA        -> colunas e tipos esperados da Gold (detecta drift de schema).
  * VOLUME        -> tabela não pode estar vazia.
  * UNICIDADE     -> chave natural sem duplicatas (Gold e dimensões).
  * NULL RATIO    -> percentual de nulos por coluna, com limites por criticidade.
  * INTERVALOS    -> valores não negativos, população > 0, taxa por mil em [0, 1000], ano = 2026...
  * INTEGRIDADE   -> % de municípios do BPC encontrados no IBGE (cruzamento entre fontes).
  * RECONCILIAÇÃO -> soma de beneficiários Silver == Gold (o JOIN não pode perder/duplicar linhas).
  * COMPLETUDE    -> % dos pares (município, mês) do escopo efetivamente extraídos.

Performance: TODAS as métricas de uma tabela (nulos de todas as colunas, unicidade e
intervalos) são calculadas em UMA única agregação Spark (um único job), em vez de um
``count()`` por coluna - que é o erro clássico que torna data quality lento.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("bpc_pipeline.quality")

CRITICA, ALTA, MEDIA, INFO = "CRITICA", "ALTA", "MEDIA", "INFO"

#: Contrato de schema da camada Gold (coluna -> prefixo do tipo Spark).
SCHEMA_GOLD_ESPERADO: dict[str, str] = {
    "codigo_ibge": "string",
    "ano_mes": "int",
    "ano": "int",
    "mes": "int",
    "data_competencia": "date",
    "nome_municipio": "string",
    "uf_sigla": "string",
    "uf_nome": "string",
    "regiao_nome": "string",
    "quantidade_beneficiados": "bigint",
    "valor_total": "decimal",
    "populacao_estimada": "bigint",
    "ano_populacao": "int",
    "beneficiarios_por_mil_hab": "decimal",
    "valor_per_capita": "decimal",
    "valor_medio_beneficio": "decimal",
    "variacao_mensal_beneficiarios_pct": "decimal",
    "participacao_valor_uf_pct": "decimal",
    "ranking_cobertura_uf": "int",
    "flag_municipio_ibge": "boolean",
    "flag_populacao": "boolean",
}


@dataclass
class Check:
    check: str
    tabela: str
    coluna: Optional[str]
    metrica: Optional[float]
    limite: Optional[str]
    status: str          # PASS | FAIL
    severidade: str      # CRITICA | ALTA | MEDIA | INFO
    detalhe: str = ""


def _ok(cond: bool) -> str:
    return "PASS" if cond else "FAIL"


# ---------------------------------------------------------------------------
# Verificações
# ---------------------------------------------------------------------------

def verificar_schema(df: DataFrame, esperado: dict[str, str], tabela: str) -> list[Check]:
    atual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    checks: list[Check] = []
    faltantes = [c for c in esperado if c not in atual]
    checks.append(Check("schema_colunas_obrigatorias", tabela, None, float(len(faltantes)), "0 faltantes",
                        _ok(not faltantes), CRITICA, f"faltantes={faltantes}" if faltantes else "ok"))
    divergentes = {c: atual[c] for c, t in esperado.items() if c in atual and not atual[c].startswith(t)}
    checks.append(Check("schema_tipos", tabela, None, float(len(divergentes)), "0 divergências",
                        _ok(not divergentes), CRITICA, json.dumps(divergentes) if divergentes else "ok"))
    return checks


def perfilar_tabela(
    df: DataFrame,
    tabela: str,
    chave: list[str],
    regras_intervalo: dict[str, str],
    limites_nulos: dict[str, tuple[float, str]],
) -> tuple[list[Check], dict[str, Any]]:
    """
    Calcula volume, unicidade, null ratio de TODAS as colunas e violações de intervalo
    em UMA única passada (``df.agg``). Retorna os checks e o perfil bruto (para o relatório).
    """
    exprs = [F.count(F.lit(1)).alias("__linhas"), F.countDistinct(*[F.col(c) for c in chave]).alias("__distintos")]
    exprs += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"__nulos__{c}") for c in df.columns]
    # Regra de intervalo = expressão SQL que deve ser VERDADEIRA; nulos não contam como violação.
    exprs += [
        F.sum(F.when(F.expr(f"NOT coalesce({regra}, true)"), 1).otherwise(0)).alias(f"__viol__{nome}")
        for nome, regra in regras_intervalo.items()
    ]
    linha = df.agg(*exprs).collect()[0].asDict()

    n = int(linha["__linhas"])
    checks = [
        Check("volume_nao_vazio", tabela, None, float(n), "> 0", _ok(n > 0), CRITICA),
        Check("unicidade_chave", tabela, ",".join(chave), float(n - int(linha["__distintos"])), "0 duplicadas",
              _ok(n == int(linha["__distintos"])), CRITICA, f"linhas={n} distintas={linha['__distintos']}"),
    ]

    null_ratio: dict[str, float] = {}
    for c in df.columns:
        ratio = (linha[f"__nulos__{c}"] / n) if n else 0.0
        null_ratio[c] = round(ratio, 6)
        if c in limites_nulos:
            limite, sev = limites_nulos[c]
            checks.append(Check("null_ratio", tabela, c, round(ratio, 6), f"<= {limite}", _ok(ratio <= limite), sev))

    for nome, regra in regras_intervalo.items():
        viol = int(linha[f"__viol__{nome}"])
        sev = MEDIA if nome.startswith("plausivel") else ALTA
        checks.append(Check(f"intervalo_{nome}", tabela, None, float(viol), "0 violações", _ok(viol == 0), sev, regra))

    return checks, {"linhas": n, "null_ratio": null_ratio}


# ---------------------------------------------------------------------------
# Suíte completa
# ---------------------------------------------------------------------------

def executar_suite(
    silver_bpc: DataFrame,
    silver_mun: DataFrame,
    silver_pop: DataFrame,
    gold: DataFrame,
    gold_uf: DataFrame,
    pares_escopo: int,
    pares_concluidos: int,
    ano_referencia: int,
) -> dict[str, Any]:
    checks: list[Check] = []
    perfis: dict[str, Any] = {}

    # --- Gold: schema + perfil completo --------------------------------------------------
    checks += verificar_schema(gold, SCHEMA_GOLD_ESPERADO, "gold_bpc_municipio_mes")
    c, perfis["gold_bpc_municipio_mes"] = perfilar_tabela(
        gold,
        "gold_bpc_municipio_mes",
        chave=["codigo_ibge", "ano_mes"],
        regras_intervalo={
            "ano_referencia": f"ano = {ano_referencia}",
            "mes_valido": "mes BETWEEN 1 AND 12",
            "beneficiarios_nao_negativo": "quantidade_beneficiados >= 0",
            "valor_nao_negativo": "valor_total >= 0",
            "populacao_positiva": "populacao_estimada > 0",
            "taxa_por_mil_0_1000": "beneficiarios_por_mil_hab BETWEEN 0 AND 1000",
            "codigo_ibge_7_digitos": "codigo_ibge RLIKE '^[0-9]{7}$'",
            # BPC = 1 salário mínimo por beneficiário: média muito fora disso indica problema na fonte.
            "plausivel_valor_medio_beneficio": "valor_medio_beneficio BETWEEN 500 AND 5000",
        },
        limites_nulos={
            "codigo_ibge": (0.0, CRITICA),
            "ano_mes": (0.0, CRITICA),
            "data_competencia": (0.0, CRITICA),
            "quantidade_beneficiados": (0.01, ALTA),
            "valor_total": (0.01, ALTA),
            "uf_sigla": (0.0, ALTA),
            "nome_municipio": (0.0, ALTA),
            "populacao_estimada": (0.01, ALTA),
            "beneficiarios_por_mil_hab": (0.01, MEDIA),
        },
    )
    checks += c

    # --- Dimensões e Silver ----------------------------------------------------------------
    for df, nome, chave in (
        (silver_mun, "silver_municipios", ["codigo_ibge"]),
        (silver_pop, "silver_populacao", ["codigo_ibge"]),
        (silver_bpc, "silver_bpc_municipio_tipo", ["codigo_ibge", "ano_mes", "tipo_id"]),
        (gold_uf, "gold_bpc_uf_mes", ["uf_sigla", "ano_mes"]),
    ):
        c, perfis[nome] = perfilar_tabela(df, nome, chave, regras_intervalo={}, limites_nulos={chave[0]: (0.0, CRITICA)})
        checks += c

    # --- Integridade referencial + reconciliação (uma agregação em cada lado) -------------
    g = gold.agg(
        F.avg(F.col("flag_municipio_ibge").cast("double")).alias("pct_mun"),
        F.avg(F.col("flag_populacao").cast("double")).alias("pct_pop"),
        F.sum("quantidade_beneficiados").alias("soma_gold"),
    ).collect()[0]
    soma_silver = silver_bpc.agg(F.sum("quantidade_beneficiados").alias("s")).collect()[0]["s"]

    pct_mun = float(g["pct_mun"] or 0.0)
    pct_pop = float(g["pct_pop"] or 0.0)
    checks.append(Check("integridade_municipio_no_ibge", "gold_bpc_municipio_mes", "codigo_ibge",
                        round(pct_mun, 6), ">= 0.995", _ok(pct_mun >= 0.995), ALTA))
    checks.append(Check("integridade_populacao_encontrada", "gold_bpc_municipio_mes", "populacao_estimada",
                        round(pct_pop, 6), ">= 0.99", _ok(pct_pop >= 0.99), ALTA))
    checks.append(Check("reconciliacao_beneficiarios_silver_gold", "gold_bpc_municipio_mes", "quantidade_beneficiados",
                        float((g["soma_gold"] or 0) - (soma_silver or 0)), "diferença = 0",
                        _ok((g["soma_gold"] or 0) == (soma_silver or 0)), CRITICA,
                        f"silver={soma_silver} gold={g['soma_gold']}"))

    # --- Completude da extração ------------------------------------------------------------
    completude = (pares_concluidos / pares_escopo) if pares_escopo else 1.0
    checks.append(Check("completude_extracao_portal", "bronze_bpc", None, round(completude, 6), ">= 0.99",
                        _ok(completude >= 0.99), ALTA, f"{pares_concluidos}/{pares_escopo} pares município x mês"))

    falhas = [ch for ch in checks if ch.status == "FAIL"]
    return {
        "resumo": {
            "total_checks": len(checks),
            "aprovados": len(checks) - len(falhas),
            "reprovados": len(falhas),
            "reprovados_criticos": sum(1 for ch in falhas if ch.severidade == CRITICA),
            "status_geral": "APROVADO" if not falhas else (
                "REPROVADO" if any(ch.severidade == CRITICA for ch in falhas) else "APROVADO_COM_ALERTAS"),
        },
        "checks": [asdict(ch) for ch in checks],
        "perfis": perfis,
    }


def checks_para_csv(checks: list[dict[str, Any]]) -> str:
    """Versão tabular dos checks (fácil de abrir no Excel / Power BI)."""
    buf = io.StringIO()
    escritor = csv.DictWriter(buf, fieldnames=list(checks[0].keys()) if checks else ["check"])
    escritor.writeheader()
    escritor.writerows(checks)
    return buf.getvalue()
