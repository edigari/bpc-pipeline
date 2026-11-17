"""
main.py
-------
Ponto de entrada de linha de comando (execução LOCAL via run.sh).
No Databricks, use o notebook ``notebooks/01_pipeline_bpc``.

Exemplos:
    PYTHONPATH=src python -m bpc_pipeline.main                         # Norte + Nordeste, meses automáticos
    PYTHONPATH=src python -m bpc_pipeline.main --ufs PE --limite-municipios 20   # teste rápido
    PYTHONPATH=src python -m bpc_pipeline.main --ufs NORTE --meses 1-6
"""

from __future__ import annotations

import argparse
import json
import sys

from .config import Settings, parse_meses, parse_ufs
from .pipeline import Contexto, configurar_logging, executar_tudo
from .security import MissingApiKeyError, resolver_api_key


def _args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline BPC (Portal da Transparência) x População (IBGE) - 2026")
    p.add_argument("--ufs", default="NORTE,NORDESTE", help="UFs ou regiões separadas por vírgula, ou ALL")
    p.add_argument("--meses", default="ALL", help="'ALL' (automático), '1,2,3' ou '1-6'")
    p.add_argument("--limite-municipios", type=int, default=0, help="0 = sem limite (use 20 para um teste rápido)")
    p.add_argument("--rate-dia", type=int, default=360, help="req/min entre 06:00 e 23:59 (limite oficial 400)")
    p.add_argument("--rate-noite", type=int, default=630, help="req/min entre 00:00 e 05:59 (limite oficial 700)")
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--base-path", default="./data")
    p.add_argument("--output-path", default="./output")
    p.add_argument("--nao-falhar-em-critico", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = _args(argv)
    configurar_logging()

    try:
        api_key, origem = resolver_api_key()  # env var ou .env (nunca argumento de linha de comando!)
    except MissingApiKeyError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 2

    from pyspark.sql import SparkSession  # import tardio: falha de chave não precisa subir a JVM

    spark = (
        SparkSession.builder.appName("bpc-pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    settings = Settings(
        ufs=parse_ufs(a.ufs),
        meses=parse_meses(a.meses),
        limite_municipios=a.limite_municipios,
        rate_dia_por_min=a.rate_dia,
        rate_noite_por_min=a.rate_noite,
        max_workers=a.workers,
        base_path=a.base_path,
        output_path=a.output_path,
        falhar_em_check_critico=not a.nao_falhar_em_critico,
    )

    try:
        relatorio = executar_tudo(Contexto(spark, settings, api_key, origem))
        print(json.dumps(relatorio["data_quality"]["resumo"], ensure_ascii=False, indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
