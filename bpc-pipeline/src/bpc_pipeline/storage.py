"""
storage.py
----------
Camada de persistência com o MESMO código para Databricks e execução local.

  * Databricks (Unity Catalog): tabelas Delta gerenciadas (``catalogo.schema.tabela``) +
    arquivos em Volume (``/Volumes/...``) para Bronze, relatórios e exportação Parquet.
  * Local: tudo vira pastas Parquet em ./data e ./output.

Decisões de performance:
  * Após gravar Silver/Gold, a tabela é RELIDA do storage. Isso "corta" a linhagem do Spark:
    as etapas seguintes não recalculam tudo desde a Bronze (substitui o ``.cache()``, que não é
    suportado no compute serverless do Databricks).
  * ``repartition(colunas_de_partição)`` antes da escrita gera 1 arquivo por partição
    (evita o problema de "muitos arquivos pequenos").
  * Parquet com compressão Snappy explícita (requisito do desafio; também é o padrão do Delta).
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from .config import Settings

log = logging.getLogger("bpc_pipeline.storage")


def em_databricks() -> bool:
    """Detecta o runtime do Databricks (clusters clássicos e serverless definem esta variável)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


class Storage:
    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings

    # ------------------------------------------------------------ arquivos
    @staticmethod
    def escrever_texto(caminho: str, conteudo: str) -> None:
        """Grava texto via API de arquivos POSIX (funciona em /Volumes no Databricks e localmente)."""
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(conteudo)

    @staticmethod
    def existe(caminho: str) -> bool:
        return os.path.exists(caminho)

    # ------------------------------------------------------------- tabelas
    def gravar_tabela(
        self,
        df: DataFrame,
        nome: str,
        particionar_por: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """
        Grava (overwrite) uma tabela Silver/Gold e devolve o DataFrame relido do storage.

        Overwrite é seguro e idempotente aqui porque Silver/Gold são sempre recalculadas
        a partir da Bronze completa (que é incremental e acumulativa).
        """
        cols = list(particionar_por or [])
        df_out = df.repartition(*cols) if cols else df.coalesce(1)

        if self.settings.usa_unity_catalog:
            fq = self.settings.nome_tabela(nome)
            writer = (
                df_out.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")  # permite evoluir schema/partições entre versões
            )
            if cols:
                writer = writer.partitionBy(*cols)
            writer.saveAsTable(fq)
            log.info("Tabela Delta gravada: %s", fq)
            return self.spark.table(fq)

        caminho = f"{self.settings.base_path.rstrip('/')}/{nome}"
        writer = df_out.write.mode("overwrite").option("compression", "snappy")
        if cols:
            writer = writer.partitionBy(*cols)
        writer.parquet(caminho)
        log.info("Tabela Parquet gravada: %s", caminho)
        return self.spark.read.parquet(caminho)

    def exportar_parquet(self, df: DataFrame, nome: str, particionar_por: Sequence[str]) -> str:
        """Exporta a camada final para ``output/<nome>/ano=AAAA/mes=M/*.snappy.parquet``."""
        caminho = f"{self.settings.output_path.rstrip('/')}/{nome}"
        (
            df.repartition(*particionar_por)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .partitionBy(*particionar_por)
            .parquet(caminho)
        )
        log.info("Exportado Parquet (snappy) particionado por %s: %s", list(particionar_por), caminho)
        return caminho
