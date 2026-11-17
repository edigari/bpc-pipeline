"""
pipeline.py
-----------
Orquestração das etapas. Cada etapa é uma função pública, para que o notebook do Databricks
possa executá-las célula a célula (com visibilidade de tempo e resultado), e ``executar_tudo``
roda o fluxo ponta a ponta (usado pelo run.sh e por Jobs agendados).

    etapa_1_ibge      -> extração IBGE (Bronze)
    etapa_2_portal    -> extração BPC paralela e incremental (Bronze)
    etapa_3_silver    -> desaninhamento, tipagem, nulos, deduplicação
    etapa_4_gold      -> JOIN entre fontes + métricas, particionado por ano/mês
    etapa_5_qualidade -> Data Quality + relatório JSON/CSV
    etapa_6_exportar  -> Parquet Snappy particionado em output/
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from . import transform as T
from .config import Settings
from .extract_ibge import ResultadoIBGE, extrair_ibge
from .extract_portal import ResultadoPortal, extrair_bpc
from .http_client import ApiClient, RateLimiter, criar_provedor_de_taxa
from .quality import checks_para_csv, executar_suite
from .security import SecretStr, instalar_filtro_de_redacao
from .storage import Storage

log = logging.getLogger("bpc_pipeline")


def configurar_logging(nivel: int = logging.INFO) -> None:
    """Logging enxuto e com horário (funciona em notebook e terminal)."""
    raiz = logging.getLogger()
    if not any(getattr(h, "_bpc", False) for h in raiz.handlers):
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s", "%H:%M:%S"))
        h._bpc = True  # type: ignore[attr-defined]
        raiz.addHandler(h)
    raiz.setLevel(nivel)
    for ruidoso in ("py4j", "urllib3"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)


@dataclass
class Contexto:
    """Estado compartilhado entre as etapas de uma execução."""

    spark: SparkSession
    settings: Settings
    api_key: SecretStr
    origem_chave: str
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6])
    inicio: float = field(default_factory=time.monotonic)
    tempos: dict[str, float] = field(default_factory=dict)
    ibge: Optional[ResultadoIBGE] = None
    portal: Optional[ResultadoPortal] = None
    dfs: dict[str, DataFrame] = field(default_factory=dict)
    relatorio: Optional[dict[str, Any]] = None
    clientes: dict[str, ApiClient] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.settings.validar()
        instalar_filtro_de_redacao(self.api_key)  # qualquer log com a chave sai mascarado
        self.storage = Storage(self.spark, self.settings)
        s = self.settings
        # Cliente do Portal: limiter dia/noite + chave no HEADER (nunca na URL).
        self.clientes["portal"] = ApiClient(
            s.portal_base_url,
            RateLimiter(criar_provedor_de_taxa(s.rate_dia_por_min, s.rate_noite_por_min), "portal"),
            s, "portal_transparencia",
            headers={"chave-api-dados": self.api_key.get_secret_value()},
        )
        # Cliente do IBGE: API aberta, limiter próprio e independente.
        self.clientes["ibge"] = ApiClient(
            s.ibge_base_url,
            RateLimiter(lambda: float(s.rate_ibge_por_min), "ibge"),
            s, "ibge",
        )
        log.info("Execução %s | chave de API obtida via: %s | escopo UFs=%s meses=%s",
                 self.run_id, self.origem_chave, ",".join(s.ufs) if s.ufs else "BRASIL", s.meses or "auto")

    def _cronometrar(self, etapa: str, t0: float) -> None:
        self.tempos[etapa] = round(time.monotonic() - t0, 2)
        log.info("Etapa '%s' concluída em %.1f s", etapa, self.tempos[etapa])


# ---------------------------------------------------------------------------
# Etapas
# ---------------------------------------------------------------------------

def etapa_1_ibge(ctx: Contexto) -> ResultadoIBGE:
    t0 = time.monotonic()
    ctx.ibge = extrair_ibge(ctx.clientes["ibge"], ctx.storage, ctx.settings, ctx.run_id)
    ctx._cronometrar("1_ibge", t0)
    return ctx.ibge


def etapa_2_portal(ctx: Contexto) -> ResultadoPortal:
    assert ctx.ibge is not None, "Execute a etapa 1 (IBGE) antes: ela define a lista de municípios."
    t0 = time.monotonic()
    ctx.portal = extrair_bpc(ctx.spark, ctx.clientes["portal"], ctx.storage, ctx.settings, ctx.ibge.municipios, ctx.run_id)
    ctx._cronometrar("2_portal", t0)
    return ctx.portal


def etapa_3_silver(ctx: Contexto) -> dict[str, DataFrame]:
    assert ctx.ibge is not None
    t0 = time.monotonic()
    T.registrar_views_bronze(
        ctx.spark,
        caminho_bpc=f"{ctx.settings.bronze_path}/portal/bpc_por_municipio",
        caminho_mun=ctx.ibge.caminho_municipios,
        caminho_pop=ctx.ibge.caminho_populacao,
    )
    # Grava e RELÊ cada tabela (corta a linhagem; as próximas etapas leem do storage).
    ctx.dfs["silver_bpc"] = ctx.storage.gravar_tabela(T.silver_bpc(ctx.spark), "silver_bpc_municipio_tipo", ["ano", "mes"])
    ctx.dfs["silver_municipios"] = ctx.storage.gravar_tabela(T.silver_municipios(ctx.spark), "silver_municipios")
    ctx.dfs["silver_populacao"] = ctx.storage.gravar_tabela(T.silver_populacao(ctx.spark), "silver_populacao")
    for nome in ("silver_bpc", "silver_municipios", "silver_populacao"):
        ctx.dfs[nome].createOrReplaceTempView(nome)
    ctx._cronometrar("3_silver", t0)
    return ctx.dfs


def etapa_4_gold(ctx: Contexto) -> dict[str, DataFrame]:
    t0 = time.monotonic()
    ctx.dfs["gold"] = ctx.storage.gravar_tabela(T.gold_bpc_municipio_mes(ctx.spark), "gold_bpc_municipio_mes", ["ano", "mes"])
    ctx.dfs["gold"].createOrReplaceTempView("gold_bpc_municipio_mes")
    ctx.dfs["gold_uf"] = ctx.storage.gravar_tabela(T.gold_bpc_uf_mes(ctx.spark), "gold_bpc_uf_mes", ["ano", "mes"])
    ctx._cronometrar("4_gold", t0)
    return ctx.dfs


def etapa_5_qualidade(ctx: Contexto) -> dict[str, Any]:
    assert ctx.portal is not None
    t0 = time.monotonic()
    dq = executar_suite(
        ctx.dfs["silver_bpc"], ctx.dfs["silver_municipios"], ctx.dfs["silver_populacao"],
        ctx.dfs["gold"], ctx.dfs["gold_uf"],
        pares_escopo=ctx.portal.pares_no_escopo,
        pares_concluidos=ctx.portal.pares_concluidos_total,
        ano_referencia=ctx.settings.ano,
    )
    ctx._cronometrar("5_qualidade", t0)

    s = ctx.settings
    ctx.relatorio = {
        "run_id": ctx.run_id,
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duracao_total_s": round(time.monotonic() - ctx.inicio, 1),
        "tempo_por_etapa_s": ctx.tempos,
        "escopo": {
            "ano": s.ano,
            "ufs": list(s.ufs) if s.ufs else "BRASIL",
            "meses_processados": ctx.portal.meses_processados,
            "limite_municipios": s.limite_municipios,
            "ano_estimativa_populacional": ctx.ibge.ano_populacao if ctx.ibge else None,
        },
        "seguranca": {"origem_chave_api": ctx.origem_chave, "chave_em_log": False},
        "extracao": {
            "portal": {**ctx.portal.to_dict(), "http": ctx.clientes["portal"].metrics.to_dict()},
            "ibge": {"http": ctx.clientes["ibge"].metrics.to_dict()},
        },
        "data_quality": dq,
    }

    # Persistência do relatório: JSON completo + CSV dos checks (histórico + "latest" em output/).
    texto = json.dumps(ctx.relatorio, ensure_ascii=False, indent=2, default=str)
    csv_txt = checks_para_csv(dq["checks"])
    ctx.storage.escrever_texto(f"{s.quality_path}/dq_report_{ctx.run_id}.json", texto)
    ctx.storage.escrever_texto(f"{s.output_path}/dq_report.json", texto)
    ctx.storage.escrever_texto(f"{s.output_path}/dq_checks.csv", csv_txt)

    # No Databricks, mantém também um histórico consultável em SQL (tabela Delta, modo append).
    if s.usa_unity_catalog:
        linhas = [{**c, "run_id": ctx.run_id, "gerado_em_utc": ctx.relatorio["gerado_em_utc"],
                   "metrica": float(c["metrica"]) if c["metrica"] is not None else None} for c in dq["checks"]]
        (ctx.spark.createDataFrame(linhas, schema=(
            "check string, tabela string, coluna string, metrica double, limite string, status string, "
            "severidade string, detalhe string, run_id string, gerado_em_utc string"))
         .write.format("delta").mode("append").saveAsTable(s.nome_tabela("dq_historico")))

    r = dq["resumo"]
    log.info("Data Quality: %s | %d/%d checks aprovados | críticos reprovados: %d",
             r["status_geral"], r["aprovados"], r["total_checks"], r["reprovados_criticos"])
    return ctx.relatorio


def etapa_6_exportar(ctx: Contexto) -> list[str]:
    t0 = time.monotonic()
    caminhos = [
        ctx.storage.exportar_parquet(ctx.dfs["gold"], "gold_bpc_municipio_mes", ["ano", "mes"]),
        ctx.storage.exportar_parquet(ctx.dfs["gold_uf"], "gold_bpc_uf_mes", ["ano", "mes"]),
    ]
    ctx._cronometrar("6_exportar", t0)
    return caminhos


def validar_gate_de_qualidade(ctx: Contexto) -> None:
    """Falha a execução (ex.: Job do Databricks fica vermelho) se houver check CRÍTICO reprovado."""
    assert ctx.relatorio is not None
    if ctx.settings.falhar_em_check_critico and ctx.relatorio["data_quality"]["resumo"]["reprovados_criticos"] > 0:
        criticos = [c["check"] for c in ctx.relatorio["data_quality"]["checks"]
                    if c["status"] == "FAIL" and c["severidade"] == "CRITICA"]
        raise RuntimeError(f"Gate de Data Quality reprovado. Checks críticos: {criticos}")


def executar_tudo(ctx: Contexto) -> dict[str, Any]:
    """Pipeline ponta a ponta."""
    etapa_1_ibge(ctx)
    etapa_2_portal(ctx)
    etapa_3_silver(ctx)
    etapa_4_gold(ctx)
    etapa_5_qualidade(ctx)
    etapa_6_exportar(ctx)  # exporta mesmo com alertas: o relatório documenta o que reprovou
    validar_gate_de_qualidade(ctx)
    log.info("Pipeline concluído em %.1f min.", (time.monotonic() - ctx.inicio) / 60)
    return ctx.relatorio or {}
