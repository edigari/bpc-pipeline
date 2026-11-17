"""
extract_portal.py
-----------------
Extração do BPC por município (Portal da Transparência / CGU) - parte que define o tempo total.

Endpoint: GET /api-de-dados/bpc-por-municipio?mesAno=AAAAMM&codigoIbge=NNNNNNN&pagina=1
Header  : chave-api-dados: <token>

A API só aceita UM município e UM mês por chamada. Para Norte + Nordeste (2.244 municípios)
x ~10 meses de 2026, são ~22 mil chamadas. Com teto de 400 req/min, o mínimo físico é ~1 h
(ou ~36 min de madrugada, com 700 req/min). As técnicas abaixo garantem que o pipeline
opere colado nesse teto e que NUNCA repita trabalho já feito:

  1. DESCOBERTA DE MESES: antes da carga, consulta 1 município "sonda" por mês (≤12 chamadas).
     Meses ainda não publicados são descartados -> evita milhares de chamadas que voltariam vazias.
  2. CARGA INCREMENTAL: lê a Bronze e remove do plano os pares (município, mês) já obtidos.
     Re-executar após uma falha ou no mês seguinte custa só o que falta.
  3. PARALELISMO NO TETO: ThreadPoolExecutor + RateLimiter global (http_client.py).
  4. CHECKPOINT: grava a Bronze em lotes (``flush_a_cada``). Se cair no meio, nada se perde.
  5. PAGINAÇÃO SOB DEMANDA: só busca a página 2 se a página 1 vier cheia (na prática, nunca).
  6. FALHA RÁPIDA: erro de autenticação cancela todas as tarefas pendentes imediatamente.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .config import CODIGO_IBGE_SONDA, Settings, agora_brasil
from .http_client import ApiAuthError, ApiClient, ApiRequestError, RateLimitAbort
from .storage import Storage

log = logging.getLogger("bpc_pipeline.portal")

ENDPOINT_BPC = "bpc-por-municipio"

#: Schema EXPLÍCITO da Bronze: leitura sem inferência (mais rápida e à prova de drift).
SCHEMA_BRONZE_BPC = StructType(
    [
        StructField("codigo_ibge", StringType(), False),
        StructField("ano_mes", IntegerType(), False),
        StructField("status", StringType(), False),      # ok | vazio | erro
        StructField("http_status", IntegerType(), True),
        StructField("n_registros", IntegerType(), True),
        StructField("payload", StringType(), True),      # array JSON bruto devolvido pela API
        StructField("erro", StringType(), True),
        StructField("ingested_at", StringType(), False),
        StructField("run_id", StringType(), False),
    ]
)

Par = tuple[str, int]  # (codigo_ibge, ano_mes)


@dataclass
class ResultadoPortal:
    """Métricas da carga do Portal (vão para o relatório de auditoria)."""

    meses_processados: list[int]
    pares_no_escopo: int = 0
    pares_ja_existentes: int = 0
    pares_pendentes: int = 0
    pares_ok: int = 0
    pares_vazios: int = 0
    pares_com_erro: int = 0
    arquivos_bronze: list[str] = field(default_factory=list)
    duracao_s: float = 0.0
    abortado_por: Optional[str] = None

    @property
    def pares_concluidos_total(self) -> int:
        return self.pares_ja_existentes + self.pares_ok + self.pares_vazios

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "arquivos_bronze"}
        d["qtd_arquivos_bronze"] = len(self.arquivos_bronze)
        d["pares_concluidos_total"] = self.pares_concluidos_total
        d["throughput_req_min"] = round(
            (self.pares_ok + self.pares_vazios + self.pares_com_erro) / (self.duracao_s / 60), 1
        ) if self.duracao_s > 0 else None
        return d


# ---------------------------------------------------------------------------
# 1. Descoberta dos meses publicados
# ---------------------------------------------------------------------------

def descobrir_meses_disponiveis(cliente: ApiClient, settings: Settings) -> list[int]:
    """
    Retorna os ``ano_mes`` (AAAAMM) de 2026 que já têm dados publicados.

    Consulta apenas o município sonda, do mês mais recente para o mais antigo; o primeiro mês
    com dados indica o fim da série publicada (a CGU publica em ordem cronológica).
    """
    hoje = agora_brasil()
    ultimo_mes_possivel = hoje.month if settings.ano == hoje.year else 12
    candidatos = [m for m in (settings.meses or range(1, 13)) if m <= ultimo_mes_possivel]

    if settings.meses:  # meses informados explicitamente: não precisa sondar
        return [settings.ano * 100 + m for m in sorted(candidatos)]

    ultimo_publicado: Optional[int] = None
    for mes in sorted(candidatos, reverse=True):
        resp = cliente.get(ENDPOINT_BPC, {"mesAno": f"{settings.ano}{mes:02d}", "codigoIbge": CODIGO_IBGE_SONDA, "pagina": 1})
        if resp.dados:
            ultimo_publicado = mes
            break
    if ultimo_publicado is None:
        raise RuntimeError(f"Nenhum mês de {settings.ano} com dados de BPC publicados no Portal da Transparência.")

    meses = [settings.ano * 100 + m for m in range(1, ultimo_publicado + 1)]
    log.info("Meses publicados de %s: %s", settings.ano, meses)
    return meses


# ---------------------------------------------------------------------------
# 2. Carga incremental: o que já existe na Bronze?
# ---------------------------------------------------------------------------

def carregar_pares_existentes(spark: SparkSession, storage: Storage, caminho_bronze: str) -> set[Par]:
    """Lê SÓ duas colunas da Bronze (projeção) e retorna os pares já concluídos (ok ou vazio)."""
    if not storage.existe(caminho_bronze):
        return set()
    linhas = (
        spark.read.schema(SCHEMA_BRONZE_BPC)
        .json(caminho_bronze)
        .where("status IN ('ok', 'vazio')")
        .select("codigo_ibge", "ano_mes")
        .distinct()
        .collect()  # no máximo dezenas de milhares de tuplas pequenas: cabe tranquilamente no driver
    )
    return {(r["codigo_ibge"], int(r["ano_mes"])) for r in linhas}


# ---------------------------------------------------------------------------
# 3. Tarefa unitária (executada em paralelo)
# ---------------------------------------------------------------------------

def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def buscar_bpc_municipio_mes(cliente: ApiClient, par: Par, settings: Settings, run_id: str) -> dict[str, Any]:
    """Busca todas as páginas de um (município, mês) e devolve UM registro Bronze."""
    codigo, ano_mes = par
    registros: list[Any] = []
    pagina = 1
    status_http: Optional[int] = None
    try:
        while True:
            resp = cliente.get(ENDPOINT_BPC, {"mesAno": str(ano_mes), "codigoIbge": codigo, "pagina": pagina})
            status_http = resp.status
            lote = resp.dados if isinstance(resp.dados, list) else [resp.dados]
            registros.extend(lote)
            # Paginação sob demanda: página incompleta = última página.
            if len(lote) < settings.tamanho_pagina_portal:
                break
            pagina += 1
    except (ApiAuthError, RateLimitAbort):
        raise  # erros fatais sobem e abortam o pipeline inteiro
    except ApiRequestError as exc:
        # Erro definitivo só deste par: registra e segue (será retentado na próxima execução).
        return {
            "codigo_ibge": codigo, "ano_mes": ano_mes, "status": "erro", "http_status": exc.status,
            "n_registros": 0, "payload": None, "erro": str(exc)[:500], "ingested_at": _agora_iso(), "run_id": run_id,
        }

    return {
        "codigo_ibge": codigo,
        "ano_mes": ano_mes,
        "status": "ok" if registros else "vazio",
        "http_status": status_http,
        "n_registros": len(registros),
        "payload": json.dumps(registros, ensure_ascii=False),
        "erro": None,
        "ingested_at": _agora_iso(),
        "run_id": run_id,
    }


# ---------------------------------------------------------------------------
# 4. Orquestração paralela com checkpoint
# ---------------------------------------------------------------------------

def _gravar_lote(storage: Storage, caminho_bronze: str, run_id: str, seq: int, lote: list[dict[str, Any]]) -> str:
    """Grava um lote em JSON Lines (1 linha = 1 par município/mês)."""
    caminho = f"{caminho_bronze}/run_id={run_id}/lote_{seq:05d}.jsonl"
    storage.escrever_texto(caminho, "\n".join(json.dumps(r, ensure_ascii=False) for r in lote) + "\n")
    return caminho


def extrair_bpc(
    spark: SparkSession,
    cliente: ApiClient,
    storage: Storage,
    settings: Settings,
    municipios: Iterable[tuple[str, str]],
    run_id: str,
) -> ResultadoPortal:
    """Planeja (escopo - já existentes) e executa a carga paralela do BPC."""
    inicio = time.monotonic()
    caminho_bronze = f"{settings.bronze_path}/portal/bpc_por_municipio"

    # --- Escopo de municípios (filtro por UF + limite opcional para testes) ---------------
    codigos = sorted(c for c, uf in municipios if settings.ufs is None or uf in settings.ufs)
    if settings.limite_municipios > 0:
        codigos = codigos[: settings.limite_municipios]

    # --- Escopo de meses (descoberta automática) ------------------------------------------
    meses = descobrir_meses_disponiveis(cliente, settings)
    resultado = ResultadoPortal(meses_processados=meses)

    # --- Plano incremental -----------------------------------------------------------------
    escopo: list[Par] = [(c, am) for am in meses for c in codigos]
    existentes = carregar_pares_existentes(spark, storage, caminho_bronze)
    pendentes = [p for p in escopo if p not in existentes]
    resultado.pares_no_escopo = len(escopo)
    resultado.pares_ja_existentes = len(escopo) - len(pendentes)
    resultado.pares_pendentes = len(pendentes)

    taxa = cliente.limiter.taxa_atual
    log.info(
        "Plano BPC: %d municípios x %d meses = %d pares | já na Bronze: %d | pendentes: %d | "
        "taxa atual: %.0f req/min | tempo estimado: ~%.1f min",
        len(codigos), len(meses), len(escopo), resultado.pares_ja_existentes, len(pendentes),
        taxa, len(pendentes) / taxa if taxa else 0,
    )
    if not pendentes:
        resultado.duracao_s = time.monotonic() - inicio
        return resultado

    # --- Execução paralela -----------------------------------------------------------------
    buffer: list[dict[str, Any]] = []
    seq = 0
    concluidos = 0
    executor = ThreadPoolExecutor(max_workers=settings.max_workers, thread_name_prefix="bpc")
    futuros: list[Future] = [executor.submit(buscar_bpc_municipio_mes, cliente, p, settings, run_id) for p in pendentes]

    try:
        for fut in as_completed(futuros):
            registro = fut.result()  # re-lança ApiAuthError / RateLimitAbort aqui
            buffer.append(registro)
            concluidos += 1
            status = registro["status"]
            if status == "ok":
                resultado.pares_ok += 1
            elif status == "vazio":
                resultado.pares_vazios += 1
            else:
                resultado.pares_com_erro += 1

            # Checkpoint periódico na Bronze
            if len(buffer) >= settings.flush_a_cada:
                seq += 1
                resultado.arquivos_bronze.append(_gravar_lote(storage, caminho_bronze, run_id, seq, buffer))
                buffer = []

            # Log de progresso a cada 5% (sem poluir a saída do notebook)
            if concluidos % max(1, len(pendentes) // 20) == 0:
                decorrido = time.monotonic() - inicio
                ritmo = concluidos / decorrido * 60
                log.info(
                    "Progresso: %d/%d (%.0f%%) | %.0f pares/min | ETA ~%.1f min",
                    concluidos, len(pendentes), 100 * concluidos / len(pendentes), ritmo,
                    (len(pendentes) - concluidos) / ritmo if ritmo else 0,
                )
    except (ApiAuthError, RateLimitAbort) as exc:
        resultado.abortado_por = f"{type(exc).__name__}: {exc}"
        log.error("Carga abortada: %s", exc)
        raise
    finally:
        # Cancela o que ainda não começou e garante que o que já chegou esteja salvo.
        executor.shutdown(wait=True, cancel_futures=True)
        if buffer:
            seq += 1
            resultado.arquivos_bronze.append(_gravar_lote(storage, caminho_bronze, run_id, seq, buffer))
        resultado.duracao_s = time.monotonic() - inicio

    log.info(
        "BPC concluído em %.1f min: ok=%d vazio=%d erro=%d",
        resultado.duracao_s / 60, resultado.pares_ok, resultado.pares_vazios, resultado.pares_com_erro,
    )
    return resultado
