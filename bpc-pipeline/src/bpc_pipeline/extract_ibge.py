"""
extract_ibge.py
---------------
Extração da API pública do IBGE (domínio: demografia/territorial). Não exige chave.

Apenas 3 requisições no total (por isso é instantâneo):
  1. /v1/localidades/municipios                -> todos os municípios com hierarquia ANINHADA
                                                  (microrregião > mesorregião > UF > região)
  2. /v3/agregados/6579/periodos               -> anos disponíveis da estimativa populacional
  3. /v3/agregados/6579/periodos/{ano}/variaveis/9324?localidades=N6[all]
                                                -> população estimada de todos os municípios
                                                  (JSON com arrays aninhados em 3 níveis + mapa)

As respostas são gravadas BRUTAS na Bronze (texto original). O desaninhamento é feito
depois, em Spark SQL (EXPLODE), na camada Silver - conforme exige o desafio.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from .config import SIDRA_TABELA_POPULACAO, SIDRA_VARIAVEL_POPULACAO, Settings
from .http_client import ApiClient
from .storage import Storage

log = logging.getLogger("bpc_pipeline.ibge")


@dataclass(frozen=True)
class ResultadoIBGE:
    """Resumo da extração do IBGE usado nas etapas seguintes."""

    #: (codigo_ibge, uf_sigla) de TODOS os municípios -> define o escopo de consulta ao Portal.
    municipios: list[tuple[str, str]]
    ano_populacao: str
    caminho_municipios: str
    caminho_populacao: str


def _uf_do_municipio(m: dict[str, Any]) -> Optional[str]:
    """
    Extrai a sigla da UF com fallback (tratamento de nulos já na extração):
    alguns municípios novos vêm com ``microrregiao = null``; nesses casos usa a
    hierarquia nova (região imediata > região intermediária > UF).
    """
    try:
        return m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
    except (KeyError, TypeError):
        try:
            return m["regiao-imediata"]["regiao-intermediaria"]["UF"]["sigla"]
        except (KeyError, TypeError):
            return None


def escolher_periodo(periodos: list[str], ano_desejado: int) -> str:
    """
    Usa a estimativa do ano de referência (2026) se já publicada; caso contrário, a mais
    recente disponível até ele. A escolha é registrada no relatório de qualidade.
    """
    validos = sorted(int(p) for p in periodos if str(p).isdigit() and int(p) <= ano_desejado)
    if not validos:
        raise RuntimeError("IBGE não retornou períodos válidos para a estimativa populacional.")
    escolhido = ano_desejado if ano_desejado in validos else validos[-1]
    if escolhido != ano_desejado:
        log.warning("Estimativa populacional %s ainda não publicada; usando %s.", ano_desejado, escolhido)
    return str(escolhido)


def extrair_ibge(cliente: ApiClient, storage: Storage, settings: Settings, run_id: str) -> ResultadoIBGE:
    """Executa as 3 chamadas ao IBGE e grava as respostas brutas na Bronze."""

    # 1) Municípios (hierarquia aninhada) ------------------------------------------------
    resp_mun = cliente.get("v1/localidades/municipios")
    caminho_mun = f"{settings.bronze_path}/ibge/municipios/run_id={run_id}/municipios.json"
    storage.escrever_texto(caminho_mun, resp_mun.texto)

    municipios = [
        (str(m["id"]).zfill(7), uf)
        for m in resp_mun.dados
        if (uf := _uf_do_municipio(m)) is not None
    ]
    log.info("IBGE: %d municípios carregados.", len(municipios))

    # 2) Períodos disponíveis da estimativa populacional ---------------------------------
    resp_per = cliente.get(f"v3/agregados/{SIDRA_TABELA_POPULACAO}/periodos")
    periodos = [str(p.get("id")) for p in resp_per.dados]
    ano_pop = escolher_periodo(periodos, settings.ano)

    # 3) População estimada de todos os municípios (N6 = nível município) ----------------
    resp_pop = cliente.get(
        f"v3/agregados/{SIDRA_TABELA_POPULACAO}/periodos/{ano_pop}/variaveis/{SIDRA_VARIAVEL_POPULACAO}",
        params={"localidades": "N6[all]"},
    )
    caminho_pop = f"{settings.bronze_path}/ibge/populacao/run_id={run_id}/populacao_{ano_pop}.json"
    storage.escrever_texto(caminho_pop, resp_pop.texto)
    log.info("IBGE: população estimada (%s) gravada na Bronze (%.1f KB).", ano_pop, len(resp_pop.texto) / 1024)

    return ResultadoIBGE(
        municipios=municipios,
        ano_populacao=ano_pop,
        caminho_municipios=caminho_mun,
        caminho_populacao=caminho_pop,
    )
