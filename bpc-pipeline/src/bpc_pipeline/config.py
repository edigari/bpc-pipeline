"""
config.py
---------
Centraliza TODOS os parâmetros da execução em um único objeto imutável (``Settings``).

Por que isso importa para performance e reprodutibilidade:
  * O escopo (UFs, meses, limite de municípios) define quantas requisições serão feitas.
    Como a API do Portal da Transparência é limitada por minuto, o escopo é o principal
    fator de tempo de execução. Deixar isso explícito evita execuções acidentais de horas.
  * Os limites de taxa ficam parametrizados (dia/noite) e com margem de segurança,
    porque ultrapassar o limite da CGU SUSPENDE o token.

Nenhum segredo é armazenado aqui. A chave de API é tratada exclusivamente em ``security.py``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Constantes de domínio
# ---------------------------------------------------------------------------

#: Fuso oficial usado pela CGU para definir as janelas de limite de requisições.
TZ_BRASIL = ZoneInfo("America/Sao_Paulo")

#: Ano de referência exigido no desafio.
ANO_REFERENCIA = 2026

#: Siglas válidas de UF (usadas para validar o parâmetro de escopo).
UFS_VALIDAS: frozenset[str] = frozenset(
    {
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
        "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
    }
)

#: Regiões (atalhos aceitos no parâmetro de UFs). Escopo padrão do projeto: Norte + Nordeste.
REGIOES: dict[str, tuple[str, ...]] = {
    "NORTE": ("AC", "AM", "AP", "PA", "RO", "RR", "TO"),                     # 450 municípios
    "NORDESTE": ("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"),      # 1.794 municípios
    "CENTRO-OESTE": ("DF", "GO", "MS", "MT"),
    "SUDESTE": ("ES", "MG", "RJ", "SP"),
    "SUL": ("PR", "RS", "SC"),
}
UFS_NORTE_NORDESTE: tuple[str, ...] = tuple(sorted(REGIOES["NORTE"] + REGIOES["NORDESTE"]))

#: Município "sonda" usado para descobrir quais meses de 2026 já têm dados publicados.
#: Salvador/BA (dentro do escopo N/NE) sempre tem BPC; sem dado no mês = mês ainda não publicado.
CODIGO_IBGE_SONDA = "2927408"

#: Tabela SIDRA 6579 = "População residente estimada"; variável 9324 = população estimada.
SIDRA_TABELA_POPULACAO = "6579"
SIDRA_VARIAVEL_POPULACAO = "9324"


def _env(nome: str, padrao: str) -> str:
    """Lê uma variável de ambiente NÃO sensível com valor padrão."""
    return os.getenv(nome, padrao)


@dataclass(frozen=True)
class Settings:
    """Parâmetros da execução do pipeline (imutável: evita alteração acidental no meio da execução)."""

    # ------------------------------------------------------------------ escopo
    ano: int = ANO_REFERENCIA
    #: Lista de UFs a processar. Padrão: Norte + Nordeste (~2.244 municípios). ``None`` = Brasil inteiro.
    ufs: Optional[tuple[str, ...]] = UFS_NORTE_NORDESTE
    #: Meses a processar (1..12). ``None`` = descobrir automaticamente os meses publicados.
    meses: Optional[tuple[int, ...]] = None
    #: Limita a quantidade de municípios (0 = sem limite). Útil para um "teste de fumaça" rápido.
    limite_municipios: int = 0

    # ------------------------------------------------------------- endpoints
    portal_base_url: str = field(
        default_factory=lambda: _env("PORTAL_BASE_URL", "https://api.portaldatransparencia.gov.br/api-de-dados")
    )
    ibge_base_url: str = field(default_factory=lambda: _env("IBGE_BASE_URL", "https://servicodados.ibge.gov.br/api"))

    # ------------------------------------------------- limites e resiliência
    #: Limite oficial: 400 req/min (06:00-23:59). Usamos 90% como margem de segurança.
    rate_dia_por_min: int = 360
    #: Limite oficial: 700 req/min (00:00-05:59). Usamos 90% como margem de segurança.
    rate_noite_por_min: int = 630
    #: Limite "educado" para o IBGE (não há limite publicado, mas são poucas chamadas).
    rate_ibge_por_min: int = 120
    #: Threads simultâneas. O rate limiter é quem garante o teto; as threads só escondem a latência.
    max_workers: int = 12
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    timeout_connect_s: float = 5.0
    timeout_read_s: float = 30.0
    #: Se a API devolver 429 mais vezes que isso, a execução é abortada para PROTEGER o token.
    max_http_429_antes_de_abortar: int = 25
    #: Tamanho de página "esperado" pela API; só busca a página seguinte se vier página cheia.
    tamanho_pagina_portal: int = 15

    # --------------------------------------------------------------- storage
    #: Raiz de armazenamento: Volume do Unity Catalog no Databricks ou ./data localmente.
    base_path: str = "./data"
    #: Pasta da camada final em Parquet (entregável ``output/``).
    output_path: str = "./output"
    #: Catálogo/schema do Unity Catalog. Se ``None`` (modo local) as tabelas viram pastas Parquet.
    catalog: Optional[str] = None
    schema: Optional[str] = None
    #: Quantos registros acumular antes de gravar um arquivo na Bronze (checkpoint contra falhas).
    flush_a_cada: int = 1000

    # ---------------------------------------------------------- data quality
    falhar_em_check_critico: bool = True

    # ---------------------------------------------------------------- helpers
    @property
    def bronze_path(self) -> str:
        return f"{self.base_path.rstrip('/')}/bronze"

    @property
    def quality_path(self) -> str:
        return f"{self.base_path.rstrip('/')}/quality"

    @property
    def usa_unity_catalog(self) -> bool:
        return bool(self.catalog and self.schema)

    def nome_tabela(self, nome: str) -> str:
        """Nome totalmente qualificado (catalogo.schema.tabela) no Unity Catalog."""
        return f"{self.catalog}.{self.schema}.{nome}"

    def com(self, **alteracoes: object) -> "Settings":
        """Retorna uma cópia com alterações (o objeto original permanece imutável)."""
        return replace(self, **alteracoes)  # type: ignore[arg-type]

    def validar(self) -> "Settings":
        """Valida o escopo e os limites ANTES de qualquer requisição (falha rápida)."""
        if self.ufs is not None:
            invalidas = set(self.ufs) - UFS_VALIDAS
            if invalidas:
                raise ValueError(f"UF(s) inválida(s): {sorted(invalidas)}")
        if self.meses is not None and any(m < 1 or m > 12 for m in self.meses):
            raise ValueError(f"Mês fora do intervalo 1..12: {self.meses}")
        if not (1 <= self.rate_dia_por_min <= 400):
            raise ValueError("rate_dia_por_min deve estar entre 1 e 400 (limite oficial da CGU).")
        if not (1 <= self.rate_noite_por_min <= 700):
            raise ValueError("rate_noite_por_min deve estar entre 1 e 700 (limite oficial da CGU).")
        if self.max_workers < 1:
            raise ValueError("max_workers deve ser >= 1")
        return self


# ---------------------------------------------------------------------------
# Parsers de parâmetros textuais (widgets do Databricks / argumentos de CLI)
# ---------------------------------------------------------------------------

def parse_ufs(texto: str | None) -> Optional[tuple[str, ...]]:
    """
    'PE, SE' -> ('PE','SE');  'NORTE,NORDESTE' -> as 16 UFs dessas regiões;
    'ALL' / 'TODAS' -> None (Brasil inteiro);  '' -> padrão do projeto (Norte + Nordeste).
    """
    if texto is None or not texto.strip():
        return UFS_NORTE_NORDESTE
    t = texto.strip().upper()
    if t in {"ALL", "TODAS", "BR", "BRASIL"}:
        return None
    ufs: set[str] = set()
    for parte in (p.strip() for p in t.split(",") if p.strip()):
        ufs.update(REGIOES.get(parte, (parte,)))  # expande atalho de região ou mantém a UF
    return tuple(sorted(ufs))


def parse_meses(texto: str | None) -> Optional[tuple[int, ...]]:
    """'1,2,3' ou '1-6' -> (1,2,3...);  'ALL' / '' -> None (descoberta automática)."""
    if texto is None:
        return None
    t = texto.strip().upper()
    if t in {"", "ALL", "TODOS", "AUTO"}:
        return None
    meses: set[int] = set()
    for parte in t.split(","):
        parte = parte.strip()
        if "-" in parte:
            ini, fim = (int(x) for x in parte.split("-", 1))
            meses.update(range(ini, fim + 1))
        elif parte:
            meses.add(int(parte))
    return tuple(sorted(meses))


def agora_brasil() -> datetime:
    """Data/hora atual no fuso de Brasília (referência das janelas de limite da CGU)."""
    return datetime.now(TZ_BRASIL)
