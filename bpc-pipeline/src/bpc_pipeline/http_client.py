"""
http_client.py
--------------
Cliente HTTP resiliente e performático, compartilhado pelas duas APIs.

Componentes:
  * ``RateLimiter``   -> controle de taxa GLOBAL e thread-safe (espaçamento uniforme entre requisições).
                         Como o limite da CGU é por TOKEN, todas as threads compartilham o mesmo limiter.
                         Ajusta a taxa automaticamente pelo horário (madrugada = limite maior).
  * ``ApiClient``     -> GET com retentativas, backoff exponencial com jitter, respeito ao
                         header Retry-After, classificação de erros e métricas.
  * ``ExtractionMetrics`` -> contadores thread-safe que alimentam o relatório de auditoria.

Por que é rápido (e por que a versão antiga levava 3h+):
  * Requisições sequenciais desperdiçam o limite: com ~500 ms de latência, 1 thread faz ~120 req/min,
    mesmo com limite de 400. Aqui N threads escondem a latência e o limiter mantém o teto exato.
  * Conexões HTTP são reaproveitadas (keep-alive) por thread -> sem custo de TLS a cada chamada.
  * Erros não-transitórios (400/401/403) NÃO são retentados; erro de autenticação aborta tudo na hora.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any, Callable, Optional

import requests
from requests.adapters import HTTPAdapter

from .config import TZ_BRASIL, Settings

log = logging.getLogger("bpc_pipeline.http")

#: Status HTTP considerados transitórios (vale a pena tentar de novo).
STATUS_RETENTAVEIS = {429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# Exceções de domínio
# ---------------------------------------------------------------------------

class ApiAuthError(RuntimeError):
    """401/403: chave inválida/expirada/suspensa. Abortar imediatamente (retentar não resolve)."""


class RateLimitAbort(RuntimeError):
    """Excesso de HTTP 429: aborta para não arriscar a suspensão do token pela CGU."""


class ApiRequestError(RuntimeError):
    """Erro definitivo de uma requisição específica (após esgotar as retentativas)."""

    def __init__(self, mensagem: str, status: Optional[int] = None) -> None:
        super().__init__(mensagem)
        self.status = status


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------

@dataclass
class ExtractionMetrics:
    """Contadores de execução (thread-safe), exportados no relatório de Data Quality."""

    nome: str
    requisicoes: int = 0
    sucesso: int = 0
    retentativas: int = 0
    http_429: int = 0
    http_5xx: int = 0
    erros_rede: int = 0
    erros_definitivos: int = 0
    bytes_recebidos: int = 0
    espera_rate_limit_s: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def inc(self, **deltas: float) -> None:
        with self._lock:
            for k, v in deltas.items():
                setattr(self, k, getattr(self, k) + v)

    def to_dict(self) -> dict[str, Any]:
        with self._lock:  # asdict() tentaria copiar o Lock; montamos o dict manualmente
            d = {f.name: getattr(self, f.name) for f in fields(self) if f.name != "_lock"}
        d["espera_rate_limit_s"] = round(d["espera_rate_limit_s"], 2)
        return d


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

def criar_provedor_de_taxa(rate_dia: int, rate_noite: int) -> Callable[[], float]:
    """
    Retorna uma função que informa a taxa permitida AGORA (req/min) no fuso de Brasília.

    Janela noturna oficial: 00:00-05:59. Encerramos a janela às 05:54 de propósito: assim,
    a "rampa" de requisições noturnas não invade o minuto das 06:00, quando o limite cai.
    """

    def provedor() -> float:
        agora = datetime.now(TZ_BRASIL)
        minutos_do_dia = agora.hour * 60 + agora.minute
        return float(rate_noite if minutos_do_dia < (5 * 60 + 55) else rate_dia)

    return provedor


class RateLimiter:
    """
    Limiter por espaçamento (variante do algoritmo GCRA/leaky bucket), thread-safe.

    Cada chamada a ``acquire()`` reserva o próximo "slot" de tempo: slots consecutivos ficam
    separados por 60/taxa segundos. Isso garante que, em QUALQUER janela de 60 s, saem no máximo
    ``taxa`` requisições, sem rajadas (bursts) que poderiam estourar a janela da CGU.
    """

    def __init__(self, provedor_de_taxa: Callable[[], float], nome: str = "limiter") -> None:
        self._provedor = provedor_de_taxa
        self._nome = nome
        self._lock = threading.Lock()
        self._proximo_slot = time.monotonic()
        self._fator = 1.0  # reduzido automaticamente quando recebemos HTTP 429
        self._sucessos_seguidos = 0

    @property
    def taxa_atual(self) -> float:
        return self._provedor() * self._fator

    def acquire(self) -> float:
        """Bloqueia até o próximo slot livre. Retorna quantos segundos esperou."""
        with self._lock:
            intervalo = 60.0 / max(self.taxa_atual, 0.1)
            agora = time.monotonic()
            slot = max(self._proximo_slot, agora)
            self._proximo_slot = slot + intervalo
        espera = slot - agora
        if espera > 0:
            time.sleep(espera)
        return max(espera, 0.0)

    def penalizar(self, segundos: float) -> None:
        """Empurra o próximo slot para frente: TODAS as threads pausam (resposta coordenada a 429)."""
        with self._lock:
            self._proximo_slot = max(self._proximo_slot, time.monotonic() + segundos)

    def desacelerar(self, fator: float = 0.5, piso: float = 0.25) -> None:
        """Redução MULTIPLICATIVA da taxa (AIMD) após 429: protege o token de suspensão."""
        with self._lock:
            self._fator = max(piso, self._fator * fator)
            self._sucessos_seguidos = 0
        log.warning("[%s] HTTP 429 recebido: taxa reduzida para %.0f req/min.", self._nome, self.taxa_atual)

    def registrar_sucesso(self, a_cada: int = 100, passo: float = 0.1) -> None:
        """Recuperação ADITIVA (AIMD): a cada ``a_cada`` sucessos seguidos, +10% até a taxa configurada."""
        with self._lock:
            if self._fator >= 1.0:
                return
            self._sucessos_seguidos += 1
            if self._sucessos_seguidos >= a_cada:
                self._fator = min(1.0, self._fator + passo)
                self._sucessos_seguidos = 0


# ---------------------------------------------------------------------------
# Cliente HTTP
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RespostaApi:
    status: int
    texto: str
    dados: Any


class ApiClient:
    """
    Cliente GET com sessão por thread, rate limiting, retentativas e métricas.

    ``headers`` pode conter a chave de API; ela vive apenas em memória, dentro das sessões HTTP,
    e nunca é registrada em log (o cliente não loga headers).
    """

    def __init__(
        self,
        base_url: str,
        limiter: RateLimiter,
        settings: Settings,
        nome: str,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.limiter = limiter
        self.settings = settings
        self.nome = nome
        self._headers = {"Accept": "application/json", "User-Agent": "bpc-pipeline/1.0 (desafio-ft03)", **(headers or {})}
        self._local = threading.local()  # uma requests.Session por thread (thread-safety + keep-alive)
        self.metrics = ExtractionMetrics(nome=nome)

    # ------------------------------------------------------------- sessão
    def _sessao(self) -> requests.Session:
        sessao = getattr(self._local, "sessao", None)
        if sessao is None:
            sessao = requests.Session()
            # Pool pequeno por thread; max_retries=0 porque NÓS controlamos as retentativas.
            adaptador = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
            sessao.mount("https://", adaptador)
            sessao.mount("http://", adaptador)
            sessao.headers.update(self._headers)
            self._local.sessao = sessao
        return sessao

    # ------------------------------------------------------------ backoff
    def _espera_backoff(self, tentativa: int, retry_after: Optional[str]) -> float:
        """Usa Retry-After quando o servidor informa; senão, backoff exponencial com jitter."""
        if retry_after:
            try:
                return min(float(retry_after), self.settings.backoff_max_s)
            except ValueError:
                pass
        base = self.settings.backoff_base_s * (2 ** tentativa)
        return min(base + random.uniform(0, base / 2), self.settings.backoff_max_s)

    # ---------------------------------------------------------------- GET
    def get(self, caminho: str, params: Optional[dict[str, Any]] = None) -> RespostaApi:
        """
        Executa um GET resiliente.

        Fluxo por tentativa:
          1. Aguarda um slot no rate limiter global.
          2. Faz a requisição com timeout de conexão e de leitura.
          3. 2xx -> decodifica JSON e retorna.
             401/403 -> ApiAuthError (aborta o pipeline).
             404 -> trata como "sem dados" (lista vazia).
             429 -> pausa global + redução de taxa + nova tentativa.
             5xx / erro de rede / JSON inválido -> backoff exponencial + nova tentativa.
             outros 4xx -> ApiRequestError (definitivo, sem retentar).
        """
        url = f"{self.base_url}/{caminho.lstrip('/')}"
        timeout = (self.settings.timeout_connect_s, self.settings.timeout_read_s)
        ultimo_erro: str = ""
        ultimo_status: Optional[int] = None

        for tentativa in range(self.settings.max_retries + 1):
            if tentativa > 0:
                self.metrics.inc(retentativas=1)

            esperou = self.limiter.acquire()
            self.metrics.inc(requisicoes=1, espera_rate_limit_s=esperou)

            try:
                resp = self._sessao().get(url, params=params, timeout=timeout)
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
                self.metrics.inc(erros_rede=1)
                ultimo_erro, ultimo_status = f"{type(exc).__name__}", None
                time.sleep(self._espera_backoff(tentativa, None))
                continue

            status = resp.status_code
            ultimo_status = status

            if 200 <= status < 300:
                texto = resp.text or ""
                try:
                    dados = json.loads(texto) if texto.strip() else []
                except json.JSONDecodeError:
                    # Página HTML de manutenção com status 200 acontece em APIs públicas: retentar.
                    ultimo_erro = "JSON inválido na resposta"
                    time.sleep(self._espera_backoff(tentativa, None))
                    continue
                self.metrics.inc(sucesso=1, bytes_recebidos=len(resp.content))
                self.limiter.registrar_sucesso()
                return RespostaApi(status=status, texto=texto, dados=dados)

            if status in (401, 403):
                self.metrics.inc(erros_definitivos=1)
                raise ApiAuthError(
                    f"[{self.nome}] HTTP {status}: chave de API inválida, expirada ou suspensa. Pipeline abortado."
                )

            if status == 404:
                self.metrics.inc(sucesso=1)
                return RespostaApi(status=status, texto="[]", dados=[])

            if status == 429:
                self.metrics.inc(http_429=1)
                if self.metrics.http_429 > self.settings.max_http_429_antes_de_abortar:
                    raise RateLimitAbort(
                        f"[{self.nome}] {self.metrics.http_429} respostas HTTP 429. Execução interrompida para "
                        "evitar a suspensão do token. Reduza rate_dia/rate_noite e rode de novo (carga incremental)."
                    )
                # Pausa coordenada: sem Retry-After, espera a janela de 1 minuto "esvaziar".
                pausa = self._espera_backoff(tentativa, resp.headers.get("Retry-After")) if resp.headers.get(
                    "Retry-After") else 60.0
                self.limiter.penalizar(pausa)
                self.limiter.desacelerar()
                ultimo_erro = "HTTP 429"
                continue

            if status in STATUS_RETENTAVEIS:
                self.metrics.inc(http_5xx=1)
                ultimo_erro = f"HTTP {status}"
                time.sleep(self._espera_backoff(tentativa, resp.headers.get("Retry-After")))
                continue

            # Demais 4xx: erro do cliente (parâmetro inválido etc.). Retentar não muda o resultado.
            self.metrics.inc(erros_definitivos=1)
            raise ApiRequestError(f"[{self.nome}] HTTP {status} em {caminho} params={params}", status=status)

        self.metrics.inc(erros_definitivos=1)
        raise ApiRequestError(
            f"[{self.nome}] Falha após {self.settings.max_retries + 1} tentativas em {caminho} "
            f"params={params}: {ultimo_erro}",
            status=ultimo_status,
        )
