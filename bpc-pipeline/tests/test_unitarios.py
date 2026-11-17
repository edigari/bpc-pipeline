"""Testes unitários rápidos (sem Spark e sem rede)."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from bpc_pipeline.config import Settings, UFS_NORTE_NORDESTE, parse_meses, parse_ufs
from bpc_pipeline.extract_ibge import escolher_periodo
from bpc_pipeline.http_client import RateLimiter
from bpc_pipeline.security import MissingApiKeyError, RedactingFilter, SecretStr, resolver_api_key


def test_secretstr_nunca_exibe_valor() -> None:
    s = SecretStr("minha-chave-super-secreta")
    assert "minha-chave" not in str(s)
    assert "minha-chave" not in repr(s)
    assert "minha-chave" not in f"{s}"
    assert s.get_secret_value() == "minha-chave-super-secreta"


def test_filtro_redige_chave_em_logs(caplog: pytest.LogCaptureFixture) -> None:
    s = SecretStr("abc123xyz")
    logger = logging.getLogger("teste.redacao")
    logger.addFilter(RedactingFilter(s))
    with caplog.at_level(logging.INFO, logger="teste.redacao"):
        logger.info("header enviado: %s", "abc123xyz")
    assert "abc123xyz" not in caplog.text
    assert "**********" in caplog.text


def test_chave_ausente_gera_erro_sem_vazar(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.delenv("PORTAL_TRANSPARENCIA_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("bpc_pipeline.security._carregar_dotenv_se_existir", lambda: None)
    with pytest.raises(MissingApiKeyError):
        resolver_api_key()


def test_chave_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_TRANSPARENCIA_API_KEY", "  valor  ")
    chave, origem = resolver_api_key()
    assert chave.get_secret_value() == "valor" and origem == "env"


def test_rate_limiter_respeita_teto_com_varias_threads() -> None:
    limiter = RateLimiter(lambda: 600.0)  # 600/min = 1 a cada 100 ms
    marcas: list[float] = []
    lock = threading.Lock()

    def trabalho() -> None:
        for _ in range(5):
            limiter.acquire()
            with lock:
                marcas.append(time.monotonic())

    threads = [threading.Thread(target=trabalho) for _ in range(4)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 20 requisições a 10/s: no mínimo ~1,9 s, mesmo com 4 threads disputando.
    assert time.monotonic() - t0 >= 1.8
    marcas.sort()
    assert min(b - a for a, b in zip(marcas, marcas[1:])) >= 0.09


def test_parse_parametros() -> None:
    assert parse_ufs("NORTE,NORDESTE") == UFS_NORTE_NORDESTE
    assert parse_ufs("") == UFS_NORTE_NORDESTE
    assert parse_ufs("pe, se") == ("PE", "SE")
    assert parse_ufs("ALL") is None
    assert len(UFS_NORTE_NORDESTE) == 16
    assert parse_meses("1-3,6") == (1, 2, 3, 6)
    assert parse_meses("ALL") is None


def test_validacao_de_limites() -> None:
    with pytest.raises(ValueError):
        Settings(rate_dia_por_min=500).validar()
    with pytest.raises(ValueError):
        Settings(ufs=("XX",)).validar()


def test_escolha_periodo_populacao() -> None:
    assert escolher_periodo(["2024", "2025", "2026"], 2026) == "2026"
    assert escolher_periodo(["2024", "2025"], 2026) == "2025"
