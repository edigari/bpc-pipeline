"""
security.py
-----------
Gerenciamento ESTRITO da chave de API do Portal da Transparência.

Princípios aplicados:
  1. A chave NUNCA aparece no código, em notebooks, em widgets com valor padrão ou no Git.
  2. Ordem de resolução (a primeira fonte encontrada vence):
        a) Databricks Secrets  -> dbutils.secrets.get(scope, key)   (produção / Databricks)
        b) Variável de ambiente PORTAL_TRANSPARENCIA_API_KEY        (CI, cluster env var, terminal)
        c) Arquivo .env local (python-dotenv), que está no .gitignore (desenvolvimento local)
  3. A chave fica encapsulada em ``SecretStr``: print(), repr(), f-strings e logs mostram '**********'.
  4. Um filtro de logging remove o valor da chave de QUALQUER mensagem de log, por garantia.
  5. A chave trafega apenas no HEADER HTTPS ``chave-api-dados`` (nunca na URL/query string),
     então não vaza em logs de proxy, histórico de URL ou mensagens de exceção do ``requests``.
  6. A camada Bronze grava apenas o corpo das respostas; nenhum header é persistido.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

ENV_VAR_API_KEY = "PORTAL_TRANSPARENCIA_API_KEY"
SECRET_SCOPE_PADRAO = "transparencia"
SECRET_KEY_PADRAO = "portal_api_key"

_MASCARA = "**********"


class MissingApiKeyError(RuntimeError):
    """Nenhuma fonte de credencial foi encontrada (mensagem nunca contém a chave)."""


class SecretStr:
    """
    Wrapper que impede o vazamento acidental do segredo.

    >>> s = SecretStr("abc"); print(s)
    **********
    O valor real só é obtido explicitamente via ``get_secret_value()``.
    """

    __slots__ = ("_valor",)

    def __init__(self, valor: str) -> None:
        self._valor = valor

    def get_secret_value(self) -> str:
        return self._valor

    def __repr__(self) -> str:  # repr() e exibição em notebooks
        return f"SecretStr('{_MASCARA}')"

    def __str__(self) -> str:  # print() e f-strings
        return _MASCARA

    def __len__(self) -> int:
        return len(self._valor)

    # Impede serialização acidental (pickle -> envio a executores Spark, cache, etc.)
    def __reduce__(self) -> Any:
        raise TypeError("SecretStr não pode ser serializado (proteção da credencial).")


class RedactingFilter(logging.Filter):
    """Filtro de logging que substitui o valor do segredo por '**********' em qualquer log."""

    def __init__(self, segredo: SecretStr) -> None:
        super().__init__()
        self._valor = segredo.get_secret_value()

    def filter(self, record: logging.LogRecord) -> bool:
        if self._valor:
            mensagem = record.getMessage()
            if self._valor in mensagem:
                record.msg = mensagem.replace(self._valor, _MASCARA)
                record.args = ()
        return True


def _carregar_dotenv_se_existir() -> None:
    """Carrega o .env (somente se existir e se python-dotenv estiver instalado)."""
    try:
        from dotenv import load_dotenv  # dependência opcional: só no modo local
    except ImportError:
        return
    # Procura o .env no diretório atual e na raiz do projeto (dois níveis acima deste arquivo).
    for candidato in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if candidato.is_file():
            # override=False -> variáveis já definidas no ambiente têm prioridade sobre o arquivo.
            load_dotenv(candidato, override=False)
            return


def resolver_api_key(
    dbutils: Optional[Any] = None,
    scope: str = SECRET_SCOPE_PADRAO,
    key: str = SECRET_KEY_PADRAO,
    env_var: str = ENV_VAR_API_KEY,
) -> tuple[SecretStr, str]:
    """
    Resolve a chave de API seguindo a ordem de precedência segura.

    Returns:
        (SecretStr, origem) - a origem ('databricks-secrets', 'env', 'dotenv') vai para o log
        e para o relatório de auditoria; o VALOR nunca vai.
    """
    # (a) Databricks Secrets: armazenamento criptografado, com ACL, e redação automática
    #     de qualquer output que contenha o valor ([REDACTED]).
    if dbutils is not None:
        try:
            valor = dbutils.secrets.get(scope=scope, key=key)
            if valor and valor.strip():
                return SecretStr(valor.strip()), "databricks-secrets"
        except Exception as exc:  # scope/chave inexistente ou sem permissão
            logging.getLogger(__name__).warning(
                "Segredo '%s/%s' indisponível no Databricks (%s). Tentando variável de ambiente.",
                scope, key, type(exc).__name__,
            )

    # (b) Variável de ambiente já definida no processo.
    valor = os.getenv(env_var)
    if valor and valor.strip():
        return SecretStr(valor.strip()), "env"

    # (c) Arquivo .env local (fora do Git).
    _carregar_dotenv_se_existir()
    valor = os.getenv(env_var)
    if valor and valor.strip():
        return SecretStr(valor.strip()), "dotenv"

    raise MissingApiKeyError(
        "Chave da API do Portal da Transparência não encontrada. Configure UMA das opções:\n"
        f"  - Databricks: secret scope '{scope}' com a chave '{key}' (veja docs/PASSO_A_PASSO_DATABRICKS.md)\n"
        f"  - Local: variável de ambiente {env_var} ou arquivo .env (copie .env.example)"
    )


def instalar_filtro_de_redacao(segredo: SecretStr) -> None:
    """Instala o filtro de redação em todos os handlers do logger raiz e nos loggers HTTP."""
    filtro = RedactingFilter(segredo)
    raiz = logging.getLogger()
    for handler in raiz.handlers:
        handler.addFilter(filtro)
    for nome in ("bpc_pipeline", "urllib3", "requests", "py4j"):
        logging.getLogger(nome).addFilter(filtro)
