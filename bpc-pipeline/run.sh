#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Execução LOCAL ponta a ponta (o Databricks usa os notebooks em notebooks/).
#
#   ./run.sh                                  # Norte + Nordeste, meses publicados de 2026
#   ./run.sh --ufs PE --limite-municipios 20  # teste rápido (~1 min)
#   ./run.sh --ufs NORTE --meses 1-6
#
# Pré-requisitos: Python 3.10+, Java 17/21 e a chave em .env ou na variável
# PORTAL_TRANSPARENCIA_API_KEY. A chave NUNCA é passada como argumento (ficaria
# no histórico do shell e na lista de processos).
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ -z "${PORTAL_TRANSPARENCIA_API_KEY:-}" && ! -f .env ]]; then
  echo "ERRO: defina PORTAL_TRANSPARENCIA_API_KEY ou crie o .env (cp .env.example .env)." >&2
  exit 2
fi
command -v java >/dev/null 2>&1 || { echo "ERRO: Java 17+ é necessário para o PySpark." >&2; exit 2; }

# Ambiente virtual isolado (criado só na primeira execução).
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi

PYTHONPATH=src .venv/bin/python -m bpc_pipeline.main "$@"
