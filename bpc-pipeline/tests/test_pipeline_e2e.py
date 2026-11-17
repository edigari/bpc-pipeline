"""
Teste ponta a ponta com APIs SIMULADAS (tests/mock_apis.py) e Spark local.

Valida: extração com 429/500 injetados, filtro N/NE, descoberta de meses, Bronze, EXPLODE,
JOIN, métricas, Data Quality, exportação Parquet Snappy particionada e carga incremental.
"""

from __future__ import annotations

import glob
import json
import os

import pytest

pyspark = pytest.importorskip("pyspark")

from tests import mock_apis  # noqa: E402


@pytest.fixture(scope="module")
def ambiente(tmp_path_factory: pytest.TempPathFactory):
    srv, porta = mock_apis.iniciar()
    os.environ["PORTAL_BASE_URL"] = f"http://127.0.0.1:{porta}/api-de-dados"
    os.environ["IBGE_BASE_URL"] = f"http://127.0.0.1:{porta}/api"
    os.environ["PORTAL_TRANSPARENCIA_API_KEY"] = mock_apis.CHAVE_VALIDA
    yield tmp_path_factory.mktemp("exec")
    srv.shutdown()


def _rodar(base, extra: list[str] | None = None) -> int:
    from bpc_pipeline.main import main

    return main(["--base-path", str(base / "data"), "--output-path", str(base / "output"),
                 "--rate-dia", "400", "--rate-noite", "700", *(extra or [])])


def test_pipeline_completo_e_incremental(ambiente) -> None:
    assert _rodar(ambiente) == 0
    rel = json.loads((ambiente / "output" / "dq_report.json").read_text(encoding="utf-8"))

    portal = rel["extracao"]["portal"]
    assert portal["meses_processados"] == [202600 + m for m in range(1, 10)]
    assert portal["pares_no_escopo"] == 21 * 9               # SP ficou fora (escopo N/NE)
    assert portal["pares_com_erro"] == 0                      # 429/500 foram absorvidos pelas retentativas
    assert portal["http"]["retentativas"] > 0
    assert rel["escopo"]["ano_estimativa_populacional"] == "2025"   # fallback documentado
    assert rel["seguranca"]["origem_chave_api"] == "env"
    assert rel["data_quality"]["resumo"]["reprovados_criticos"] == 0

    # Parquet particionado ano/mes com Snappy
    arquivos = glob.glob(str(ambiente / "output" / "gold_bpc_municipio_mes" / "ano=2026" / "mes=*" / "*.parquet"))
    assert len(arquivos) == 9 and all(".snappy." in a for a in arquivos)

    # Nenhum vestígio da chave em disco
    for caminho in glob.glob(str(ambiente / "**" / "*.*"), recursive=True):
        if caminho.endswith((".json", ".jsonl", ".csv")):
            assert mock_apis.CHAVE_VALIDA not in open(caminho, encoding="utf-8").read()

    # Segunda execução: incremental -> zero pares pendentes
    assert _rodar(ambiente) == 0
    rel2 = json.loads((ambiente / "output" / "dq_report.json").read_text(encoding="utf-8"))
    assert rel2["extracao"]["portal"]["pares_pendentes"] == 0
    assert rel2["extracao"]["portal"]["pares_ja_existentes"] == 21 * 9
