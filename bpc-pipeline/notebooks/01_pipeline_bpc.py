# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Pipeline BPC × População — Norte e Nordeste, 2026
# MAGIC
# MAGIC | Etapa | O que faz | Camada |
# MAGIC |---|---|---|
# MAGIC | 1 | IBGE: municípios (hierarquia aninhada) + estimativa populacional | Bronze |
# MAGIC | 2 | Portal da Transparência: BPC por município × mês, **paralelo e incremental** | Bronze |
# MAGIC | 3 | EXPLODE dos JSONs, tipos, nulos, deduplicação | Silver |
# MAGIC | 4 | JOIN entre fontes + métricas, particionado por ano/mês | Gold |
# MAGIC | 5 | Data Quality + relatório JSON/CSV | Auditoria |
# MAGIC | 6 | Exportação Parquet Snappy particionada (`output/`) | Entrega |
# MAGIC
# MAGIC **Tempo esperado (Norte + Nordeste, ~2.244 municípios × meses publicados):** a etapa 2 é limitada pela API
# MAGIC (≈ 360 req/min de dia, ≈ 630 req/min de 00h às 06h). Ex.: 9 meses ≈ 20 mil requisições ≈ **56 min de dia / 32 min de madrugada**.
# MAGIC Re-execuções são **incrementais**: só buscam o que ainda falta (segundos, se nada mudou).
# MAGIC
# MAGIC > Primeira vez? Rode com `limite_municipios = 20` para um teste de ~1 minuto e depois volte para `0`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "bpc_pipeline", "Schema")
dbutils.widgets.text("volume", "arquivos", "Volume")
dbutils.widgets.text("secret_scope", "transparencia", "Secret scope")
dbutils.widgets.text("secret_key", "portal_api_key", "Nome do segredo")
dbutils.widgets.text("ufs", "NORTE,NORDESTE", "UFs/regiões (ou ALL)")
dbutils.widgets.text("meses", "ALL", "Meses (ALL, 1-6, 1,2,3)")
dbutils.widgets.text("limite_municipios", "0", "Limite de municípios (0 = todos)")
dbutils.widgets.text("rate_dia", "360", "Req/min 06h-23h59 (máx. 400)")
dbutils.widgets.text("rate_noite", "630", "Req/min 00h-05h59 (máx. 700)")
dbutils.widgets.text("workers", "12", "Threads HTTP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importa o código do repositório (`src/`)
# MAGIC Em uma *Git folder* do Databricks, o diretório de trabalho é a pasta deste notebook; `src/` fica um nível acima.

# COMMAND ----------

import importlib
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import bpc_pipeline  # noqa: E402
from bpc_pipeline import config, extract_ibge, extract_portal, http_client, pipeline, quality, security, storage, transform  # noqa: E402

# Recarrega os módulos (útil se você editar o código e rodar o notebook de novo sem reiniciar).
for m in (config, security, http_client, storage, extract_ibge, extract_portal, transform, quality, pipeline):
    importlib.reload(m)

from bpc_pipeline.config import Settings, parse_meses, parse_ufs  # noqa: E402
from bpc_pipeline.pipeline import Contexto, configurar_logging  # noqa: E402
from bpc_pipeline.security import resolver_api_key  # noqa: E402

configurar_logging()
print("bpc_pipeline", bpc_pipeline.__version__, "carregado de", SRC)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto da execução + chave de API (Databricks Secrets)
# MAGIC A chave é lida do secret scope e encapsulada em `SecretStr` — `print(api_key)` mostra `**********`.

# COMMAND ----------

w = dbutils.widgets.get
CATALOG, SCHEMA, VOLUME = w("catalog"), w("schema"), w("volume")
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

settings = Settings(
    ufs=parse_ufs(w("ufs")),
    meses=parse_meses(w("meses")),
    limite_municipios=int(w("limite_municipios")),
    rate_dia_por_min=int(w("rate_dia")),
    rate_noite_por_min=int(w("rate_noite")),
    max_workers=int(w("workers")),
    base_path=BASE,
    output_path=f"{BASE}/output",
    catalog=CATALOG,
    schema=SCHEMA,
).validar()

api_key, origem = resolver_api_key(dbutils=dbutils, scope=w("secret_scope"), key=w("secret_key"))
ctx = Contexto(spark, settings, api_key, origem)
print("run_id:", ctx.run_id, "| chave:", api_key, "| origem:", origem, "| UFs:", settings.ufs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1 · IBGE → Bronze (3 requisições, segundos)

# COMMAND ----------

res_ibge = pipeline.etapa_1_ibge(ctx)
print(f"{len(res_ibge.municipios)} municípios | estimativa populacional usada: {res_ibge.ano_populacao}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2 · Portal da Transparência → Bronze (etapa longa)
# MAGIC Descobre os meses publicados, remove o que já está na Bronze e baixa o restante em paralelo, no teto do limite.
# MAGIC Se cair no meio, **rode de novo**: os lotes já gravados não são baixados outra vez.

# COMMAND ----------

res_portal = pipeline.etapa_2_portal(ctx)
display(spark.createDataFrame([{k: str(v) for k, v in res_portal.to_dict().items()}]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3 · Silver (EXPLODE, tipos, nulos, deduplicação)

# COMMAND ----------

pipeline.etapa_3_silver(ctx)
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_bpc_municipio_tipo LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 4 · Gold (JOIN + métricas, particionado por ano/mês)

# COMMAND ----------

pipeline.etapa_4_gold(ctx)
display(spark.sql(f"""
    SELECT uf_sigla, ano_mes, qtd_municipios, quantidade_beneficiados, valor_total, beneficiarios_por_mil_hab
    FROM {CATALOG}.{SCHEMA}.gold_bpc_uf_mes ORDER BY ano_mes DESC, beneficiarios_por_mil_hab DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5 · Data Quality (relatório JSON + CSV + histórico Delta)

# COMMAND ----------

relatorio = pipeline.etapa_5_qualidade(ctx)
print(relatorio["data_quality"]["resumo"])
display(spark.createDataFrame(relatorio["data_quality"]["checks"]).orderBy("status", "severidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6 · Exportação Parquet (Snappy, `ano=/mes=`) + gate de qualidade

# COMMAND ----------

for caminho in pipeline.etapa_6_exportar(ctx):
    print(caminho)
display(dbutils.fs.ls(f"{BASE}/output"))

# O gate falha a execução (Job fica vermelho) se algum check CRÍTICO reprovar.
pipeline.validar_gate_de_qualidade(ctx)
print(f"Pipeline concluído. Tempos por etapa (s): {ctx.tempos}")
