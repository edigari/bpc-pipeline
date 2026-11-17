# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Análise dos resultados e preparação da entrega
# MAGIC
# MAGIC 1. Consultas de validação/negócio sobre a Gold (SQL).
# MAGIC 2. Histórico de Data Quality.
# MAGIC 3. (Opcional) Copia `output/` do Volume para a pasta do repositório, para commitar no GitHub.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "bpc_pipeline", "Schema")
dbutils.widgets.text("volume", "arquivos", "Volume")
CATALOG, SCHEMA, VOLUME = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 15 municípios com maior cobertura do BPC (beneficiários por mil habitantes) no último mês

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nome_municipio, uf_sigla, populacao_estimada, quantidade_beneficiados,
# MAGIC        beneficiarios_por_mil_hab, valor_total, valor_medio_beneficio, ranking_cobertura_uf
# MAGIC FROM gold_bpc_municipio_mes
# MAGIC WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold_bpc_municipio_mes)
# MAGIC ORDER BY beneficiarios_por_mil_hab DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evolução mensal por região

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao_nome, ano_mes,
# MAGIC        SUM(quantidade_beneficiados) AS beneficiarios,
# MAGIC        ROUND(SUM(valor_total) / 1e6, 2) AS valor_milhoes,
# MAGIC        ROUND(SUM(quantidade_beneficiados) * 1000.0 / SUM(populacao_estimada), 2) AS por_mil_hab
# MAGIC FROM gold_bpc_uf_mes
# MAGIC GROUP BY regiao_nome, ano_mes
# MAGIC ORDER BY regiao_nome, ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico de Data Quality (todas as execuções)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, status, severidade, COUNT(*) AS checks
# MAGIC FROM dq_historico
# MAGIC GROUP BY run_id, status, severidade
# MAGIC ORDER BY run_id DESC, status

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Opcional) Copiar `output/` para o repositório Git
# MAGIC Copia os Parquet e o relatório de DQ do Volume para a pasta `output/` desta Git folder.
# MAGIC Depois: botão do Git (nome da branch no topo) → selecione os arquivos → **Commit & Push**.
# MAGIC Se a cópia falhar no seu tipo de compute, baixe pelo **Catalog Explorer → Volume → output** e suba no GitHub.

# COMMAND ----------

import os
import shutil

origem = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/output"
destino = os.path.abspath(os.path.join(os.getcwd(), "..", "output"))

# Ignora arquivos de controle do Spark/Databricks (não são dados).
ignorar = shutil.ignore_patterns("_SUCCESS", "_committed_*", "_started_*", "*.crc")
try:
    shutil.copytree(origem, destino, dirs_exist_ok=True, ignore=ignorar)
    total = sum(len(f) for _, _, f in os.walk(destino))
    print(f"{total} arquivos copiados para {destino}")
except Exception as exc:
    print(f"Não foi possível copiar ({type(exc).__name__}: {exc}). Use o download pelo Catalog Explorer.")
