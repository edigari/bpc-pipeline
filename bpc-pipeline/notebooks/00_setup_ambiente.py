# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente (rodar UMA vez)
# MAGIC
# MAGIC Este notebook prepara tudo o que o pipeline precisa, **sem nunca expor a chave da API**:
# MAGIC
# MAGIC 1. Cria o **schema** e o **Volume** no Unity Catalog (onde ficam Bronze, relatórios e o `output/`).
# MAGIC 2. Confere se o **secret scope** com a chave do Portal da Transparência existe (mostra só os NOMES, nunca valores).
# MAGIC 3. Faz **1 requisição de teste** à API e mostra apenas o status HTTP.
# MAGIC
# MAGIC > A chave deve ser cadastrada ANTES, pelo Databricks CLI (passo 3 do `docs/PASSO_A_PASSO_DATABRICKS.md`).
# MAGIC > Se não for possível usar o CLI, use a **célula opcional** no fim deste notebook.

# COMMAND ----------

# Parâmetros (widgets). Nenhum deles é sensível: são só NOMES de catálogo, schema, volume e do segredo.
dbutils.widgets.text("catalog", "workspace", "Catálogo (Unity Catalog)")
dbutils.widgets.text("schema", "bpc_pipeline", "Schema")
dbutils.widgets.text("volume", "arquivos", "Volume")
dbutils.widgets.text("secret_scope", "transparencia", "Secret scope")
dbutils.widgets.text("secret_key", "portal_api_key", "Nome do segredo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SECRET_KEY = dbutils.widgets.get("secret_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema e Volume no Unity Catalog

# COMMAND ----------

# IF NOT EXISTS -> o notebook é idempotente (pode rodar de novo sem erro).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Desafio FT03 - BPC x População (2026)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} COMMENT 'Bronze (JSON bruto), relatórios de DQ e output Parquet'")

base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
for sub in ("bronze", "quality", "output"):
    dbutils.fs.mkdirs(f"{base}/{sub}")
print(f"OK -> schema {CATALOG}.{SCHEMA} e volume {base}")
display(dbutils.fs.ls(base))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar o secret scope (sem revelar o valor)

# COMMAND ----------

scopes = [s.name for s in dbutils.secrets.listScopes()]
if SECRET_SCOPE not in scopes:
    raise RuntimeError(
        f"Secret scope '{SECRET_SCOPE}' não existe. Crie com o Databricks CLI:\n"
        f"  databricks secrets create-scope {SECRET_SCOPE}\n"
        f"  databricks secrets put-secret {SECRET_SCOPE} {SECRET_KEY}\n"
        "ou use a célula opcional no fim deste notebook."
    )
chaves = [k.key for k in dbutils.secrets.list(SECRET_SCOPE)]  # lista apenas NOMES
print(f"Scope '{SECRET_SCOPE}' encontrado. Segredos cadastrados: {chaves}")
assert SECRET_KEY in chaves, f"O segredo '{SECRET_KEY}' não está no scope '{SECRET_SCOPE}'."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Teste de conectividade (1 requisição)
# MAGIC O valor da chave vai só no header HTTPS. Se por engano alguém der `print` nele, o Databricks mostra `[REDACTED]`.

# COMMAND ----------

import requests

resp = requests.get(
    "https://api.portaldatransparencia.gov.br/api-de-dados/bpc-por-municipio",
    params={"mesAno": "202601", "codigoIbge": "2927408", "pagina": 1},   # Salvador/BA, jan/2026
    headers={"chave-api-dados": dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY), "Accept": "application/json"},
    timeout=(5, 30),
)
print("Portal da Transparência -> HTTP", resp.status_code)
if resp.status_code == 200:
    print("Chave OK. Amostra:", resp.json()[:1])
elif resp.status_code in (401, 403):
    print("Chave inválida/expirada. Gere outra em https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email")

ibge = requests.get("https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos", timeout=(5, 30))
print("IBGE -> HTTP", ibge.status_code, "| últimos períodos de população:", [p["id"] for p in ibge.json()][-3:])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## (OPCIONAL) Cadastrar a chave sem o Databricks CLI
# MAGIC
# MAGIC Use **apenas** se não conseguir instalar o CLI. Passos:
# MAGIC 1. Rode a **célula A** (cria os widgets `cadastrar_chave` e `chave_temporaria` no topo da página).
# MAGIC 2. Mude `cadastrar_chave` para **SIM**, cole a chave em `chave_temporaria` e rode a **célula B**.
# MAGIC 3. Rode a **célula C** (apaga os widgets) e limpe as saídas: menu **Run → Clear → Clear all outputs**.
# MAGIC
# MAGIC Por que é aceitável: o valor de um widget **não** faz parte do código-fonte do notebook,
# MAGIC então não vai para o Git nem para o histórico de versões. Mesmo assim, o CLI é o caminho preferido.

# COMMAND ----------

# Célula A: cria os widgets do cadastro opcional (vazios por padrão).
dbutils.widgets.dropdown("cadastrar_chave", "NAO", ["NAO", "SIM"], "Cadastrar chave agora?")
dbutils.widgets.text("chave_temporaria", "", "Cole a chave aqui (temporário)")

# COMMAND ----------

# Célula B: grava a chave no secret scope (só se cadastrar_chave = SIM).
if dbutils.widgets.get("cadastrar_chave") == "SIM":
    from databricks.sdk import WorkspaceClient

    valor = dbutils.widgets.get("chave_temporaria").strip()
    if not valor:
        print("Preencha o widget 'chave_temporaria' e rode esta célula de novo.")
    else:
        w = WorkspaceClient()  # autenticação automática do próprio notebook
        if SECRET_SCOPE not in [s.name for s in w.secrets.list_scopes()]:
            w.secrets.create_scope(scope=SECRET_SCOPE)
        w.secrets.put_secret(scope=SECRET_SCOPE, key=SECRET_KEY, string_value=valor)
        del valor
        print(f"Segredo gravado em {SECRET_SCOPE}/{SECRET_KEY}. Rode a célula C para apagar os widgets.")
else:
    print("Cadastro opcional desativado (padrão). Nada foi feito.")

# COMMAND ----------

# Célula C: remove os widgets temporários (o Databricks exige remoção em célula separada da criação).
dbutils.widgets.remove("chave_temporaria")
dbutils.widgets.remove("cadastrar_chave")
print("Widgets removidos. Agora: Run -> Clear -> Clear all outputs.")
