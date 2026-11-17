"""
transform.py
------------
Transformações em Spark SQL (Silver e Gold). Tudo é declarativo e roda distribuído:
nenhum loop Python, nenhuma UDF, nenhum ``collect()`` de dados.

Transformações estruturadas aplicadas (o desafio pede no mínimo 5):
  T1. DESANINHAMENTO  -> EXPLODE de arrays JSON (BPC; séries SIDRA em 3 níveis + MAP) e
                         achatamento de structs aninhados (hierarquia territorial do IBGE).
  T2. PADRONIZAÇÃO DE TIPOS E DATAS -> códigos IBGE com 7 dígitos, valores DECIMAL(18,2),
                         inteiros, data de competência (DATE) e parsing de datas da API.
  T3. TRATAMENTO DE NULOS -> descarte de registros sem métrica, COALESCE com fontes alternativas
                         (nome/UF do IBGE, hierarquia nova do IBGE), 'NAO INFORMADO' em categorias,
                         NULLIF para evitar divisão por zero.
  T4. DEDUPLICAÇÃO   -> ROW_NUMBER() mantendo a ingestão mais recente por chave natural.
  T5. CRUZAMENTO DE FONTES (JOIN) -> BPC x municípios x população, com BROADCAST das dimensões.
  T6. ENRIQUECIMENTO DE MÉTRICAS -> beneficiários por mil hab., valor per capita, valor médio,
                         variação mensal (LAG), participação na UF e ranking (funções de janela).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from .extract_portal import SCHEMA_BRONZE_BPC

log = logging.getLogger("bpc_pipeline.transform")

# ---------------------------------------------------------------------------
# Schemas (DDL) das respostas JSON. Campos-folha como STRING de propósito: a conversão de tipos
# é feita com TRY_CAST na Silver, então um valor inesperado vira NULL (e aparece no Data Quality)
# em vez de derrubar o job.
# ---------------------------------------------------------------------------

DDL_BPC_PAYLOAD = (
    "array<struct<"
    "id:string,"
    "dataReferencia:string,"
    "municipio:struct<codigoIBGE:string,nomeIBGE:string,codigoRegiao:string,nomeRegiao:string,pais:string,"
    "uf:struct<sigla:string,nome:string>>,"
    "tipo:struct<id:string,descricao:string,descricaoDetalhada:string>,"
    "valor:string,"
    "quantidadeBeneficiados:string>>"
)

_DDL_UF = "struct<id:string,sigla:string,nome:string,regiao:struct<id:string,sigla:string,nome:string>>"
DDL_IBGE_MUNICIPIOS = (
    "array<struct<"
    "id:string,nome:string,"
    f"microrregiao:struct<id:string,nome:string,mesorregiao:struct<id:string,nome:string,UF:{_DDL_UF}>>,"
    f"`regiao-imediata`:struct<id:string,nome:string,`regiao-intermediaria`:struct<id:string,nome:string,UF:{_DDL_UF}>>"
    ">>"
)

DDL_IBGE_POPULACAO = (
    "array<struct<id:string,variavel:string,unidade:string,"
    "resultados:array<struct<series:array<struct<"
    "localidade:struct<id:string,nome:string,nivel:struct<id:string,nome:string>>,"
    "serie:map<string,string>>>>>>>"
)


def _configurar_sessao(spark: SparkSession) -> None:
    """Ajustes para volume pequeno/médio. Em serverless algumas confs são bloqueadas: ignoramos."""
    for chave, valor in {
        "spark.sql.shuffle.partitions": "16",          # padrão 200 é exagero para ~25 mil linhas
        "spark.sql.session.timeZone": "America/Sao_Paulo",
    }.items():
        try:
            spark.conf.set(chave, valor)
        except Exception:  # pragma: no cover - depende do tipo de compute
            pass


# ---------------------------------------------------------------------------
# Registro das views de origem (Bronze)
# ---------------------------------------------------------------------------

def registrar_views_bronze(spark: SparkSession, caminho_bpc: str, caminho_mun: str, caminho_pop: str) -> None:
    """Cria views temporárias sobre os arquivos brutos (leitura com schema explícito, sem inferência)."""
    _configurar_sessao(spark)
    spark.read.schema(SCHEMA_BRONZE_BPC).json(caminho_bpc).createOrReplaceTempView("bronze_bpc")
    # wholetext=True: cada arquivo JSON vira UMA linha de texto, interpretada depois com from_json.
    spark.read.text(caminho_mun, wholetext=True).createOrReplaceTempView("bronze_ibge_municipios")
    spark.read.text(caminho_pop, wholetext=True).createOrReplaceTempView("bronze_ibge_populacao")


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------

def silver_bpc(spark: SparkSession) -> DataFrame:
    """BPC desaninhado, tipado, sem nulos inválidos e deduplicado (grão: município x mês x tipo)."""
    return spark.sql(f"""
        WITH explodido AS (
            -- T1: cada resposta da API é um ARRAY JSON -> uma linha por elemento
            SELECT
                b.codigo_ibge          AS codigo_ibge_req,
                b.ano_mes              AS ano_mes_req,
                b.ingested_at,
                r                      AS rec
            FROM bronze_bpc b
            LATERAL VIEW explode(from_json(b.payload, '{DDL_BPC_PAYLOAD}')) t AS r
            WHERE b.status = 'ok'
        ),
        padronizado AS (
            -- T2: tipos e datas padronizados  |  T3: nulos tratados
            SELECT
                lpad(regexp_replace(codigo_ibge_req, '[^0-9]', ''), 7, '0')          AS codigo_ibge,
                CAST(ano_mes_req AS INT)                                             AS ano_mes,
                CAST(ano_mes_req DIV 100 AS INT)                                     AS ano,
                CAST(ano_mes_req % 100 AS INT)                                       AS mes,
                make_date(ano_mes_req DIV 100, ano_mes_req % 100, 1)                 AS data_competencia,
                CAST(coalesce(
                    try_to_timestamp(rec.dataReferencia, 'dd/MM/yyyy'),
                    try_to_timestamp(rec.dataReferencia, 'yyyy-MM-dd')) AS DATE)     AS data_referencia_api,
                nullif(upper(trim(rec.municipio.nomeIBGE)), '')                      AS nome_municipio_api,
                nullif(upper(trim(rec.municipio.uf.sigla)), '')                      AS uf_sigla_api,
                coalesce(nullif(trim(rec.tipo.id), ''), '0')                         AS tipo_id,
                coalesce(nullif(upper(trim(rec.tipo.descricao)), ''), 'NAO INFORMADO') AS tipo_descricao,
                try_cast(rec.quantidadeBeneficiados AS INT)                          AS quantidade_beneficiados,
                try_cast(rec.valor AS DECIMAL(18,2))                                 AS valor,
                CAST(ingested_at AS TIMESTAMP)                                       AS ingested_at
            FROM explodido
        ),
        deduplicado AS (
            -- T4: a mesma chave pode existir em mais de uma execução -> fica a mais recente
            SELECT *,
                   row_number() OVER (
                       PARTITION BY codigo_ibge, ano_mes, tipo_id
                       ORDER BY ingested_at DESC) AS rn
            FROM padronizado
            -- T3: registro sem nenhuma métrica não tem valor analítico
            WHERE quantidade_beneficiados IS NOT NULL OR valor IS NOT NULL
        )
        SELECT codigo_ibge, ano_mes, ano, mes, data_competencia, data_referencia_api,
               nome_municipio_api, uf_sigla_api, tipo_id, tipo_descricao,
               quantidade_beneficiados, valor, ingested_at
        FROM deduplicado
        WHERE rn = 1
    """)


def silver_municipios(spark: SparkSession) -> DataFrame:
    """Dimensão de municípios: struct aninhado do IBGE achatado em colunas (grão: município)."""
    return spark.sql(f"""
        WITH m AS (
            -- T1: array JSON -> uma linha por município
            SELECT explode(from_json(value, '{DDL_IBGE_MUNICIPIOS}')) AS m
            FROM bronze_ibge_municipios
        ),
        achatado AS (
            SELECT
                lpad(m.id, 7, '0')                                                    AS codigo_ibge,
                trim(m.nome)                                                          AS nome_municipio,
                -- T3: microrregião pode ser NULL em municípios novos -> usa a hierarquia nova
                coalesce(m.microrregiao.mesorregiao.UF.sigla,
                         m.`regiao-imediata`.`regiao-intermediaria`.UF.sigla)         AS uf_sigla,
                coalesce(m.microrregiao.mesorregiao.UF.nome,
                         m.`regiao-imediata`.`regiao-intermediaria`.UF.nome)          AS uf_nome,
                coalesce(m.microrregiao.mesorregiao.UF.regiao.nome,
                         m.`regiao-imediata`.`regiao-intermediaria`.UF.regiao.nome)   AS regiao_nome,
                coalesce(m.microrregiao.mesorregiao.nome, 'NAO INFORMADO')            AS mesorregiao_nome,
                coalesce(m.microrregiao.nome, 'NAO INFORMADO')                        AS microrregiao_nome,
                m.`regiao-imediata`.nome                                              AS regiao_imediata_nome,
                m.`regiao-imediata`.`regiao-intermediaria`.nome                       AS regiao_intermediaria_nome
            FROM m
        )
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY codigo_ibge ORDER BY nome_municipio) AS rn  -- T4
            FROM achatado
            WHERE codigo_ibge IS NOT NULL
        ) WHERE rn = 1
    """).drop("rn")


def silver_populacao(spark: SparkSession) -> DataFrame:
    """População estimada: 3 níveis de arrays + 1 MAP desaninhados com LATERAL VIEW EXPLODE."""
    return spark.sql(f"""
        WITH desaninhado AS (
            SELECT
                lpad(s.localidade.id, 7, '0')   AS codigo_ibge,
                s.localidade.nome               AS nome_localidade,
                CAST(p_ano AS INT)              AS ano_populacao,
                -- T2/T3: o SIDRA usa '-', '...' ou 'X' para dado ausente/sigiloso -> NULL
                try_cast(p_valor AS BIGINT)     AS populacao_estimada
            FROM (SELECT explode(from_json(value, '{DDL_IBGE_POPULACAO}')) AS v FROM bronze_ibge_populacao) x
            LATERAL VIEW explode(v.resultados) tr AS r        -- nível 1: resultados
            LATERAL VIEW explode(r.series)     ts AS s        -- nível 2: séries (1 por município)
            LATERAL VIEW explode(s.serie)      tp AS p_ano, p_valor   -- nível 3: MAP {{ano: valor}}
            WHERE v.id = '9324'
        )
        SELECT codigo_ibge, nome_localidade, ano_populacao, populacao_estimada FROM (
            SELECT *, row_number() OVER (PARTITION BY codigo_ibge ORDER BY ano_populacao DESC) AS rn  -- T4
            FROM desaninhado
            WHERE populacao_estimada IS NOT NULL
        ) WHERE rn = 1
    """)


# ---------------------------------------------------------------------------
# GOLD
# ---------------------------------------------------------------------------

def gold_bpc_municipio_mes(spark: SparkSession) -> DataFrame:
    """
    Fato analítico (grão: município x mês) = BPC + território + população + métricas.
    Requer as views ``silver_bpc``, ``silver_municipios`` e ``silver_populacao``.
    """
    return spark.sql("""
        WITH bpc_mes AS (
            -- consolida os tipos de benefício no grão município x mês
            SELECT codigo_ibge, ano_mes, ano, mes, data_competencia,
                   SUM(quantidade_beneficiados)  AS quantidade_beneficiados,
                   SUM(valor)                    AS valor_total,
                   COUNT(*)                      AS qtd_tipos_beneficio,
                   MAX(nome_municipio_api)       AS nome_municipio_api,
                   MAX(uf_sigla_api)             AS uf_sigla_api
            FROM silver_bpc
            GROUP BY codigo_ibge, ano_mes, ano, mes, data_competencia
        ),
        cruzado AS (
            -- T5: JOIN entre fontes. Dimensões pequenas (5,5 mil linhas) -> BROADCAST evita shuffle.
            SELECT /*+ BROADCAST(m), BROADCAST(p) */
                b.codigo_ibge, b.ano_mes, b.ano, b.mes, b.data_competencia,
                coalesce(m.nome_municipio, initcap(b.nome_municipio_api))   AS nome_municipio,   -- T3
                coalesce(m.uf_sigla, b.uf_sigla_api)                         AS uf_sigla,         -- T3
                m.uf_nome, m.regiao_nome, m.mesorregiao_nome, m.microrregiao_nome,
                b.quantidade_beneficiados,
                b.valor_total,
                b.qtd_tipos_beneficio,
                p.populacao_estimada,
                p.ano_populacao,
                (m.codigo_ibge IS NOT NULL)                                   AS flag_municipio_ibge,
                (p.populacao_estimada IS NOT NULL)                            AS flag_populacao
            FROM bpc_mes b
            LEFT JOIN silver_municipios m ON b.codigo_ibge = m.codigo_ibge
            LEFT JOIN silver_populacao  p ON b.codigo_ibge = p.codigo_ibge
        )
        -- T6: enriquecimento de métricas (NULLIF evita divisão por zero)
        SELECT
            c.*,
            CAST(c.quantidade_beneficiados * 1000.0 / nullif(c.populacao_estimada, 0) AS DECIMAL(12,3))
                AS beneficiarios_por_mil_hab,
            CAST(c.valor_total / nullif(c.populacao_estimada, 0) AS DECIMAL(14,2))
                AS valor_per_capita,
            CAST(c.valor_total / nullif(c.quantidade_beneficiados, 0) AS DECIMAL(14,2))
                AS valor_medio_beneficio,
            CAST((c.quantidade_beneficiados - lag(c.quantidade_beneficiados) OVER w_mun) * 100.0
                 / nullif(lag(c.quantidade_beneficiados) OVER w_mun, 0) AS DECIMAL(12,3))
                AS variacao_mensal_beneficiarios_pct,
            CAST(c.valor_total * 100.0 / nullif(SUM(c.valor_total) OVER w_uf, 0) AS DECIMAL(12,4))
                AS participacao_valor_uf_pct,
            CAST(dense_rank() OVER (
                PARTITION BY c.uf_sigla, c.ano_mes
                ORDER BY c.quantidade_beneficiados / nullif(c.populacao_estimada, 0) DESC NULLS LAST) AS INT)
                AS ranking_cobertura_uf,
            current_timestamp() AS processado_em
        FROM cruzado c
        WINDOW
            w_mun AS (PARTITION BY c.codigo_ibge ORDER BY c.ano_mes),
            w_uf  AS (PARTITION BY c.uf_sigla, c.ano_mes)
    """)


def gold_bpc_uf_mes(spark: SparkSession) -> DataFrame:
    """Agregado por UF x mês (pronto para BI). Requer a view ``gold_bpc_municipio_mes``."""
    return spark.sql("""
        SELECT
            uf_sigla,
            MAX(uf_nome)                                   AS uf_nome,
            MAX(regiao_nome)                               AS regiao_nome,
            ano_mes, ano, mes,
            COUNT(*)                                       AS qtd_municipios,
            SUM(quantidade_beneficiados)                   AS quantidade_beneficiados,
            SUM(valor_total)                               AS valor_total,
            SUM(populacao_estimada)                        AS populacao_estimada,
            CAST(SUM(quantidade_beneficiados) * 1000.0 / nullif(SUM(populacao_estimada), 0) AS DECIMAL(12,3))
                                                           AS beneficiarios_por_mil_hab,
            CAST(SUM(valor_total) / nullif(SUM(quantidade_beneficiados), 0) AS DECIMAL(14,2))
                                                           AS valor_medio_beneficio
        FROM gold_bpc_municipio_mes
        GROUP BY uf_sigla, ano_mes, ano, mes
    """)
