"""
bpc_pipeline
============

Pipeline de Engenharia de Dados (Desafio FT03 - Projeto 1).

Fontes (domínios distintos):
  1. Portal da Transparência (CGU)  -> BPC (Benefício de Prestação Continuada) por município, 2026
  2. IBGE (servicodados.ibge.gov.br) -> malha de municípios + estimativa populacional

Arquitetura em camadas (medalhão):
  Bronze  -> respostas brutas das APIs (JSON), imutáveis, com metadados de ingestão
  Silver  -> dados desaninhados (EXPLODE), tipados, sem nulos inválidos e deduplicados
  Gold    -> cruzamento das fontes (JOIN) + métricas de negócio, particionado por ano/mês

Módulos:
  config       -> parâmetros da execução (escopo, limites de taxa, caminhos)
  security     -> resolução segura da chave de API (Databricks Secrets / env / .env) e mascaramento
  http_client  -> cliente HTTP com rate limiting, retentativas e backoff exponencial
  extract_ibge -> extração da API do IBGE
  extract_portal -> extração paralela e incremental da API do Portal da Transparência
  storage      -> leitura/escrita (Delta no Databricks, Parquet local) e exportação Snappy
  transform    -> transformações em Spark SQL (Silver e Gold)
  quality      -> suíte de Data Quality e relatório JSON/CSV
  pipeline     -> orquestração das etapas
  main         -> ponto de entrada de linha de comando (execução local / run.sh)
"""

__version__ = "1.0.0"
