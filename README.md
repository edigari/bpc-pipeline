Desafio Python FT03 · Projeto 1. Pipeline de extração, transformação, Data Quality e disponibilização de dados com **Python + Spark SQL**, pronto para **Databricks** (Unity Catalog, serverless) e também executável localmente (`run.sh`).

**Pergunta de negócio:** qual o alcance do Benefício de Prestação Continuada (BPC) em cada município do Norte e do Nordeste em 2026, em relação à população (beneficiários por mil habitantes, valor per capita, evolução mensal e ranking dentro da UF)?

---

## 1. Fontes de dados (2 APIs públicas de domínios distintos)

| # | API | Domínio | Autenticação | Endpoints usados |
|---|-----|---------|--------------|------------------|
| 1 | [Portal da Transparência (CGU)](https://api.portaldatransparencia.gov.br/swagger-ui/index.html) | Assistência social / gasto público | Header `chave-api-dados` (token gratuito) | `GET /api-de-dados/bpc-por-municipio?mesAno=AAAAMM&codigoIbge=NNNNNNN&pagina=N` |
| 2 | [IBGE – Serviço de Dados](https://servicodados.ibge.gov.br/api/docs) | Demografia / território | Nenhuma | `GET /api/v1/localidades/municipios` · `GET /api/v3/agregados/6579/periodos` · `GET /api/v3/agregados/6579/periodos/{ano}/variaveis/9324?localidades=N6[all]` |

* **BPC por município**: um registro por município × mês com `quantidadeBeneficiados`, `valor`, `municipio{codigoIBGE, nomeIBGE, uf{sigla,nome}}`, `tipo{...}`, `dataReferencia`.
* **Localidades IBGE**: municípios com hierarquia aninhada `microrregiao → mesorregiao → UF → regiao` (e `regiao-imediata → regiao-intermediaria → UF`).
* **SIDRA 6579 / variável 9324**: população residente estimada. A resposta tem **3 níveis de arrays + um mapa** (`resultados[] → series[] → serie{ano: valor}`). Se a estimativa de 2026 ainda não tiver sido publicada, o pipeline usa a mais recente e registra isso no relatório (`escopo.ano_estimativa_populacional`).

**Limites da API do Portal (CGU):** 400 req/min das 06:00 às 23:59 e 700 req/min das 00:00 às 05:59. **Se você passar do limite, o token é suspenso.** O pipeline usa 90% desses valores por padrão.

---

## 2. Arquitetura

```
 IBGE API ──┐                                      ┌─ silver_municipios ─┐
            ├─► BRONZE (Volume, JSON bruto) ─► SILVER (Delta) ─ silver_populacao ──┼─► GOLD (Delta, ano/mes) ─► output/ (Parquet Snappy)
 Portal API ┘   incremental, em lotes          └─ silver_bpc_municipio_tipo ┘      gold_bpc_municipio_mes        + dq_report.json / dq_checks.csv
                                                                                    gold_bpc_uf_mes               + dq_historico (Delta)
```

| Camada | Onde fica (Databricks) | Conteúdo |
|---|---|---|
| Bronze | `/Volumes/<cat>/<schema>/<vol>/bronze/` | Respostas brutas, 1 linha por (município, mês), com `status`, `http_status`, `ingested_at`, `run_id`. Imutável e acumulativa. |
| Silver | `<cat>.<schema>.silver_*` | Dados desaninhados, tipados, sem nulos inválidos e deduplicados. |
| Gold | `<cat>.<schema>.gold_*` | JOIN BPC × território × população + métricas, particionado por `ano/mes`. |
| Entrega | `/Volumes/.../output/` e `output/` do repo | Parquet Snappy `ano=2026/mes=M/` + relatórios de Data Quality. |

---

## 3. Performance: por que a versão anterior levava 3h+ e o que mudou

A API do BPC aceita **um município e um mês por chamada**. Norte + Nordeste = 2.244 municípios; com 10 meses publicados são ~22,4 mil chamadas. **O limite da CGU define um tempo mínimo físico** — nenhum código passa dele sem arriscar a suspensão do token. O objetivo, então, é operar **colado no teto** e **nunca repetir trabalho**:

| Técnica | Onde | Efeito |
|---|---|---|
| Paralelismo (12 threads) + **rate limiter global thread-safe** | `http_client.RateLimiter` | Esconde a latência de rede. Em modo sequencial, com ~0,5 s por chamada, o máximo seria ~120 req/min mesmo com limite de 400; aqui fica em ~360/min. |
| **Taxa dia/noite automática** (fuso de Brasília) | `criar_provedor_de_taxa` | De 00h às 05h54 sobe para 630 req/min (−43% de tempo). |
| **Descoberta de meses publicados** (≤ 12 chamadas) | `descobrir_meses_disponiveis` | Não gasta milhares de chamadas em meses que ainda não saíram. |
| **Carga incremental** (anti-join com a Bronze) | `carregar_pares_existentes` | Re-execução só busca o que falta. Se nada mudou: segundos. |
| **Checkpoint em lotes** de 1.000 registros | `_gravar_lote` | Uma falha no meio não perde o que já foi baixado. |
| Paginação sob demanda | `buscar_bpc_municipio_mes` | Não faz a chamada extra "página 2 vazia" para cada município. |
| Keep-alive (sessão HTTP por thread) | `ApiClient._sessao` | Sem novo handshake TLS a cada chamada. |
| Falha rápida em 401/403 e circuit breaker de 429 | `ApiClient.get` | Não gasta horas retentando uma chave inválida. |
| Spark: schema explícito, `BROADCAST` das dimensões, 1 agregação para todo o DQ, releitura após gravar (substitui `cache`, que não existe no serverless), 1 arquivo por partição | `transform.py`, `quality.py`, `storage.py` | Etapas Spark em segundos, sem arquivos pequenos. |

**Tempo estimado da extração (considerando 10 meses publicados):**

| Escopo | Requisições | Dia (360/min) | Madrugada (630/min) |
|---|---:|---:|---:|
| Teste (`limite_municipios=20`) | ~200 | < 1 min | < 1 min |
| Norte (450 municípios) | ~4.500 | ~13 min | ~7 min |
| Nordeste (1.794) | ~17.900 | ~50 min | ~28 min |
| **Norte + Nordeste (2.244)** — padrão | **~22.400** | **~62 min** | **~36 min** |
| Brasil (5.571) | ~55.700 | ~2h35 | ~1h28 |

Dica: agende um **Job** do Databricks às 00:05 para usar a janela de 700 req/min.

---

## 4. Segurança da chave de API

* A chave **nunca** está no código, nos notebooks, em valores padrão de widgets nem no Git.
* Ordem de resolução (`security.resolver_api_key`): **Databricks Secrets** → variável de ambiente `PORTAL_TRANSPARENCIA_API_KEY` → arquivo `.env` (no `.gitignore`; modelo em `.env.example`).
* A chave fica em `SecretStr`: `print`, `repr` e f-strings exibem `**********`; o objeto se recusa a ser serializado.
* Um filtro de logging (`RedactingFilter`) mascara a chave em qualquer mensagem de log. O Databricks também mostra `[REDACTED]` se um segredo for impresso.
* A chave vai **somente no header HTTPS** (`chave-api-dados`), nunca na URL — então não aparece em logs, mensagens de exceção ou histórico de navegação. Nenhum header é gravado na Bronze.
* `run.sh` não aceita a chave como argumento (isso deixaria a chave no histórico do shell).
* Opcional: `pre-commit` com **gitleaks** (`.pre-commit-config.yaml`) bloqueia commits com segredos.
* Se a chave vazar: gere uma nova no portal e atualize o segredo (`databricks secrets put-secret ...`).

---

## 5. Transformações (Spark SQL — `src/bpc_pipeline/transform.py`)

| # | Transformação | Implementação |
|---|---|---|
| T1 | **Desaninhamento** | `LATERAL VIEW explode(from_json(...))` no array do BPC; 3 níveis de `explode` + `explode` de **MAP** no SIDRA; achatamento de structs aninhados (incluindo campos com hífen, como `` `regiao-imediata` ``). |
| T2 | **Padronização de tipos e datas** | Código IBGE com 7 dígitos (`lpad`), `DECIMAL(18,2)`, `INT`/`BIGINT` com `try_cast`, `make_date` para a competência, `try_to_timestamp` com dois formatos para `dataReferencia`. |
| T3 | **Tratamento de nulos** | Descarte de registros sem métrica; `COALESCE` com fonte alternativa (nome/UF do IBGE; hierarquia nova do IBGE quando a microrregião é nula); `'NAO INFORMADO'` em categorias; `NULLIF` contra divisão por zero; `'...'` do SIDRA → NULL. |
| T4 | **Deduplicação** | `ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ingested_at DESC)` em todas as Silver. |
| T5 | **Cruzamento de fontes (JOIN)** | BPC × municípios × população com `/*+ BROADCAST */`. |
| T6 | **Enriquecimento de métricas** | Beneficiários por mil habitantes, valor per capita, valor médio do benefício, variação mensal (`LAG`), participação no valor da UF e ranking de cobertura na UF (`DENSE_RANK`), agregado por UF. |

---

## 6. Data Quality (`src/bpc_pipeline/quality.py`)

Validações automáticas: **schema** (colunas e tipos da Gold), **volume**, **unicidade** da chave em todas as tabelas, **null ratio** de todas as colunas (limites por criticidade), **testes de intervalo** (ano = 2026, mês 1–12, valores ≥ 0, população > 0, taxa por mil entre 0 e 1000, código IBGE com 7 dígitos, valor médio plausível), **integridade referencial** (municípios do BPC encontrados no IBGE), **reconciliação** Silver × Gold (o JOIN não pode perder nem duplicar beneficiários) e **completude da extração**.

Tudo é calculado em **uma única agregação por tabela**. Saídas:

* `output/dq_report.json` — relatório completo: escopo, tempos por etapa, métricas HTTP (requisições, retentativas, 429, 5xx, throughput), checks e perfil de nulos;
* `output/dq_checks.csv` — os checks em formato tabular;
* `<cat>.<schema>.dq_historico` — histórico de todas as execuções (Delta, append).

Se algum check **CRÍTICO** reprovar, o pipeline termina com erro (o Job fica vermelho). Os checks ALTA/MÉDIA viram alertas no relatório.

---

## 7. Como executar

### Databricks (recomendado)
Siga **[`docs/PASSO_A_PASSO_DATABRICKS.md`](docs/PASSO_A_PASSO_DATABRICKS.md)**. Resumo: Git folder → secret scope → `00_setup_ambiente` → `01_pipeline_bpc` → `02_analise_e_entrega`.

### Local
```bash
cp .env.example .env          # edite e coloque sua chave
./run.sh --ufs PE --limite-municipios 20    # teste rápido
./run.sh                                    # Norte + Nordeste completo
```
Variáveis de ambiente:

| Variável | Obrigatória | Descrição |
|---|---|---|
| `PORTAL_TRANSPARENCIA_API_KEY` | sim (local) | Token do Portal da Transparência |
| `PORTAL_BASE_URL` / `IBGE_BASE_URL` | não | Sobrescrevem as URLs (usado nos testes com APIs simuladas) |

Parâmetros de `run.sh`: `--ufs` (UFs, `NORTE`, `NORDESTE` ou `ALL`), `--meses` (`ALL`, `1-6`, `1,2,3`), `--limite-municipios`, `--rate-dia`, `--rate-noite`, `--workers`, `--base-path`, `--output-path`.

### Testes
```bash
PYTHONPATH=src python -m pytest -q
```
Inclui um teste ponta a ponta com um **servidor que simula as duas APIs** (`tests/mock_apis.py`), que injeta erros 429 e 500. Ele valida retentativas, filtro N/NE, EXPLODE, JOIN, Data Quality, Parquet Snappy particionado, a carga incremental e se a chave aparece em algum arquivo gravado.

---

## 8. Estrutura do repositório

```
├── README.md
├── requirements.txt          # dependências fixadas (execução local)
├── run.sh                    # execução local ponta a ponta
├── .env.example              # modelo de credenciais (o .env real é ignorado pelo Git)
├── .gitignore
├── .pre-commit-config.yaml   # gitleaks (opcional)
├── docs/
│   └── PASSO_A_PASSO_DATABRICKS.md
├── notebooks/                # notebooks Databricks (formato .py "source")
│   ├── 00_setup_ambiente.py
│   ├── 01_pipeline_bpc.py
│   └── 02_analise_e_entrega.py
├── src/bpc_pipeline/
│   ├── config.py             # parâmetros, escopo, limites
│   ├── security.py           # chave de API (Secrets/env/.env), SecretStr, redação de logs
│   ├── http_client.py        # rate limiter, retentativas, backoff, métricas
│   ├── extract_ibge.py       # API do IBGE -> Bronze
│   ├── extract_portal.py     # API do Portal -> Bronze (paralelo + incremental)
│   ├── storage.py            # Delta/Parquet, exportação Snappy
│   ├── transform.py          # Spark SQL: Silver e Gold
│   ├── quality.py            # Data Quality + relatório
│   ├── pipeline.py           # orquestração das etapas
│   └── main.py               # CLI local
├── tests/
│   ├── mock_apis.py          # simulador das duas APIs
│   ├── test_unitarios.py
│   └── test_pipeline_e2e.py
└── output/                   # entregável: Parquet particionado + dq_report.json + dq_checks.csv
```

### Dicionário da Gold (`gold_bpc_municipio_mes`, grão município × mês)

| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo_ibge` | string | Código IBGE do município (7 dígitos) |
| `ano_mes`, `ano`, `mes`, `data_competencia` | int/date | Competência do BPC |
| `nome_municipio`, `uf_sigla`, `uf_nome`, `regiao_nome`, `mesorregiao_nome`, `microrregiao_nome` | string | Território (IBGE) |
| `quantidade_beneficiados` | bigint | Beneficiários do BPC no mês |
| `valor_total` | decimal | Valor pago no mês (R$) |
| `populacao_estimada`, `ano_populacao` | bigint/int | Estimativa populacional do IBGE e o ano usado |
| `beneficiarios_por_mil_hab` | decimal | Cobertura: beneficiários ÷ população × 1000 |
| `valor_per_capita` | decimal | Valor ÷ população |
| `valor_medio_beneficio` | decimal | Valor ÷ beneficiários |
| `variacao_mensal_beneficiarios_pct` | decimal | Variação % frente ao mês anterior |
| `participacao_valor_uf_pct` | decimal | % do valor da UF no mês |
| `ranking_cobertura_uf` | int | Posição do município na UF por cobertura |
| `flag_municipio_ibge`, `flag_populacao` | boolean | Indicadores de integridade do cruzamento |
