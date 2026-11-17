# Passo a passo: implementação no Databricks

Siga na ordem. Tempo total: ~20 min de configuração + o tempo da carga (de ~36 a ~62 min, dependendo do horário).

```
 PASSO 1  Código no GitHub
 PASSO 2  Pasta Git no Databricks  ──►  /Workspace/Users/<você>/bpc-pipeline
 PASSO 3  Chave da API no Databricks Secrets   (nunca no código!)
 PASSO 4  Rodar notebooks/00_setup_ambiente     (uma vez só)
 PASSO 5  Rodar notebooks/01_pipeline_bpc       (teste com 20 municípios → carga completa)
 PASSO 6  Rodar notebooks/02_analise_e_entrega  (conferir + copiar output/ para o repo)
 PASSO 7  Commit & push → enviar o link do GitHub
 (extra)  Agendar um Job à meia-noite para carregar 43% mais rápido
```

---

## Pré-requisitos

| Item | Onde conseguir |
|---|---|
| Workspace Databricks (Free Edition serve) | https://www.databricks.com/learn/free-edition |
| Conta no GitHub | https://github.com |
| Chave da API do Portal da Transparência (gratuita) | https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email — exige login gov.br prata ou ouro; a chave chega por e-mail |

> **Regra de ouro:** a chave só pode existir em dois lugares: no seu e-mail e no **Databricks Secrets**. Não cole a chave em nenhum notebook, arquivo, chat, print ou commit.

---

## PASSO 1 — Colocar o código no GitHub

1. No GitHub, clique em **New repository** → nome `bpc-pipeline` → **Public** → **Create repository**. Não marque "Add README".
2. Descompacte o `bpc-pipeline.zip` no seu computador e envie os arquivos:
   * **Pelo site (mais simples):** na página do repositório vazio, clique em **uploading an existing file**, arraste **o conteúdo** da pasta `bpc-pipeline` (não a pasta em si) e clique em **Commit changes**.
     Atenção: arquivos que começam com ponto (`.gitignore`, `.env.example`, `.pre-commit-config.yaml`) ficam ocultos no Windows/macOS. Ative "mostrar arquivos ocultos" antes de arrastar.
   * **Pelo terminal:**
     ```bash
     cd bpc-pipeline
     git init && git add . && git commit -m "Pipeline BPC x Populacao"
     git branch -M main
     git remote add origin https://github.com/<seu-usuario>/bpc-pipeline.git
     git push -u origin main
     ```
3. Confira no GitHub que **não existe** nenhum arquivo `.env`, só o `.env.example`.

---

## PASSO 2 — Criar a pasta Git no Databricks

1. Conecte o GitHub (uma vez): avatar no canto superior direito → **Settings** → **Linked accounts** → **Add Git credential** → provedor **GitHub** → **Link Git account** e autorize.
2. No menu lateral, clique em **Workspace** → **Users** → **seu e-mail**.
3. Clique em **Create** → **Git folder**.
4. Em **Git repository URL**, cole `https://github.com/<seu-usuario>/bpc-pipeline` → **Create Git folder**.

Resultado esperado:

```
/Workspace/Users/<seu-email>/bpc-pipeline/
   ├── notebooks/   00_setup_ambiente · 01_pipeline_bpc · 02_analise_e_entrega
   ├── src/bpc_pipeline/   (código Python)
   └── docs/, tests/, output/, README.md ...
```

> Os notebooks importam o código de `../src`. **Não mova os notebooks para fora da pasta Git**, senão aparece o erro `ModuleNotFoundError: bpc_pipeline`.

---

## PASSO 3 — Guardar a chave da API no Databricks Secrets

### Opção A (recomendada): Databricks CLI no seu computador

1. Instale o CLI:
   * Windows: `winget install Databricks.DatabricksCLI`
   * macOS: `brew tap databricks/tap && brew install databricks`
2. Faça login. A URL é a do seu workspace, que aparece no navegador:
   ```bash
   databricks auth login --host https://<seu-workspace>.cloud.databricks.com
   ```
3. Crie o scope e grave a chave:
   ```bash
   databricks secrets create-scope transparencia
   databricks secrets put-secret transparencia portal_api_key
   ```
   O último comando **pede a chave de forma interativa**. Cole a chave e confirme.
   **Não use `--string-value`**: com essa opção a chave ficaria salva no histórico do terminal.
4. Confira (aparece só o nome, nunca o valor):
   ```bash
   databricks secrets list-secrets transparencia
   ```

### Opção B: sem CLI (só se a opção A não funcionar)
No PASSO 4, use a seção **"(OPCIONAL) Cadastrar a chave sem o Databricks CLI"** no fim do notebook `00_setup_ambiente` (células A, B e C). Depois de gravar a chave: **Run → Clear → Clear all outputs**.

> **Boas práticas incluídas no código:** a chave é lida com `dbutils.secrets.get` e guardada em `SecretStr`, então `print(api_key)` mostra `**********`. Logs passam por um filtro que mascara a chave. Ela vai só no header HTTPS, nunca na URL, e nada disso é gravado em disco.

---

## PASSO 4 — Preparar o ambiente (notebook `00_setup_ambiente`)

1. Abra `notebooks/00_setup_ambiente`.
2. No seletor de compute (canto superior direito), escolha **Serverless**.
3. Confira os widgets no topo. Os padrões são: catálogo `workspace`, schema `bpc_pipeline`, volume `arquivos`, scope `transparencia`, segredo `portal_api_key`.
   Se você não tiver permissão no catálogo `workspace`, use outro catálogo seu.
4. Clique em **Run all**.

Resultado esperado:
```
OK -> schema workspace.bpc_pipeline e volume /Volumes/workspace/bpc_pipeline/arquivos
Scope 'transparencia' encontrado. Segredos cadastrados: ['portal_api_key']
Portal da Transparência -> HTTP 200
Chave OK. Amostra: [{...}]
IBGE -> HTTP 200 | últimos períodos de população: [...]
```

---

## PASSO 5 — Rodar o pipeline (notebook `01_pipeline_bpc`)

### 5.1 Teste rápido (~1 minuto)
1. Abra `notebooks/01_pipeline_bpc` (compute **Serverless**).
2. No widget **`limite_municipios`**, coloque `20`.
3. Clique em **Run all**. As 6 etapas devem terminar sem erro, e a etapa 5 deve mostrar `status_geral`.

### 5.2 Carga completa (Norte + Nordeste)
1. Volte **`limite_municipios`** para `0`. Mantenha `ufs = NORTE,NORDESTE` e `meses = ALL`.
2. Clique em **Run all**. Na etapa 2 o log mostra o plano e o andamento:
   ```
   Plano BPC: 2244 municípios x 10 meses = 22440 pares | já na Bronze: 200 | pendentes: 22240 | taxa atual: 360 req/min | tempo estimado: ~61.8 min
   Progresso: 1112/22240 (5%) | 358 pares/min | ETA ~59.0 min
   ```
   Os 20 municípios do teste **não são baixados de novo**: a carga é incremental.
3. **Se cair ou você interromper:** clique em **Run all** de novo. O pipeline continua de onde parou.

### Ordem de execução e o que cada célula faz

| Ordem | Célula | Tempo típico |
|---|---|---|
| 1 | Parâmetros (widgets) | instantâneo |
| 2 | Importa `src/` | instantâneo |
| 3 | Contexto + chave (Secrets) | instantâneo |
| 4 | **Etapa 1** IBGE → Bronze | ~5 s |
| 5 | **Etapa 2** Portal → Bronze | ~36–62 min (1ª vez); segundos depois disso |
| 6 | **Etapa 3** Silver | ~30 s |
| 7 | **Etapa 4** Gold | ~20 s |
| 8 | **Etapa 5** Data Quality | ~15 s |
| 9 | **Etapa 6** Exportação + gate | ~20 s |

---

## PASSO 6 — Conferir e preparar a entrega (notebook `02_analise_e_entrega`)

1. Abra `notebooks/02_analise_e_entrega` e clique em **Run all**.
2. Confira as consultas: top municípios por cobertura, evolução por região e histórico de DQ.
3. A última célula copia `output/` do Volume para a pasta `output/` do repositório.
   * Se a cópia falhar: **Catalog** → `workspace` → `bpc_pipeline` → **Volumes** → `arquivos` → `output`. Baixe os arquivos e coloque-os na pasta `output/` do repositório pelo GitHub.

Onde ficou cada coisa:

| O quê | Local |
|---|---|
| Tabelas Delta | **Catalog** → `workspace.bpc_pipeline` → `silver_*`, `gold_bpc_municipio_mes`, `gold_bpc_uf_mes`, `dq_historico` |
| JSON bruto (Bronze) | `/Volumes/workspace/bpc_pipeline/arquivos/bronze/` |
| Parquet final + relatórios | `/Volumes/workspace/bpc_pipeline/arquivos/output/` |

---

## PASSO 7 — Commit e envio

1. Dentro da pasta Git, clique no **nome da branch** (botão no topo do notebook ou da pasta).
2. Marque os arquivos de `output/` (Parquet, `dq_report.json`, `dq_checks.csv`).
3. Escreva a mensagem (ex.: `Output da carga N/NE 2026`) → **Commit & Push**.
4. Confira no GitHub que `output/` tem as pastas `ano=2026/mes=*` e que **não há `.env`**.
5. Envie o link do repositório pelo formulário até **23/11/2026, 08h00**.

---

## (Extra) Agendar a carga para a madrugada (~43% mais rápido)

1. **Jobs & Pipelines** → **Create** → **Job**.
2. Task: tipo **Notebook**, source **Workspace**, caminho `.../bpc-pipeline/notebooks/01_pipeline_bpc`, compute **Serverless**.
3. **Schedules & Triggers** → **Add trigger** → **Scheduled** → diário às **00:05**, fuso **America/Sao_Paulo**.
4. Se algum check CRÍTICO de Data Quality reprovar, o Job termina **com falha** e você recebe o alerta.

---

## Problemas comuns

| Sintoma | Causa | Solução |
|---|---|---|
| `ModuleNotFoundError: bpc_pipeline` | O notebook está fora da pasta Git ou foi movido | Abra o notebook dentro de `/Workspace/Users/<você>/bpc-pipeline/notebooks/` |
| `Secret scope 'transparencia' não existe` | O PASSO 3 não foi feito | Faça o PASSO 3 (opção A ou B) |
| `HTTP 401/403 ... chave inválida` | Chave errada, expirada ou suspensa | Gere uma nova chave no portal e repita `put-secret` |
| `RateLimitAbort` | Muitos HTTP 429 (outra execução usando a mesma chave?) | Espere alguns minutos, reduza `rate_dia` (ex.: 300) e rode de novo (é incremental) |
| `PERMISSION_DENIED` ao criar o schema | Sem permissão no catálogo | Mude o widget `catalog` para um catálogo em que você possa criar schemas |
| `Gate de Data Quality reprovado` | Check crítico falhou | Abra `output/dq_checks.csv` ou a tabela `dq_historico` e veja qual check falhou |
| Etapa 2 abaixo de ~300 pares/min | Latência alta da API | Aumente `workers` para 16–20. O limiter continua garantindo o teto |
