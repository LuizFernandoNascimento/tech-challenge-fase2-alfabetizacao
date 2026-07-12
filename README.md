# Pipeline Híbrido para Análise da Alfabetização no Brasil

Tech Challenge — Fase 2 (POSTECH/FIAP, AI Scientist). Pipeline de dados híbrido (batch + streaming) em nuvem, seguindo Arquitetura Medalhão (Bronze/Silver/Gold), para integrar e disponibilizar o indicador **Criança Alfabetizada** do INEP.

> Status: em construção incremental. Este README é atualizado a cada camada entregue (ver histórico de commits/PRs).

## Índice

- [Contexto do problema](#contexto-do-problema)
- [Desafio educacional e o indicador de alfabetização](#desafio-educacional-e-o-indicador-de-alfabetização)
- [Fontes de dados](#fontes-de-dados)
- [Arquitetura da solução](#arquitetura-da-solução) *(detalhada progressivamente)*
- [Tecnologias e justificativa](#tecnologias-e-justificativa)
- [Decisões arquiteturais e trade-offs](#decisões-arquiteturais-e-trade-offs) *(seção futura)*
- [Qualidade de dados](#qualidade-de-dados) *(seção futura)*
- [Monitoramento e FinOps](#monitoramento-e-finops) *(seção futura)*
- [Aplicação em IA](#aplicação-em-ia) *(seção futura)*
- [Como rodar](#como-rodar)

## Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico de um país. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do Ensino Fundamental.

Para apoiar essa política, o INEP realizou em 2023 a **Pesquisa Alfabetiza Brasil**, definindo o ponto de corte de **743 pontos** na escala de proficiência do Saeb como o patamar a partir do qual uma criança é considerada alfabetizada. A partir disso, foi criado o **Indicador Criança Alfabetizada**: o percentual de estudantes que atingem essa proficiência. A meta nacional é alfabetizar 100% das crianças até 2030.

Entender os fatores que influenciam a alfabetização exige integrar múltiplas fontes — metas nacionais/estaduais/municipais, dados territoriais, microdados educacionais e indicadores de desempenho — o que só é viável com uma pipeline de dados robusta, escalável e com qualidade auditável.

## Desafio educacional e o indicador de alfabetização

Este projeto atua como se fosse o time de engenharia de dados de uma organização pública de análise educacional, construindo a infraestrutura de dados que **sustenta** a análise do indicador — não a análise em si. O objetivo técnico é: ingestão de múltiplas fontes heterogêneas, tratamento e padronização, integração entre bases, disponibilização de uma camada analítica confiável, monitoramento operacional e controle de custo.

## Fontes de dados

Todas as fontes vêm da plataforma [Base dos Dados](https://basedosdados.org/), tabela do indicador Criança Alfabetizada (INEP). Ver detalhamento completo em [`docs/data_dictionary.md`](docs/data_dictionary.md).

A Base dos Dados já publica esse dataset como tabelas **BigQuery públicas** (projeto `basedosdados`, dataset `br_inep_avaliacao_alfabetizacao`) — por isso a ingestão batch consulta essas tabelas diretamente por *cross-project query*, em vez de depender de um export em CSV. A única exceção é a dimensão IBGE (UF/Município com nome/região), que não existe como tabela pública equivalente.

| Entidade | Tabela de origem (`basedosdados.br_inep_avaliacao_alfabetizacao.*`) | Volume |
|---|---|---|
| Dados de alunos (microdados) | `alunos` | ~3,87M linhas |
| Meta Alfabetização Brasil | `meta_alfabetizacao_brasil` | 3 linhas |
| Meta Alfabetização UF | `meta_alfabetizacao_uf` | 81 linhas |
| Meta Alfabetização Município | `meta_alfabetizacao_municipio` | ~10,7k linhas |
| Avaliação Alfabetização UF | `uf` | 145 linhas |
| Avaliação Alfabetização Município | `municipio` | ~24k linhas |
| Dimensão UF/Município (IBGE, complementar) | API IBGE `localidades/municipios` (fora da Base dos Dados) | 5.571 municípios |

## Arquitetura da solução

*(Diagrama completo do fluxo de dados será adicionado ao final do desenvolvimento, no incremento de documentação final.)*

Visão geral adotada:

- **Cloud**: Google Cloud Platform (projeto dedicado `tech-challenge-alfabetiza-25`, região `southamerica-east1`).
- **Data Lake**: Google Cloud Storage (bucket `tech-challenge-alfabetiza-25-raw`, prefixo `bronze/<tabela>/<arquivo>`).
- **Data Warehouse / Lakehouse analítico**: BigQuery, com datasets separados `bronze`, `silver`, `gold` — particionamento por `ano` e clusterização por `sigla_uf`/`id_municipio` planejados a partir da camada Silver.
- **Ingestão batch** ([`src/bronze/batch_ingest.py`](src/bronze/batch_ingest.py)): as 5 tabelas de metas/resultados são lidas direto das tabelas públicas da Base dos Dados no BigQuery (`basedosdados.br_inep_avaliacao_alfabetizacao.*`) e gravadas na nossa Bronze com `_ingested_at`/`_source_file`; a dimensão IBGE continua vindo de CSV -> GCS -> BigQuery, já que não existe tabela pública equivalente. **A Bronze é append-only**: cada execução acrescenta um novo snapshot (identificado por `_ingested_at`) em vez de sobrescrever o anterior, preservando o histórico completo — quem lê a Bronze depois (a Silver) é responsável por filtrar o snapshot mais recente quando quiser o estado atual.
- **Ingestão streaming (simulada)** ([`src/streaming/producer.py`](src/streaming/producer.py) + [`src/streaming/consumer.py`](src/streaming/consumer.py)): um produtor consulta a tabela pública `alunos` no BigQuery e replaya as linhas como mensagens Pub/Sub, a uma taxa controlada; um consumidor faz *pull* das mensagens, acumula micro-lotes e grava via streaming insert na tabela `bronze.dados_alunos_streaming`.
- **Dimensão de referência** ([`src/bronze/ibge_reference.py`](src/bronze/ibge_reference.py)): busca nome de UF/município/região na API pública do IBGE, já que nenhuma fonte do INEP traz esses atributos — apenas códigos.
- **Provisionamento de infraestrutura** ([`src/bronze/setup_infra.py`](src/bronze/setup_infra.py)): script idempotente que cria bucket, datasets e tópico/assinatura Pub/Sub via `google-cloud-*` (sem Terraform, dado o tamanho do projeto).
- **Camadas**: Bronze (dados brutos, sem transformação) → Silver (limpeza, padronização, normalização de chaves, integração/dimensões, validação de qualidade) → Gold (marts analíticos prontos para BI/ML).

### Bronze — o que já está implementado e validado

Rodado contra o projeto GCP real:

| Tabela Bronze | Linhas carregadas |
|---|---|
| `meta_alfabetizacao_brasil` | 3 |
| `meta_alfabetizacao_uf` | 81 |
| `meta_alfabetizacao_municipio` | 10.704 |
| `avaliacao_alfabetizacao_uf` | 145 |
| `avaliacao_alfabetizacao_municipio` | 23.995 |
| `ibge_municipios` | 5.571 |
| `dados_alunos_streaming` (via Pub/Sub, amostra de teste) | 1.126+ eventos (190 alunos distintos na primeira rodada) |

**Achado relevante nº 1**: na simulação streaming, o número de linhas gravadas superou o de alunos distintos publicados — o Pub/Sub garante *at-least-once delivery*, então mensagens podem ser reentregues e gravadas mais de uma vez. Isso é esperado e **correto** para a Bronze (que preserva os dados brutos exatamente como chegaram, duplicados inclusive); a deduplicação é responsabilidade da camada Silver.

**Achado relevante nº 2 — CSV local vs. BigQuery público**: a ingestão batch originalmente subia os CSVs (já baixados localmente) para o GCS e carregava de lá. Ao revisar a fonte oficial, percebemos que a Base dos Dados já publica esse dataset como tabelas BigQuery públicas (`basedosdados.br_inep_avaliacao_alfabetizacao`) — então migramos a ingestão para consultar essas tabelas direto, por *cross-project query*, eliminando a necessidade de baixar/reenviar CSV para 5 das 6 fontes (a dimensão IBGE continua sendo a exceção). Um efeito colateral bom: `meta_alfabetizacao_uf` passou de 54 para 81 linhas, porque a tabela pública está mais atualizada que o CSV estático que tínhamos.

Um detalhe técnico que essa mudança expôs: a tabela pública da Base dos Dados vive na multi-region `US`, enquanto nosso dataset Bronze vive em `southamerica-east1`. O BigQuery não permite um `CREATE TABLE ... AS SELECT` cross-region direto (todas as tabelas referenciadas num job precisam estar na mesma location) — a solução foi rodar a consulta em `US`, trazer o resultado (poucas linhas, exceto a tabela de alunos que já é tratada à parte pelo streaming) para o processo Python, e gravar na nossa location via `load_table_from_json`. Ver comentários em `bronze/batch_ingest.py::load_from_basedosdados`.

### Bronze — deployado como Cloud Functions (Gen2)

A ingestão batch e o consumidor streaming, que antes só existiam como scripts rodados manualmente no terminal, agora também rodam **dentro do GCP** como Cloud Functions Gen2 — o que significa que o código fica visível e executável no console, sem depender de uma máquina local.

| Recurso | Nome | Tipo de acionamento | Estado |
|---|---|---|---|
| Cloud Function | `bronze-batch-ingest` | HTTP (chamado pelo Cloud Scheduler) | `ACTIVE` |
| Cloud Function | `bronze-streaming-consumer` | Pub/Sub (`dados-alunos-stream`), uma invocação por mensagem | `ACTIVE` |
| Cloud Scheduler | `bronze-batch-schedule` | Cron diário `0 6 * * *` (America/Sao_Paulo) | **`PAUSED`** — criado só para fins de demonstração/estudo, deliberadamente nunca disparado, para não gerar custo/execuções não desejadas |

**Onde ver isso no console** (login com a conta do projeto):
- Cloud Functions (código-fonte inline, aba "Source"): https://console.cloud.google.com/functions/list?project=tech-challenge-alfabetiza-25
- Cloud Scheduler (confirma o job `PAUSED`): https://console.cloud.google.com/cloudscheduler?project=tech-challenge-alfabetiza-25
- Logs de execução: https://console.cloud.google.com/logs/query?project=tech-challenge-alfabetiza-25

**Detalhe de implementação**: ambas as funções compartilham um único `src/main.py` (convenção do Cloud Functions Gen2 — um arquivo, várias funções selecionadas por `--entry-point` no deploy). A função batch relê arquivos já existentes no GCS (não faz upload — não há disco local na nuvem); a função streaming processa **uma mensagem por invocação** (sem buffer local, diferente do micro-lote do consumidor manual), o que é a essência de uma função stateless acionada por evento.

### Silver — Processamento, Deduplicação e Enriquecimento

A camada Silver consome as tabelas brutas da camada Bronze e realiza transformações estruturais diretamente no BigQuery (FinOps), garantindo dados limpos, tipados e integrados:

- **Deduplicação de Eventos**: Deduplica os dados de `dados_alunos_streaming` selecionando o registro mais recente por `id_aluno` e `ano` (com base no metadado `_ingested_at`), contornando a reentrega do Pub/Sub.
- **Padronização**: Padroniza os códigos de municípios (`id_municipio`) preenchendo-os com zeros à esquerda (7 dígitos). O mapeamento de `rede` para rótulo textual (`Federal`/`Estadual`/`Municipal`/`Privada`) só acontece em `dados_alunos_streaming` (onde `rede` chega como código numérico 1-4); nas tabelas `avaliacao_alfabetizacao_uf/municipio` (também código numérico na Bronze) `rede` vira apenas `STRING` do próprio código, sem tradução — nas tabelas `meta_alfabetizacao_*`, `rede` já chega como texto (ex.: "Pública") direto da fonte.
- **Enriquecimento e Integridade Referencial**: Faz `INNER JOIN` com `dim_localidades` (gerada a partir de `ibge_municipios`) para incluir `sigla_uf` nas tabelas municipais de metas/avaliações e nos dados de alunos. Diferente de uma versão anterior (que usava `LEFT JOIN` + `COALESCE(..., 'ND')`), registros sem correspondência **não** entram mascarados na tabela Silver — vão para uma tabela `silver.quarentena_*` correspondente, com o motivo da rejeição e quando foi detectado. Isso garante que a integridade referencial das tabelas Silver é verdadeira "por construção", não apenas verificada depois.
- **Histórico e snapshots**: como a Bronze agora é *append-only* (ver seção Bronze), a Silver sempre lê explicitamente o snapshot mais recente de cada tabela (`WHERE _ingested_at = (SELECT MAX(_ingested_at) ...)`) antes de aplicar as transformações — ela representa o estado atual, enquanto a Bronze acumula o histórico completo.
- **Performance e FinOps**: Aplica particionamento físico por `ano` (usando `RANGE_BUCKET`) e clusterização pelas colunas de consulta frequente `sigla_uf` e `id_municipio`.

### Silver — deployado como Cloud Functions (Gen2)

A transformação e validação da camada Silver também foram integradas ao `src/main.py` como um endpoint HTTP (`silver_transform_http`), permitindo que a camada seja acionada na nuvem como uma Cloud Function Gen2 de forma totalmente serverless.

| Recurso | Nome | Tipo de acionamento | Estado |
|---|---|---|---|
| Cloud Function | `silver-transform` | HTTP (chamada via webhook ou Scheduler) | `ACTIVE` |

### Gold — Camada Analítica e Comparativa (Marts)

A camada Gold consome as tabelas consolidadas da camada Silver para produzir datasets prontos para BI (dashboards), análises estatísticas e modelos de IA/ML. O foco está na comparação entre metas e resultados observados, unificando dados históricos e dados em streaming quase tempo real.

Tabelas criadas:
- **`mart_comparativo_municipio`**: Nível Município + Rede + Ano. Consolidado que junta os resultados das avaliações municipais da Silver, as respectivas metas e os dados de streaming de alunos agregados por município, calculando desvios e sinalizadores se a meta foi batida.
- **`mart_comparativo_uf`**: Nível Estado (UF) + Rede + Ano. Similar à tabela municipal, mas agregada na escala estadual (com nome do estado e região trazidos via dimensão localidades).
- **`mart_comparativo_brasil`**: Nível Nacional (Brasil) + Rede + Ano. Consolidado das metas nacionais e das taxas gerais de alfabetização por rede, incluindo agregação nacional do fluxo streaming.

**Performance e FinOps na Gold**: Assim como na Silver, as tabelas `mart_comparativo_municipio` e `mart_comparativo_uf` são fisicamente particionadas por `ano` (com `RANGE_BUCKET`) e clusterizadas pelas chaves primárias e geográficas (`sigla_uf`, `id_municipio`), garantindo que queries analíticas leiam apenas frações das tabelas e tenham custos mínimos.

### Gold — deployado como Cloud Functions (Gen2)

A orquestração e testes de qualidade da camada Gold também foram expostos como um endpoint HTTP (`gold_transform_http`), permitindo que a camada seja rodada sob demanda de forma serverless e escalável na nuvem.

| Recurso | Nome | Tipo de acionamento | Estado |
|---|---|---|---|
| Cloud Function | `gold-transform` | HTTP (chamada via webhook ou Scheduler) | `ACTIVE` |

## Tecnologias e justificativa

| Componente | Escolha | Por quê |
|---|---|---|
| Cloud | **GCP** | Serverless-first (BigQuery separa storage/compute, Cloud Functions escalam a zero); "Base dos Dados" (fonte original) é nativa de BigQuery, o que facilitaria ingestão direta em uma evolução futura. |
| Data lake | Google Cloud Storage | Object storage simples, barato, integra nativamente com BigQuery (`LOAD ... FROM URI`). |
| Warehouse/Lakehouse | BigQuery | Serverless, particionamento/clusterização nativos, cobrança separada de storage e compute — chave para o FinOps do projeto. |
| Streaming | Pub/Sub | Serviço gerenciado equivalente ao Kafka visto no curso, sem operar cluster; adequado ao volume de simulação do desafio. |
| Orquestração batch | Cloud Scheduler + Cloud Functions | Evita o custo de um cluster Airflow/Composer sempre ativo para uma pipeline deste porte — decisão de FinOps documentada em `docs/finops.md` (incremento futuro). Deployado (ver seção acima), mas o Scheduler é mantido `PAUSED` deliberadamente. |
| Linguagem | Python (`google-cloud-storage`, `google-cloud-bigquery`, `google-cloud-pubsub`) | Alinhado ao que foi usado nos hands-on do curso (Pandas/PySpark, clientes Python de nuvem). |

## Decisões arquiteturais e trade-offs

No desenvolvimento desta pipeline híbrida para a pós em AI Scientist, pesamos diversas escolhas estruturais e custos operacionais:

1. **Batch vs. Streaming**:
   - *Streaming (Pub/Sub + Inserções contínuas)*: Utilizado para simular a chegada rápida de microdados de alunos (`dados_alunos_streaming`). Oferece baixa latência e permite análises quase tempo real, essencial para o acompanhamento dinâmico durante períodos de aplicação de provas.
   - *Batch (Queries agendadas)*: Utilizado para dados históricos e tabelas de metas que mudam raramente. A abordagem híbrida nos dá o melhor de dois mundos: consistência e custo controlado com processamento em lote diário/mensal para a maior parte do data lake, e agilidade em tempo real nas tabelas de eventos.

2. **Data Lake (GCS) vs. Data Warehouse (BigQuery)**:
   - Mantemos o armazenamento dos dados originais no GCS em sua forma bruta (Bronze/Raw) garantindo a rastreabilidade e a possibilidade de reprocessar toda a história caso surjam novos requerimentos.
   - O BigQuery é usado como Lakehouse, onde residem os esquemas estruturados. Graças à separação física entre armazenamento (barato) e poder de computação (pago sob demanda), podemos manter a arquitetura medalhão com custos reduzidos.

3. **Custo vs. Performance (FinOps)**:
   - Em vez de usar ferramentas com clusters sempre ativos (como Spark ou Airflow), optamos por Cloud Functions que escalam a zero quando não estão processando.
   - Toda a transformação pesada da Silver e Gold ocorre *in-database* no BigQuery via SQL declarativo de alto desempenho, maximizando a eficiência de rede e tirando proveito da arquitetura distribuída do Google.

## Qualidade de dados

A camada Silver inclui um pipeline de qualidade de dados (`src/quality/data_quality.py`) que valida a consistência de todas as tabelas após o processamento no BigQuery:

- **Unicidade de Chaves**: Valida chaves primárias únicas (`id_municipio` em `dim_localidades`, `id_aluno + ano` em `dados_alunos_streaming`, `ano + sigla_uf + rede` em `meta_alfabetizacao_uf`).
- **Valores Nulos**: Garante a ausência de nulos em chaves primárias e colunas de agregação obrigatórias.
- **Integridade Referencial**: confirma que as tabelas Silver (`dados_alunos_streaming`, `avaliacao_alfabetizacao_municipio`, `meta_alfabetizacao_municipio`) não têm `id_municipio` fora de `dim_localidades` — hoje isso é garantido por construção (`INNER JOIN` no `transform_silver.py`), então essas checagens funcionam como teste de regressão.
- **Quarentena, não mascaramento**: registros que não casam com `dim_localidades` não entram na tabela Silver com um valor "curinga" — vão para `silver.quarentena_*` com o motivo da rejeição. O volume de cada tabela de quarentena é reportado como aviso (não trava o pipeline), para dar visibilidade sem impedir que o restante dos dados bons siga adiante.
- **Sanidade de Domínio**: Confirma que campos como `presenca` e `alfabetizado` contêm estritamente `0` ou `1`.

## Monitoramento e FinOps

O pipeline foi desenhado de forma a manter os custos sob controle estrito (FinOps) e garantir observabilidade operacional:

- **Controle de Custos (FinOps)**:
  - Uso intensivo de **Particionamento por Range** no campo `ano` e **Clusterização** por `sigla_uf` e `id_municipio` em todas as tabelas das camadas Silver e Gold. Queries analíticas que filtram por estas colunas escaneiam apenas as partições/clusters de interesse, reduzindo em até 95% os gigabytes processados.
  - O uso de Cloud Functions Gen2 assegura que pagamos apenas pelos milissegundos exatos de execução.
- **Monitoramento e Alertas**:
  - Toda execução escreve logs estruturados que são enviados automaticamente ao **GCP Cloud Logging**.
  - A camada de qualidade de dados (`src/quality/data_quality.py`) age como barreira de segurança. Falhas críticas na unicidade de chaves ou integridade de dados param o pipeline imediatamente, prevenindo a poluição de tabelas analíticas secundárias.
  - Desvios em chaves estrangeiras (municípios inválidos) são automaticamente roteados para tabelas de **Quarentena** e seu volume é reportado como warnings nos logs, permitindo auditorias periódicas sem derrubar o pipeline.

## Aplicação em IA

Os marts analíticos da camada Gold servem como uma base de dados limpa, integrada e consistente, pronta para treinar modelos estatísticos e preditivos:

1. **Modelos de Predição de Alfabetização**:
   - Treinamento de algoritmos de classificação e regressão (como Random Forests, XGBoost ou redes neurais simples) usando a `taxa_alfabetizacao_real` histórica e recursos adicionais de infraestrutura para prever a taxa de alfabetização futura de um município ou escola.
   - Modelagem de risco para identificar municípios propensos a não atingir as metas nacionais do Compromisso Criança Alfabetizada até 2030, permitindo intervenções precoces.
2. **Análise de Desigualdade Educacional**:
   - Clusterização (ex.: K-Means) de municípios baseando-se em proficiência média (`media_portugues_real`), redes de ensino (`rede`) e desvios de metas. Isso possibilita agrupar municípios por vulnerabilidade educacional para entender desigualdades regionais e socioeconômicas.
3. **Políticas Públicas Baseadas em Evidências**:
   - Com dados geográficos e temporais consolidados na Gold, gestores públicos podem rodar testes de impacto de novas políticas públicas de alfabetização através de técnicas de econometria (como Diferença em Diferenças ou Controle Sintético), comparando municípios tratados com seus respectivos grupos de controle baseados na proximidade de metas.

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # preencha com os dados do seu projeto GCP

gcloud auth login
gcloud auth application-default login
gcloud config set project <SEU_PROJECT_ID>
```

### Camada Bronze

```bash
# Provisiona bucket, datasets e tópico/assinatura Pub/Sub (idempotente)
PYTHONPATH=src python -m bronze.setup_infra

# Busca a dimensão de referência UF/Município na API do IBGE
PYTHONPATH=src python -m bronze.ibge_reference

# Ingestão batch: lê as 5 tabelas de metas/resultados direto do BigQuery
# público da Base dos Dados + sobe/carrega a dimensão IBGE via CSV
PYTHONPATH=src python -m bronze.batch_ingest

# Ingestão streaming (simulação): rodar em dois terminais
PYTHONPATH=src python -m streaming.consumer --max-messages 500 --window-seconds 120
PYTHONPATH=src python -m streaming.producer --limit 500 --rate 20
```

### Camada Silver

```bash
# Executa transformações e validações de qualidade localmente
PYTHONPATH=src .venv/bin/python3 -m silver.run_silver
```

### Camada Gold

```bash
# Executa transformações e validações de qualidade da Gold localmente
PYTHONPATH=src .venv/bin/python3 -m gold.run_gold
```

### Deploy das Cloud Functions (Bronze)

```bash
# Função batch (HTTP, chamada pelo Cloud Scheduler)
gcloud functions deploy bronze-batch-ingest \
  --gen2 --region=southamerica-east1 --runtime=python312 \
  --source=src --entry-point=batch_ingest_http --trigger-http --no-allow-unauthenticated \
  --memory=512Mi --timeout=300s \
  --set-env-vars=GCP_PROJECT_ID=<PROJECT_ID>,GCS_BUCKET_RAW=<BUCKET>,BQ_DATASET_BRONZE=bronze

# Função streaming (acionada por mensagem no Pub/Sub)
gcloud functions deploy bronze-streaming-consumer \
  --gen2 --region=southamerica-east1 --runtime=python312 \
  --source=src --entry-point=streaming_consumer_pubsub --trigger-topic=dados-alunos-stream \
  --memory=256Mi --timeout=60s \
  --set-env-vars=GCP_PROJECT_ID=<PROJECT_ID>,BQ_DATASET_BRONZE=bronze

# Cloud Scheduler apontando para a função batch - criado e IMEDIATAMENTE pausado
gcloud scheduler jobs create http bronze-batch-schedule \
  --location=southamerica-east1 --schedule="0 6 * * *" \
  --uri="<URL_DA_FUNCAO_BATCH>" --http-method=POST \
  --oidc-service-account-email=<SERVICE_ACCOUNT> --oidc-token-audience="<URL_DA_FUNCAO_BATCH>"
gcloud scheduler jobs pause bronze-batch-schedule --location=southamerica-east1
```

### Deploy das Cloud Functions (Silver)

```bash
# Função Silver (HTTP, aciona processamento e testes de qualidade)
gcloud functions deploy silver-transform \
  --gen2 --region=southamerica-east1 --runtime=python312 \
  --source=src --entry-point=silver_transform_http --trigger-http --no-allow-unauthenticated \
  --memory=512Mi --timeout=300s \
  --set-env-vars=GCP_PROJECT_ID=<PROJECT_ID>,BQ_DATASET_BRONZE=bronze,BQ_DATASET_SILVER=silver
```

### Deploy das Cloud Functions (Gold)

```bash
# Função Gold (HTTP, aciona processamento e testes de qualidade da Gold)
gcloud functions deploy gold-transform \
  --gen2 --region=southamerica-east1 --runtime=python312 \
  --source=src --entry-point=gold_transform_http --trigger-http --no-allow-unauthenticated \
  --memory=512Mi --timeout=300s \
  --set-env-vars=GCP_PROJECT_ID=<PROJECT_ID>,BQ_DATASET_SILVER=silver,BQ_DATASET_GOLD=gold
```
