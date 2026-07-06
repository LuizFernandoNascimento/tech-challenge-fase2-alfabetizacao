# Status Técnico do Projeto — Handoff

> Documento gerado para dar contexto completo a outra sessão de IA (ou outro engenheiro) continuar o trabalho sem precisar re-derivar decisões já tomadas. Reflete o estado em 2026-07-06.

## 1. Objetivo do projeto

Tech Challenge Fase 2 (POSTECH/FIAP, AI Scientist) — pipeline de dados híbrida (batch + streaming) em nuvem, seguindo Arquitetura Medalhão (Bronze/Silver/Gold), para o indicador **Criança Alfabetizada** (INEP). Peso: 90% da nota da fase. Brief completo em `tech challenge/[IAST] - Tech Challenge - Fase 2.pdf` (fora deste repo, ver seção 7).

Cloud escolhida: **GCP**. Projeto dedicado: `tech-challenge-alfabetiza-25`, região `southamerica-east1`.

## 2. Estado atual: só a camada Bronze está implementada

Silver, Gold, monitoramento/FinOps e o README final **ainda não existem**. Não presuma que existem — confira antes de referenciar.

## 3. Repositório Git

- Remoto: `https://github.com/LuizFernandoNascimento/tech-challenge-fase2-alfabetizacao` (público)
- Local: `/Users/luizfernandonascimento/github/POSTECH_AI_SCIENTIST/module 2/tech-challenge-fase2` (repo próprio, não confundir com o repo maior do curso que fica um nível acima)
- Colaboradores GitHub com acesso `write`: `franfrane`, `renatapraisler` (convites enviados em 2026-07-06, aceite pendente de confirmação)
- Histórico de PRs:
  - PR #1 `feature/bronze-ingestion` → `main` — **MERGED**. Ingestão batch + streaming local.
  - PR #2 `feature/bronze-cloud-deploy` → `main` — **MERGED**. Deploy como Cloud Functions.
  - PR #3 `docs/resumo-time` → `main` — **OPEN** (aguardando o usuário mergear). Doc não-técnico para o time.
- Branch `main` está com HEAD em `731ab0a` (merge do PR #2); PR #3 ainda não incorporado a `main`.

## 4. Estrutura de código

```
tech-challenge-fase2/
├── README.md                    # visão geral, arquitetura, tecnologias (seções Silver/Gold/FinOps/IA ainda são placeholders "seção futura")
├── docs/
│   ├── data_dictionary.md       # dicionário completo das 6 fontes + gap de dimensão UF/Município
│   ├── resumo_para_time.md      # doc não-técnico (PR #3, ainda não mergeado)
│   └── status_tecnico.md        # este arquivo
├── data/sample/                 # amostras (~2k linhas) de cada CSV para dev local
├── requirements.txt              # deps completas para dev local (inclui pandas/pyarrow/requests)
├── .env / .env.example           # config do projeto GCP (GCP_PROJECT_ID, GCS_BUCKET_RAW, datasets, tópico Pub/Sub)
└── src/
    ├── main.py                   # entry points das Cloud Functions Gen2 (batch_ingest_http, streaming_consumer_pubsub)
    ├── requirements.txt           # deps enxutas só do deploy (sem pandas/pyarrow/requests)
    ├── bronze/
    │   ├── config.py              # load_settings() lê .env
    │   ├── sources.py             # catálogo fonte->tabela (BATCH_SOURCES, STREAMING_SOURCE, IBGE_REFERENCE_SOURCE), RAW_DATA_DIR
    │   ├── ibge_reference.py      # busca dimensão UF/Município na API IBGE (fetch_municipios, save_csv)
    │   ├── setup_infra.py         # provisiona bucket/datasets/tópico+assinatura (idempotente)
    │   ├── batch_ingest.py        # run() = bootstrap local (upload+load); reload_bronze_from_gcs() = usado pela Cloud Function
    │   └── streaming_schema.py    # SCHEMA/coerce_row/ensure_streaming_table compartilhados entre consumer.py e main.py
    ├── streaming/
    │   ├── producer.py            # replay do CSV de alunos -> Pub/Sub, taxa controlável (--limit, --rate)
    │   └── consumer.py            # pull manual + micro-lote -> BigQuery (uso local/manual, não é o caminho de produção)
    ├── silver/__init__.py         # vazio, placeholder
    ├── gold/__init__.py           # vazio, placeholder
    └── quality/__init__.py        # vazio, placeholder
```

## 5. Inventário de recursos GCP (real, verificado em 2026-07-06)

### IAM
| Principal | Papel |
|---|---|
| `luiz.sussu@gmail.com` | `roles/owner` |
| `napraisler@gmail.com` | `roles/editor` |
| `francielly.nrs@gmail.com` | `roles/editor` |
| `<PROJECT_NUMBER>-compute@developer.gserviceaccount.com` (SA padrão, usada pelas Cloud Functions) | `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/storage.objectAdmin` |

### BigQuery — dataset `bronze` (datasets `silver` e `gold` existem mas estão vazios)
| Tabela | Linhas | Origem |
|---|---|---|
| `meta_alfabetizacao_brasil` | 3 | batch |
| `meta_alfabetizacao_uf` | 54 | batch |
| `meta_alfabetizacao_municipio` | 10.704 | batch |
| `avaliacao_alfabetizacao_uf` | 145 | batch |
| `avaliacao_alfabetizacao_municipio` | 23.995 | batch |
| `ibge_municipios` | 5.571 | batch (API IBGE) |
| `dados_alunos_streaming` | 1.126 (190 alunos distintos — duplicidade por *at-least-once delivery* do Pub/Sub, esperada na Bronze) | streaming |

Todas as tabelas batch têm colunas `_ingested_at` (TIMESTAMP) e `_source_file` (STRING) adicionadas via `CREATE OR REPLACE TABLE ... AS SELECT` (staging + CTAS, não ALTER+UPDATE).

### Cloud Storage
- Bucket: `tech-challenge-alfabetiza-25-raw`, região `southamerica-east1`, storage class `STANDARD`
- Layout: `bronze/<nome_tabela>/<arquivo_original>.csv`

### Pub/Sub
- Tópico: `dados-alunos-stream`
- Assinatura pull manual: `dados-alunos-stream-sub` (usada por `streaming/consumer.py` local — não usada pela Cloud Function, que tem sua própria assinatura gerenciada pelo Eventarc)

### Cloud Functions (Gen2, ambas em `southamerica-east1`)
| Nome | Trigger | Estado | Entry point |
|---|---|---|---|
| `bronze-batch-ingest` | HTTP (`--no-allow-unauthenticated`) | `ACTIVE` | `batch_ingest_http` |
| `bronze-streaming-consumer` | Pub/Sub `dados-alunos-stream`, uma invocação por mensagem | `ACTIVE` | `streaming_consumer_pubsub` |

Ambas deployadas com `--source=src` (o diretório `src/` inteiro é o pacote implantado; por isso `bronze/sources.py::RAW_DATA_DIR` tem um fallback defensivo para quando `Path(__file__).resolve().parents[3]` não existe no ambiente da function — ver comentário no código).

### Cloud Scheduler
- Job `bronze-batch-schedule`, cron `0 6 * * *` (`America/Sao_Paulo`), aponta para a URL da função batch via OIDC.
- **Estado: `PAUSED` deliberadamente.** Decisão explícita do usuário: infraestrutura existe para fins de estudo/demonstração, mas não deve disparar automaticamente nem gerar custo recorrente. **Não reativar sem confirmação explícita do usuário.**

## 6. Decisões arquiteturais já tomadas (não re-abrir sem motivo)

1. **GCP** escolhido entre AWS/GCP/Azure (justificativa no README: serverless-first, alinhamento com "Base dos Dados" que é BigQuery-nativa).
2. **Sem Terraform** — provisionamento via scripts Python idempotentes (`bronze/setup_infra.py`) usando os clientes `google-cloud-*`. Decisão de manter escopo simples dado o tamanho do projeto.
3. **Sem Cloud Composer/Airflow** — orquestração via Cloud Scheduler + Cloud Functions, para evitar custo de cluster always-on. Documentado como decisão de FinOps.
4. **Dimensão UF/Município via API do IBGE** — nenhuma fonte do INEP fornecida traz nomes, só códigos (`sigla_uf`, `id_municipio`). Ver `docs/data_dictionary.md`, seção "Gap identificado".
5. **Streaming simulation**: usa os microdados de aluno (`Dados de alunos.csv`, granularidade de evento) via Pub/Sub, já que é a fonte mais próxima de "eventos chegando um a um" entre as disponíveis.
6. **Duplicidade no streaming é esperada e correta na Bronze** (at-least-once delivery do Pub/Sub) — a deduplicação é responsabilidade explícita da Silver, não um bug a corrigir aqui.
7. **A Cloud Function batch relê do GCS (`reload_bronze_from_gcs`), não faz upload** — não há disco local no ambiente de function; o upload (`run()` em `batch_ingest.py`) é só bootstrap local, roda uma vez, e o GCS já fica populado para releituras futuras.
8. **Cada invocação da function de streaming processa 1 mensagem**, sem buffer local — é o comportamento correto/idiomático de uma function stateless acionada por evento, diferente do micro-lote do consumidor manual (`streaming/consumer.py`), que continua existindo só para testes locais.

## 7. Referências externas (fora deste repo)

- Brief do desafio: `../../tech challenge/[IAST] - Tech Challenge - Fase 2.pdf` (relativo à raiz deste repo)
- Dados brutos completos: `../../tech challenge/base de dados/*.csv` (não versionados — ver `data/sample/` para amostras)
- Notas consolidadas do curso (medallion architecture, FinOps, Kafka, etc conforme ensinado): `../course_notes.md`

## 8. Próximos passos (nesta ordem, conforme plano acordado com o usuário)

1. **Silver**: limpeza, padronização de tipos/chaves (`id_municipio` zero-padded, `sigla_uf` maiúsculo), construção das dimensões UF/Município a partir de `ibge_municipios`, integração das fact tables com as dimensões, deduplicação do streaming, checks de qualidade (duplicidade, nulos, integridade referencial, consistência entre tabelas) com quarentena para registros reprovados.
2. **Gold**: marts `indicador_alfabetizacao_por_municipio`, `metas_vs_resultados`, `evolucao_temporal_indicador`, particionados por `ano`, clusterizados por `sigla_uf`/`id_municipio`.
3. **Monitoramento + FinOps**: `docs/finops.md` com estimativa de custo, alertas via Cloud Monitoring.
4. **README final**: diagrama de arquitetura (Mermaid), trade-offs, seção de aplicação em IA, roteiro do vídeo executivo.

## 9. Coisas para NÃO fazer sem confirmação explícita do usuário

- Reativar/disparar `bronze-batch-schedule`.
- Fazer merge de PRs sem perguntar (usuário às vezes quer revisar antes).
- Rodar `streaming/producer.py` contra o tópico de produção gerando volume além de pequenos testes (poucas mensagens) sem perguntar antes.
- Conceder acesso (IAM no GCP ou collaborators no GitHub) a terceiros sem confirmação explícita do e-mail/usuário exato.
