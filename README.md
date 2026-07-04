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

| Entidade | Arquivo de origem | Volume |
|---|---|---|
| Dados de alunos (microdados) | `Dados de alunos.csv` | ~3,87M linhas / 214MB |
| Meta Alfabetização Brasil | `..._meta_alfabetizacao_brasil.csv` | 3 linhas |
| Meta Alfabetização UF | `..._meta_alfabetizacao_uf.csv` | 54 linhas |
| Meta Alfabetização Município | `..._meta_alfabetizacao_municipio.csv` | ~10,7k linhas |
| Avaliação Alfabetização UF | `br_inep_avaliacao_alfabetizacao_uf.csv` | 145 linhas |
| Avaliação Alfabetização Município | `br_inep_avaliacao_alfabetizacao_municipio.csv` | ~24k linhas |
| Dimensão UF/Município (IBGE, complementar) | a definir na Bronze | referência |

## Arquitetura da solução

*(Diagrama e fluxo de dados completos serão adicionados ao final do desenvolvimento, no incremento de documentação final.)*

Visão geral adotada:

- **Cloud**: Google Cloud Platform.
- **Data Lake**: Google Cloud Storage (zona raw, particionada por camada/fonte/data).
- **Data Warehouse / Lakehouse analítico**: BigQuery, com datasets separados `bronze`, `silver`, `gold` — particionamento por `ano` e clusterização por `sigla_uf`/`id_municipio` nas camadas Silver/Gold.
- **Ingestão batch**: scripts Python (`google-cloud-storage` + `google-cloud-bigquery`) para as fontes de metas/resultados agregados, orquestrados por Cloud Scheduler + Cloud Functions (sem cluster always-on).
- **Ingestão streaming (simulada)**: Pub/Sub — um produtor replaya os microdados de alunos como eventos quase em tempo real; um consumidor grava micro-lotes na Bronze.
- **Camadas**: Bronze (dados brutos, sem transformação) → Silver (limpeza, padronização, normalização de chaves, integração/dimensões, validação de qualidade) → Gold (marts analíticos prontos para BI/ML).

## Tecnologias e justificativa

*(Seção detalhada nos próximos incrementos, à medida que cada componente é implementado.)*

## Decisões arquiteturais e trade-offs

*(Seção futura — batch vs streaming, data lake vs data warehouse, custo vs performance.)*

## Qualidade de dados

*(Seção futura — regras de duplicidade, nulos, integridade referencial e consistência entre tabelas, implementadas na camada Silver.)*

## Monitoramento e FinOps

*(Seção futura.)*

## Aplicação em IA

*(Seção futura — como a camada Gold pode alimentar modelos preditivos de alfabetização, análises de desigualdade educacional e políticas públicas baseadas em evidências.)*

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # preencha com os dados do seu projeto GCP
```

Instruções detalhadas de execução de cada camada serão adicionadas junto com o respectivo incremento (Bronze, Silver, Gold).
