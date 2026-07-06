# Resumo do projeto — para compartilhar com o time

## O que estamos construindo

Uma pipeline de dados para organizar e analisar o **Indicador Criança Alfabetizada** (INEP) — o percentual de crianças brasileiras que atingem o nível de alfabetização esperado até o 2º ano do Ensino Fundamental.

A ideia é pegar dados que hoje estão espalhados em vários arquivos (metas nacionais, metas por estado, metas por município, resultados de avaliações, dados de alunos) e organizá-los em três "camadas" de qualidade crescente, na nuvem (Google Cloud):

- **Bronze**: os dados exatamente como vêm da fonte, sem mexer em nada — serve como histórico bruto e ponto de partida.
- **Silver** *(ainda não construída)*: os mesmos dados, só que limpos, padronizados e já cruzados entre si.
- **Gold** *(ainda não construída)*: tabelas prontas para análise, dashboards e modelos de IA.

Esse projeto é o Tech Challenge da Fase 2 da pós-graduação (POSTECH/FIAP), e vale 90% da nota da fase.

## O que já está pronto: a camada Bronze

Concluímos a primeira camada (Bronze), com **duas formas diferentes de trazer os dados para dentro da nuvem**:

1. **Ingestão em lote (batch)**: os arquivos de metas e resultados (que são atualizados periodicamente, não o tempo todo) são carregados de uma vez, de tempos em tempos.
2. **Ingestão em tempo quase real (streaming)**: simulamos a chegada de resultados de alunos "um por um", como se fossem avaliações sendo processadas ao vivo, em vez de esperar um arquivo completo.

Por que os dois? Porque no mundo real, esse tipo de projeto sempre tem uma mistura: dados que chegam aos poucos (avaliações) e dados que chegam em blocos (metas anuais). Mostrar que conseguimos lidar com os dois é parte do que o desafio pede.

### Um passo a mais: colocar o código para rodar dentro da nuvem

Depois de validar que os scripts funcionavam (rodando no computador), avançamos mais um passo: colocamos esse mesmo código para rodar **dentro do Google Cloud**, como dois serviços gerenciados ("Cloud Functions"):

- Um que faz a ingestão em lote.
- Um que processa os eventos de streaming, automaticamente, sempre que uma nova mensagem chega.

Isso é importante para o time porque agora dá para **ver o código rodando de verdade na nuvem**, sem depender do computador de quem desenvolveu.

Criamos também um agendador (Cloud Scheduler) que, no futuro, dispararia a ingestão em lote automaticamente todos os dias — mas ele está **propositalmente pausado**, para não gerar nenhum custo enquanto ainda estamos em fase de estudo/demonstração.

## Onde o time pode ver tudo isso

**Código-fonte (GitHub):**
https://github.com/LuizFernandoNascimento/tech-challenge-fase2-alfabetizacao

**Dados carregados (BigQuery):**
https://console.cloud.google.com/bigquery?project=tech-challenge-alfabetiza-25

**Arquivos brutos (Cloud Storage):**
https://console.cloud.google.com/storage/browser/tech-challenge-alfabetiza-25-raw?project=tech-challenge-alfabetiza-25

**Serviços rodando na nuvem (Cloud Functions — dá para ver o código clicando em "Source"):**
https://console.cloud.google.com/functions/list?project=tech-challenge-alfabetiza-25

**Agendamento (Cloud Scheduler — confirma que está pausado, sem custo):**
https://console.cloud.google.com/cloudscheduler?project=tech-challenge-alfabetiza-25

**Mensageria em tempo real (Pub/Sub):**
https://console.cloud.google.com/cloudpubsub/topic/list?project=tech-challenge-alfabetiza-25

## O que confirma que funcionou

Já testamos e confirmamos que os números carregados batem exatamente com os arquivos originais (por exemplo, uma tabela com 23.995 linhas na fonte tem 23.995 linhas no BigQuery). Também testamos o caminho de streaming com uma simulação de eventos chegando e confirmamos que os dados aparecem no banco em tempo real, não tudo de uma vez.

## O que falta

- **Silver**: limpar e padronizar os dados, cruzar as diferentes fontes (por exemplo, ligar cada município ao seu estado e região) e aplicar verificações de qualidade (duplicidade, valores ausentes, etc).
- **Gold**: montar as tabelas finais de análise (indicador por município, comparação entre metas e resultados, evolução ao longo do tempo).
- **Monitoramento e custos (FinOps)**: documentar como acompanhamos falhas e como controlamos gastos na nuvem.
- **README final, diagrama da arquitetura e vídeo executivo** para a entrega.
