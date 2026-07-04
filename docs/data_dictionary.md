# Dicionário de Dados — Fontes Brutas

Fonte: [Base dos Dados](https://basedosdados.org/) — indicador **Criança Alfabetizada** (INEP / Compromisso Nacional Criança Alfabetizada), Pesquisa Alfabetiza Brasil (2023).

Todos os arquivos originais estão em [`../../tech challenge/base de dados`](../../tech%20challenge/base%20de%20dados) (não versionados neste repositório por tamanho — ver `data/sample/` para amostras de ~2.000 linhas usadas em desenvolvimento local).

## 1. `dados_alunos` (origem: `Dados de alunos.csv`)

Microdados de aluno — granularidade mais fina, usada como fonte da **ingestão streaming** (simula chegada de resultados de avaliação individual quase em tempo real). ~3,87 milhões de linhas / 214 MB.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano` | int | Ano da avaliação |
| `id_municipio` | string (7 dígitos) | Código IBGE do município |
| `id_escola` | string | Código INEP da escola |
| `id_aluno` | string | Identificador do aluno (anonimizado) |
| `caderno` | int | Caderno de prova aplicado |
| `serie` | int | Série/ano escolar avaliado (2 = 2º ano do Ensino Fundamental) |
| `rede` | int | Código da rede de ensino (1=Federal, 2=Estadual, 3=Municipal, 4=Privada — confirmar mapeamento na Silver) |
| `presenca` | int (0/1) | Se o aluno esteve presente na aplicação |
| `preenchimento_caderno` | int (0/1) | Se o caderno foi preenchido validamente |
| `alfabetizado` | int (0/1) | Se o aluno atingiu o ponto de corte de proficiência (743 pts Saeb) |
| `proficiencia` | float (nullable) | Proficiência estimada na escala Saeb |
| `peso_aluno` | float (nullable) | Peso amostral do aluno |

## 2. `meta_alfabetizacao_brasil` (origem: `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv`)

Metas nacionais do indicador, agregadas por ano/rede. 3 linhas.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano` | int | Ano de referência |
| `rede` | string | Rede de ensino (ex.: "Pública") |
| `taxa_alfabetizacao` | float | Taxa observada no ano |
| `meta_alfabetizacao_2024`…`2030` | float | Metas nacionais por ano-alvo |
| `percentual_participacao` | float | % de participação na avaliação |

## 3. `meta_alfabetizacao_uf` (origem: `..._meta_alfabetizacao_uf.csv`)

Mesma estrutura da tabela nacional, quebrada por UF. 54 linhas.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano`, `rede`, `taxa_alfabetizacao`, `meta_alfabetizacao_2024..2030`, `percentual_participacao` | — | Igual à tabela Brasil |
| `sigla_uf` | string (2 letras) | Sigla da UF |

## 4. `meta_alfabetizacao_municipio` (origem: `..._meta_alfabetizacao_municipio.csv`)

Metas por município. ~10.700 linhas.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano`, `rede`, `taxa_alfabetizacao`, `meta_alfabetizacao_2024..2030`, `percentual_participacao` | — | Igual à tabela Brasil |
| `id_municipio` | string (7 dígitos) | Código IBGE do município |
| `nivel_alfabetizacao` | int/nullable | Nível de alfabetização classificado do município |

## 5. `avaliacao_alfabetizacao_uf` (origem: `br_inep_avaliacao_alfabetizacao_uf.csv`)

Resultados agregados de desempenho por UF/série/rede. 145 linhas.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano`, `serie`, `rede` | — | Chaves de agregação |
| `sigla_uf` | string (2 letras) | Sigla da UF |
| `taxa_alfabetizacao` | float | Taxa de alfabetização observada |
| `media_portugues` | float | Proficiência média em Língua Portuguesa |
| `proporcao_aluno_nivel_0`…`8` | float | Distribuição percentual dos alunos por nível de proficiência |

## 6. `avaliacao_alfabetizacao_municipio` (origem: `br_inep_avaliacao_alfabetizacao_municipio.csv`)

Mesma estrutura da tabela de UF, por município. ~24.000 linhas.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `ano`, `serie`, `rede` | — | Chaves de agregação |
| `id_municipio` | string (7 dígitos) | Código IBGE do município |
| `taxa_alfabetizacao`, `media_portugues`, `proporcao_aluno_nivel_0..8` | — | Igual à tabela de UF |

## Gap identificado — dimensões UF e Município

O enunciado do desafio lista **UF** e **Município** como entidades próprias, mas nenhuma fonte fornecida traz nome de UF, nome de município, região ou outros atributos descritivos — apenas os códigos (`sigla_uf`, `id_municipio`). Para materializar essas dimensões na camada Silver, uma tabela de referência do IBGE (código do município, nome, UF, região, código UF) será ingerida como fonte adicional na Bronze (`ibge_municipios`). Isso é diferente do enriquecimento *opcional* citado no enunciado (Censo Escolar, PNAD, Atlas do Desenvolvimento Humano, CadÚnico, FUNDEB) — aquelas ficam fora do MVP e documentadas como trabalho futuro.

## Convenções aplicadas a partir da camada Silver

- `id_municipio`: sempre string, zero-padded para 7 dígitos.
- `sigla_uf`: sempre string, maiúscula, 2 caracteres.
- `ano`: inteiro.
- Colunas de metadados de ingestão adicionadas na Bronze: `_ingested_at` (timestamp UTC), `_source_file` (nome do arquivo de origem).
