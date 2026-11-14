# ZelaBairro — Big Data Architecture & Data Integration

**FIAP · Global Solution 2026.2 · Engenharia de Software**

O **ZelaBairro** é uma proposta de plataforma CitizenTech para o registro e acompanhamento de problemas em espaços públicos, como sinalizações, lixeiras, bancos e calçadas. Este repositório apresenta seu **módulo de dados**: um pipeline ETL em Python e Pandas, orquestrado pelo Apache Airflow, que transforma registros de ocorrências em indicadores estruturados.

> **Sobre a fonte dos dados:** esta demonstração utiliza exclusivamente dados **sintéticos** gerados de forma documentada e reproduzível. O módulo não consulta sistemas de prefeitura nem representa estatísticas reais de bairros.

## Objetivo do módulo

O processamento recebe ocorrências, confere os campos necessários, padroniza informações, remove duplicatas e gera indicadores que podem alimentar um painel do ZelaBairro. Dessa forma, uma lista de registros individuais pode ser consultada por **categoria**, **local** e **status interno**.

Na versão atual, os indicadores são disponibilizados em arquivos **CSV** e **JSON**. A integração efetiva com a aplicação web do grupo ainda não foi demonstrada; os arquivos e seus campos constituem o contrato de saída proposto para essa integração.

## Fluxo de processamento

```text
Base sintética de ocorrências (CSV)
            |
            v
    gerar_base_sintetica
            |
            v
    extrair_ocorrencias
            |
            v
    tratar_ocorrencias
            |
            v
     gerar_indicadores
            |
            v
     validar_resultados
            |
            v
  CSVs e JSON para consumo pelo MVP
```

A DAG [`zelabairro_etl`](dags/zelabairro_dag.py) coordena as cinco tarefas. Ela utiliza `schedule=None` (execução manual), com geração de dados reproduzível por semente fixa. O processamento foi validado por **uma execução de teste da DAG no Airflow**, registrada com as cinco tarefas em estado `success`. Isso **não equivale a uma execução agendada automaticamente**.

## Resultados da demonstração

| Indicador | Resultado |
|---|---:|
| Registros brutos sintéticos | 248 |
| IDs únicos após tratamento | 240 |
| Duplicatas removidas | 8 |
| Ocorrências de sinalização | 101 |
| Ocorrências de lixeiras | 67 |
| Ocorrências de bancos | 42 |
| Ocorrências de calçadas | 30 |
| Tarefas da DAG concluídas no teste | 5 de 5 |

Os números acima são resultados da **base simulada**, e não informações sobre a condição real de espaços públicos.

## Estrutura do repositório

```text
BigDataGS/
├── notebooks/
│   └── ZelaBairro_BigData_Colab.ipynb
├── dags/
│   ├── zelabairro_dag.py
│   └── zelabairro_pipeline.py
├── dados/
│   └── zelabairro_ocorrencias_brutas.csv
├── saidas/
│   ├── ocorrencias_tratadas.csv
│   ├── indicadores.json
│   ├── indicadores_categoria.csv
│   ├── indicadores_local.csv
│   ├── indicadores_status.csv
│   └── indicadores_categoria_status.csv
├── tests/
│   └── test_pipeline.py
├── METODOLOGIA_DADOS_SINTETICOS.md
├── ZelaBairro_Relatorio_Big_Data_GS_2026.pdf
└── README.md
```

## Como reproduzir

**No Google Colab (sem instalação local):** abra o notebook [`ZelaBairro_BigData_Colab.ipynb`](notebooks/ZelaBairro_BigData_Colab.ipynb) e execute as células do processamento e da configuração do Airflow na ordem. O ambiente de execução do Colab é temporário. A interface web do Airflow pode não ser exibida corretamente dentro do Colab; o teste da DAG pode ser feito pelo comando `airflow dags test`, e seu resultado consultado no banco de metadados do Airflow.

**Para testar apenas as funções Python**, em um ambiente com Python e Pandas instalados, execute na raiz do repositório:

```bash
python -m pip install pandas==2.2.3
python -m unittest discover -s tests -v
python dags/zelabairro_pipeline.py
```

Os testes Python verificam a integridade e a reprodutibilidade do processamento; **não substituem o teste da DAG no Airflow**.

## Dados e integração

A entrada segue os campos `id`, `categoria`, `local`, `descricao`, `foto`, `status`, `data` e `origem_dado`.

As saídas principais são:

- [`ocorrencias_tratadas.csv`](saidas/ocorrencias_tratadas.csv): registros normalizados e sem IDs duplicados;
- [`indicadores.json`](saidas/indicadores.json): total de ocorrências e contagens por categoria, local e status;
- Arquivos `indicadores_*.csv` em [`saidas/`](saidas): tabelas agregadas para consulta ou visualização.

O campo `origem_dado` identifica os registros como sintéticos. Um status como `concluida` representa **encerramento interno simulado**, não comprova reparo físico ou atendimento oficial.

A metodologia de geração, as regras de distribuição e as inconsistências introduzidas intencionalmente estão em [`METODOLOGIA_DADOS_SINTETICOS.md`](METODOLOGIA_DADOS_SINTETICOS.md).

## Documentação

O relatório técnico com arquitetura, dicionário de dados, gráficos e evidências da execução está disponível em:

**[ZelaBairro — Relatório Técnico de Big Data (PDF)](ZelaBairro_Relatorio_Big_Data_GS_2026.pdf)**

## Integrantes

| Integrante | RM |
|---|---|
| Matheus Farias | 554254 |
| Miguel Parrado | 554007 |
| Vitor Pinheiro | 553693 |
| Gabriel Leão | 552642 |
| Pedro Chaves | 553988 |

---

*Projeto acadêmico desenvolvido para a Global Solution 2026.2 da FIAP. O ZelaBairro não possui integração oficial com órgãos públicos.*
