# ZelaBairro — Big Data Architecture & Data Integration

**GS FIAP 2026.2 · Módulo de Dados do ZelaBairro · Etapa 2 (DAG Airflow)**

O projeto simula registros de falhas em espaços urbanos para demonstrar um pipeline ETL orquestrado no **Apache Airflow**. Todas as ocorrências são **sintéticas**, sem vínculo com uma prefeitura. `concluida` significa encerramento **interno simulado**, nunca conserto físico comprovado.

## Contrato de dados

- **Entrada**: `dados/zelabairro_ocorrencias_brutas.csv` (8 campos: `id`, `categoria`, `local`, `descricao`, `foto`, `status`, `data`, `origem_dado`).
- **Processamento**: normalização de categorias, status e datas; deduplicação por ID; validação dos campos.
- **Saídas consumíveis**: `saidas/ocorrencias_tratadas.csv`, `saidas/indicadores.json` e `saidas/indicadores_{categoria,local,status,categoria_status}.csv`.
- **Integração futura**: painel do ZelaBairro poderá ler `saidas/indicadores.json` ou um arquivo CSV. A conexão efetiva com a aplicação do grupo ainda depende da definição do backend.

## Airflow

Arquivo da DAG: `dags/zelabairro_dag.py` — ID `zelabairro_etl`, 5 tarefas:

`gerar_base_sintetica → extrair_ocorrencias → tratar_ocorrencias → gerar_indicadores → validar_resultados`

A DAG tem acionamento manual (`schedule=None`) e gera os mesmos arquivos a cada execução (seed fixa). O Colab é um **ambiente temporário de desenvolvimento**, não hospedagem pública permanente do Airflow ou do MVP.

### Sem instalações locais: notebook Colab

Abra `notebooks/02_ZelaBairro_Airflow_Colab.ipynb` no Google Colab e execute as células na ordem. Ele cria os scripts do pipeline, instala o Airflow **no ambiente virtual do Colab**, inicia o Airflow em modo `standalone` e tenta exibir a interface na saída. Na interface, execute `zelabairro_etl`, acompanhe as cinco tarefas e registre prints.

> **Importante:** a interface Colab/Airflow não pôde ser validada neste ambiente de elaboração. Se a renderização do Airflow falhar por limitações do navegador ou do Colab, o notebook mostra o log de inicialização. Não declare a execução da DAG como concluída sem um registro real no Airflow.

### Teste Python (não substitui a execução no Airflow)

```bash
pip install pandas==2.2.3
python -m unittest discover -s tests -v
python dags/zelabairro_pipeline.py
```

## Avaliação FIAP — pendências

- [x] ETL repetível e integrado por contrato de arquivo
- [x] DAG Airflow criada
- [ ] **DAG executada no Airflow e registrada em prints/vídeo**
- [ ] Evidência de indicadores lidos pelo MVP do grupo
- [ ] Diagrama atualizado e PDF final com dicionário, logs e arquitetura
- [ ] Repositório GitHub público com URLs verificadas

## Regras da geração sintética

240 IDs únicos e 8 duplicatas didáticas; data entre 01/08/2026 e 10/11/2026; categorias `sinalizacao`, `lixeira`, `banco`, `calcada`; locais fictícios; status de tramitação interna. Os pesos de geração são hipóteses didáticas, **não estatísticas reais da cidade**. Consulte `METODOLOGIA_DADOS_SINTETICOS.md`.

## Segurança

Não publicar credenciais do Airflow, arquivos internos de autenticação, dumps de ambiente ou fotos/dados pessoais. O Airflow standalone usa um banco SQLite **internamente**; não é necessário instalar SQLite à parte. Mantenha a interface e portas com acesso privado. A hospedagem estável do MVP não é fornecida por esta etapa.
