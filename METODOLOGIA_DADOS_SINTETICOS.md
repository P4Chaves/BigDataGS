# ZelaBairro — metodologia da base sintética (etapa 1)

## Contexto
O projeto do grupo ZelaBairro registra e organiza ocorrências em espaços urbanos. O primeiro módulo de Big Data consome ocorrências e gera indicadores que o painel poderá ler. O território piloto e a infraestrutura real ainda serão definidos pelo grupo.

## Por que usamos dados sintéticos?
O grupo não dispõe de uma fonte autorizada de dados reais de ocorrências. O edital FIAP permite dados simulados desde que a geração seja justificada e documentada. Não usamos nem alegamos dados de prefeitura, participação de cidadãos ou efetivos consertos.

## Regras de geração (todas hipotéticas)
- Gerador: código Python dentro de `01_ZelaBairro_Base_Sintetica_ETL.ipynb`; semente fixa `2026`.
- Volume: 240 IDs únicos (`OC-0001` até `OC-0240`), com 8 cópias adicionais para teste de deduplicação (248 linhas no bruto).
- Datas simuladas: 2026-08-01 até 2026-11-10.
- Locais fictícios: Praça A, Praça B, Parque A, Rua A, Avenida A; não indicam bairros reais.
- Pesos arbitrários de categorias: sinalizacao 40%, lixeira 25%, banco 20%, calcada 15%; o número realizado pode variar por sorteio.
- Status internos: pendente, em_analise, em_andamento, concluida. A simulação concede maior probabilidade de encerramento administrativo a ocorrências antigas. `concluida` não significa reparo físico.
- Descrições de defeitos genéricos são sorteadas de três frases por categoria.
- `foto`: vazio, pois não foram geradas imagens reais nem fictícias. O campo permanece para compatibilidade.
- `origem_dado`: `sintetico` em todos os registros para identificação clara.
- Ruídos: inserção de caixa alta, acentuação, espaços extras e quatro datas no formato brasileiro; a ETL padroniza esses valores.

## Dicionário dos campos da tabela tratada
| Campo | Tipo | Descrição |
|---|---|---|
| id | string | Identificador único `OC-0001` |
| categoria | string | `sinalizacao`, `lixeira`, `banco`, `calcada` |
| local | string | Local simulado, sem endereço real |
| descricao | string | Texto genérico de problema urbano |
| foto | string opcional | Vazio nesta versão; sem arquivo anexado |
| status | string | Etapa administrativa interna, sem implicação de reparo |
| data | string ISO 8601 (`YYYY-MM-DD`) | Data fictícia do registro |
| origem_dado | string | Marcador de procedência `sintetico` |

## Outputs do ETL
- `ocorrencias_tratadas.csv`: 240 ocorrências com ID único e campos normalizados.
- `indicadores_categoria.csv`: contagem por categoria.
- `indicadores_local.csv`: contagem por local.
- `indicadores_status.csv`: contagem por status interno.
- `indicadores_categoria_status.csv`: cruzamento de categoria e status.
- `indicadores.json`: resumo consumível pelo painel MVP.

## Validação e limites
- A rotina verifica unicidade de ID, categorias e status permitidos, datas válidas e contagens que somam 240.
- Métricas e tendências **não** podem ser generalizadas para São Paulo ou qualquer território real.
- O notebook sozinho **não supre** a exigência de DAG funcional e repetível no Airflow da FIAP.
- A integração com o MVP central só poderá ser declarada concluída quando a aplicação efetivamente consumir os outputs.

## Execução
Abra o notebook no Google Colab e clique em `Executar tudo`. A base bruta é gerada automaticamente. Faça download do ZIP gerado pela última célula.

## Fonte de requisitos
- `ZelaBairro_Documento_de_Alinhamento_do_Grupo.pdf`, especialmente páginas 3–7.
- `Desafio-FIAP-GS-CitizenTech_4ESPA - Preenchido_v3.pdf`, especialmente página 30 (Airflow e entregáveis).
