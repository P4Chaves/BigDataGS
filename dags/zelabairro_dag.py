"""DAG didática da FIAP: ZelaBairro (Airflow 3.x)."""
from datetime import datetime, timezone
from pathlib import Path
import os
import sys
from airflow.sdk import dag, task

PASTA_DAG = str(Path(__file__).resolve().parent)
if PASTA_DAG not in sys.path:
    sys.path.insert(0, PASTA_DAG)

from zelabairro_pipeline import gerar_base, extrair, transformar, carregar, validar

BASE_DIR = os.environ.get('ZELABAIRRO_BASE_DIR', str(Path(__file__).resolve().parents[1]))


@dag(
    dag_id='zelabairro_etl',
    description='ETL orquestrado de ocorrências sintéticas do ZelaBairro',
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    schedule=None,  # disparo manual para a demonstração acadêmica
    catchup=False,
    tags=['fiap', 'citizentech', 'zelabairro'],
)
def pipeline_zelabairro():
    @task(task_id='gerar_base_sintetica')
    def gerar():
        return gerar_base(BASE_DIR)

    @task(task_id='extrair_ocorrencias')
    def extrair_tarefa(caminho_bruto: str):
        return extrair(caminho_bruto)

    @task(task_id='tratar_ocorrencias')
    def transformar_tarefa(caminho_bruto: str):
        return transformar(caminho_bruto, BASE_DIR)

    @task(task_id='gerar_indicadores')
    def carregar_tarefa(caminho_tratado: str):
        return carregar(caminho_tratado, BASE_DIR)

    @task(task_id='validar_resultados')
    def validar_tarefa(caminho_indicadores: str):
        return validar(caminho_indicadores, BASE_DIR)

    validar_tarefa(carregar_tarefa(transformar_tarefa(extrair_tarefa(gerar()))))


pipeline_zelabairro()
