"""ETL do ZelaBairro: geração reproduzível de dados sintéticos e indicadores.

Dados não oficiais; status de processamento interno, sem vínculo com prefeitura.
Artefatos de dados são trocados via arquivos, não DataFrames no XCom do Airflow.
"""
from datetime import date, timedelta
from pathlib import Path
import json
import random
import unicodedata
import pandas as pd

SEED = 2026
TOTAL_UNICOS = 240
CATEGORIAS = ['sinalizacao', 'lixeira', 'banco', 'calcada']
STATUS = ['pendente', 'em_analise', 'em_andamento', 'concluida']
CAMPOS = ['id', 'categoria', 'local', 'descricao', 'foto', 'status', 'data', 'origem_dado']


def gerar_base(base_dir: str) -> str:
    """Cria 240 ocorrências fictícias e oito cópias para testar deduplicação."""
    base = Path(base_dir)
    dados = base / 'dados'
    dados.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    pesos = [40, 25, 20, 15]
    locais = ['Praça A', 'Praça B', 'Parque A', 'Rua A', 'Avenida A']
    descricoes = {
        'sinalizacao': ['Placa com texto pouco legível', 'Sinalização inclinada', 'Placa com suporte desgastado'],
        'lixeira': ['Lixeira sem tampa', 'Lixeira com estrutura danificada', 'Lixeira com peça solta'],
        'banco': ['Banco com assento danificado', 'Banco com parafuso aparente', 'Banco com encosto solto'],
        'calcada': ['Calçada com desnível', 'Trecho de calçada com piso solto', 'Calçada com obstáculo de passagem'],
    }
    inicio, fim = date(2026, 8, 1), date(2026, 11, 10)
    linhas = []
    for n in range(1, TOTAL_UNICOS + 1):
        cat = rng.choices(CATEGORIAS, weights=pesos, k=1)[0]
        local = rng.choice(locais)
        dia = inicio + timedelta(days=rng.randint(0, (fim - inicio).days))
        idade = (fim - dia).days
        if idade >= 60:
            pesos_status = [15, 20, 30, 35]
        elif idade >= 30:
            pesos_status = [30, 30, 25, 15]
        else:
            pesos_status = [50, 30, 15, 5]
        status_atual = rng.choices(STATUS, weights=pesos_status, k=1)[0]
        linhas.append({
            'id': f'OC-{n:04d}', 'categoria': cat,
            'local': local, 'descricao': rng.choice(descricoes[cat]), 'foto': '',
            'status': status_atual,
            'data': dia.isoformat(), 'origem_dado': 'sintetico',
        })

    # Introdução documentada de ruídos de teste, mantendo os mesmos casos da Etapa 1.
    for pos in [2, 12, 27, 48, 96, 135, 180, 224]:
        linhas[pos]['categoria'] = ' SINALIZAÇÃO ' if linhas[pos]['categoria'] == 'sinalizacao' else ' ' + linhas[pos]['categoria'].upper() + ' '
    for pos in [7, 36, 85, 122, 199]:
        linhas[pos]['local'] = '  ' + linhas[pos]['local'].upper() + ' '
    for pos in [18, 51, 91, 151, 201]:
        linhas[pos]['status'] = ' ' + linhas[pos]['status'].replace('_', ' ').upper() + ' '
    for pos in [21, 60, 101, 170]:
        linhas[pos]['data'] = date.fromisoformat(linhas[pos]['data']).strftime('%d/%m/%Y')
    for pos in [4, 24, 45, 78, 103, 141, 177, 230]:
        linhas.append(linhas[pos].copy())
    rng.shuffle(linhas)

    caminho = dados / 'zelabairro_ocorrencias_brutas.csv'
    pd.DataFrame(linhas)[CAMPOS].to_csv(caminho, index=False, encoding='utf-8-sig')
    print(f'[GERAR BASE] {caminho}: {len(linhas)} linhas fictícias (240 únicas + 8 duplicadas)')
    return str(caminho)


def extrair(caminho_bruto: str) -> str:
    """Confere a existência e os campos da fonte de entrada."""
    caminho = Path(caminho_bruto)
    tabela = pd.read_csv(caminho, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    faltantes = set(CAMPOS).difference(tabela.columns)
    if faltantes:
        raise ValueError(f'Campos obrigatórios ausentes: {sorted(faltantes)}')
    if tabela.empty:
        raise ValueError('Base de entrada vazia')
    print(f'[EXTRACT] {len(tabela)} registros; {tabela.duplicated(subset=["id"]).sum()} IDs repetidos')
    return str(caminho)


def _normalizar_token(valor: str) -> str:
    texto = str(valor).strip().lower().replace(' ', '_')
    return ''.join(ch for ch in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(ch))


def transformar(caminho_bruto: str, base_dir: str) -> str:
    """Remove IDs repetidos e padroniza campos; preserva a primeira versão do ID."""
    tabela = pd.read_csv(caminho_bruto, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    tabela['id'] = tabela['id'].str.strip()
    duplicadas = int(tabela.duplicated(subset=['id']).sum())
    limpa = tabela.drop_duplicates(subset=['id'], keep='first').copy()
    limpa['categoria'] = limpa['categoria'].map(_normalizar_token)
    limpa['status'] = limpa['status'].map(_normalizar_token)
    limpa['local'] = limpa['local'].str.strip().str.title()
    limpa['descricao'] = limpa['descricao'].str.strip()
    limpa['foto'] = limpa['foto'].str.strip()
    limpa['origem_dado'] = limpa['origem_dado'].map(_normalizar_token)
    iso = pd.to_datetime(limpa['data'], format='%Y-%m-%d', errors='coerce')
    br = pd.to_datetime(limpa['data'], format='%d/%m/%Y', errors='coerce')
    limpa['data'] = iso.fillna(br).dt.strftime('%Y-%m-%d')

    if not limpa['categoria'].isin(CATEGORIAS).all():
        raise ValueError('Categoria inválida')
    if not limpa['status'].isin(STATUS).all():
        raise ValueError('Status inválido')
    if limpa['data'].isna().any():
        raise ValueError('Data inválida')
    if not limpa['id'].is_unique or limpa['id'].eq('').any():
        raise ValueError('ID ausente ou duplicado após tratamento')
    if not limpa['origem_dado'].eq('sintetico').all():
        raise ValueError('O conjunto de demonstração deve estar identificado como sintético')

    limpa = limpa.sort_values(['data', 'id']).reset_index(drop=True)
    pasta = Path(base_dir) / 'saidas'
    pasta.mkdir(parents=True, exist_ok=True)
    caminho_saida = pasta / 'ocorrencias_tratadas.csv'
    limpa.to_csv(caminho_saida, index=False, encoding='utf-8-sig')
    print(f'[TRANSFORM] {duplicadas} duplicatas descartadas; {len(limpa)} linhas válidas em {caminho_saida}')
    return str(caminho_saida)


def carregar(caminho_tratado: str, base_dir: str) -> str:
    """Entrega CSVs e JSON que podem ser lidos por um painel do MVP."""
    tabela = pd.read_csv(caminho_tratado, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    pasta = Path(base_dir) / 'saidas'
    por_categoria = tabela.groupby('categoria').size().reset_index(name='quantidade')
    por_local = tabela.groupby('local').size().reset_index(name='quantidade')
    por_status = tabela.groupby('status').size().reset_index(name='quantidade')
    por_cat_status = tabela.groupby(['categoria', 'status']).size().reset_index(name='quantidade')

    for nome, df in [
        ('indicadores_categoria.csv', por_categoria),
        ('indicadores_local.csv', por_local),
        ('indicadores_status.csv', por_status),
        ('indicadores_categoria_status.csv', por_cat_status),
    ]:
        df.to_csv(pasta / nome, index=False, encoding='utf-8-sig')

    payload = {
        'produto': 'ZelaBairro', 'origem_dado': 'sintetico',
        'territorio': 'locais ficticios (piloto ainda nao definido)',
        'periodo_inicio': tabela['data'].min(),
        'periodo_fim': tabela['data'].max(),
        'total_ocorrencias': len(tabela),
        'por_categoria': por_categoria.to_dict(orient='records'),
        'por_local': por_local.to_dict(orient='records'),
        'por_status': por_status.to_dict(orient='records'),
    }
    caminho = pasta / 'indicadores.json'
    caminho.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[LOAD] {caminho}; totais gerados: {len(tabela)} ocorrências fictícias')
    return str(caminho)


def validar(caminho_json: str, base_dir: str) -> dict:
    """Confere integridade entre tabela tratada e tabelas agregadas."""
    payload = json.loads(Path(caminho_json).read_text(encoding='utf-8'))
    tabela = pd.read_csv(Path(base_dir) / 'saidas' / 'ocorrencias_tratadas.csv', dtype=str, keep_default_na=False)
    total = len(tabela)
    for chave in ['por_categoria', 'por_local', 'por_status']:
        if sum(item['quantidade'] for item in payload[chave]) != total:
            raise ValueError(f'Indicador {chave} não fecha com a base tratada')
    if payload['total_ocorrencias'] != total:
        raise ValueError('Total do JSON não coincide com CSV')
    if payload['origem_dado'] != 'sintetico':
        raise ValueError('Origem da base não documentada')
    resultado = {'resultado': 'OK', 'registros': total, 'origem_dado': 'sintetico'}
    print('[VALIDAÇÃO]', resultado)
    return resultado


def executar_local(base_dir: str) -> dict:
    """Teste Python das mesmas operações que a DAG invocará (não simula Airflow)."""
    bruto = extrair(gerar_base(base_dir))
    tratado = transformar(bruto, base_dir)
    indicadores = carregar(tratado, base_dir)
    return validar(indicadores, base_dir)


if __name__ == '__main__':
    import os
    raiz = os.environ.get('ZELABAIRRO_BASE_DIR', str(Path(__file__).resolve().parents[1]))
    print(executar_local(raiz))
