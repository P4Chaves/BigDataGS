"""Testes executáveis com Python, sem instalar Airflow."""
import json
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from zelabairro_pipeline import executar_local


class TestETLZelaBairro(unittest.TestCase):
    def test_geracao_e_integridade(self):
        with tempfile.TemporaryDirectory() as pasta:
            resultado = executar_local(pasta)
            self.assertEqual(resultado['registros'], 240)
            self.assertEqual(resultado['resultado'], 'OK')
            bruto = (Path(pasta) / 'dados' / 'zelabairro_ocorrencias_brutas.csv').read_text(encoding='utf-8-sig')
            self.assertEqual(len(bruto.splitlines()) - 1, 248)
            payload = json.loads((Path(pasta) / 'saidas' / 'indicadores.json').read_text(encoding='utf-8'))
            self.assertEqual(sum(r['quantidade'] for r in payload['por_categoria']), 240)

    def test_reexecucao_reproduzivel(self):
        with tempfile.TemporaryDirectory() as pasta:
            executar_local(pasta)
            primeira = (Path(pasta) / 'saidas' / 'indicadores.json').read_bytes()
            executar_local(pasta)
            segunda = (Path(pasta) / 'saidas' / 'indicadores.json').read_bytes()
            self.assertEqual(primeira, segunda)


if __name__ == '__main__':
    unittest.main()
