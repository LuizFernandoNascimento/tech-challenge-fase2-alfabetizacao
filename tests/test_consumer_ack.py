"""Testes da semântica de entrega do consumidor de streaming.

O que está sendo protegido aqui: uma mensagem do Pub/Sub nunca pode ser
confirmada (ack) sem que a gravação no BigQuery tenha sido confirmada
antes. Se isso regredir, o pipeline volta a perder microdado de aluno
silenciosamente numa falha - o bug apontado na avaliação.

Roda sem dependências extras:

    PYTHONPATH=src python tests/test_consumer_ack.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.consumer import flush_batch


class FakeMessage:
    """Substituto da mensagem do Pub/Sub, que registra ack/nack."""

    def __init__(self):
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True


class FakeBigQuery:
    """Cliente BigQuery falso, capaz de simular os modos de falha reais."""

    def __init__(self, errors=None, raises=False):
        self.errors = errors
        self.raises = raises

    def insert_rows_json(self, table_id, rows):
        if self.raises:
            raise RuntimeError("falha simulada de rede/API")
        return self.errors or []


def _run(bq_client):
    messages = [FakeMessage(), FakeMessage()]
    pending = [({"id_aluno": "1"}, messages[0]), ({"id_aluno": "2"}, messages[1])]
    inserted = flush_batch(bq_client, "projeto.dataset.tabela", pending)
    return inserted, messages


def test_gravacao_ok_confirma_mensagens():
    inserted, messages = _run(FakeBigQuery())
    assert inserted == 2
    assert all(m.acked for m in messages), "deveria confirmar após gravar"
    assert not any(m.nacked for m in messages)


def test_erro_do_bigquery_devolve_mensagens():
    inserted, messages = _run(FakeBigQuery(errors=[{"index": 0, "errors": ["boom"]}]))
    assert inserted == 0
    assert all(m.nacked for m in messages), "deveria devolver para reentrega"
    assert not any(m.acked for m in messages), "NUNCA confirmar sem gravar"


def test_excecao_na_api_devolve_mensagens():
    inserted, messages = _run(FakeBigQuery(raises=True))
    assert inserted == 0
    assert all(m.nacked for m in messages)
    assert not any(m.acked for m in messages), "NUNCA confirmar sem gravar"


def test_lote_vazio_nao_faz_nada():
    assert flush_batch(FakeBigQuery(), "projeto.dataset.tabela", []) == 0


if __name__ == "__main__":
    testes = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    falhas = 0
    for teste in testes:
        try:
            teste()
            print(f"PASS  {teste.__name__}")
        except AssertionError as erro:
            falhas += 1
            print(f"FALHA {teste.__name__}: {erro}")
    print(f"\n{len(testes) - falhas}/{len(testes)} testes passaram")
    sys.exit(1 if falhas else 0)
