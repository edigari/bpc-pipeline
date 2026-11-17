"""
Servidor HTTP local que SIMULA as duas APIs (Portal da Transparência e IBGE).

Usado pelos testes ponta a ponta: permite validar o pipeline inteiro (rate limit, retentativas,
429, 500, 401, EXPLODE, JOIN, Data Quality e exportação) sem gastar a cota real da API.
Injeta falhas de propósito: 1 resposta 429 a cada 97 chamadas e 1 resposta 500 a cada 53.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CHAVE_VALIDA = "chave-de-teste-123"
ULTIMO_MES_PUBLICADO = 9  # simula dados de jan..set/2026

# (codigo, nome, uf_sigla, uf_id, regiao) - inclui Salvador (município sonda)
UFS = {"PE": (26, "Pernambuco", "Nordeste"), "BA": (29, "Bahia", "Nordeste"),
       "AM": (13, "Amazonas", "Norte"), "SP": (35, "São Paulo", "Sudeste")}
MUNICIPIOS = [("2927408", "Salvador", "BA")]
MUNICIPIOS += [(f"26{i:05d}", f"Cidade PE {i}", "PE") for i in range(1, 16)]
MUNICIPIOS += [(f"13{i:05d}", f"Cidade AM {i}", "AM") for i in range(1, 6)]
MUNICIPIOS += [(f"35{i:05d}", f"Cidade SP {i}", "SP") for i in range(1, 6)]  # fora do escopo N/NE


def _uf_json(sigla: str) -> dict:
    uid, nome, reg = UFS[sigla]
    return {"id": uid, "sigla": sigla, "nome": nome, "regiao": {"id": 1, "sigla": reg[:2].upper(), "nome": reg}}


def municipios_json() -> list[dict]:
    saida = []
    for i, (cod, nome, uf) in enumerate(MUNICIPIOS):
        micro = None if i == 3 else {  # 1 município sem microrregião (caso real do IBGE)
            "id": 1, "nome": "Micro", "mesorregiao": {"id": 1, "nome": "Meso", "UF": _uf_json(uf)}}
        saida.append({
            "id": int(cod), "nome": nome, "microrregiao": micro,
            "regiao-imediata": {"id": 1, "nome": "Imediata",
                                "regiao-intermediaria": {"id": 1, "nome": "Intermediaria", "UF": _uf_json(uf)}},
        })
    return saida


def populacao_json(ano: str) -> list[dict]:
    series = [{"localidade": {"id": cod, "nivel": {"id": "N6", "nome": "Município"}, "nome": f"{nome} - {uf}"},
               "serie": {ano: ("..." if cod == "2600005" else str(10_000 + int(cod[-3:]) * 1000))}}
              for cod, nome, uf in MUNICIPIOS]
    return [{"id": "9324", "variavel": "População residente estimada", "unidade": "Pessoas",
             "resultados": [{"classificacoes": [], "series": series}]}]


class _Handler(BaseHTTPRequestHandler):
    contador = 0
    lock = threading.Lock()

    def log_message(self, *args) -> None:  # silencioso
        pass

    def _json(self, status: int, corpo, headers: dict | None = None) -> None:
        dados = json.dumps(corpo, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}

        if url.path.startswith("/api/v1/localidades/municipios"):
            return self._json(200, municipios_json())
        if url.path == "/api/v3/agregados/6579/periodos":
            return self._json(200, [{"id": "2024"}, {"id": "2025"}])  # 2026 "não publicado" -> fallback
        if url.path.startswith("/api/v3/agregados/6579/periodos/"):
            return self._json(200, populacao_json(url.path.split("/")[6]))

        if url.path == "/api-de-dados/bpc-por-municipio":
            if self.headers.get("chave-api-dados") != CHAVE_VALIDA:
                return self._json(401, {"erro": "chave inválida"})
            with _Handler.lock:
                _Handler.contador += 1
                n = _Handler.contador
            if n % 97 == 0:
                return self._json(429, {"erro": "limite"}, {"Retry-After": "1"})
            if n % 53 == 0:
                return self._json(500, {"erro": "interno"})
            mes_ano, cod = q["mesAno"], q["codigoIbge"]
            mes = int(mes_ano[-2:])
            if mes > ULTIMO_MES_PUBLICADO or q.get("pagina") != "1":
                return self._json(200, [])
            nome = next((n_ for c, n_, _ in MUNICIPIOS if c == cod), "X")
            uf = next((u for c, _, u in MUNICIPIOS if c == cod), "XX")
            qtd = 100 + int(cod[-3:]) * 3 + mes
            return self._json(200, [{
                "id": int(cod) * 100 + mes, "dataReferencia": f"01/{mes:02d}/2026",
                "municipio": {"codigoIBGE": cod, "nomeIBGE": nome.upper(), "codigoRegiao": "2", "nomeRegiao": "NORDESTE",
                              "pais": "BRASIL", "uf": {"sigla": uf, "nome": UFS[uf][1]}},
                "tipo": {"id": 5, "descricao": "BPC", "descricaoDetalhada": "Benefício de Prestação Continuada"},
                "valor": round(qtd * 1621.0, 2), "quantidadeBeneficiados": qtd}])

        return self._json(404, {"erro": "rota inexistente"})


def iniciar(porta: int = 0) -> tuple[ThreadingHTTPServer, int]:
    srv = ThreadingHTTPServer(("127.0.0.1", porta), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]
