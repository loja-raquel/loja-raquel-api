from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os, json, ssl
from urllib.request import Request as URLRequest, urlopen
from urllib.parse import urlencode

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB ──────────────────────────────────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()

def _headers():
    key = os.environ["SUPABASE_KEY"]
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

def _url(table, params=""):
    base = os.environ["SUPABASE_URL"].rstrip("/")
    return f"{base}/rest/v1/{table}{params}"

def sb_get(table, params=""):
    req = URLRequest(_url(table, params), headers=_headers(), method="GET")
    with urlopen(req, timeout=15, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())

def sb_post(table, data):
    body = json.dumps(data).encode()
    req = URLRequest(_url(table), data=body, headers=_headers(), method="POST")
    with urlopen(req, timeout=15, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())

def sb_patch(table, data, filter_param):
    body = json.dumps(data).encode()
    req = URLRequest(_url(table, f"?{filter_param}"), data=body, headers=_headers(), method="PATCH")
    with urlopen(req, timeout=15, context=_ssl_ctx) as r:
        return json.loads(r.read().decode())

def sb_delete(table, filter_param):
    req = URLRequest(_url(table, f"?{filter_param}"), headers=_headers(), method="DELETE")
    with urlopen(req, timeout=15, context=_ssl_ctx) as r:
        return True

def ok(data=None, status=200, **kwargs):
    return JSONResponse({"sucesso": True, "dados": data, **kwargs}, status_code=status)

def err(msg, status=500):
    return JSONResponse({"sucesso": False, "erro": msg}, status_code=status)

# ── PRODUTOS ─────────────────────────────────────────────────────────────────

@app.get("/api/produtos")
def get_produtos():
    try:
        return ok(sb_get("produtos", "?order=nome"))
    except Exception as e:
        return err(str(e))

@app.post("/api/produtos")
async def post_produtos(request: Request):
    try:
        body = await request.json()
        acao = body.get("acao", "criar")

        if acao == "criar":
            dados = {
                "nome":      body.get("nome"),
                "codigo":    body.get("codigo", ""),
                "categoria": body.get("categoria", ""),
                "custo":     float(body.get("custo", 0)),
                "venda":     float(body.get("venda", 0)),
                "qtd":       int(body.get("qtd", 0)),
                "min":       int(body.get("min", 5)),
                "validade":  body.get("validade") or None,
                "unidade":   body.get("unidade", "un"),
            }
            if not dados["nome"]:
                return err("Nome obrigatório", 400)
            res = sb_post("produtos", dados)
            return ok(res if isinstance(res, list) else [res], status=201)

        elif acao == "atualizar_qtd":
            pid = body.get("id")
            qtd = body.get("qtd")
            if pid is None or qtd is None:
                return err("ID e qtd obrigatórios", 400)
            return ok(sb_patch("produtos", {"qtd": int(qtd)}, f"id=eq.{pid}"))

        elif acao == "excluir":
            pid = body.get("id")
            if not pid:
                return err("ID obrigatório", 400)
            sb_delete("produtos", f"id=eq.{pid}")
            return ok()

        else:
            return err(f"Ação desconhecida: {acao}", 400)

    except Exception as e:
        return err(str(e))

# ── VENDAS ───────────────────────────────────────────────────────────────────

@app.get("/api/vendas")
def get_vendas(de: str = None, ate: str = None):
    try:
        params = "?order=data.desc,hora.desc"
        if de:  params += f"&data=gte.{de}"
        if ate: params += f"&data=lte.{ate}"
        if not de and not ate: params += "&limit=50"
        return ok(sb_get("vendas", params))
    except Exception as e:
        return err(str(e))

@app.post("/api/vendas")
async def post_vendas(request: Request):
    try:
        body = await request.json()
        itens = body.get("itens", [])
        if not itens:
            return err("Venda sem itens", 400)

        venda = {
            "data":     body.get("data"),
            "hora":     body.get("hora"),
            "itens":    itens,
            "subtotal": float(body.get("subtotal", 0)),
            "desconto": float(body.get("desconto", 0)),
            "total":    float(body.get("total", 0)),
            "custo":    float(body.get("custo", 0)),
            "lucro":    float(body.get("lucro", 0)),
            "pgto":     body.get("pgto", "Dinheiro"),
        }
        res_venda = sb_post("vendas", venda)
        venda_id  = res_venda[0]["id"] if isinstance(res_venda, list) and res_venda else "?"

        erros = []
        for item in itens:
            pid = item.get("id")
            qtd = int(item.get("qtd", 1))
            if not pid: continue
            try:
                prods    = sb_get("produtos", f"?id=eq.{pid}&select=qtd")
                qtd_atual = prods[0]["qtd"] if prods else 0
                sb_patch("produtos", {"qtd": max(0, qtd_atual - qtd)}, f"id=eq.{pid}")
            except Exception as ex:
                erros.append(str(ex))

        custo = float(body.get("custo", 0))
        if custo > 0:
            sb_post("despesas", {
                "data":      body.get("data"),
                "descricao": f"CMV — venda #{venda_id}",
                "valor":     custo,
                "categoria": "CMV",
                "tipo":      "saida",
            })

        return JSONResponse({"sucesso": True, "venda_id": venda_id, "erros_estoque": erros}, status_code=201)

    except Exception as e:
        return err(str(e))

# ── ENTRADAS ─────────────────────────────────────────────────────────────────

@app.get("/api/entradas")
def get_entradas(de: str = None, ate: str = None):
    try:
        params = "?order=data.desc"
        if de:  params += f"&data=gte.{de}"
        if ate: params += f"&data=lte.{ate}"
        if not de and not ate: params += "&limit=50"
        return ok(sb_get("entradas", params))
    except Exception as e:
        return err(str(e))

@app.post("/api/entradas")
async def post_entradas(request: Request):
    try:
        body = await request.json()
        produto_id   = body.get("produto_id")
        produto_nome = body.get("produto_nome", "")
        qtd          = int(body.get("qtd", 0))
        custo        = float(body.get("custo", 0))
        validade     = body.get("validade") or None
        fornecedor   = body.get("fornecedor", "")
        data         = body.get("data")

        if not produto_id or qtd <= 0:
            return err("produto_id e qtd são obrigatórios", 400)

        prods = sb_get("produtos", f"?id=eq.{produto_id}&select=qtd,custo")
        if not prods:
            return err("Produto não encontrado", 404)

        qtd_anterior = prods[0]["qtd"]
        qtd_nova     = qtd_anterior + qtd
        total_pago   = custo * qtd

        update = {"qtd": qtd_nova}
        if custo > 0:  update["custo"]    = custo
        if validade:   update["validade"] = validade
        sb_patch("produtos", update, f"id=eq.{produto_id}")

        res = sb_post("entradas", {
            "data": data, "produto_id": produto_id, "produto_nome": produto_nome,
            "qtd": qtd, "custo": custo, "total_pago": total_pago,
            "fornecedor": fornecedor, "qtd_anterior": qtd_anterior, "qtd_nova": qtd_nova,
        })

        if total_pago > 0:
            desc = f"Compra de mercadoria — {produto_nome} ({qtd} un)"
            if fornecedor: desc += f" — {fornecedor}"
            sb_post("despesas", {"data": data, "descricao": desc, "valor": total_pago,
                                  "categoria": "Compra de mercadoria", "tipo": "saida"})

        return JSONResponse({"sucesso": True, "dados": res, "qtd_anterior": qtd_anterior,
                             "qtd_nova": qtd_nova, "total_pago": total_pago}, status_code=201)

    except Exception as e:
        return err(str(e))

# ── DESPESAS ─────────────────────────────────────────────────────────────────

@app.get("/api/despesas")
def get_despesas(de: str = None, ate: str = None):
    try:
        params = "?categoria=neq.CMV&order=data.desc"
        if de:  params += f"&data=gte.{de}"
        if ate: params += f"&data=lte.{ate}"
        if not de and not ate: params += "&limit=50"
        return ok(sb_get("despesas", params))
    except Exception as e:
        return err(str(e))

@app.post("/api/despesas")
async def post_despesas(request: Request):
    try:
        body = await request.json()
        acao = body.get("acao", "criar")

        if acao == "criar":
            descricao = body.get("descricao", "").strip()
            valor     = float(body.get("valor", 0))
            categoria = body.get("categoria", "Outros")
            if not descricao or valor <= 0:
                return err("Descrição e valor são obrigatórios", 400)
            if categoria == "CMV":
                return err("Categoria CMV é gerada automaticamente", 400)
            res = sb_post("despesas", {"data": body.get("data"), "descricao": descricao,
                                        "valor": valor, "categoria": categoria, "tipo": "saida"})
            return ok(res, status=201)

        elif acao == "excluir":
            did = body.get("id")
            if not did:
                return err("ID obrigatório", 400)
            sb_delete("despesas", f"id=eq.{did}")
            return ok()

        else:
            return err(f"Ação desconhecida: {acao}", 400)

    except Exception as e:
        return err(str(e))

# ── RELATÓRIO ────────────────────────────────────────────────────────────────

@app.get("/api/relatorio")
def get_relatorio(de: str = None, ate: str = None):
    try:
        if not de or not ate:
            return err("Parâmetros 'de' e 'ate' são obrigatórios", 400)

        vendas   = sb_get("vendas",   f"?data=gte.{de}&data=lte.{ate}")
        entradas = sb_get("entradas", f"?data=gte.{de}&data=lte.{ate}&order=data.desc")
        despesas = sb_get("despesas", f"?data=gte.{de}&data=lte.{ate}&categoria=neq.CMV&order=data.desc")

        faturamento  = sum(float(v.get("total", 0)) for v in vendas)
        cmv          = sum(float(v.get("custo", 0)) for v in vendas)
        lucro_bruto  = faturamento - cmv
        total_desp   = sum(float(d.get("valor", 0)) for d in despesas)
        total_compras = sum(float(e.get("total_pago", 0)) for e in entradas)

        por_produto = {}
        for v in vendas:
            for item in (v.get("itens") or []):
                nome    = item.get("nome", "Desconhecido")
                qtd     = int(item.get("qtd", 0))
                receita = float(item.get("preco", 0)) * qtd
                custo   = float(item.get("custo", 0)) * qtd
                if nome not in por_produto:
                    por_produto[nome] = {"qtd": 0, "receita": 0, "custo": 0}
                por_produto[nome]["qtd"]     += qtd
                por_produto[nome]["receita"] += receita
                por_produto[nome]["custo"]   += custo

        produtos_lista = []
        for nome, d in sorted(por_produto.items(), key=lambda x: -x[1]["receita"]):
            lucro_prod = d["receita"] - d["custo"]
            mg_prod    = round(lucro_prod / d["receita"] * 100, 1) if d["receita"] > 0 else 0
            produtos_lista.append({"nome": nome, "qtd": d["qtd"],
                "receita": round(d["receita"], 2), "custo": round(d["custo"], 2),
                "lucro": round(lucro_prod, 2), "margem": mg_prod})

        return JSONResponse({"sucesso": True, "periodo": {"de": de, "ate": ate},
            "resumo": {
                "faturamento":    round(faturamento, 2),
                "cmv":            round(cmv, 2),
                "lucro_bruto":    round(lucro_bruto, 2),
                "lucro_liquido":  round(lucro_bruto - total_desp, 2),
                "total_despesas": round(total_desp, 2),
                "total_compras":  round(total_compras, 2),
                "margem":         round(lucro_bruto / faturamento * 100, 1) if faturamento > 0 else 0,
                "ticket_medio":   round(faturamento / len(vendas), 2) if vendas else 0,
                "qtd_vendas":     len(vendas),
                "qtd_entradas":   len(entradas),
            },
            "por_produto": produtos_lista,
            "entradas":    entradas,
            "despesas":    despesas,
        })

    except Exception as e:
        return err(str(e))
