print("APP CARREGADO:", __file__)
from flask import Flask, render_template, request, redirect, session, send_file
import sqlite3
import os
from datetime import datetime, timedelta
import secrets   # 🔒 ADICIONADO
from werkzeug.security import generate_password_hash, check_password_hash
import re



def calcular_dias_sem_compra(data_ultima_compra):
    if not data_ultima_compra:
        return None
    try:
        data = datetime.strptime(data_ultima_compra, "%Y-%m-%d")
        hoje = datetime.now()
        return (hoje - data).days
    except:
        return None


app = Flask(__name__)


def moeda_br(valor):
    try:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"


app.jinja_env.filters["moeda_br"] = moeda_br

# 🔒 SECRET KEY PROFISSIONAL (antes era fixa)
app.secret_key = secrets.token_hex(32)

# ⚠️ NÃO REMOVI, só marquei para futura segurança
SENHA_CRM = "1234"   # depois vamos eliminar isso




# ================= BANCO PERSISTENTE =================
DB_PATH = "/var/dados/banco.db"

def conectar_db():
    os.makedirs("/var/dados", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def criar_banco():
    conn = conectar_db()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            usuario TEXT UNIQUE,
            senha TEXT,
            tipo TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            telefone TEXT,
            fazenda TEXT,
            cpf TEXT,
            observacoes TEXT,
            id_usuario INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS vendas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            cliente TEXT,
            valor_total REAL,
            comissao_total REAL,
            parcelas INTEGER,
            primeiro_mes TEXT,
            id_usuario INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS itens_venda (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_venda INTEGER,
            produto TEXT,
            quantidade REAL,
            valor_unitario REAL,
            total_item REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS comissoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_venda INTEGER,
            mes TEXT,
            valor REAL,
            status TEXT,
            id_usuario INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS alertas_controle (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente TEXT,
            produto TEXT,
            ocultar_ate TEXT,
            observacao TEXT,
            id_usuario INTEGER
        )
    """)

    conn.commit()
    conn.close()


# 🔥 CRIA BANCO AUTOMATICAMENTE AO SUBIR
criar_banco()



# ================= LOGIN =================
@app.route("/", methods=["GET","POST"])
def login():
    if request.method == "POST":
        usuario = request.form["usuario"]
        senha = request.form["senha"]

        conn = conectar_db()
        c = conn.cursor()
        c.execute("SELECT * FROM usuarios WHERE usuario=?", (usuario,))
        user = c.fetchone()
        conn.close()

        if not user:
            return "❌ Usuário ou senha inválidos"

        senha_banco = user["senha"]

        # SE A SENHA NO BANCO FOR HASH
        if senha_banco.startswith("scrypt") or senha_banco.startswith("pbkdf2"):
            if not check_password_hash(senha_banco, senha):
                return "❌ Usuário ou senha inválidos"
        else:
            # SENHA SIMPLES (1234, etc)
            if senha != senha_banco:
                return "❌ Usuário ou senha inválidos"

        # LOGIN OK
        session["logado"] = True
        session["id_usuario"] = user["id"]
        session["nome_usuario"] = user["nome"]
        session["tipo"] = user["tipo"]

        return redirect("/dashboard")

    return render_template("login.html")


# ================= LOGOUT =================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ================= TROCAR SENHA =================
# ================= TROCAR SENHA =================
@app.route("/trocar_senha", methods=["GET", "POST"])
def trocar_senha():
    if not session.get("logado"):
        return redirect("/")

    if request.method == "POST":
        senha_atual = request.form["senha_atual"]
        nova = request.form["nova_senha"]
        confirmar = request.form["confirmar_senha"]

        if nova != confirmar:
            return "Senhas não conferem"

        id_usuario = session["id_usuario"]

        conn = conectar_db()
        c = conn.cursor()
        c.execute("SELECT senha FROM usuarios WHERE id=?", (id_usuario,))
        senha_banco = c.fetchone()["senha"]

        if senha_atual != senha_banco:
            conn.close()
            return "Senha atual errada"

        c.execute("UPDATE usuarios SET senha=? WHERE id=?", (nova, id_usuario))
        conn.commit()
        conn.close()

        # logout depois de trocar senha
        session.clear()
        return redirect("/")

    return render_template("trocar_senha.html")

# ================= ADMIN CRIAR USUÁRIO =================
@app.route("/admin_criar_usuario", methods=["POST"])
def admin_criar_usuario():
    if session.get("tipo") != "ADMIN":
        return "Acesso negado"

    nome = request.form["nome"]
    usuario = request.form["usuario"]
    tipo = request.form["tipo"]

    senha_hash = generate_password_hash("1234")

    conn = conectar_db()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO usuarios (nome, usuario, senha, tipo) VALUES (?,?,?,?)",
                  (nome, usuario, senha_hash, tipo))
        conn.commit()
    except:
        conn.close()
        return "Usuário já existe"

    conn.close()
    return "Usuário criado com senha 1234"


# ================= ADMIN RESET SENHA =================
@app.route("/admin_reset_senha/<int:id_usuario>")
def admin_reset_senha(id_usuario):
    if session.get("tipo") != "ADMIN":
        return "Acesso negado"

    senha_hash = generate_password_hash("1234")

    conn = conectar_db()
    c = conn.cursor()
    c.execute("UPDATE usuarios SET senha=? WHERE id=?", (senha_hash, id_usuario))
    conn.commit()
    conn.close()

    return "Senha resetada para 1234"


# ================= DASHBOARD =================
@app.route("/dashboard")
def dashboard():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]
    conn = conectar_db()
    c = conn.cursor()

    # TOTAL VENDAS
    c.execute("SELECT SUM(valor_total) FROM vendas WHERE id_usuario=?", (id_usuario,))
    total_vendas = c.fetchone()[0] or 0

    hoje = datetime.now()
    hoje_data = hoje.date()
    mes_atual = hoje.strftime("%Y-%m")
    mes_anterior = (hoje.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    # VENDAS POR MÊS
    c.execute("SELECT SUM(valor_total) FROM vendas WHERE substr(data,1,7)=? AND id_usuario=?", (mes_atual, id_usuario))
    vendas_mes_atual = c.fetchone()[0] or 0

    c.execute("SELECT SUM(valor_total) FROM vendas WHERE substr(data,1,7)=? AND id_usuario=?", (mes_anterior, id_usuario))
    vendas_mes_anterior = c.fetchone()[0] or 0

    # COMISSÃO
    c.execute("SELECT SUM(valor) FROM comissoes WHERE mes=? AND id_usuario=?", (mes_atual, id_usuario))
    total_comissao = c.fetchone()[0] or 0

    # TOP CLIENTES
    c.execute("""
        SELECT cliente, SUM(valor_total) total
        FROM vendas
        WHERE id_usuario=?
        GROUP BY cliente
        ORDER BY total DESC
        LIMIT 3
    """, (id_usuario,))
    top_clientes = c.fetchall()

    # ================= ALERTAS CONTAGEM =================
    c.execute("""
        SELECT v.cliente, i.produto, v.data
        FROM vendas v
        JOIN itens_venda i ON v.id = i.id_venda
        WHERE v.id_usuario=?
        ORDER BY v.cliente, i.produto, v.data
    """, (id_usuario,))
    registros = c.fetchall()

    historico = {}
    for r in registros:
        chave = (r["cliente"], r["produto"])
        historico.setdefault(chave, []).append(datetime.strptime(r["data"], "%Y-%m-%d"))

    qtd_atrasados = 0
    qtd_hoje = 0
    qtd_proximos = 0

    for (cliente, produto), datas in historico.items():
        if len(datas) < 2:
            continue

        intervalos = [(datas[i] - datas[i-1]).days for i in range(1, len(datas)) if (datas[i] - datas[i-1]).days > 0]
        if not intervalos:
            continue

        media = round(sum(intervalos) / len(intervalos))
        ultima = datas[-1].date()
        proxima = ultima + timedelta(days=media)
        diff = (proxima - hoje_data).days

        if diff < 0:
            qtd_atrasados += 1
        elif diff == 0:
            qtd_hoje += 1
        elif diff <= 3:
            qtd_proximos += 1

    # ================= CLIENTES EM RISCO =================
    c.execute("""
        SELECT cliente, MAX(data) ultima
        FROM vendas
        WHERE id_usuario=?
        GROUP BY cliente
    """, (id_usuario,))
    ultimas = c.fetchall()

    risco_30 = risco_60 = risco_90 = 0

    for u in ultimas:
        ultima_data = datetime.strptime(u["ultima"], "%Y-%m-%d").date()
        dias = (hoje_data - ultima_data).days

        if dias >= 30: risco_30 += 1
        if dias >= 60: risco_60 += 1
        if dias >= 90: risco_90 += 1

    conn.close()

    return render_template(
        "dashboard.html",
        total_vendas=total_vendas,
        total_comissao=total_comissao,
        vendas_mes_atual=vendas_mes_atual,
        vendas_mes_anterior=vendas_mes_anterior,
        top_clientes=top_clientes,

        # CONTADORES
        qtd_atrasados=qtd_atrasados,
        qtd_hoje=qtd_hoje,
        qtd_proximos=qtd_proximos,
        risco_30=risco_30,
        risco_60=risco_60,
        risco_90=risco_90
    )

# ================= CLIENTES =================
@app.route("/clientes", methods=["GET","POST"])
def clientes():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]
    conn = conectar_db()
    c = conn.cursor()

    # CADASTRO
    if request.method == "POST":
        c.execute("""
            INSERT INTO clientes (nome, telefone, fazenda, cpf, observacoes, id_usuario)
            VALUES (?,?,?,?,?,?)
        """, (
            request.form["nome"],
            request.form["telefone"],
            request.form["fazenda"],
            request.form["cpf"],
            request.form["observacoes"],
            id_usuario
        ))
        conn.commit()

    # BUSCA
    termo = request.args.get("q","")
    if termo:
        c.execute("SELECT * FROM clientes WHERE nome LIKE ? AND id_usuario=?", (f"%{termo}%", id_usuario))
    else:
        c.execute("SELECT * FROM clientes WHERE id_usuario=?", (id_usuario,))

    dados = c.fetchall()
    conn.close()
    return render_template("clientes.html", clientes=dados, termo=termo)

# ================= VENDAS =================
@app.route("/vendas", methods=["GET", "POST"])
def vendas():
    if not session.get("logado"):
        return redirect("/")

    conn = conectar_db()
    c = conn.cursor()

    id_usuario = session["id_usuario"]  # 👤 usuário logado

    # ================= CLIENTES =================
    c.execute("SELECT nome FROM clientes WHERE id_usuario=? ORDER BY nome", (id_usuario,))
    clientes = [r["nome"] for r in c.fetchall()]

    # ================= PRODUTOS (EXISTENTES) =================
    c.execute("""
        SELECT DISTINCT iv.produto
        FROM itens_venda iv
        JOIN vendas v ON v.id = iv.id_venda
        WHERE v.id_usuario = ?
        ORDER BY iv.produto
    """, (id_usuario,))
    produtos = [r["produto"] for r in c.fetchall()]

    # ================= CADASTRO =================
    if request.method == "POST":
        data = request.form["data"]
        cliente = request.form["cliente"]
        comissao = float(request.form["comissao"])
        parcelas = int(request.form["parcelas"])
        primeiro_mes = request.form["primeiro_mes"]

        produtos_form = request.form.getlist("produto[]")
        quantidades = request.form.getlist("quantidade[]")
        valores = request.form.getlist("valor[]")

        valor_total = 0
        itens = []

        for p, q, v in zip(produtos_form, quantidades, valores):
            if not p:
                continue
            q = int(q)
            v = float(v)
            total = q * v
            valor_total += total
            itens.append((p, q, v, total))

        # INSERIR VENDA (COM USUÁRIO)
        c.execute("""
            INSERT INTO vendas
            (data, cliente, valor_total, comissao_total, parcelas, primeiro_mes, id_usuario)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data, cliente, valor_total, comissao, parcelas, primeiro_mes, id_usuario))

        id_venda = c.lastrowid

        # INSERIR ITENS + REATIVAR ALERTAS
        for i in itens:
            produto = i[0]

            c.execute("""
                INSERT INTO itens_venda
                (id_venda, produto, quantidade, valor_unitario, total_item)
                VALUES (?, ?, ?, ?, ?)
            """, (id_venda, i[0], i[1], i[2], i[3]))

            # REATIVAR ALERTA AUTOMATICAMENTE
            c.execute("""
                DELETE FROM alertas_controle 
                WHERE cliente=? AND produto=? AND id_usuario=?
            """, (cliente, produto, id_usuario))

        conn.commit()

    # ================= FILTROS =================
    data_inicio = request.args.get("inicio")
    data_fim = request.args.get("fim")

    sql = """
        SELECT id, data, cliente, valor_total
        FROM vendas
        WHERE id_usuario = ?
    """
    params = [id_usuario]

    if data_inicio:
        sql += " AND data >= ?"
        params.append(data_inicio)

    if data_fim:
        sql += " AND data <= ?"
        params.append(data_fim)

    sql += " ORDER BY data ASC"

    c.execute(sql, params)
    historico = c.fetchall()

    # ================= TOTAL DO PERÍODO =================
    sql_total = "SELECT SUM(valor_total) FROM vendas WHERE id_usuario = ?"
    params_total = [id_usuario]

    if data_inicio:
        sql_total += " AND data >= ?"
        params_total.append(data_inicio)

    if data_fim:
        sql_total += " AND data <= ?"
        params_total.append(data_fim)

    c.execute(sql_total, params_total)
    total_periodo = c.fetchone()[0] or 0

    conn.close()

    return render_template(
        "vendas.html",
        clientes=clientes,
        produtos=produtos,
        historico=historico,
        data_inicio=data_inicio,
        data_fim=data_fim,
        total_periodo=total_periodo
    )


# ================= DETALHE VENDA =================
@app.route("/venda/<int:id_venda>")
def venda_detalhe(id_venda):
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    # 🔒 SÓ PODE VER A PRÓPRIA VENDA
    c.execute("""
        SELECT *
        FROM vendas
        WHERE id = ? AND id_usuario = ?
    """, (id_venda, id_usuario))
    venda = c.fetchone()

    # Se tentar acessar venda de outro usuário
    if not venda:
        conn.close()
        return "❌ Acesso negado"

    # 🔒 ITENS SÓ SE FOR DO USUÁRIO
    c.execute("""
        SELECT iv.produto, iv.quantidade, iv.valor_unitario, iv.total_item
        FROM itens_venda iv
        JOIN vendas v ON v.id = iv.id_venda
        WHERE iv.id_venda = ? AND v.id_usuario = ?
    """, (id_venda, id_usuario))
    itens = c.fetchall()

    conn.close()

    return render_template(
        "venda_detalhe.html",
        venda=venda,
        itens=itens
    )


# ================= EDITAR VENDA =================
@app.route("/venda/<int:id_venda>/editar", methods=["GET", "POST"])
def editar_venda(id_venda):
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    if request.method == "POST":
        data = request.form["data"]
        cliente = request.form["cliente"]
        comissao = float(request.form["comissao"])
        parcelas = int(request.form["parcelas"])
        primeiro_mes = request.form["primeiro_mes"]

        produtos_form = request.form.getlist("produto[]")
        quantidades = request.form.getlist("quantidade[]")
        valores = request.form.getlist("valor[]")

        valor_total = 0
        itens = []

        for p, q, v in zip(produtos_form, quantidades, valores):
            if not p:
                continue
            q = int(q)
            v = float(v)
            total = q * v
            valor_total += total
            itens.append((p, q, v, total))

        # 🔒 SÓ EDITA SE FOR DO USUÁRIO
        c.execute("""
            UPDATE vendas
            SET data = ?, cliente = ?, valor_total = ?, comissao_total = ?, parcelas = ?, primeiro_mes = ?
            WHERE id = ? AND id_usuario = ?
        """, (data, cliente, valor_total, comissao, parcelas, primeiro_mes, id_venda, id_usuario))

        # 🔒 APAGAR ITENS SÓ SE A VENDA FOR DO USUÁRIO
        c.execute("""
            DELETE FROM itens_venda 
            WHERE id_venda = ? 
            AND id_venda IN (SELECT id FROM vendas WHERE id=? AND id_usuario=?)
        """, (id_venda, id_venda, id_usuario))

        for i in itens:
            c.execute("""
                INSERT INTO itens_venda
                (id_venda, produto, quantidade, valor_unitario, total_item)
                VALUES (?, ?, ?, ?, ?)
            """, (id_venda, i[0], i[1], i[2], i[3]))

        conn.commit()
        conn.close()

        return redirect(f"/venda/{id_venda}")

    # CARREGAR VENDA (SÓ DO USUÁRIO)
    c.execute("SELECT * FROM vendas WHERE id = ? AND id_usuario = ?", (id_venda, id_usuario))
    venda = c.fetchone()

    c.execute("""
        SELECT produto, quantidade, valor_unitario
        FROM itens_venda
        WHERE id_venda = ?
    """, (id_venda,))
    itens = c.fetchall()

    c.execute("SELECT nome FROM clientes WHERE id_usuario=? ORDER BY nome", (id_usuario,))
    clientes = [r["nome"] for r in c.fetchall()]

    c.execute("""
        SELECT DISTINCT iv.produto
        FROM itens_venda iv
        JOIN vendas v ON v.id = iv.id_venda
        WHERE v.id_usuario = ?
        ORDER BY iv.produto
    """, (id_usuario,))
    produtos = [r["produto"] for r in c.fetchall()]

    conn.close()

    return render_template(
        "venda_editar.html",
        venda=venda,
        itens=itens,
        clientes=clientes,
        produtos=produtos
    )


# ================= EXCLUIR VENDA =================
@app.route("/venda/<int:id_venda>/excluir")
def excluir_venda(id_venda):
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    # 🔒 SÓ EXCLUI ITENS SE A VENDA FOR DO USUÁRIO
    c.execute("""
        DELETE FROM itens_venda 
        WHERE id_venda = ? 
        AND id_venda IN (SELECT id FROM vendas WHERE id=? AND id_usuario=?)
    """, (id_venda, id_venda, id_usuario))

    # 🔒 SÓ EXCLUI VENDA SE FOR DO USUÁRIO
    c.execute("DELETE FROM vendas WHERE id = ? AND id_usuario = ?", (id_venda, id_usuario))

    conn.commit()
    conn.close()

    return redirect("/vendas")

# ================= EXPORTAR EXCEL =================
@app.route("/vendas/exportar")
def exportar_vendas():
    if not session.get("logado"):
        return redirect("/")

    import pandas as pd
    import uuid   # 🔒 ADICIONADO PARA ARQUIVO ÚNICO
    id_usuario = session["id_usuario"]

    conn = conectar_db()
    df = pd.read_sql_query("""
        SELECT data, cliente, valor_total
        FROM vendas
        WHERE id_usuario = ?
        ORDER BY data DESC
    """, conn, params=(id_usuario,))

    # 🔒 Nome único para não sobrescrever arquivos
    arquivo = f"vendas_usuario_{id_usuario}_{uuid.uuid4().hex}.xlsx"
    df.to_excel(arquivo, index=False)

    conn.close()
    return send_file(arquivo, as_attachment=True)


# ================= FINANCEIRO =================
@app.route("/financeiro")
def financeiro():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    c.execute("""
        SELECT mes, SUM(valor) total
        FROM comissoes
        WHERE id_usuario = ?
        GROUP BY mes
        ORDER BY mes DESC
    """, (id_usuario,))
    dados = c.fetchall()

    conn.close()
    return render_template("financeiro.html", dados=dados)


# ================= CONTATOS =================
@app.route("/contatos")
def contatos():
    if not session.get("logado"):
        return redirect("/")

    risco = request.args.get("risco")
    hoje = datetime.now().date()
    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    c.execute("""
        SELECT cliente, MAX(data) AS ultima_compra
        FROM vendas
        WHERE id_usuario = ?
        GROUP BY cliente
    """, (id_usuario,))
    registros = c.fetchall()

    contatos = []

    for r in registros:
        if not r["ultima_compra"]:
            continue

        ultima = datetime.strptime(r["ultima_compra"], "%Y-%m-%d").date()
        dias = (hoje - ultima).days

        if risco == "30" and not (30 <= dias < 60):
            continue
        elif risco == "60" and not (60 <= dias < 90):
            continue
        elif risco == "90" and not (dias >= 90):
            continue

        c.execute("""
            SELECT id
            FROM vendas
            WHERE cliente = ? AND data = ? AND id_usuario = ?
            LIMIT 1
        """, (r["cliente"], r["ultima_compra"], id_usuario))
        venda = c.fetchone()

        produtos = []
        if venda:
            # 🔒 FILTRAR PRODUTOS PELO USUÁRIO
            c.execute("""
                SELECT iv.produto
                FROM itens_venda iv
                JOIN vendas v ON v.id = iv.id_venda
                WHERE iv.id_venda = ? AND v.id_usuario = ?
            """, (venda["id"], id_usuario))
            produtos = [p["produto"] for p in c.fetchall()]

        contatos.append({
            "cliente": r["cliente"],
            "data_ultima_compra": r["ultima_compra"],
            "dias_sem_compra": dias,
            "produtos_ultima_compra": produtos
        })

    conn.close()
    contatos.sort(key=lambda x: x["dias_sem_compra"], reverse=True)

    return render_template("contatos.html", contatos=contatos, risco=risco)


# ================= RANKING CLIENTES =================
@app.route("/ranking_clientes")
def ranking_clientes():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    c.execute("""
        SELECT cliente, SUM(valor_total) AS total
        FROM vendas
        WHERE id_usuario = ?
        GROUP BY cliente
        ORDER BY total DESC
    """, (id_usuario,))
    dados = c.fetchall()
    conn.close()

    return render_template("ranking_clientes.html", dados=dados)


# ================= DETALHE RANKING CLIENTE =================
@app.route("/ranking_clientes/<cliente>")
def ranking_cliente_detalhe(cliente):
    if not session.get("logado"):
        return redirect("/")

    ordem = request.args.get("ordem", "valor")
    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    c.execute("""
        SELECT SUM(valor_total) AS total
        FROM vendas
        WHERE cliente = ? AND id_usuario = ?
    """, (cliente, id_usuario))
    total = c.fetchone()["total"] or 0

    # 🔒 ORDEM CONTROLADA (ANTI SQL INJECTION)
    if ordem == "quantidade":
        order_sql = "quantidade_total DESC"
    else:
        order_sql = "valor_total DESC"

    c.execute(f"""
        SELECT 
            iv.produto,
            SUM(iv.quantidade) AS quantidade_total,
            SUM(iv.total_item) AS valor_total
        FROM vendas v
        JOIN itens_venda iv ON iv.id_venda = v.id
        WHERE v.cliente = ? AND v.id_usuario = ?
        GROUP BY iv.produto
        ORDER BY {order_sql}
    """, (cliente, id_usuario))
    produtos = c.fetchall()

    conn.close()

    return render_template(
        "ranking_cliente_detalhe.html",
        cliente=cliente,
        total=total,
        produtos=produtos,
        ordem=ordem
    )


# ================= RANKING PRODUTOS =================
@app.route("/ranking_produtos")
def ranking_produtos():
    if not session.get("logado"):
        return redirect("/")

    ordem = request.args.get("ordem", "valor")
    id_usuario = session["id_usuario"]

    if ordem == "quantidade":
        order_by = "qtd_total DESC"
    elif ordem == "ticket":
        order_by = "ticket_medio DESC"
    else:
        order_by = "valor_total DESC"

    conn = conectar_db()
    c = conn.cursor()

    c.execute(f"""
        SELECT
            iv.produto,
            SUM(iv.quantidade) AS qtd_total,
            SUM(iv.total_item) AS valor_total,
            ROUND(
                CASE 
                    WHEN SUM(iv.quantidade) > 0 
                    THEN SUM(iv.total_item) / SUM(iv.quantidade)
                    ELSE 0
                END, 2
            ) AS ticket_medio
        FROM itens_venda iv
        JOIN vendas v ON v.id = iv.id_venda
        WHERE v.id_usuario = ?
        GROUP BY iv.produto
        ORDER BY {order_by}
    """, (id_usuario,))

    dados = c.fetchall()
    conn.close()

    return render_template("ranking_produtos.html", dados=dados, ordem=ordem)


# ================= DETALHE DO PRODUTO =================
@app.route("/ranking_produtos/<produto>")
def ranking_produto_detalhe(produto):
    if not session.get("logado"):
        return redirect("/")

    ordem = request.args.get("ordem", "valor")
    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    if ordem == "quantidade":
        order_sql = "SUM(iv.quantidade) DESC"
    else:
        ordem = "valor"
        order_sql = "SUM(iv.total_item) DESC"

    sql = f"""
        SELECT
            v.cliente,
            SUM(iv.quantidade) AS qtd_total,
            SUM(iv.total_item) AS valor_total
        FROM vendas v
        JOIN itens_venda iv ON iv.id_venda = v.id
        WHERE iv.produto = ? AND v.id_usuario = ?
        GROUP BY v.cliente
        ORDER BY {order_sql}
    """

    c.execute(sql, (produto, id_usuario))
    dados = c.fetchall()
    conn.close()

    return render_template(
        "ranking_produto_detalhe.html",
        produto=produto,
        dados=dados,
        ordem=ordem
    )


# ================= ALERTAS =================
@app.route("/alertas")
def alertas():
    if not session.get("logado"):
        return redirect("/")

    status_filtro = request.args.get("status")
    id_usuario = session["id_usuario"]

    conn = conectar_db()
    c = conn.cursor()

    # SEM LIMIT (histórico completo)
    c.execute("""
        SELECT v.cliente, i.produto, v.data
        FROM vendas v
        JOIN itens_venda i ON v.id = i.id_venda
        WHERE v.id_usuario = ?
        ORDER BY v.cliente, i.produto, v.data
    """, (id_usuario,))
    registros = c.fetchall()

    historico = {}
    for r in registros:
        chave = (r["cliente"], r["produto"])
        historico.setdefault(chave, []).append(datetime.strptime(r["data"], "%Y-%m-%d"))

    hoje = datetime.now().date()
    alertas = []

    for (cliente, produto), datas in historico.items():
        if len(datas) < 2:
            continue

        intervalos = [(datas[i] - datas[i-1]).days for i in range(1, len(datas)) if (datas[i] - datas[i-1]).days > 0]
        if not intervalos:
            continue

        media = round(sum(intervalos) / len(intervalos))
        ultima = datas[-1].date()
        proxima = ultima + timedelta(days=media)
        diff = (proxima - hoje).days

        # controle por usuário
        c.execute("""
            SELECT ocultar_ate, observacao
            FROM alertas_controle
            WHERE cliente=? AND produto=? AND id_usuario=?
        """, (cliente, produto, id_usuario))
        controle = c.fetchone()

        if controle:
            ocultar_ate = datetime.strptime(controle["ocultar_ate"], "%Y-%m-%d").date()
            if ocultar_ate >= hoje:
                continue
            observacao = controle["observacao"]
        else:
            observacao = ""

        if diff < 0:
            status = "ATRASADO"
        elif diff == 0:
            status = "HOJE"
        elif diff <= 3:
            status = "PROXIMO"
        else:
            continue

        alertas.append({
            "cliente": cliente,
            "produto": produto,
            "ultima": ultima.strftime("%d/%m/%Y"),
            "frequencia": media,
            "proxima": proxima.strftime("%d/%m/%Y"),
            "status": status,
            "observacao": observacao
        })

    if status_filtro:
        alertas = [a for a in alertas if a["status"] == status_filtro]

    conn.close()
    return render_template("alertas.html", alertas=alertas)


# ================= ADIAR ALERTA =================
@app.route("/alertas/adiar", methods=["POST"])
def alertas_adiar():
    if not session.get("logado"):
        return redirect("/")

    cliente = request.form["cliente"]
    produto = request.form["produto"]
    dias = int(request.form["dias"])
    observacao = request.form.get("observacao", "")
    id_usuario = session["id_usuario"]

    ocultar_ate = (datetime.now() + timedelta(days=dias)).strftime("%Y-%m-%d")

    conn = conectar_db()
    c = conn.cursor()

    # 🔒 VALIDAR SE PRODUTO É DO USUÁRIO
    c.execute("""
        SELECT 1 FROM vendas v
        JOIN itens_venda i ON i.id_venda = v.id
        WHERE v.id_usuario=? AND v.cliente=? AND i.produto=?
        LIMIT 1
    """, (id_usuario, cliente, produto))
    if not c.fetchone():
        conn.close()
        return "❌ Acesso inválido"

    c.execute("""
        DELETE FROM alertas_controle 
        WHERE cliente=? AND produto=? AND id_usuario=?
    """, (cliente, produto, id_usuario))

    c.execute("""
        INSERT INTO alertas_controle (cliente, produto, ocultar_ate, observacao, id_usuario)
        VALUES (?,?,?,?,?)
    """, (cliente, produto, ocultar_ate, observacao, id_usuario))

    conn.commit()
    conn.close()
    return redirect("/alertas")


# ================= DESCARTAR ALERTA (ATÉ VOLTAR A COMPRAR) =================
@app.route("/alertas/descartar", methods=["POST"])
def alertas_descartar():
    if not session.get("logado"):
        return redirect("/")

    cliente = request.form["cliente"]
    produto = request.form["produto"]
    id_usuario = session["id_usuario"]

    # 31/12/2099 = descartado praticamente permanente
    ocultar_ate = "2099-12-31"
    observacao = "DESCARTADO MANUALMENTE"

    conn = conectar_db()
    c = conn.cursor()

    # 🔒 APAGAR CONTROLE APENAS DO USUÁRIO LOGADO
    c.execute("""
        DELETE FROM alertas_controle 
        WHERE cliente=? AND produto=? AND id_usuario=?
    """, (cliente, produto, id_usuario))

    # 🔒 INSERIR DESCARTE COM USUÁRIO
    c.execute("""
        INSERT INTO alertas_controle
        (cliente, produto, ocultar_ate, observacao, id_usuario)
        VALUES (?,?,?,?,?)
    """, (cliente, produto, ocultar_ate, observacao, id_usuario))

    conn.commit()
    conn.close()

    return redirect("/alertas")


# ================= IMPORTADOR =================
@app.route("/importar_vendas", methods=["GET", "POST"])
def importar_vendas():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]

    if request.method == "POST":
        import pandas as pd

        arquivo = request.files["arquivo"]
        df = pd.read_excel(arquivo)

        # NORMALIZAR NOMES
        df.columns = [c.strip() for c in df.columns]

        # MAPEAR COLUNAS DO SEU EXCEL PARA PADRÃO CRM
        col_map = {
            "Nro.Nota": "nota",
            "Dt.Ent/Sai": "data",
            "Parceiro": "cliente",
            "DescriÂ£oo": "produto",
            "Qtd Neg": "quantidade",
            "Vlr.UnitÂ¡rio": "valor_unitario"
        }

        # Renomear colunas
        df = df.rename(columns=col_map)

        # CONVERTER DATA
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

        # CONVERTER NUMEROS
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
        df["valor_unitario"] = pd.to_numeric(df["valor_unitario"], errors="coerce")

        # REMOVER LINHAS RUINS
        df = df.dropna(subset=["nota", "data", "cliente", "produto", "quantidade", "valor_unitario"])

        conn = conectar_db()
        c = conn.cursor()

        # AGRUPAR POR NOTA
        grupos = df.groupby(["nota", "cliente", "data"])

        for (nota, cliente, data), grupo in grupos:
            data_str = data.strftime("%Y-%m-%d")
            cliente = str(cliente).strip()

            valor_total = (grupo["quantidade"] * grupo["valor_unitario"]).sum()

            # 🔒 INSERIR VENDA COM USUÁRIO
            c.execute("""
                INSERT INTO vendas (data, cliente, valor_total, comissao_total, parcelas, primeiro_mes, id_usuario)
                VALUES (?, ?, ?, 0, 1, ?, ?)
            """, (data_str, cliente, float(valor_total), data_str[:7], id_usuario))

            id_venda = c.lastrowid

            for _, row in grupo.iterrows():
                produto = str(row["produto"]).strip()
                qtd = float(row["quantidade"])
                valor = float(row["valor_unitario"])
                total = qtd * valor

                c.execute("""
                    INSERT INTO itens_venda (id_venda, produto, quantidade, valor_unitario, total_item)
                    VALUES (?, ?, ?, ?, ?)
                """, (id_venda, produto, qtd, valor, total))

                # ALERTA POR USUÁRIO
                c.execute("""
                    DELETE FROM alertas_controle 
                    WHERE cliente=? AND produto=? AND id_usuario=?
                """, (cliente, produto, id_usuario))

        conn.commit()
        conn.close()

        return redirect("/vendas")

    return render_template("importar_vendas.html")



# ================= IMPORTADOR PDF HASS E ARRUDA =================
@app.route("/importar_pdf", methods=["GET", "POST"])
def importar_pdf():
    if not session.get("logado"):
        return redirect("/")

    id_usuario = session["id_usuario"]  # 🔒 usuário logado

    if request.method == "POST":
        import pdfplumber
        import re

        arquivo = request.files["arquivo"]
        data_venda = datetime.now().strftime("%Y-%m-%d")
        primeiro_mes = data_venda[:7]

        conn = conectar_db()
        c = conn.cursor()

        itens = []
        valor_total = 0
        cliente = "CLIENTE PDF"
        nota = "SEM_NOTA"

        with pdfplumber.open(arquivo) as pdf:
            texto = ""
            for page in pdf.pages:
                texto += page.extract_text() + "\n"

        # ================= PEGAR CLIENTE =================
        m_cliente = re.search(r"Raz\. Social\.*:\s*(.+)", texto)
        if m_cliente:
            cliente = m_cliente.group(1).strip()

        # REMOVER EMAIL DO NOME
        cliente = re.sub(r"Email.*", "", cliente).strip()

        # ================= PEGAR NOTA =================
        m_nota = re.search(r"N°\s*Único:\s*(\d+)", texto)
        if m_nota:
            nota = m_nota.group(1)

        # ================= PEGAR PRODUTOS =================
        linhas = texto.split("\n")
        for l in linhas:
            m = re.match(r"^\d+\s+(.+?)\s+\w+\s+([\d,]+)\s+([\d,]+)\s+([\d.,]+)", l)
            if m:
                produto = m.group(1).strip()
                qtd = float(m.group(2).replace(".", "").replace(",", "."))
                valor = float(m.group(3).replace(".", "").replace(",", "."))

                total = qtd * valor
                valor_total += total
                itens.append((produto, qtd, valor, total))

        # 🔒 NÃO DUPLICAR NOTA (POR USUÁRIO)
        c.execute("""
            SELECT id FROM vendas 
            WHERE cliente=? AND data=? AND valor_total=? AND id_usuario=?
        """, (cliente, data_venda, valor_total, id_usuario))
        if c.fetchone():
            return "❌ PDF JÁ IMPORTADO"

        # INSERIR VENDA (🔒 COM USUÁRIO)
        c.execute("""
            INSERT INTO vendas (data, cliente, valor_total, comissao_total, parcelas, primeiro_mes, id_usuario)
            VALUES (?, ?, ?, 0, 1, ?, ?)
        """, (data_venda, cliente, valor_total, primeiro_mes, id_usuario))

        id_venda = c.lastrowid

        # INSERIR ITENS
        for p in itens:
            c.execute("""
                INSERT INTO itens_venda (id_venda, produto, quantidade, valor_unitario, total_item)
                VALUES (?, ?, ?, ?, ?)
            """, (id_venda, p[0], p[1], p[2], p[3]))

            # 🔒 REATIVAR ALERTAS SÓ DO USUÁRIO
            c.execute("""
                DELETE FROM alertas_controle WHERE cliente=? AND produto=? AND id_usuario=?
            """, (cliente, p[0], id_usuario))

        conn.commit()
        conn.close()

        return redirect(f"/venda/{id_venda}")

    return render_template("importar_pdf.html")

# ================= ADMIN USUÁRIOS =================
@app.route("/admin_usuarios", methods=["GET", "POST"])
def admin_usuarios():
    if not session.get("logado"):
        return redirect("/")

    # Só admin pode acessar
    if session.get("id_usuario") != 1:
        return "❌ Acesso negado. Apenas ADMIN."

    conn = conectar_db()
    c = conn.cursor()

    # CADASTRAR USUÁRIO
    if request.method == "POST":
        nome = request.form["nome"]
        usuario = request.form["usuario"]
        senha = request.form["senha"]
        tipo = request.form["tipo"]

        c.execute("""
            INSERT INTO usuarios (nome, usuario, senha, tipo)
            VALUES (?, ?, ?, ?)
        """, (nome, usuario, senha, tipo))

        conn.commit()

    # LISTAR USUÁRIOS
    c.execute("SELECT id, nome, usuario, tipo FROM usuarios")
    usuarios = c.fetchall()
    conn.close()

    return render_template("admin_usuarios.html", usuarios=usuarios)

# DELETAR USUÁRIO
@app.route("/admin_deletar_usuario/<int:id>")
def admin_deletar_usuario(id):
    if not session.get("logado"):
        return redirect("/")

    if session.get("id_usuario") != 1:
        return "Acesso negado"

    conn = conectar_db()
    c = conn.cursor()
    c.execute("DELETE FROM usuarios WHERE id=?", (id,))
    conn.commit()
    conn.close()

    return redirect("/admin_usuarios")


# ================= START =================
criar_banco()














