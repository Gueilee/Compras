from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
import sqlite3
import os
import shutil
from datetime import datetime

app = FastAPI(title="SaaS Compras Vendemmia - Enterprise", version="2.3")

# --- CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_PATH = os.path.normpath(os.path.join(BASE_DIR, ".."))
FRONTEND_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "frontend"))
DB_PATH = os.path.join(BASE_DIR, 'vendemmia_compras.db')

# Serve frontend pages
app.mount("/frontend", StaticFiles(directory=FRONTEND_PATH), name="frontend")
# Serve project root assets (logo.png, login.png, etc.)
app.mount("/img", StaticFiles(directory=ROOT_PATH), name="img")
# Serve uploaded files (requisições, cotações, contas_fixas)
_uploads_dir = os.path.join(BASE_DIR, "uploads")
os.makedirs(_uploads_dir, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=_uploads_dir), name="uploads")
# Serve NF uploads (notas fiscais da conciliação)
_nf_uploads_dir = os.path.join(BASE_DIR, "nf_uploads")
os.makedirs(_nf_uploads_dir, exist_ok=True)
app.mount("/nf-uploads", StaticFiles(directory=_nf_uploads_dir), name="nf-uploads")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# DB INIT — cria tabelas se não existirem
# ==========================================
def _init_config_tables():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Usuarios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nome        TEXT NOT NULL,
            email       TEXT UNIQUE NOT NULL,
            unidade     TEXT,
            cargo       TEXT,
            gestor_id   INTEGER,
            gestor_nome TEXT,
            ativo       INTEGER DEFAULT 1,
            criado_em   TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS Compradores_Responsabilidade (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            comprador   TEXT NOT NULL,
            email       TEXT,
            unidade     TEXT,
            categoria   TEXT,
            prioridade  INTEGER DEFAULT 1,
            ativo       INTEGER DEFAULT 1
        );
    """)
    # Tabela de uploads de NF
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS NF_Uploads (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            id_pedido   INTEGER NOT NULL,
            numero_nf   TEXT,
            nome_arquivo TEXT,
            caminho     TEXT,
            tamanho_kb  REAL,
            tipo        TEXT,
            enviado_em  TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    # Garante tabela Orcamentos com suporte a ano
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Orcamentos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            unidade         TEXT NOT NULL,
            ano             INTEGER NOT NULL DEFAULT 2026,
            orcamento_anual REAL NOT NULL DEFAULT 0,
            consumido       REAL DEFAULT 0,
            UNIQUE(unidade, ano)
        );
    """)
    # Tabelas de Contas Fixas (contratos anuais recorrentes)
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Contas_Fixas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nome        TEXT NOT NULL,
            fornecedor  TEXT,
            categoria   TEXT,
            unidade     TEXT,
            valor_anual REAL DEFAULT 0,
            valor_mensal REAL DEFAULT 0,
            data_inicio TEXT,
            data_fim    TEXT,
            status      TEXT DEFAULT 'ativo',
            descricao   TEXT,
            criado_em   TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS Lancamentos_CF (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id_conta        INTEGER NOT NULL,
            mes             INTEGER NOT NULL,
            ano             INTEGER NOT NULL,
            valor           REAL NOT NULL,
            tipo_doc        TEXT DEFAULT 'NF',
            numero_doc      TEXT,
            arquivo_path    TEXT,
            arquivo_nome    TEXT,
            data_lancamento TEXT DEFAULT (datetime('now','localtime')),
            obs             TEXT
        );
    """)

    # Tabela de arquivos vinculados a requisições
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Arquivos_Requisicao (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            id_requisicao INTEGER NOT NULL,
            origem      TEXT NOT NULL DEFAULT 'requisitante',
            nome_arquivo TEXT NOT NULL,
            caminho     TEXT NOT NULL,
            tamanho_kb  REAL,
            tipo_mime   TEXT,
            enviado_em  TEXT DEFAULT (datetime('now','localtime')),
            enviado_por TEXT
        );
    """)

    # Migrações seguras — adiciona colunas novas se não existirem
    for migration in [
        "ALTER TABLE Usuarios ADD COLUMN gestor_nome TEXT",
        "ALTER TABLE Usuarios ADD COLUMN solicitacao_pendente INTEGER DEFAULT 0",
        "ALTER TABLE Requisicoes ADD COLUMN justificativa TEXT",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN pagamento TEXT DEFAULT '30 DDL'",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN validade_dias INTEGER DEFAULT 15",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN observacoes TEXT",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN selecionado INTEGER DEFAULT 0",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN frete_incluso INTEGER DEFAULT 1",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN imposto_incluso INTEGER DEFAULT 1",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN arquivo_nome TEXT",
        "ALTER TABLE Lances_Fornecedor ADD COLUMN arquivo_path TEXT",
        "ALTER TABLE Requisicoes ADD COLUMN setor TEXT",
        "ALTER TABLE Orcamentos ADD COLUMN ano INTEGER NOT NULL DEFAULT 2026",
    ]:
        try:
            cursor.execute(migration)
            conn.commit()
        except Exception:
            pass  # coluna já existe
    # Backfill: espelha arquivos de cotação existentes em Arquivos_Requisicao
    try:
        cursor.execute("""
            INSERT INTO Arquivos_Requisicao
              (id_requisicao, origem, nome_arquivo, caminho, enviado_por)
            SELECT
              l.id_requisicao,
              'fornecedor',
              l.arquivo_nome,
              l.arquivo_path,
              COALESCE(f.razao_social, l.cnpj_fornecedor)
            FROM Lances_Fornecedor l
            LEFT JOIN Fornecedores f ON f.cnpj = l.cnpj_fornecedor
            WHERE l.arquivo_nome IS NOT NULL
              AND l.arquivo_nome != ''
              AND l.arquivo_path IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM Arquivos_Requisicao ar
                  WHERE ar.id_requisicao = l.id_requisicao
                    AND ar.caminho = l.arquivo_path
              )
        """)
        conn.commit()
    except Exception:
        pass

    # Backfill: espelha NF_Uploads (conciliação) em Arquivos_Requisicao se ainda não foram espelhadas
    try:
        cursor.execute("""
            INSERT INTO Arquivos_Requisicao (id_requisicao, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_por)
            SELECT
              n.id_pedido,
              'comprador',
              n.nome_arquivo,
              '/nf-uploads/' || n.caminho,
              n.tamanho_kb,
              CASE WHEN LOWER(n.tipo)='pdf' THEN 'application/pdf' ELSE 'application/xml' END,
              COALESCE('Conciliação — NF ' || NULLIF(n.numero_nf,''), 'Nota Fiscal')
            FROM NF_Uploads n
            WHERE NOT EXISTS (
                SELECT 1 FROM Arquivos_Requisicao ar
                WHERE ar.id_requisicao = n.id_pedido
                  AND ar.nome_arquivo = n.nome_arquivo
                  AND ar.origem = 'comprador'
            )
        """)
        conn.commit()
    except Exception:
        pass

    # Backfill: requisições com lances respondidos (preco > 0) mas ainda em
    # 'Aguardando Cotação' → avança para 'Em Cotação'
    try:
        cursor.execute("""
            UPDATE Requisicoes
            SET status = 'Em Cotação'
            WHERE status = 'Aguardando Cotação'
              AND id_sharepoint IN (
                  SELECT DISTINCT id_requisicao
                  FROM Lances_Fornecedor
                  WHERE preco_unitario > 0
              )
        """)
        conn.commit()
    except Exception:
        pass

    # Tabela de preços por item por lance de fornecedor
    try:
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS Lances_Fornecedor_Itens (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                id_lance       INTEGER NOT NULL,
                id_requisicao  INTEGER NOT NULL,
                descricao      TEXT NOT NULL,
                quantidade     INTEGER NOT NULL DEFAULT 1,
                preco_unitario REAL NOT NULL DEFAULT 0
            );
        """)
    except Exception:
        pass

    conn.commit(); conn.close()

_init_config_tables()

REQ_UPLOAD_DIR = os.path.join(BASE_DIR, "uploads", "requisicoes")
os.makedirs(REQ_UPLOAD_DIR, exist_ok=True)

# --- MODELOS DE DADOS ---
class ItemRequisicao(BaseModel):
    descricao: str
    quantidade: float
    segmento: str

class NovaRequisicao(BaseModel):
    unidade: str
    setor: str
    comprador: str
    justificativa: str
    itens: List[ItemRequisicao]

class AcaoWorkflow(BaseModel):
    acao: str
    justificativa: str = ""

class ItemPreco(BaseModel):
    descricao: str
    quantidade: int
    preco_unitario: float

class LanceFornecedor(BaseModel):
    id_requisicao: int
    cnpj_fornecedor: str
    preco_unitario: float          # valor total da proposta
    prazo_entrega: int
    pagamento: Optional[str] = "30 DDL"
    validade_dias: Optional[int] = 15
    observacoes: Optional[str] = None
    frete_incluso: Optional[int] = 1
    imposto_incluso: Optional[int] = 1
    itens: Optional[List[ItemPreco]] = None  # preços por item

class SelecionarFornecedor(BaseModel):
    cnpj_fornecedor: str

class DadosRecebimento(BaseModel):
    numero_nf: str
    qtd_recebida: float
    valor_nf: float

class AtualizacaoRequisicao(BaseModel):
    status: Optional[str] = None
    fornecedor: Optional[str] = None
    valor_fechado: Optional[float] = None
    observacoes: Optional[str] = None

class UsuarioIn(BaseModel):
    nome: str
    email: str
    unidade: Optional[str] = None
    cargo: Optional[str] = None
    gestor_nome: Optional[str] = None
    ativo: Optional[int] = 1

class SolicitacaoAcessoIn(BaseModel):
    nome: str
    email: str
    gestor_nome: Optional[str] = None

class CompradorIn(BaseModel):
    comprador: str
    email: Optional[str] = None
    unidade: Optional[str] = None
    categoria: Optional[str] = None
    prioridade: Optional[int] = 1
    ativo: Optional[int] = 1

class OrcamentoIn(BaseModel):
    unidade: str
    ano: int
    orcamento_anual: float

class ContaFixaIn(BaseModel):
    nome: str
    fornecedor: Optional[str] = None
    categoria: Optional[str] = None
    unidade: Optional[str] = None
    valor_anual: Optional[float] = 0
    valor_mensal: Optional[float] = 0
    data_inicio: Optional[str] = None
    data_fim: Optional[str] = None
    status: Optional[str] = 'ativo'
    descricao: Optional[str] = None

class LancamentoCFIn(BaseModel):
    mes: int
    ano: int
    valor: float
    tipo_doc: Optional[str] = 'NF'
    numero_doc: Optional[str] = None
    obs: Optional[str] = None

# ==========================================
# 1. MÓDULO DASHBOARD (DADOS REAIS)
# ==========================================
@app.get("/dashboard-dados")
def obter_dados_dashboard(unidade: str = "", period: str = "", mes: str = ""):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build WHERE conditions from filters
    where, params = [], []
    if unidade:
        where.append("unidade = ?"); params.append(unidade)
    if period:
        where.append("data_solicitacao LIKE ?"); params.append(f"%/{period}")
    if mes:
        where.append("SUBSTR(data_solicitacao, 4, 2) = ?"); params.append(mes.zfill(2))
    wc = ("WHERE " + " AND ".join(where)) if where else ""

    # Sazonalidade needs an extra NOT NULL guard — build a combined clause
    sazon_cond = where + ["data_solicitacao IS NOT NULL", "data_solicitacao != ''"]
    sazon_wc   = "WHERE " + " AND ".join(sazon_cond)

    # KPIs Gerais
    cursor.execute(f"SELECT COUNT(id_sharepoint), SUM(valor_fechado) FROM Requisicoes {wc}", params)
    kpis = cursor.fetchone()
    total_pedidos = kpis[0] or 0
    total_gasto   = kpis[1] or 0.0

    # Sazonalidade — sort chronologically by YYYY then MM
    cursor.execute(f"""
        SELECT SUBSTR(data_solicitacao, 4, 7) as mes, COUNT(id_sharepoint)
        FROM Requisicoes {sazon_wc}
        GROUP BY mes
        ORDER BY SUBSTR(data_solicitacao, 7, 4), SUBSTR(data_solicitacao, 4, 2)
    """, params)
    sazonalidade = [{"mes": r[0] if r[0] else "Sem Data", "qtd": r[1]} for r in cursor.fetchall()]

    # Status dos Processos — label NULLs
    cursor.execute(f"SELECT COALESCE(status,'Não Classificado'), COUNT(id_sharepoint) FROM Requisicoes {wc} GROUP BY status ORDER BY COUNT(id_sharepoint) DESC", params)
    status_pedidos = [{"status": r[0], "qtd": r[1]} for r in cursor.fetchall()]

    # Top Compradores
    cursor.execute(f"SELECT COALESCE(comprador,'N/A'), COUNT(id_sharepoint) as qtd FROM Requisicoes {wc} GROUP BY comprador ORDER BY qtd DESC LIMIT 5", params)
    compradores = [{"nome": r[0], "qtd": r[1]} for r in cursor.fetchall()]

    # Requisições por Unidade
    cursor.execute(f"SELECT COALESCE(unidade,'N/A'), COUNT(id_sharepoint) FROM Requisicoes {wc} GROUP BY unidade ORDER BY COUNT(id_sharepoint) DESC", params)
    unidades = [{"unidade": r[0], "qtd": r[1]} for r in cursor.fetchall()]

    # Available filter options
    cursor.execute("SELECT DISTINCT unidade FROM Requisicoes WHERE unidade IS NOT NULL ORDER BY unidade")
    opt_unidades = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT SUBSTR(data_solicitacao,7,4) as ano FROM Requisicoes WHERE data_solicitacao IS NOT NULL AND data_solicitacao != '' ORDER BY ano DESC")
    opt_anos = [r[0] for r in cursor.fetchall() if r[0]]

    conn.close()
    return {
        "kpis": {"total_pedidos": total_pedidos, "total_gasto": total_gasto},
        "sazonalidade": sazonalidade,
        "status": status_pedidos,
        "compradores": compradores,
        "unidades": unidades,
        "opts": {"unidades": opt_unidades, "anos": opt_anos},
    }
# ==========================================
# 2. MÓDULO INTAKE
# ==========================================
@app.get("/api/opcoes-formulario")
def obter_opcoes_formulario():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    # Unidades reais do banco
    cursor.execute("SELECT DISTINCT unidade FROM Requisicoes WHERE unidade IS NOT NULL AND unidade != '' ORDER BY unidade")
    unidades = [r[0] for r in cursor.fetchall()]

    # Categorias do banco (uppercase)
    cursor.execute("SELECT macro_categoria, segmento FROM Categorias ORDER BY macro_categoria, segmento")
    categorias = {}
    for row in cursor.fetchall():
        macro = (row[0] or '').upper()
        seg   = (row[1] or '').upper()
        if macro not in categorias: categorias[macro] = []
        categorias[macro].append(seg)
    conn.close()

    setores = [
        "ADMINISTRATIVO", "COMERCIAL", "COMPRAS", "FINANCEIRO",
        "LOGÍSTICA", "MANUTENÇÃO", "OPERAÇÕES", "QUALIDADE",
        "RH / RECURSOS HUMANOS", "SEGURANÇA", "TI / TECNOLOGIA", "OUTROS"
    ]

    return {"unidades": unidades, "setores": setores, "categorias": categorias}

@app.post("/requisicoes")
def criar_nova_requisicao(req: NovaRequisicao):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT MAX(id_sharepoint) FROM Requisicoes"); max_id = cursor.fetchone()[0]; novo_id = (max_id if max_id else 0) + 1
    cursor.execute("INSERT INTO Requisicoes (id_sharepoint, unidade, setor, data_solicitacao, comprador, status, justificativa) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (novo_id, req.unidade, req.setor, datetime.now().strftime("%d/%m/%Y"), req.comprador, "Aguardando Aprovação do Gestor", req.justificativa))
    for item in req.itens:
        cursor.execute("INSERT INTO Itens_Requisicao (id_requisicao, descricao, quantidade, segmento_historico) VALUES (?, ?, ?, ?)", 
                       (novo_id, item.descricao, item.quantidade, item.segmento))
    conn.commit(); conn.close()
    return {"id_pedido": novo_id}

# ==========================================
# 3. MÓDULO WORKFLOW
# ==========================================
@app.get("/api/aprovacoes/pendentes")
def buscar_pendencias():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id_sharepoint, unidade, data_solicitacao, comprador, justificativa FROM Requisicoes WHERE status = 'Aguardando Aprovação do Gestor'")
    pedidos = []
    for p in cursor.fetchall():
        cursor.execute("SELECT descricao, quantidade, segmento_historico FROM Itens_Requisicao WHERE id_requisicao = ?", (p[0],))
        itens = [{"descricao": i[0], "quantidade": i[1], "segmento": i[2]} for i in cursor.fetchall()]
        pedidos.append({"id_pedido": p[0], "unidade": p[1], "data": p[2], "solicitante": p[3], "justificativa": p[4], "itens": itens})
    conn.close(); return {"pedidos": pedidos}

@app.post("/api/aprovacoes/{id_pedido}")
def processar_workflow(id_pedido: int, acao: AcaoWorkflow):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    novo_status = "Aguardando Cotação" if acao.acao == 'aprovar' else "Reprovado"
    cursor.execute("UPDATE Requisicoes SET status = ?, observacoes = ? WHERE id_sharepoint = ?", (novo_status, acao.justificativa, id_pedido))
    conn.commit(); conn.close(); return {"status": "ok"}

# ==========================================
# 4. MÓDULO SOURCING
# ==========================================
@app.get("/api/sourcing/pedidos-aprovados")
def buscar_pedidos_para_cotar():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id_sharepoint, comprador, unidade, data_solicitacao, status
        FROM Requisicoes
        WHERE status IN ('Aguardando Cotação', 'Em Cotação')
        ORDER BY id_sharepoint DESC
    """)
    pedidos = [{"id": r[0], "solicitante": r[1], "unidade": r[2], "data": r[3], "status": r[4]} for r in cursor.fetchall()]
    conn.close(); return pedidos

@app.get("/fornecedores/{segmento}")
def buscar_inteligencia_sourcing(segmento: str):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute('''SELECT f.razao_social, f.cnpj, f.email, f.telefone, f.vendedor FROM Fornecedores f 
                      JOIN Fornecedores_Segmentos fs ON f.cnpj = fs.cnpj_fornecedor 
                      JOIN Categorias c ON fs.id_categoria = c.id WHERE c.segmento LIKE ?''', ('%' + segmento + '%',))
    fornecedores = [{"razao_social": r[0], "cnpj": r[1], "email": r[2], "telefone": r[3], "vendedor": r[4]} for r in cursor.fetchall()]
    cursor.execute('''SELECT r.data_solicitacao, i.descricao, i.quantidade, r.valor_fechado, r.fornecedor FROM Itens_Requisicao i 
                      JOIN Requisicoes r ON i.id_requisicao = r.id_sharepoint 
                      WHERE i.segmento_historico LIKE ? AND r.status LIKE '%Concluído%' LIMIT 5''', ('%' + segmento + '%',))
    historico = [{"data": r[0], "item": r[1], "qtd": r[2], "valor": r[3], "fornecedor": r[4]} for r in cursor.fetchall()]
    conn.close(); return {"fornecedores": fornecedores, "historico_precos": historico, "total_fornecedores": len(fornecedores)}

@app.post("/api/cotacao/enviar")
def salvar_lance_fornecedor(lance: LanceFornecedor):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    # Se vieram itens com preços individuais, recalcula o total
    valor_total = lance.preco_unitario
    if lance.itens:
        valor_total = round(sum(i.quantidade * i.preco_unitario for i in lance.itens), 2)

    # UPSERT — busca por CNPJ normalizado para evitar duplicatas por formato diferente
    cursor.execute("""
        SELECT id FROM Lances_Fornecedor
        WHERE id_requisicao = ?
          AND REPLACE(REPLACE(REPLACE(cnpj_fornecedor,'.',''),'/',''),'-','')
            = REPLACE(REPLACE(REPLACE(?,'.',''),'/',''),'-','')
    """, (lance.id_requisicao, lance.cnpj_fornecedor))
    existing = cursor.fetchone()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    if existing:
        lance_id = existing[0]
        cursor.execute("""
            UPDATE Lances_Fornecedor
            SET preco_unitario=?, prazo_entrega_dias=?, data_resposta=?,
                pagamento=?, validade_dias=?, observacoes=?, frete_incluso=?, imposto_incluso=?
            WHERE id = ?
        """, (valor_total, lance.prazo_entrega, now,
              lance.pagamento, lance.validade_dias, lance.observacoes,
              lance.frete_incluso, lance.imposto_incluso, lance_id))
    else:
        cursor.execute("""
            INSERT INTO Lances_Fornecedor
              (id_requisicao, cnpj_fornecedor, preco_unitario, prazo_entrega_dias, data_resposta,
               pagamento, validade_dias, observacoes, frete_incluso, imposto_incluso)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (lance.id_requisicao, lance.cnpj_fornecedor, valor_total, lance.prazo_entrega, now,
              lance.pagamento, lance.validade_dias, lance.observacoes,
              lance.frete_incluso, lance.imposto_incluso))
        lance_id = cursor.lastrowid

    # Salva preços por item (remove anteriores e reinsere)
    if lance.itens:
        cursor.execute("DELETE FROM Lances_Fornecedor_Itens WHERE id_lance = ?", (lance_id,))
        for it in lance.itens:
            cursor.execute("""
                INSERT INTO Lances_Fornecedor_Itens (id_lance, id_requisicao, descricao, quantidade, preco_unitario)
                VALUES (?,?,?,?,?)
            """, (lance_id, lance.id_requisicao, it.descricao, it.quantidade, it.preco_unitario))

    conn.commit(); conn.close(); return {"status": "ok"}

COTACAO_UPLOAD_DIR = os.path.join(BASE_DIR, "uploads", "cotacoes")
os.makedirs(COTACAO_UPLOAD_DIR, exist_ok=True)

@app.post("/api/cotacao/upload-doc")
async def upload_cotacao_doc(
    id_requisicao: int = Form(...),
    cnpj_fornecedor: str = Form(...),
    arquivo: UploadFile = File(...)
):
    """Vincula um arquivo a uma cotação já submetida e espelha em Arquivos_Requisicao."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM Lances_Fornecedor WHERE id_requisicao=? AND cnpj_fornecedor=?",
        (id_requisicao, cnpj_fornecedor)
    )
    row = cursor.fetchone()
    if not row:
        conn.close(); raise HTTPException(status_code=404, detail="Cotação não encontrada")
    safe_name = f"{id_requisicao}_{cnpj_fornecedor}_{arquivo.filename}"
    dest = os.path.join(COTACAO_UPLOAD_DIR, safe_name)
    content = await arquivo.read()
    with open(dest, "wb") as f:
        f.write(content)
    arquivo_path = f"/uploads/cotacoes/{safe_name}"
    tamanho_kb   = round(len(content) / 1024, 1)

    # Atualiza Lances_Fornecedor
    cursor.execute(
        "UPDATE Lances_Fornecedor SET arquivo_nome=?, arquivo_path=? WHERE id=?",
        (arquivo.filename, arquivo_path, row[0])
    )

    # Busca nome do fornecedor para registrar como enviado_por
    cursor.execute(
        "SELECT COALESCE(f.razao_social, ?) FROM Fornecedores f WHERE f.cnpj=?",
        (cnpj_fornecedor, cnpj_fornecedor)
    )
    nome_forn = (cursor.fetchone() or [cnpj_fornecedor])[0]

    # Espelha em Arquivos_Requisicao (origem='fornecedor') evitando duplicata
    cursor.execute("""
        SELECT id FROM Arquivos_Requisicao
        WHERE id_requisicao=? AND caminho=?
    """, (id_requisicao, arquivo_path))
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO Arquivos_Requisicao
              (id_requisicao, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_por)
            VALUES (?,?,?,?,?,?,?)
        """, (id_requisicao, 'fornecedor', arquivo.filename, arquivo_path,
              tamanho_kb, arquivo.content_type, nome_forn))

    conn.commit(); conn.close()
    return {"status": "ok", "arquivo_nome": arquivo.filename, "arquivo_path": arquivo_path}

@app.get("/api/cotacao/historico")
def historico_fornecedor(cnpj: str):
    """Retorna o histórico de cotações enviadas por um CNPJ."""
    cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "").strip()
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT l.id, l.id_requisicao,
               COALESCE(r.unidade,'—') as unidade,
               COALESCE(r.comprador,'—') as comprador,
               COALESCE(r.data_solicitacao,'—') as data_req,
               l.preco_unitario, l.prazo_entrega_dias, l.pagamento,
               l.data_resposta, l.selecionado, l.observacoes,
               l.validade_dias, l.frete_incluso, l.imposto_incluso,
               l.arquivo_nome, COALESCE(r.status,'—') as status_req
        FROM Lances_Fornecedor l
        LEFT JOIN Requisicoes r ON l.id_requisicao = r.id_sharepoint
        WHERE REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','') = ?
        ORDER BY l.data_resposta DESC
    """, (cnpj_clean,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "id": r[0], "id_requisicao": r[1],
            "titulo": f"Requisição #{r[1]} — {r[2]}",
            "unidade": r[2], "comprador": r[3], "data_requisicao": r[4],
            "preco_unitario": r[5], "prazo": r[6], "pagamento": r[7],
            "data_resposta": r[8], "selecionado": bool(r[9]),
            "observacoes": r[10], "validade_dias": r[11],
            "frete_incluso": bool(r[12]), "imposto_incluso": bool(r[13]),
            "arquivo_nome": r[14], "status_req": r[15]
        }
        for r in rows
    ]

@app.post("/api/requisicoes/{id_requisicao}/upload")
async def upload_arquivo_requisicao(
    id_requisicao: int,
    origem: str = Form("requisitante"),
    enviado_por: str = Form(""),
    arquivo: UploadFile = File(...)
):
    """Faz upload de arquivo vinculado a uma requisição."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id_sharepoint FROM Requisicoes WHERE id_sharepoint=?", (id_requisicao,))
    if not cursor.fetchone():
        conn.close(); raise HTTPException(404, "Requisição não encontrada")
    safe_name = f"{id_requisicao}_{origem}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{arquivo.filename}"
    dest = os.path.join(REQ_UPLOAD_DIR, safe_name)
    content = await arquivo.read()
    with open(dest, "wb") as f:
        f.write(content)
    tamanho_kb = len(content) / 1024
    cursor.execute("""
        INSERT INTO Arquivos_Requisicao (id_requisicao, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_por)
        VALUES (?,?,?,?,?,?,?)
    """, (id_requisicao, origem, arquivo.filename,
          f"/uploads/requisicoes/{safe_name}", round(tamanho_kb, 1),
          arquivo.content_type, enviado_por))
    conn.commit()
    arq_id = cursor.lastrowid
    conn.close()
    return {"status":"ok","id":arq_id,"nome_arquivo":arquivo.filename,
            "caminho":f"/uploads/requisicoes/{safe_name}","tamanho_kb":round(tamanho_kb,1)}

@app.get("/api/requisicoes/{id_requisicao}/arquivos")
def listar_arquivos_requisicao(id_requisicao: int):
    """Lista todos os arquivos de uma requisição, agrupados por origem."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_em, enviado_por
        FROM Arquivos_Requisicao WHERE id_requisicao=? ORDER BY enviado_em
    """, (id_requisicao,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id":r[0],"origem":r[1],"nome_arquivo":r[2],"caminho":r[3],
             "tamanho_kb":r[4],"tipo_mime":r[5],"enviado_em":r[6],"enviado_por":r[7]}
            for r in rows]

@app.delete("/api/requisicoes/arquivo/{arq_id}")
def deletar_arquivo_requisicao(arq_id: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT caminho FROM Arquivos_Requisicao WHERE id=?", (arq_id,))
    row = cursor.fetchone()
    if row:
        full_path = os.path.join(BASE_DIR, row[0].lstrip('/').replace('/', os.sep))
        try: os.remove(full_path)
        except: pass
        cursor.execute("DELETE FROM Arquivos_Requisicao WHERE id=?", (arq_id,))
        conn.commit()
    conn.close()
    return {"status":"ok"}

@app.get("/api/requisicoes/{id_requisicao}/detalhes-completos")
def detalhes_completos_requisicao(id_requisicao: int):
    """Retorna tudo sobre uma requisição: dados, itens, workflow, cotações e arquivos."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT r.id_sharepoint, r.unidade, r.comprador, r.data_solicitacao, r.status,
               r.justificativa, r.fornecedor, r.valor_fechado, r.observacoes, r.setor,
               (SELECT cr.comprador FROM Compradores_Responsabilidade cr
                WHERE cr.unidade = r.unidade AND cr.ativo = 1
                ORDER BY cr.prioridade ASC LIMIT 1) as comprador_responsavel
        FROM Requisicoes r WHERE r.id_sharepoint=?
    """, (id_requisicao,))
    ped = cursor.fetchone()
    if not ped:
        conn.close(); raise HTTPException(404, "Requisição não encontrada")
    cursor.execute("SELECT descricao, quantidade, segmento_historico FROM Itens_Requisicao WHERE id_requisicao=?", (id_requisicao,))
    itens = [{"descricao":r[0],"quantidade":r[1],"segmento":r[2]} for r in cursor.fetchall()]
    cursor.execute("""
        SELECT l.cnpj_fornecedor, COALESCE(f.razao_social, l.cnpj_fornecedor) as nome,
               l.preco_unitario, l.prazo_entrega_dias, l.pagamento, l.data_resposta,
               l.selecionado, l.observacoes, l.frete_incluso, l.imposto_incluso,
               l.arquivo_nome, l.arquivo_path, l.validade_dias
        FROM Lances_Fornecedor l
        LEFT JOIN Fornecedores f
          ON REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','')
           = REPLACE(REPLACE(REPLACE(f.cnpj,'.',''),'/',''),'-','')
        WHERE l.id_requisicao=? ORDER BY l.selecionado DESC, l.preco_unitario ASC
    """, (id_requisicao,))
    _seen_cnpj = set()
    cotacoes = []
    for r in cursor.fetchall():
        _cnpj_key = r[0].replace('.','').replace('/','').replace('-','').strip() if r[0] else r[0]
        if _cnpj_key in _seen_cnpj:
            continue
        _seen_cnpj.add(_cnpj_key)
        cotacoes.append({"cnpj":r[0],"nome":r[1],"preco_unitario":r[2],"prazo":r[3],
                 "pagamento":r[4],"data":r[5],"selecionado":bool(r[6]),
                 "observacoes":r[7],"frete_incluso":bool(r[8]),"imposto_incluso":bool(r[9]),
                 "arquivo_nome":r[10],"arquivo_path":r[11],"validade_dias":r[12]})
    # Busca preços por item para cada cotação
    for c in cotacoes:
        cnpj_norm = c["cnpj"].replace('.','').replace('/','').replace('-','')
        cursor.execute("""
            SELECT lfi.descricao, lfi.quantidade, lfi.preco_unitario
            FROM Lances_Fornecedor_Itens lfi
            JOIN Lances_Fornecedor lf ON lf.id = lfi.id_lance
            WHERE lf.id_requisicao = ?
              AND REPLACE(REPLACE(REPLACE(lf.cnpj_fornecedor,'.',''),'/',''),'-','') = ?
            ORDER BY lfi.id
        """, (id_requisicao, cnpj_norm))
        c["itens_precos"] = [
            {"descricao": r[0], "quantidade": r[1], "preco_unitario": r[2]}
            for r in cursor.fetchall()
        ]
    cursor.execute("""
        SELECT id, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_em, enviado_por
        FROM Arquivos_Requisicao WHERE id_requisicao=? ORDER BY enviado_em
    """, (id_requisicao,))
    arquivos = [{"id":r[0],"origem":r[1],"nome_arquivo":r[2],"caminho":r[3],
                 "tamanho_kb":r[4],"tipo_mime":r[5],"enviado_em":r[6],"enviado_por":r[7]}
                for r in cursor.fetchall()]
    conn.close()
    total_itens = sum(i["quantidade"] for i in itens)
    melhor_preco = min((c["preco_unitario"] for c in cotacoes), default=None)
    return {
        "id": ped[0], "unidade": ped[1], "solicitante": ped[2],
        "data": ped[3], "status": ped[4], "justificativa": ped[5],
        "fornecedor": ped[6], "valor_fechado": ped[7],
        "observacoes": ped[8], "setor": ped[9],
        "comprador_responsavel": ped[10],
        # manter 'comprador' como alias para retrocompatibilidade
        "comprador": ped[2],
        "itens": itens, "cotacoes": cotacoes, "arquivos": arquivos,
        "total_itens": total_itens, "total_cotacoes": len(cotacoes),
        "melhor_preco": melhor_preco
    }

@app.get("/api/cotacao/comparativo/{id_requisicao}")
def ver_comparativo(id_requisicao: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    # JOIN normalizado — ignora pontuação no CNPJ para evitar mismatch de formato
    cursor.execute('''
        SELECT COALESCE(f.razao_social, l.cnpj_fornecedor) as fornecedor,
               l.preco_unitario, l.prazo_entrega_dias, l.data_resposta,
               l.cnpj_fornecedor,
               l.pagamento, l.validade_dias, l.observacoes,
               l.frete_incluso, l.imposto_incluso, l.selecionado,
               f.email, f.telefone
        FROM Lances_Fornecedor l
        LEFT JOIN Fornecedores f
          ON REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','')
           = REPLACE(REPLACE(REPLACE(f.cnpj,'.',''),'/',''),'-','')
        WHERE l.id_requisicao = ?
        ORDER BY l.selecionado DESC, l.preco_unitario ASC
    ''', (id_requisicao,))
    _seen_cnpj_cmp = set()
    lances = []
    for r in cursor.fetchall():
        _cnpj_key = r[4].replace('.','').replace('/','').replace('-','').strip() if r[4] else r[4]
        if _cnpj_key in _seen_cnpj_cmp:
            continue
        _seen_cnpj_cmp.add(_cnpj_key)
        lances.append({
            "fornecedor": r[0], "preco": r[1], "prazo": r[2], "data": r[3],
            "cnpj": r[4], "pagamento": r[5] or "30 DDL", "validade_dias": r[6] or 15,
            "observacoes": r[7], "frete_incluso": r[8], "imposto_incluso": r[9],
            "selecionado": bool(r[10]), "email": r[11], "telefone": r[12]
        })
    # Busca itens por lance
    for l in lances:
        cnpj_norm = l["cnpj"].replace('.','').replace('/','').replace('-','')
        cursor.execute("""
            SELECT lfi.descricao, lfi.quantidade, lfi.preco_unitario
            FROM Lances_Fornecedor_Itens lfi
            JOIN Lances_Fornecedor lf ON lf.id = lfi.id_lance
            WHERE lf.id_requisicao = ?
              AND REPLACE(REPLACE(REPLACE(lf.cnpj_fornecedor,'.',''),'/',''),'-','') = ?
            ORDER BY lfi.id
        """, (id_requisicao, cnpj_norm))
        l["itens_precos"] = [
            {"descricao": r[0], "quantidade": r[1], "preco_unitario": r[2]}
            for r in cursor.fetchall()
        ]
    conn.close(); return lances

@app.post("/api/cotacao/disparar-email")
def disparar_email(lance: LanceFornecedor):
    # Registra o convite mesmo sem email real — gera o lance com preco=0
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id FROM Lances_Fornecedor WHERE id_requisicao=? AND cnpj_fornecedor=?",
                   (lance.id_requisicao, lance.cnpj_fornecedor))
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO Lances_Fornecedor (id_requisicao, cnpj_fornecedor, preco_unitario, prazo_entrega_dias, data_resposta)
            VALUES (?,?,0,0,?)
        """, (lance.id_requisicao, lance.cnpj_fornecedor, datetime.now().strftime("%d/%m/%Y %H:%M")))
    # Avança o status para 'Em Cotação' quando o primeiro fornecedor é convidado
    cursor.execute("""
        UPDATE Requisicoes SET status = 'Em Cotação'
        WHERE id_sharepoint = ? AND status = 'Aguardando Cotação'
    """, (lance.id_requisicao,))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/sourcing/selecionar/{id_requisicao}")
def selecionar_fornecedor(id_requisicao: int, body: SelecionarFornecedor):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    # Normaliza o CNPJ recebido (remove pontuação)
    def _norm(cnpj): return cnpj.replace('.','').replace('/','').replace('-','').strip() if cnpj else ''
    cnpj_norm = _norm(body.cnpj_fornecedor)

    # Desmarca todos os lances desta requisição
    cursor.execute("UPDATE Lances_Fornecedor SET selecionado=0 WHERE id_requisicao=?", (id_requisicao,))

    # Marca o selecionado usando CNPJ normalizado dos dois lados
    cursor.execute("""
        UPDATE Lances_Fornecedor SET selecionado=1
        WHERE id_requisicao=?
          AND REPLACE(REPLACE(REPLACE(cnpj_fornecedor,'.',''),'/',''),'-','') = ?
    """, (id_requisicao, cnpj_norm))

    # Busca nome e valor do lance selecionado (JOIN normalizado)
    cursor.execute("""
        SELECT COALESCE(f.razao_social, l.cnpj_fornecedor), l.preco_unitario, l.cnpj_fornecedor
        FROM Lances_Fornecedor l
        LEFT JOIN Fornecedores f
          ON REPLACE(REPLACE(REPLACE(f.cnpj,'.',''),'/',''),'-','')
           = REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','')
        WHERE l.id_requisicao=?
          AND REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','') = ?
    """, (id_requisicao, cnpj_norm))
    row = cursor.fetchone()

    # Atualiza a requisição — garante que status muda mesmo sem match em Fornecedores
    nome_forn   = (row[0] if row else None) or body.cnpj_fornecedor
    preco_forn  = row[1] if row else None
    cursor.execute("""
        UPDATE Requisicoes
        SET fornecedor=?, valor_fechado=?, status='Aguardando Conciliação'
        WHERE id_sharepoint=?
    """, (nome_forn, preco_forn, id_requisicao))

    conn.commit(); conn.close()
    return {"status": "ok", "fornecedor": nome_forn}

@app.get("/api/sourcing/requisicao/{id_requisicao}")
def detalhes_requisicao_sourcing(id_requisicao: int):
    """Retorna todos os detalhes de uma requisição para o portal do fornecedor."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id_sharepoint, unidade, comprador, data_solicitacao, status, justificativa
        FROM Requisicoes WHERE id_sharepoint=?
    """, (id_requisicao,))
    ped = cursor.fetchone()
    if not ped:
        conn.close(); raise HTTPException(404, "Requisição não encontrada")
    cursor.execute("""
        SELECT descricao, quantidade, segmento_historico
        FROM Itens_Requisicao WHERE id_requisicao=?
    """, (id_requisicao,))
    itens = [{"descricao": r[0], "quantidade": r[1], "segmento": r[2]} for r in cursor.fetchall()]
    conn.close()
    return {
        "id": ped[0], "unidade": ped[1], "comprador": ped[2],
        "data": ped[3], "status": ped[4], "justificativa": ped[5],
        "itens": itens
    }

# ==========================================
# 5. MÓDULO PO
# ==========================================
@app.get("/api/po/dados/{id_pedido}")
def obter_dados_po(id_pedido: int, cnpj: str = ""):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT unidade, comprador, data_solicitacao, justificativa FROM Requisicoes WHERE id_sharepoint = ?", (id_pedido,))
    ped = cursor.fetchone()
    if not ped:
        conn.close(); raise HTTPException(404, "Requisição não encontrada")
    forn_nome = cnpj
    if cnpj:
        cursor.execute("SELECT razao_social FROM Fornecedores WHERE cnpj = ?", (cnpj,))
        frow = cursor.fetchone()
        if frow: forn_nome = frow[0]
    lance_preco = None
    if cnpj:
        cursor.execute("SELECT preco_unitario FROM Lances_Fornecedor WHERE id_requisicao = ? AND cnpj_fornecedor = ? AND preco_unitario > 0", (id_pedido, cnpj))
        lrow = cursor.fetchone()
        if lrow: lance_preco = lrow[0]
    cursor.execute("SELECT descricao, quantidade, segmento_historico FROM Itens_Requisicao WHERE id_requisicao = ?", (id_pedido,))
    itens = [{"descricao": r[0], "qtd": r[1], "segmento": r[2]} for r in cursor.fetchall()]
    conn.close()
    return {
        "pedido": {"unidade": ped[0], "comprador": ped[1], "data": ped[2], "justificativa": ped[3]},
        "fornecedor": {"nome": forn_nome, "cnpj": cnpj},
        "lance": {"preco": lance_preco},
        "itens": itens
    }

# ==========================================
# 6. MÓDULO BUDGETING
# ==========================================
@app.get("/api/orcamento/{unidade}")
def obter_orcamento(unidade: str, ano: int = 0):
    try:
        conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
        if ano:
            cursor.execute("SELECT orcamento_anual, ano FROM Orcamentos WHERE unidade = ? AND ano = ?", (unidade, ano))
        else:
            cursor.execute("SELECT orcamento_anual, ano FROM Orcamentos WHERE unidade = ? ORDER BY ano DESC LIMIT 1", (unidade,))
        res = cursor.fetchone()
        if not res:
            conn.close(); return {"erro": "Sem orçamento"}
        total = res[0] or 0; ano_ref = res[1]
        # Realizado real: soma valor_fechado das requisições da unidade no ano
        cursor.execute("""
            SELECT COALESCE(SUM(valor_fechado), 0)
            FROM Requisicoes
            WHERE unidade = ?
              AND valor_fechado IS NOT NULL
              AND valor_fechado > 0
              AND SUBSTR(data_solicitacao, 7, 4) = ?
        """, (unidade, str(ano_ref)))
        consumido = cursor.fetchone()[0] or 0
        conn.close()
        saldo = total - consumido
        percentual = (consumido / total) * 100 if total > 0 else 0
        cor_status = "#01E18E"
        if percentual > 75: cor_status = "#422c76"
        if percentual > 90: cor_status = "#ff2f69"
        return {"unidade": unidade, "ano": ano_ref, "total": total, "consumido": consumido,
                "saldo": saldo, "percentual": round(percentual, 1), "cor_status": cor_status}
    except Exception as e: return {"erro": str(e)}

@app.get("/api/orcamentos")
def listar_orcamentos():
    """Retorna todos os orçamentos com realizado calculado diretamente de Requisicoes."""
    try:
        conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
        # Descobre se a coluna ano existe
        cursor.execute("PRAGMA table_info(Orcamentos)")
        cols = {row[1] for row in cursor.fetchall()}
        has_ano = 'ano' in cols
        if has_ano:
            cursor.execute("SELECT unidade, ano, orcamento_anual FROM Orcamentos ORDER BY ano DESC, unidade")
        else:
            cursor.execute("SELECT unidade, 2026, orcamento_anual FROM Orcamentos ORDER BY unidade")
        rows = cursor.fetchall()
        # Unidades disponíveis para o select
        cursor.execute("SELECT DISTINCT unidade FROM Requisicoes WHERE unidade IS NOT NULL AND unidade != '' ORDER BY unidade")
        unidades = [r[0] for r in cursor.fetchall()]
        conn.close()
        orcamentos = []
        for r in rows:
            unidade = r[0]; ano = r[1]; total = r[2] or 0
            # Calcula realizado real: soma valor_fechado das requisições da unidade no ano
            conn2 = sqlite3.connect(DB_PATH); c2 = conn2.cursor()
            c2.execute("""
                SELECT COALESCE(SUM(valor_fechado), 0)
                FROM Requisicoes
                WHERE unidade = ?
                  AND valor_fechado IS NOT NULL
                  AND valor_fechado > 0
                  AND SUBSTR(data_solicitacao, 7, 4) = ?
            """, (unidade, str(ano)))
            consumido = c2.fetchone()[0] or 0
            conn2.close()
            saldo = total - consumido
            pct = round((consumido / total) * 100, 1) if total > 0 else 0
            orcamentos.append({
                "unidade": unidade, "ano": ano,
                "orcamento_anual": total, "consumido": consumido,
                "saldo": saldo, "percentual": pct
            })
        return {"orcamentos": orcamentos, "unidades": unidades}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/orcamentos/salvar")
def salvar_orcamento(orc: OrcamentoIn):
    """UPSERT de orçamento por unidade+ano — compatível com tabelas sem coluna id."""
    try:
        conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
        # Garante coluna ano (tabelas legadas podem não ter)
        try:
            cursor.execute("ALTER TABLE Orcamentos ADD COLUMN ano INTEGER DEFAULT 2026")
            conn.commit()
        except Exception:
            pass
        # Verifica existência usando COUNT(*) — não depende de coluna id
        cursor.execute(
            "SELECT COUNT(*) FROM Orcamentos WHERE unidade = ? AND ano = ?",
            (orc.unidade, orc.ano)
        )
        exists = cursor.fetchone()[0] > 0
        if exists:
            cursor.execute(
                "UPDATE Orcamentos SET orcamento_anual = ? WHERE unidade = ? AND ano = ?",
                (orc.orcamento_anual, orc.unidade, orc.ano)
            )
        else:
            # INSERT sem coluna consumido — realizado vem de Requisicoes, não é armazenado aqui
            try:
                cursor.execute(
                    "INSERT INTO Orcamentos (unidade, ano, orcamento_anual) VALUES (?, ?, ?)",
                    (orc.unidade, orc.ano, orc.orcamento_anual)
                )
            except Exception:
                # Fallback para tabelas sem coluna ano
                cursor.execute(
                    "INSERT INTO Orcamentos (unidade, orcamento_anual) VALUES (?, ?)",
                    (orc.unidade, orc.orcamento_anual)
                )
        conn.commit(); conn.close()
        return {"ok": True, "unidade": orc.unidade, "ano": orc.ano}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/orcamentos/{unidade}/{ano}")
def deletar_orcamento(unidade: str, ano: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Orcamentos WHERE unidade = ? AND ano = ?", (unidade, ano))
    except Exception:
        # Fallback: tabela sem coluna ano — exclui só por unidade
        cursor.execute("DELETE FROM Orcamentos WHERE unidade = ?", (unidade,))
    conn.commit(); conn.close()
    return {"ok": True}

# ==========================================
# 7. MÓDULO REQUISIÇÕES — LISTAGEM & DRILL-DOWN
# ==========================================

@app.get("/api/requisicoes/filtros")
def opcoes_filtro_requisicoes():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT status FROM Requisicoes WHERE status IS NOT NULL ORDER BY status")
    statuses = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT unidade FROM Requisicoes WHERE unidade IS NOT NULL ORDER BY unidade")
    unidades = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT comprador FROM Requisicoes WHERE comprador IS NOT NULL ORDER BY comprador")
    compradores = [r[0] for r in cursor.fetchall()]
    conn.close()
    return {"statuses": statuses, "unidades": unidades, "compradores": compradores}

@app.get("/api/requisicoes/por-unidade")
def requisicoes_por_unidade():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT
            unidade,
            COUNT(id_sharepoint) as total,
            SUM(CASE WHEN valor_fechado IS NOT NULL THEN valor_fechado ELSE 0 END) as total_valor,
            SUM(CASE WHEN status LIKE '%Concluí%' THEN 1 ELSE 0 END) as concluidos,
            SUM(CASE WHEN status NOT LIKE '%Concluí%' AND status != 'Reprovado' THEN 1 ELSE 0 END) as em_andamento,
            SUM(CASE WHEN status = 'Reprovado' THEN 1 ELSE 0 END) as reprovados
        FROM Requisicoes
        WHERE unidade IS NOT NULL
        GROUP BY unidade
        ORDER BY total DESC
    """)
    unidades = []
    for row in cursor.fetchall():
        u = {
            "unidade": row[0], "total": row[1], "total_valor": row[2] or 0,
            "concluidos": row[3], "em_andamento": row[4], "reprovados": row[5]
        }
        cursor.execute("""
            SELECT id_sharepoint, comprador, data_solicitacao, status, valor_fechado
            FROM Requisicoes WHERE unidade = ?
            ORDER BY id_sharepoint DESC LIMIT 5
        """, (row[0],))
        u["recentes"] = [
            {"id": r[0], "comprador": r[1], "data": r[2], "status": r[3], "valor": r[4]}
            for r in cursor.fetchall()
        ]
        unidades.append(u)
    conn.close()
    return unidades

@app.get("/api/home/atividade-recente")
def atividade_recente():
    """Retorna as últimas 30 movimentações reais do sistema para o feed da Home."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    eventos = []

    # Últimas requisições abertas
    cursor.execute("""
        SELECT id_sharepoint, comprador, data_solicitacao, status, unidade
        FROM Requisicoes
        WHERE data_solicitacao IS NOT NULL
        ORDER BY id_sharepoint DESC LIMIT 30
    """)
    for r in cursor.fetchall():
        id_r, comp, data, status, unid = r
        comp_limpo = comp.split('|')[0].strip() if comp else 'Usuário'
        s = (status or '').lower()
        if 'concluí' in s or 'concluido' in s:
            cor = '#01E18E'; acao = f'Requisição #{id_r} concluída'
        elif 'reprovad' in s:
            cor = '#ff2f69'; acao = f'Requisição #{id_r} reprovada'
        elif 'aprovado' in s and 'aguardando' not in s:
            cor = '#3B82F6'; acao = f'Requisição #{id_r} aprovada'
        elif 'aguardando aprovação' in s:
            cor = '#F59E0B'; acao = f'Requisição #{id_r} aberta por {comp_limpo}'
        elif 'aguardando cotação' in s or 'conciliação' in s:
            cor = '#8B5CF6'; acao = f'Requisição #{id_r} avançou para {status}'
        else:
            cor = '#64748B'; acao = f'Requisição #{id_r} — {status}'
        eventos.append({
            'cor': cor, 'texto': acao,
            'usuario': comp_limpo, 'unidade': unid or '',
            'data': data or '', 'tipo': 'requisicao', 'id': id_r
        })

    # Lances de fornecedores (cotações recebidas)
    cursor.execute("""
        SELECT l.id_requisicao, COALESCE(f.razao_social, l.cnpj_fornecedor), l.data_resposta, l.preco_unitario, l.selecionado
        FROM Lances_Fornecedor l
        LEFT JOIN Fornecedores f ON f.cnpj = l.cnpj_fornecedor
        WHERE l.data_resposta IS NOT NULL AND l.preco_unitario > 0
        ORDER BY l.id DESC LIMIT 15
    """)
    for r in cursor.fetchall():
        id_r, forn, data, preco, selecionado = r
        if selecionado:
            cor = '#01E18E'
            texto = f'Fornecedor {forn} selecionado para Req #{id_r}'
        else:
            cor = '#8B5CF6'
            texto = f'Cotação recebida de {forn} para Req #{id_r}'
        eventos.append({
            'cor': cor, 'texto': texto,
            'usuario': forn, 'unidade': '',
            'data': data or '', 'tipo': 'cotacao', 'id': id_r
        })

    # Uploads de NF
    cursor.execute("""
        SELECT id_pedido, numero_nf, enviado_em, tipo
        FROM NF_Uploads
        ORDER BY id DESC LIMIT 10
    """)
    for r in cursor.fetchall():
        id_r, nf, data, tipo = r
        eventos.append({
            'cor': '#0EA5E9',
            'texto': f'NF {nf or "s/n"} ({tipo}) anexada para PO #{id_r}',
            'usuario': '', 'unidade': '',
            'data': data or '', 'tipo': 'nf', 'id': id_r
        })

    # Ordena todos por ID desc (proxy de data) e retorna os 25 mais recentes
    def sort_key(e):
        return (e.get('id') or 0, e.get('data') or '')
    eventos.sort(key=sort_key, reverse=True)
    conn.close()
    return eventos[:25]

@app.get("/api/requisicoes")
def listar_requisicoes(
    page: int = 1, per_page: int = 20,
    status: str = "", unidade: str = "",
    comprador: str = "", busca: str = "",
    id_req: int = 0,
    sort_by: str = "id", sort_order: str = "desc"
):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    where, params = [], []

    if status == 'abertos':
        where.append("r.status NOT LIKE '%Concluí%' AND r.status != 'Reprovado'")
    elif status:
        where.append("r.status = ?"); params.append(status)

    if unidade:
        where.append("r.unidade = ?"); params.append(unidade)
    if comprador:
        where.append("r.comprador = ?"); params.append(comprador)
    if id_req:
        where.append("r.id_sharepoint = ?"); params.append(id_req)
    elif busca:
        where.append("(CAST(r.id_sharepoint AS TEXT) LIKE ? OR LOWER(r.comprador) LIKE ? OR LOWER(r.fornecedor) LIKE ?)")
        b = f"%{busca.lower()}%"; params.extend([b, b, b])

    wc = ("WHERE " + " AND ".join(where)) if where else ""

    # Safe sort mapping (whitelist to prevent SQL injection)
    _sort_map = {
        "id":        "r.id_sharepoint",
        "valor":     "COALESCE(r.valor_fechado, -1)",
        "comprador": "LOWER(r.comprador)",
        "unidade":   "LOWER(r.unidade)",
        "status":    "LOWER(r.status)",
    }
    order_col = _sort_map.get(sort_by, "r.id_sharepoint")
    order_dir = "DESC" if sort_order.lower() == "desc" else "ASC"
    order_clause = f"ORDER BY {order_col} {order_dir}"

    cursor.execute(f"SELECT COUNT(DISTINCT r.id_sharepoint) FROM Requisicoes r {wc}", params)
    total = cursor.fetchone()[0]

    offset = (page - 1) * per_page
    cursor.execute(f"""
        SELECT
            r.id_sharepoint, r.unidade, r.data_solicitacao, r.comprador,
            r.status, r.valor_fechado, r.fornecedor,
            COUNT(i.rowid) as itens_count,
            GROUP_CONCAT(i.descricao, ' • ') as itens_preview,
            r.setor,
            (SELECT cr.comprador FROM Compradores_Responsabilidade cr
             WHERE cr.unidade = r.unidade AND cr.ativo = 1
             ORDER BY cr.prioridade ASC LIMIT 1) as comprador_responsavel
        FROM Requisicoes r
        LEFT JOIN Itens_Requisicao i ON i.id_requisicao = r.id_sharepoint
        {wc}
        GROUP BY r.id_sharepoint
        {order_clause}
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    cols = ['id','unidade','data','solicitante','status','valor','fornecedor','itens_count','itens_preview','setor','comprador_responsavel']
    items = [dict(zip(cols, row)) for row in cursor.fetchall()]
    conn.close()
    return {
        "total": total, "page": page, "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "items": items
    }

@app.patch("/api/requisicoes/{id_pedido}")
def atualizar_requisicao(id_pedido: int, dados: AtualizacaoRequisicao):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    fields, params = [], []
    if dados.status       is not None: fields.append("status = ?");        params.append(dados.status)
    if dados.fornecedor   is not None: fields.append("fornecedor = ?");     params.append(dados.fornecedor)
    if dados.valor_fechado is not None: fields.append("valor_fechado = ?"); params.append(dados.valor_fechado)
    if dados.observacoes  is not None: fields.append("observacoes = ?");    params.append(dados.observacoes)
    if not fields:
        conn.close(); return {"status": "noop"}
    params.append(id_pedido)
    cursor.execute(f"UPDATE Requisicoes SET {', '.join(fields)} WHERE id_sharepoint = ?", params)
    conn.commit(); conn.close()
    return {"status": "ok"}

@app.delete("/api/requisicoes/{id_pedido}")
def deletar_requisicao(id_pedido: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("DELETE FROM Itens_Requisicao WHERE id_requisicao = ?", (id_pedido,))
    cursor.execute("DELETE FROM Lances_Fornecedor WHERE id_requisicao = ?", (id_pedido,))
    cursor.execute("DELETE FROM Requisicoes WHERE id_sharepoint = ?", (id_pedido,))
    conn.commit(); conn.close()
    return {"status": "ok"}

@app.get("/api/requisicoes/{id_pedido}")
def obter_requisicao(id_pedido: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id_sharepoint, unidade, data_solicitacao, comprador, status, valor_fechado, fornecedor, observacoes, justificativa FROM Requisicoes WHERE id_sharepoint = ?", (id_pedido,))
    row = cursor.fetchone()
    if not row: conn.close(); raise HTTPException(status_code=404, detail="Não encontrado")
    cursor.execute("SELECT descricao, quantidade, segmento_historico FROM Itens_Requisicao WHERE id_requisicao = ?", (id_pedido,))
    itens = [{"descricao": i[0], "quantidade": i[1], "segmento": i[2]} for i in cursor.fetchall()]
    conn.close()
    return {
        "id": row[0], "unidade": row[1], "data": row[2], "comprador": row[3],
        "status": row[4], "valor_fechado": row[5], "fornecedor": row[6],
        "observacoes": row[7], "justificativa": row[8], "itens": itens
    }

# ==========================================
# 8. MÓDULO 3-WAY MATCHING
# ==========================================
@app.get("/api/recebimento/pendentes")
def buscar_po_para_receber():
    # Busca apenas requisições com fornecedor SELECIONADO (selecionado=1)
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute('''
        SELECT r.id_sharepoint,
               MAX(COALESCE(f.razao_social, l.cnpj_fornecedor)) as fornecedor,
               r.status
        FROM Requisicoes r
        JOIN Lances_Fornecedor l ON r.id_sharepoint = l.id_requisicao AND l.selecionado = 1
        LEFT JOIN Fornecedores f
          ON REPLACE(REPLACE(REPLACE(l.cnpj_fornecedor,'.',''),'/',''),'-','')
           = REPLACE(REPLACE(REPLACE(f.cnpj,'.',''),'/',''),'-','')
        WHERE r.status = 'Aguardando Conciliação'
        GROUP BY r.id_sharepoint
        ORDER BY r.id_sharepoint DESC
        LIMIT 50
    ''')
    pos = [{"id_pedido": r[0], "fornecedor": r[1]} for r in cursor.fetchall()]
    conn.close(); return pos

@app.get("/api/recebimento/dados-po/{id_pedido}")
def dados_po_para_conciliacao(id_pedido: int):
    """Retorna os valores esperados da PO para pré-preencher a tela de conciliação."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT SUM(quantidade) FROM Itens_Requisicao WHERE id_requisicao = ?", (id_pedido,))
    qtd_po = cursor.fetchone()[0] or 0
    cursor.execute("SELECT preco_unitario FROM Lances_Fornecedor WHERE id_requisicao = ? AND selecionado = 1", (id_pedido,))
    row = cursor.fetchone()
    preco_unit_po = row[0] if row else 0
    valor_total_po = round(preco_unit_po, 2)  # preco_unitario já é o total
    conn.close()
    return {"qtd_esperada": qtd_po, "valor_esperado": valor_total_po, "preco_unitario": preco_unit_po}

@app.post("/api/recebimento/match/{id_pedido}")
def realizar_3way_match(id_pedido: int, dados_nf: DadosRecebimento):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    # 1. Pega os dados originais da PO
    cursor.execute("SELECT SUM(quantidade) FROM Itens_Requisicao WHERE id_requisicao = ?", (id_pedido,))
    qtd_po = cursor.fetchone()[0] or 0

    # preco_unitario agora guarda o valor TOTAL da proposta do fornecedor selecionado
    cursor.execute("SELECT preco_unitario FROM Lances_Fornecedor WHERE id_requisicao = ? AND selecionado = 1", (id_pedido,))
    row = cursor.fetchone()
    valor_total_po = round(row[0], 2) if row else 0

    # 2. Faz a Conciliação com tolerância de R$ 0,01 e 0,001 unidades
    divergencias = []

    qtd_recebida = round(dados_nf.qtd_recebida, 3)
    qtd_esperada = round(float(qtd_po), 3)
    if abs(qtd_recebida - qtd_esperada) > 0.001:
        divergencias.append(
            f"Quantidade pedida ({int(qtd_esperada) if qtd_esperada == int(qtd_esperada) else qtd_esperada}) "
            f"difere da quantidade recebida ({int(qtd_recebida) if qtd_recebida == int(qtd_recebida) else qtd_recebida})"
        )

    valor_nf = round(dados_nf.valor_nf, 2)
    # Só compara valor se há um preço definido na PO
    if valor_total_po > 0 and abs(valor_nf - valor_total_po) > 0.01:
        divergencias.append(
            f"Valor da PO (R$ {valor_total_po:,.2f}) difere do Valor da NF (R$ {valor_nf:,.2f})"
        )

    if divergencias:
        conn.close()
        return {
            "status": "BLOQUEADO",
            "cor": "#ff2f69",
            "mensagem": "Divergência encontrada na conciliação. Pagamento bloqueado.",
            "detalhes": divergencias,
            "qtd_esperada": qtd_po,
            "valor_esperado": valor_total_po
        }

    # 3. Aprovado — atualiza status e registra valor fechado
    cursor.execute("""
        UPDATE Requisicoes
        SET status = 'Concluído', valor_fechado = ?
        WHERE id_sharepoint = ?
    """, (valor_nf, id_pedido))
    conn.commit()
    conn.close()
    return {
        "status": "APROVADO",
        "cor": "#01E18E",
        "mensagem": "Conciliação aprovada! NF liberada para pagamento.",
        "detalhes": [
            f"Quantidade recebida: {int(dados_nf.qtd_recebida) if dados_nf.qtd_recebida == int(dados_nf.qtd_recebida) else dados_nf.qtd_recebida} unidades",
            f"Valor NF: R$ {valor_nf:,.2f} — dentro da tolerância da PO.",
            "Requisição finalizada e movida para Concluído."
        ],
        "qtd_esperada": qtd_po,
        "valor_esperado": valor_total_po
    }

# ==========================================
# 9. MÓDULO CONFIGURAÇÕES
# ==========================================

# ── Opções para dropdowns ──────────────────
def _limpar_nome(nome: str) -> str:
    """Remove sufixos como ' | Vendemmia', ' | VCI', etc."""
    if not nome: return nome
    return nome.split('|')[0].strip()

def _nome_para_email(nome: str) -> str:
    """Gera e-mail placeholder a partir do nome: 'Ana Paula Alves' → 'ana.paula.alves@vendemmia.com.br'"""
    import unicodedata, re
    s = unicodedata.normalize('NFD', nome.lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')  # remove acentos
    s = re.sub(r'[^a-z0-9\s]', '', s).strip()
    return '.'.join(s.split()) + '@vendemmia.com.br'

@app.post("/api/usuarios/importar-historico")
def importar_usuarios_historico():
    """Lê o CSV de histórico e importa usuários únicos com seus gestores para a tabela Usuarios."""
    import csv as csv_module
    csv_path = os.path.join(BASE_DIR, '..', 'data', 'Requisição de Compras.csv')
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="Arquivo CSV não encontrado")

    # Coleta pares (nome, unidade, gestor)
    pessoas: dict[str, dict] = {}  # chave = nome limpo

    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            unidade    = str(row.get('Unidade', '') or '').strip()
            criado_por = _limpar_nome(str(row.get('Criado por', '') or '').strip())
            comprador  = _limpar_nome(str(row.get('Comprador', '') or '').strip())
            gestor_raw = str(row.get('Gestor Imediato', '') or '').strip()
            gestor     = _limpar_nome(gestor_raw)

            # Usa 'Criado por' como nome principal; fallback para 'Comprador'
            nome = criado_por or comprador
            if not nome or nome.upper() in ('NAN', ''):
                continue

            if nome not in pessoas:
                pessoas[nome] = {'nome': nome, 'unidade': unidade, 'gestor_nome': gestor or None}
            else:
                # Complementa com unidade/gestor se ainda não tiver
                if not pessoas[nome]['unidade'] and unidade:
                    pessoas[nome]['unidade'] = unidade
                if not pessoas[nome]['gestor_nome'] and gestor:
                    pessoas[nome]['gestor_nome'] = gestor

            # Garante que o gestor também entre como pessoa (sem gestor dele)
            if gestor and gestor not in pessoas:
                pessoas[gestor] = {'nome': gestor, 'unidade': unidade, 'gestor_nome': None}

    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT nome FROM Usuarios")
    ja_cadastrados = {r[0] for r in cursor.fetchall()}

    inseridos = []
    ignorados = []
    for nome, dados in sorted(pessoas.items()):
        if nome in ja_cadastrados:
            ignorados.append(nome)
            continue
        email = _nome_para_email(nome)
        # Evita email duplicado
        cursor.execute("SELECT id FROM Usuarios WHERE email = ?", (email,))
        if cursor.fetchone():
            email = email.replace('@', f'.{len(inseridos)+1}@')
        cursor.execute("""
            INSERT INTO Usuarios (nome, email, unidade, gestor_nome, ativo)
            VALUES (?, ?, ?, ?, 1)
        """, (nome, email, dados.get('unidade') or None, dados.get('gestor_nome') or None))
        inseridos.append(nome)

    conn.commit(); conn.close()
    return {
        "inseridos": len(inseridos),
        "ignorados": len(ignorados),
        "nomes_inseridos": inseridos
    }

@app.get("/api/configuracoes/opcoes")
def config_opcoes():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT unidade FROM Requisicoes WHERE unidade IS NOT NULL AND unidade != '' ORDER BY unidade")
    unidades = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT segmento_historico FROM Itens_Requisicao WHERE segmento_historico IS NOT NULL AND segmento_historico != '' ORDER BY segmento_historico")
    categorias = [r[0].upper() for r in cursor.fetchall()]

    # Nomes de usuários cadastrados + compradores existentes no banco (sem "| Vendemmia")
    cursor.execute("SELECT DISTINCT comprador FROM Requisicoes WHERE comprador IS NOT NULL AND comprador != '' ORDER BY comprador")
    nomes_req = [_limpar_nome(r[0]) for r in cursor.fetchall()]

    cursor.execute("SELECT id, nome FROM Usuarios WHERE ativo=1 ORDER BY nome")
    usuarios_cadastrados = [{"id": r[0], "nome": r[1]} for r in cursor.fetchall()]

    # Lista unificada para datalist (sem duplicatas, ordenada)
    nomes_usuarios_cadastrados = {u["nome"] for u in usuarios_cadastrados}
    gestores_sugestoes = sorted(set(nomes_req) | nomes_usuarios_cadastrados)

    conn.close()
    return {
        "unidades": unidades,
        "categorias": list(set(categorias)),
        "usuarios": usuarios_cadastrados,
        "gestores_sugestoes": gestores_sugestoes,
    }

# ── USUÁRIOS ───────────────────────────────
@app.get("/api/usuarios")
def listar_usuarios():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id, nome, email, unidade, cargo, gestor_nome, ativo, criado_em
        FROM Usuarios ORDER BY nome
    """)
    cols = ['id','nome','email','unidade','cargo','gestor_nome','ativo','criado_em']
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    conn.close(); return rows

@app.post("/api/usuarios", status_code=201)
def criar_usuario(u: UsuarioIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO Usuarios (nome, email, unidade, cargo, gestor_nome, ativo) VALUES (?,?,?,?,?,?)",
            (u.nome, u.email, u.unidade, u.cargo, u.gestor_nome, u.ativo)
        )
        conn.commit(); new_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        conn.close(); raise HTTPException(400, "E-mail já cadastrado")
    conn.close(); return {"id": new_id, "status": "criado"}

@app.patch("/api/usuarios/{uid}")
def atualizar_usuario(uid: int, u: UsuarioIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "UPDATE Usuarios SET nome=?, email=?, unidade=?, cargo=?, gestor_nome=?, ativo=? WHERE id=?",
        (u.nome, u.email, u.unidade, u.cargo, u.gestor_nome, u.ativo, uid)
    )
    if cursor.rowcount == 0:
        conn.close(); raise HTTPException(404, "Usuário não encontrado")
    conn.commit(); conn.close(); return {"status": "ok"}

@app.delete("/api/usuarios/{uid}")
def deletar_usuario(uid: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("UPDATE Usuarios SET gestor_id=NULL WHERE gestor_id=?", (uid,))
    cursor.execute("DELETE FROM Usuarios WHERE id=?", (uid,))
    conn.commit(); conn.close(); return {"status": "ok"}

@app.post("/api/usuarios/solicitar-acesso", status_code=201)
def solicitar_acesso(s: SolicitacaoAcessoIn):
    """Cria usuário inativo com flag de solicitação pendente (sem acesso ao sistema)."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO Usuarios (nome, email, gestor_nome, ativo, solicitacao_pendente) VALUES (?,?,?,0,1)",
            (s.nome, s.email, s.gestor_nome)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close(); raise HTTPException(400, "Este e-mail já está cadastrado ou possui uma solicitação em andamento.")
    conn.close(); return {"status": "aguardando_ativacao"}

@app.get("/api/usuarios/pendentes-acesso")
def listar_pendentes_acesso():
    """Lista usuários com solicitação de acesso pendente (aguardando ativação pelo time de Compras)."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id, nome, email, gestor_nome, criado_em
        FROM Usuarios WHERE solicitacao_pendente = 1 ORDER BY criado_em DESC
    """)
    cols = ['id','nome','email','gestor_nome','criado_em']
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    conn.close(); return rows

@app.patch("/api/usuarios/{uid}/ativar")
def ativar_usuario(uid: int):
    """Ativa um usuário e limpa a flag de solicitação pendente."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("UPDATE Usuarios SET ativo=1, solicitacao_pendente=0 WHERE id=?", (uid,))
    if cursor.rowcount == 0:
        conn.close(); raise HTTPException(404, "Usuário não encontrado")
    conn.commit(); conn.close(); return {"status": "ativado"}

@app.patch("/api/usuarios/{uid}/rejeitar")
def rejeitar_usuario(uid: int):
    """Remove uma solicitação de acesso rejeitada."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("DELETE FROM Usuarios WHERE id=? AND solicitacao_pendente=1", (uid,))
    if cursor.rowcount == 0:
        conn.close(); raise HTTPException(404, "Solicitação não encontrada")
    conn.commit(); conn.close(); return {"status": "rejeitado"}

@app.get("/api/usuarios/verificar")
def verificar_usuario(email: str = ""):
    """Verifica se um e-mail pertence a um usuário ativo no sistema."""
    if not email:
        raise HTTPException(400, "E-mail não informado")
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "SELECT id, nome, email, unidade, cargo, gestor_nome, ativo, solicitacao_pendente FROM Usuarios WHERE email=?",
        (email.lower().strip(),)
    )
    row = cursor.fetchone(); conn.close()
    if not row:
        raise HTTPException(404, "Usuário não encontrado")
    cols = ['id','nome','email','unidade','cargo','gestor_nome','ativo','solicitacao_pendente']
    return dict(zip(cols, row))

# ── COMPRADORES ────────────────────────────
@app.post("/api/compradores/importar-historico")
def importar_compradores_historico():
    """Lê o CSV de histórico e importa compradores únicos por unidade para Compradores_Responsabilidade."""
    import csv as csv_module
    from collections import Counter
    csv_path = os.path.join(BASE_DIR, '..', 'data', 'Requisição de Compras.csv')
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="Arquivo CSV não encontrado")

    # Agrupa: {(comprador, unidade) -> Counter de categorias}
    dados: dict[tuple, Counter] = {}

    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            comprador = _limpar_nome(str(row.get('Comprador', '') or '').strip())
            unidade   = str(row.get('Unidade', '') or '').strip()
            categoria = str(row.get('Tipo de despesa', '') or '').strip().upper()

            if not comprador or comprador.upper() in ('NAN', ''):
                continue

            chave = (comprador, unidade)
            if chave not in dados:
                dados[chave] = Counter()
            if categoria and categoria not in ('NAN', ''):
                dados[chave][categoria] += 1

    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    # Já cadastrados: chave (comprador, unidade)
    cursor.execute("SELECT comprador, COALESCE(unidade,'') FROM Compradores_Responsabilidade")
    ja_cadastrados = {(r[0], r[1]) for r in cursor.fetchall()}

    inseridos = 0
    ignorados = 0
    for (comprador, unidade), cat_counter in sorted(dados.items()):
        chave_norm = (comprador, unidade or '')
        if chave_norm in ja_cadastrados:
            ignorados += 1
            continue
        # Categoria mais frequente (ou None se variado demais)
        if cat_counter:
            top_cat, top_n = cat_counter.most_common(1)[0]
            total_cats = sum(cat_counter.values())
            # Se concentra >60% numa categoria, usa ela; senão, deixa em branco (atende tudo)
            categoria = top_cat if (top_n / total_cats) >= 0.60 else None
        else:
            categoria = None
        email = _nome_para_email(comprador)
        cursor.execute("SELECT id FROM Compradores_Responsabilidade WHERE email = ?", (email,))
        if cursor.fetchone():
            email = None
        cursor.execute("""
            INSERT INTO Compradores_Responsabilidade (comprador, email, unidade, categoria, prioridade, ativo)
            VALUES (?, ?, ?, ?, 1, 1)
        """, (comprador, email, unidade or None, categoria))
        inseridos += 1

    conn.commit(); conn.close()
    return {"inseridos": inseridos, "ignorados": ignorados}

@app.get("/api/compradores")
def listar_compradores():
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id,comprador,email,unidade,categoria,prioridade,ativo FROM Compradores_Responsabilidade ORDER BY comprador, unidade")
    cols = ['id','comprador','email','unidade','categoria','prioridade','ativo']
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    conn.close(); return rows

@app.post("/api/compradores", status_code=201)
def criar_comprador(c: CompradorIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO Compradores_Responsabilidade (comprador,email,unidade,categoria,prioridade,ativo) VALUES (?,?,?,?,?,?)",
        (c.comprador, c.email, c.unidade, c.categoria, c.prioridade, c.ativo)
    )
    conn.commit(); new_id = cursor.lastrowid
    conn.close(); return {"id": new_id, "status": "criado"}

@app.patch("/api/compradores/{cid}")
def atualizar_comprador(cid: int, c: CompradorIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "UPDATE Compradores_Responsabilidade SET comprador=?,email=?,unidade=?,categoria=?,prioridade=?,ativo=? WHERE id=?",
        (c.comprador, c.email, c.unidade, c.categoria, c.prioridade, c.ativo, cid)
    )
    if cursor.rowcount == 0:
        conn.close(); raise HTTPException(404, "Não encontrado")
    conn.commit(); conn.close(); return {"status": "ok"}

@app.delete("/api/compradores/{cid}")
def deletar_comprador(cid: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("DELETE FROM Compradores_Responsabilidade WHERE id=?", (cid,))
    conn.commit(); conn.close(); return {"status": "ok"}

# ==========================================
# 10b. MÓDULO CONTAS FIXAS
# ==========================================

def _cf_totais(cursor, id_conta: int, ano: int):
    """Retorna total pago no ano para uma conta fixa."""
    cursor.execute(
        "SELECT COALESCE(SUM(valor),0) FROM Lancamentos_CF WHERE id_conta=? AND ano=?",
        (id_conta, ano)
    )
    return cursor.fetchone()[0] or 0

@app.get("/api/contas-fixas")
def listar_contas_fixas():
    ano_ref = datetime.now().year
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id,nome,fornecedor,categoria,unidade,valor_anual,valor_mensal,data_inicio,data_fim,status,descricao,criado_em FROM Contas_Fixas ORDER BY nome")
    cols = ['id','nome','fornecedor','categoria','unidade','valor_anual','valor_mensal','data_inicio','data_fim','status','descricao','criado_em']
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    for r in rows:
        r['pago_ano'] = _cf_totais(cursor, r['id'], ano_ref)
        r['saldo_ano'] = (r['valor_anual'] or 0) - r['pago_ano']
        r['pct_ano']   = round((r['pago_ano'] / r['valor_anual']) * 100, 1) if r['valor_anual'] else 0
    conn.close(); return rows

@app.post("/api/contas-fixas", status_code=201)
def criar_conta_fixa(c: ContaFixaIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Contas_Fixas (nome,fornecedor,categoria,unidade,valor_anual,valor_mensal,data_inicio,data_fim,status,descricao)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (c.nome, c.fornecedor, c.categoria, c.unidade, c.valor_anual or 0, c.valor_mensal or 0,
          c.data_inicio, c.data_fim, c.status or 'ativo', c.descricao))
    conn.commit(); new_id = cursor.lastrowid
    conn.close(); return {"id": new_id, "status": "criado"}

@app.patch("/api/contas-fixas/{cid}")
def atualizar_conta_fixa(cid: int, c: ContaFixaIn):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        UPDATE Contas_Fixas SET nome=?,fornecedor=?,categoria=?,unidade=?,valor_anual=?,
        valor_mensal=?,data_inicio=?,data_fim=?,status=?,descricao=? WHERE id=?
    """, (c.nome, c.fornecedor, c.categoria, c.unidade, c.valor_anual or 0, c.valor_mensal or 0,
          c.data_inicio, c.data_fim, c.status or 'ativo', c.descricao, cid))
    if cursor.rowcount == 0:
        conn.close(); raise HTTPException(404, "Conta não encontrada")
    conn.commit(); conn.close(); return {"status": "ok"}

@app.delete("/api/contas-fixas/{cid}")
def deletar_conta_fixa(cid: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("DELETE FROM Lancamentos_CF WHERE id_conta=?", (cid,))
    cursor.execute("DELETE FROM Contas_Fixas WHERE id=?", (cid,))
    conn.commit(); conn.close(); return {"status": "ok"}

@app.get("/api/contas-fixas/{cid}/lancamentos")
def listar_lancamentos(cid: int, ano: int = 0):
    if not ano: ano = datetime.now().year
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        SELECT id,id_conta,mes,ano,valor,tipo_doc,numero_doc,arquivo_path,arquivo_nome,data_lancamento,obs
        FROM Lancamentos_CF WHERE id_conta=? AND ano=? ORDER BY mes
    """, (cid, ano))
    cols = ['id','id_conta','mes','ano','valor','tipo_doc','numero_doc','arquivo_path','arquivo_nome','data_lancamento','obs']
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    conn.close(); return rows

@app.post("/api/contas-fixas/{cid}/lancamentos", status_code=201)
async def criar_lancamento(
    cid: int,
    mes: int = Form(...),
    ano: int = Form(...),
    valor: float = Form(...),
    tipo_doc: str = Form('NF'),
    numero_doc: str = Form(''),
    obs: str = Form(''),
    arquivo: Optional[UploadFile] = File(None)
):
    arquivo_path = None; arquivo_nome = None
    if arquivo and arquivo.filename:
        uploads_dir = os.path.join(BASE_DIR, 'uploads', 'contas_fixas')
        os.makedirs(uploads_dir, exist_ok=True)
        safe_name = f"{cid}_{ano}_{mes:02d}_{arquivo.filename}"
        dest = os.path.join(uploads_dir, safe_name)
        with open(dest, 'wb') as f:
            f.write(await arquivo.read())
        arquivo_path = f"/uploads/contas_fixas/{safe_name}"
        arquivo_nome = arquivo.filename
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Lancamentos_CF (id_conta,mes,ano,valor,tipo_doc,numero_doc,arquivo_path,arquivo_nome,obs)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (cid, mes, ano, valor, tipo_doc, numero_doc or None, arquivo_path, arquivo_nome, obs or None))
    conn.commit(); new_id = cursor.lastrowid
    conn.close(); return {"id": new_id, "status": "criado"}

@app.delete("/api/contas-fixas/lancamentos/{lid}")
def deletar_lancamento(lid: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT arquivo_path FROM Lancamentos_CF WHERE id=?", (lid,))
    row = cursor.fetchone()
    if row and row[0]:
        try:
            path = os.path.join(BASE_DIR, row[0].lstrip('/'))
            if os.path.exists(path): os.remove(path)
        except Exception: pass
    cursor.execute("DELETE FROM Lancamentos_CF WHERE id=?", (lid,))
    conn.commit(); conn.close(); return {"status": "ok"}

# ==========================================
# 10. SOURCING — SEGMENTOS
# ==========================================

@app.get("/api/sourcing/segmentos")
def listar_segmentos():
    """Retorna lista distinta de segmentos disponíveis para busca de fornecedores."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    # Busca segmentos da tabela Categorias (usada no JOIN de /fornecedores/{segmento})
    cursor.execute("SELECT DISTINCT segmento FROM Categorias WHERE segmento IS NOT NULL AND segmento != '' ORDER BY segmento")
    segs = [r[0] for r in cursor.fetchall()]
    # Complementa com segmentos históricos dos itens caso Categorias esteja vazia
    if not segs:
        cursor.execute("SELECT DISTINCT segmento_historico FROM Itens_Requisicao WHERE segmento_historico IS NOT NULL AND segmento_historico != '' ORDER BY segmento_historico")
        segs = [r[0] for r in cursor.fetchall()]
    conn.close(); return segs

# ==========================================
# 11. CATÁLOGO — itens derivados das requisições
# ==========================================

@app.get("/api/catalogo/stats")
def stats_catalogo():
    """KPIs globais do catálogo — calculados sem paginação."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    # Total itens únicos
    cursor.execute("""
        SELECT COUNT(DISTINCT LOWER(TRIM(descricao)) || '|' || COALESCE(segmento_historico,''))
        FROM Itens_Requisicao WHERE descricao IS NOT NULL AND descricao != ''
    """)
    total_itens = cursor.fetchone()[0] or 0

    # Total segmentos distintos
    cursor.execute("""
        SELECT COUNT(DISTINCT segmento_historico)
        FROM Itens_Requisicao
        WHERE segmento_historico IS NOT NULL AND segmento_historico != ''
    """)
    total_segs = cursor.fetchone()[0] or 0

    # Itens com pelo menos 1 req concluída e valor_fechado > 0
    cursor.execute("""
        SELECT COUNT(DISTINCT LOWER(TRIM(i.descricao)) || '|' || COALESCE(i.segmento_historico,''))
        FROM Itens_Requisicao i
        JOIN Requisicoes r ON r.id_sharepoint = i.id_requisicao
        WHERE (r.status LIKE '%Concluí%' OR r.status LIKE '%Concluido%')
          AND r.valor_fechado IS NOT NULL AND r.valor_fechado > 0
          AND i.descricao IS NOT NULL AND i.descricao != ''
    """)
    com_preco = cursor.fetchone()[0] or 0

    # Total requisições na base
    cursor.execute("SELECT COUNT(*) FROM Requisicoes")
    total_req = cursor.fetchone()[0] or 0

    conn.close()
    return {
        "total_itens": total_itens,
        "total_segmentos": total_segs,
        "com_preco": com_preco,
        "total_requisicoes": total_req
    }

@app.get("/api/catalogo")
def listar_catalogo(
    busca: str = "",
    segmento: str = "",
    page: int = 1,
    per_page: int = 20
):
    """
    Catálogo derivado de Itens_Requisicao.
    Agrupa por (descricao, segmento_historico), conta requisições,
    calcula preço médio de requisições concluídas e fornecedores distintos.
    """
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    where_clauses = ["i.descricao IS NOT NULL", "i.descricao != ''"]
    params = []
    if busca:
        where_clauses.append("LOWER(i.descricao) LIKE ?")
        params.append(f"%{busca.lower()}%")
    if segmento:
        where_clauses.append("i.segmento_historico = ?")
        params.append(segmento)

    where_sql = " AND ".join(where_clauses)

    # Total para paginação
    cursor.execute(f"""
        SELECT COUNT(DISTINCT LOWER(TRIM(i.descricao)) || '|' || COALESCE(i.segmento_historico,''))
        FROM Itens_Requisicao i
        WHERE {where_sql}
    """, params)
    total = cursor.fetchone()[0] or 0

    offset = (page - 1) * per_page

    cursor.execute(f"""
        SELECT
            TRIM(i.descricao)                                  AS descricao,
            COALESCE(i.segmento_historico, 'Sem Segmento')     AS segmento,
            COUNT(*)                                           AS total_requisicoes,
            COUNT(DISTINCT r.fornecedor)                       AS total_fornecedores,
            AVG(CASE WHEN r.status LIKE '%Concluí%' AND r.valor_fechado > 0
                     THEN r.valor_fechado END)                  AS preco_medio,
            MAX(r.data_solicitacao)                            AS ultima_requisicao,
            SUM(i.quantidade)                                  AS qtd_total,
            COUNT(CASE WHEN r.status LIKE '%Concluí%' THEN 1 END) AS total_concluidos
        FROM Itens_Requisicao i
        LEFT JOIN Requisicoes r ON r.id_sharepoint = i.id_requisicao
        WHERE {where_sql}
        GROUP BY LOWER(TRIM(i.descricao)), COALESCE(i.segmento_historico,'')
        ORDER BY total_requisicoes DESC, descricao ASC
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    rows = cursor.fetchall()
    items = []
    for r in rows:
        items.append({
            "descricao": r[0],
            "segmento": r[1],
            "total_requisicoes": r[2],
            "total_fornecedores": r[3] or 0,
            "preco_medio": round(r[4], 2) if r[4] else None,
            "ultima_requisicao": r[5],
            "qtd_total": round(r[6] or 0, 2),
            "total_concluidos": r[7] or 0,
        })

    # Segmentos distintos para filtro
    cursor.execute("""
        SELECT DISTINCT segmento_historico FROM Itens_Requisicao
        WHERE segmento_historico IS NOT NULL AND segmento_historico != ''
        ORDER BY segmento_historico
    """)
    segmentos = [s[0] for s in cursor.fetchall()]

    conn.close()
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, -(-total // per_page)),  # ceil division
        "segmentos": segmentos
    }

@app.get("/api/catalogo/detalhe")
def detalhe_catalogo(descricao: str, segmento: str = ""):
    """Histórico completo de um item do catálogo."""
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()

    params = [descricao]
    seg_filter = ""
    if segmento and segmento != "Sem Segmento":
        seg_filter = "AND i.segmento_historico = ?"
        params.append(segmento)

    cursor.execute(f"""
        SELECT
            r.id_sharepoint, r.data_solicitacao, r.status, r.fornecedor,
            r.valor_fechado, r.unidade, r.comprador, i.quantidade
        FROM Itens_Requisicao i
        LEFT JOIN Requisicoes r ON r.id_sharepoint = i.id_requisicao
        WHERE LOWER(TRIM(i.descricao)) = LOWER(TRIM(?)) {seg_filter}
        ORDER BY r.data_solicitacao DESC
        LIMIT 50
    """, params)

    historico = []
    for row in cursor.fetchall():
        historico.append({
            "id": row[0], "data": row[1], "status": row[2],
            "fornecedor": row[3], "valor": row[4], "unidade": row[5],
            "comprador": row[6], "quantidade": row[7]
        })

    conn.close()
    return {"historico": historico}

# ==========================================
# 12. RECEBIMENTO — UPLOAD DE NF
# ==========================================

NF_UPLOAD_DIR = _nf_uploads_dir

@app.post("/api/recebimento/upload-nf")
async def upload_nf(
    file: UploadFile = File(...),
    id_pedido: str = Form(...),
    numero_nf: str = Form(default="")
):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.pdf', '.xml']:
        raise HTTPException(400, "Apenas arquivos PDF ou XML são aceitos.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = f"NF_{id_pedido}_{timestamp}{ext}"
    dest = os.path.join(NF_UPLOAD_DIR, safe_name)

    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    size_kb = round(os.path.getsize(dest) / 1024, 1)

    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO NF_Uploads (id_pedido, numero_nf, nome_arquivo, caminho, tamanho_kb, tipo) VALUES (?,?,?,?,?,?)",
        (int(id_pedido), numero_nf, file.filename, safe_name, size_kb, ext.lstrip('.').upper())
    )
    # Espelha em Arquivos_Requisicao para aparecer na Consulta de Requisições
    caminho_publico = f"/nf-uploads/{safe_name}"
    label = f"NF {numero_nf}" if numero_nf else "Nota Fiscal"
    try:
        cursor.execute("""
            INSERT INTO Arquivos_Requisicao (id_requisicao, origem, nome_arquivo, caminho, tamanho_kb, tipo_mime, enviado_por)
            VALUES (?,?,?,?,?,?,?)
        """, (int(id_pedido), 'comprador', file.filename, caminho_publico,
              size_kb, f"application/{ext.lstrip('.')}", f"Conciliação — {label}"))
    except Exception:
        pass
    conn.commit(); conn.close()

    return {"status": "ok", "arquivo": safe_name, "tamanho_kb": size_kb}

@app.get("/api/recebimento/nf-uploads/{id_pedido}")
def listar_nf_uploads(id_pedido: int):
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("SELECT id, numero_nf, nome_arquivo, tamanho_kb, tipo, enviado_em FROM NF_Uploads WHERE id_pedido=? ORDER BY enviado_em DESC", (id_pedido,))
    rows = [{"id": r[0], "numero_nf": r[1], "nome_arquivo": r[2], "tamanho_kb": r[3], "tipo": r[4], "enviado_em": r[5]} for r in cursor.fetchall()]
    conn.close(); return rows