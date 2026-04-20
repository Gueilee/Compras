import sqlite3
import os

# Define o caminho onde o banco será salvo
DB_PATH = os.path.join(os.path.dirname(__file__), 'vendemmia_compras.db')

def criar_banco_dados():
    print("Iniciando a criação do banco de dados relacional...")
    
    # Conecta (ou cria) o banco de dados
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Tabela de Macro Categorias e Segmentos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Categorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        macro_categoria TEXT NOT NULL,
        segmento TEXT NOT NULL UNIQUE
    )
    ''')

    # 2. Tabela de Fornecedores (Chave Primária = CNPJ)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Fornecedores (
        cnpj TEXT PRIMARY KEY,
        razao_social TEXT NOT NULL,
        email TEXT,
        telefone TEXT,
        vendedor TEXT
    )
    ''')

    # 3. Tabela de Relacionamento (Fornecedores <-> Categorias)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Fornecedores_Segmentos (
        cnpj_fornecedor TEXT,
        id_categoria INTEGER,
        FOREIGN KEY (cnpj_fornecedor) REFERENCES Fornecedores(cnpj),
        FOREIGN KEY (id_categoria) REFERENCES Categorias(id),
        PRIMARY KEY (cnpj_fornecedor, id_categoria)
    )
    ''')

    # Alimentando as categorias padronizadas no banco
    categorias_iniciais = [
        ('Insumos Operacionais', 'Embalagens e Unitização'),
        ('Insumos Operacionais', 'Identificação e Rastreio'),
        ('Insumos Operacionais', 'EPIs e Segurança do Trabalho'),
        ('Facilities e Infraestrutura', 'Material de Limpeza'),
        ('Facilities e Infraestrutura', 'Manutenção Predial'),
        ('Tecnologia (TI)', 'Hardware e Eletrônicos'),
        ('Tecnologia (TI)', 'Software e Licenciamento'),
        ('Administrativo', 'Material de Escritório')
    ]

    cursor.executemany('''
    INSERT OR IGNORE INTO Categorias (macro_categoria, segmento) 
    VALUES (?, ?)
    ''', categorias_iniciais)

    conn.commit()
    conn.close()
    print(f"Sucesso! Banco criado e categorias inseridas em: {DB_PATH}")

if __name__ == '__main__':
    criar_banco_dados()
    