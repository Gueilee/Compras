import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vendemmia_compras.db')

def criar_tabela_orcamento():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Cria a tabela de controle financeiro
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orcamentos (
            unidade TEXT PRIMARY KEY,
            orcamento_anual REAL,
            consumido REAL
        )
    ''')
    
    # Injeta dados fictícios para podermos testar o visual
    # VCI está com folga, Garuva está quase estourando o limite
    cursor.execute("INSERT OR IGNORE INTO Orcamentos (unidade, orcamento_anual, consumido) VALUES ('VCI', 500000.00, 125000.00)")
    cursor.execute("INSERT OR IGNORE INTO Orcamentos (unidade, orcamento_anual, consumido) VALUES ('Garuva', 300000.00, 285000.00)")
    
    conn.commit()
    conn.close()
    print("Sucesso: Tabela de Orçamentos criada e populada!")

if __name__ == '__main__':
    criar_tabela_orcamento()