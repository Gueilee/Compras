import sqlite3
import os

# Ajustado para o caminho onde seu banco realmente está
DB_PATH = os.path.join(os.path.dirname(__file__), 'vendemmia_compras.db')

def criar_tabela_lances():
    if not os.path.exists(DB_PATH):
        print(f"Erro: Banco de dados não encontrado em {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Criando a tabela que receberá os preços dos fornecedores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Lances_Fornecedor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_requisicao INTEGER,
            cnpj_fornecedor TEXT,
            preco_unitario REAL,
            prazo_entrega_dias INTEGER,
            data_resposta TEXT,
            FOREIGN KEY (id_requisicao) REFERENCES Requisicoes(id_sharepoint),
            FOREIGN KEY (cnpj_fornecedor) REFERENCES Fornecedores(cnpj)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Sucesso: Tabela Lances_Fornecedor criada com sucesso!")

if __name__ == '__main__':
    criar_tabela_lances()