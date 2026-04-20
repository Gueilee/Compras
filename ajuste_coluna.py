import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vendemmia_compras.db')

def adicionar_coluna_obs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        # Tenta adicionar a coluna de observações caso ela não exista
        cursor.execute('ALTER TABLE Requisicoes ADD COLUMN observacoes TEXT')
        conn.commit()
        print("Coluna 'observacoes' adicionada com sucesso!")
    except sqlite3.OperationalError:
        print("A coluna 'observacoes' já existe ou a tabela não foi encontrada.")
    finally:
        conn.close()

if __name__ == '__main__':
    adicionar_coluna_obs()