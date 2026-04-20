import csv
import sqlite3
import os

CSV_PATH = os.path.join('data', 'Requisição de Compras.csv')
DB_PATH = os.path.join('backend', 'vendemmia_compras.db')

def reprocessar_historico():
    print("A iniciar o re-processamento do histórico com o novo ficheiro CSV...")

    if not os.path.exists(CSV_PATH):
        print(f"Erro: Ficheiro não encontrado em: {CSV_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Limpar as tabelas antigas para não misturar dados maus com dados bons
    cursor.execute('DROP TABLE IF EXISTS Itens_Requisicao')
    cursor.execute('DROP TABLE IF EXISTS Requisicoes')

    # 2. Recriar a tabela de Requisições
    cursor.execute('''
    CREATE TABLE Requisicoes (
        id_sharepoint INTEGER PRIMARY KEY,
        unidade TEXT,
        data_solicitacao TEXT,
        comprador TEXT,
        status TEXT,
        valor_fechado REAL,
        fornecedor TEXT
    )
    ''')

    # 3. Recriar a tabela de Itens (Agora com a coluna QTD real e o Segmento/Tipo de Despesa!)
    cursor.execute('''
    CREATE TABLE Itens_Requisicao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_requisicao INTEGER,
        descricao TEXT,
        quantidade REAL,
        segmento_historico TEXT,
        FOREIGN KEY (id_requisicao) REFERENCES Requisicoes(id_sharepoint)
    )
    ''')

    novas_reqs = 0
    novos_itens = 0

    # Lendo o ficheiro CSV nativamente (utf-8-sig remove caracteres ocultos do início do ficheiro)
    with open(CSV_PATH, mode='r', encoding='utf-8-sig') as file:
        # Lê como dicionário para facilitar a busca pelo nome da coluna
        reader = csv.DictReader(file)
        
        for row in reader:
            # Obtém os dados principais com segurança
            try:
                id_sharepoint = int(row.get('ID', 0))
            except ValueError:
                continue # Se não tiver ID válido, pula
            
            if id_sharepoint == 0:
                continue

            unidade = str(row.get('Unidade', '')).strip()
            data_solicitacao = str(row.get('Data da Solicitação', '')).strip()
            comprador = str(row.get('Comprador', '')).strip()
            status = str(row.get('STATUS', '')).strip()
            fornecedor = str(row.get('Fornecedor', '')).strip()
            tipo_despesa = str(row.get('Tipo de despesa', '')).strip()
            
            # Tratamento do Valor Fechado (Remover "R$", espaços e trocar vírgula por ponto)
            valor_str = str(row.get('Valor Fechado', '')).replace('R$', '').replace('.', '').replace(',', '.').strip()
            try:
                valor_fechado = float(valor_str) if valor_str else 0.0
            except ValueError:
                valor_fechado = 0.0

            # 4. Inserir Cabeçalho da Requisição
            cursor.execute('''
                INSERT INTO Requisicoes 
                (id_sharepoint, unidade, data_solicitacao, comprador, status, valor_fechado, fornecedor)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (id_sharepoint, unidade, data_solicitacao, comprador, status, valor_fechado, fornecedor))
            novas_reqs += 1

            # 5. Função interna para inserir itens, capturando a Quantidade exata
            def inserir_item(col_desc, col_qtd):
                desc = str(row.get(col_desc, '')).strip()
                if desc and desc.upper() != 'NAN' and desc != '':
                    
                    qtd_str = str(row.get(col_qtd, '1')).replace(',', '.').strip()
                    try:
                        qtd = float(qtd_str) if qtd_str and qtd_str.upper() != 'NAN' else 1.0
                    except ValueError:
                        qtd = 1.0

                    cursor.execute('''
                        INSERT INTO Itens_Requisicao (id_requisicao, descricao, quantidade, segmento_historico)
                        VALUES (?, ?, ?, ?)
                    ''', (id_sharepoint, desc, qtd, tipo_despesa))
                    return 1
                return 0

            # Inserir o Item 1 (Agora tem a própria coluna de Quantidade no seu CSV)
            novos_itens += inserir_item('Descrição do Material ou Serviço', 'Quantidade')
            
            # Inserir os Itens extras (se existirem na linha)
            novos_itens += inserir_item('Descrição do Material ou Serviço 2', 'Quantidade 2')
            novos_itens += inserir_item('Descrição do Material ou Serviço 3', 'Quantidade 3')
            novos_itens += inserir_item('Descrição do Material ou Serviço 4', 'Quantidade 4')
            novos_itens += inserir_item('Descrição do Material ou Serviço 5', 'Quantidade 5')

    conn.commit()
    conn.close()

    print("\n--- RE-PROCESSAMENTO CONCLUÍDO ---")
    print(f"Total de Requisições importadas: {novas_reqs}")
    print(f"Total de Itens (com quantidades corretas e segmentos vinculados): {novos_itens}")
    print("O banco de dados histórico foi atualizado com sucesso!")

if __name__ == '__main__':
    reprocessar_historico()