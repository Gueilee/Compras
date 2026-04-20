import openpyxl
import sqlite3
import os

# Caminhos baseados na nossa estrutura
EXCEL_PATH = os.path.join('data', 'base_histórica.xlsx')
DB_PATH = os.path.join('backend', 'vendemmia_compras.db')

def processar_historico():
    print("A iniciar a leitura do ficheiro histórico de requisições...")

    if not os.path.exists(EXCEL_PATH):
        print(f"Erro: Ficheiro não encontrado em: {EXCEL_PATH}")
        print("Certifique-se de que o ficheiro base_histórica.xlsx está na pasta 'data'.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Criar as novas tabelas relacionais
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Requisicoes (
        id_sharepoint INTEGER PRIMARY KEY,
        unidade TEXT,
        data_solicitacao TEXT,
        comprador TEXT,
        status TEXT,
        valor_fechado REAL,
        fornecedor TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Itens_Requisicao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_requisicao INTEGER,
        descricao TEXT,
        quantidade TEXT,
        FOREIGN KEY (id_requisicao) REFERENCES Requisicoes(id_sharepoint)
    )
    ''')

    # Carrega a folha de cálculo (data_only extrai valores finais, ignorando fórmulas)
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True, read_only=True)
    sheet = wb.active

    # Lê os cabeçalhos para mapeamento dinâmico
    headers = [str(cell.value).strip().upper() if cell.value else "" for cell in sheet[1]]

    try:
        # Colunas principais da Requisição
        idx_id = headers.index('ID')
        idx_unidade = headers.index('UNIDADE')
        idx_data = headers.index('DATA DA SOLICITAÇÃO')
        idx_comprador = headers.index('COMPRADOR')
        idx_status = headers.index('STATUS')
        idx_valor = headers.index('VALOR FECHADO')
        idx_fornecedor = headers.index('FORNECEDOR')

        # Colunas dos Itens
        idx_desc1 = headers.index('DESCRIÇÃO DO MATERIAL OU SERVIÇO')
        
        # Mapeamento seguro para colunas extra (caso alguma falte na folha de cálculo)
        idx_desc2 = headers.index('DESCRIÇÃO DO MATERIAL OU SERVIÇO 2') if 'DESCRIÇÃO DO MATERIAL OU SERVIÇO 2' in headers else -1
        idx_qtd2 = headers.index('QUANTIDADE 2') if 'QUANTIDADE 2' in headers else -1
        idx_desc3 = headers.index('DESCRIÇÃO DO MATERIAL OU SERVIÇO 3') if 'DESCRIÇÃO DO MATERIAL OU SERVIÇO 3' in headers else -1
        idx_qtd3 = headers.index('QUANTIDADE 3') if 'QUANTIDADE 3' in headers else -1
        idx_desc4 = headers.index('DESCRIÇÃO DO MATERIAL OU SERVIÇO 4') if 'DESCRIÇÃO DO MATERIAL OU SERVIÇO 4' in headers else -1
        idx_qtd4 = headers.index('QUANTIDADE 4') if 'QUANTIDADE 4' in headers else -1
        idx_desc5 = headers.index('DESCRIÇÃO DO MATERIAL OU SERVIÇO 5') if 'DESCRIÇÃO DO MATERIAL OU SERVIÇO 5' in headers else -1
        idx_qtd5 = headers.index('QUANTIDADE 5') if 'QUANTIDADE 5' in headers else -1

    except ValueError as e:
        print(f"Erro no mapeamento das colunas: {e}")
        return

    novas_reqs = 0
    novos_itens = 0

    # Iterar sobre as linhas de dados
    for row in sheet.iter_rows(min_row=2, values_only=True):
        id_sharepoint = row[idx_id]
        if not id_sharepoint:
            continue

        # 2. Inserir o cabeçalho (A Requisição em si)
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO Requisicoes 
                (id_sharepoint, unidade, data_solicitacao, comprador, status, valor_fechado, fornecedor)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                id_sharepoint,
                str(row[idx_unidade]).strip() if row[idx_unidade] else "",
                str(row[idx_data]).strip() if row[idx_data] else "",
                str(row[idx_comprador]).strip() if row[idx_comprador] else "",
                str(row[idx_status]).strip() if row[idx_status] else "",
                row[idx_valor] if row[idx_valor] else 0.0,
                str(row[idx_fornecedor]).strip() if row[idx_fornecedor] else ""
            ))
            if cursor.rowcount > 0:
                novas_reqs += 1
        except Exception:
            continue

        # 3. Função para inserir os materiais isoladamente
        def inserir_item(desc_idx, qtd_idx, qtd_default="1"):
            if desc_idx != -1 and row[desc_idx] and str(row[desc_idx]).strip().upper() not in ['NONE', '']:
                descricao = str(row[desc_idx]).strip()
                quantidade = str(row[qtd_idx]).strip() if qtd_idx != -1 and row[qtd_idx] else qtd_default
                
                cursor.execute('''
                    INSERT INTO Itens_Requisicao (id_requisicao, descricao, quantidade)
                    VALUES (?, ?, ?)
                ''', (id_sharepoint, descricao, quantidade))
                return 1
            return 0

        # Inserir o Item 1 (que muitas vezes não tem a coluna de quantidade explícita no Forms)
        novos_itens += inserir_item(idx_desc1, -1)
        # Inserir os Itens 2 a 5
        novos_itens += inserir_item(idx_desc2, idx_qtd2)
        novos_itens += inserir_item(idx_desc3, idx_qtd3)
        novos_itens += inserir_item(idx_desc4, idx_qtd4)
        novos_itens += inserir_item(idx_desc5, idx_qtd5)

    conn.commit()
    conn.close()

    print("Transformação do histórico concluída com sucesso!")
    print(f"- {novas_reqs} requisições de compra processadas.")
    print(f"- {novos_itens} itens individuais desdobrados na base de dados.")

if __name__ == '__main__':
    processar_historico()