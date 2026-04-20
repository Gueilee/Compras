import openpyxl
import sqlite3
import os

# Ajustado para o nome e formato exato do seu arquivo Excel
EXCEL_PATH = os.path.join('data', 'Planilha Fornecedores.xlsx')
DB_PATH = os.path.join('backend', 'vendemmia_compras.db')

def importar_fornecedores():
    print("Iniciando a leitura do arquivo Excel de Fornecedores...")
    
    if not os.path.exists(EXCEL_PATH):
        print(f"Erro: Arquivo não encontrado no caminho: {EXCEL_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    novos_fornecedores = 0
    ligacoes_feitas = 0

    # Carrega a planilha (data_only=True pega o valor final das células, ignorando fórmulas)
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True, read_only=True)
    sheet = wb.active # Pega a primeira aba da planilha

    # Lê a primeira linha para mapear as colunas
    headers = [str(cell.value).strip().upper() if cell.value else "" for cell in sheet[1]]
    
    try:
        idx_razao = headers.index('RAZÃO SOCIAL')
        idx_cnpj = headers.index('CNPJ')
        idx_segmento = headers.index('SEGMENTO')
        idx_email = headers.index('E-MAIL')
        idx_whats = headers.index('WHATS')
        idx_vendedor = headers.index('VENDEDOR')
    except ValueError as e:
        print(f"Erro: Alguma coluna padrão não foi encontrada na primeira linha. {e}")
        return

    # Itera a partir da segunda linha (onde começam os dados)
    for row in sheet.iter_rows(min_row=2, values_only=True):
        cnpj = str(row[idx_cnpj]).strip() if row[idx_cnpj] else ""
        
        # Se a linha não tiver CNPJ ou for vazia, pula para a próxima
        if not cnpj or cnpj.upper() == 'NONE' or cnpj == '':
            continue

        razao_social = str(row[idx_razao]).strip() if row[idx_razao] else ""
        segmento_planilha = str(row[idx_segmento]).strip() if row[idx_segmento] else ""
        email = str(row[idx_email]).strip() if row[idx_email] else ""
        whats = str(row[idx_whats]).strip() if row[idx_whats] else ""
        vendedor = str(row[idx_vendedor]).strip() if row[idx_vendedor] else ""

        # Insere o Fornecedor (IGNORA se o CNPJ já existir)
        cursor.execute('''
            INSERT OR IGNORE INTO Fornecedores (cnpj, razao_social, email, telefone, vendedor)
            VALUES (?, ?, ?, ?, ?)
        ''', (cnpj, razao_social, email, whats, vendedor))
        
        if cursor.rowcount > 0:
            novos_fornecedores += 1

        # Garante que o Segmento da planilha existe na tabela Categorias
        if segmento_planilha and segmento_planilha.upper() != 'NONE':
            cursor.execute('SELECT id FROM Categorias WHERE segmento = ?', (segmento_planilha,))
            resultado_cat = cursor.fetchone()
            
            if resultado_cat:
                id_categoria = resultado_cat[0]
            else:
                # Se for um segmento novo, cria dinamicamente vinculando a uma categoria "A Classificar"
                cursor.execute('''
                    INSERT INTO Categorias (macro_categoria, segmento)
                    VALUES ('A Classificar', ?)
                ''', (segmento_planilha,))
                id_categoria = cursor.lastrowid

            # Cria a ligação entre Fornecedor e Categoria
            cursor.execute('''
                INSERT OR IGNORE INTO Fornecedores_Segmentos (cnpj_fornecedor, id_categoria)
                VALUES (?, ?)
            ''', (cnpj, id_categoria))
            
            if cursor.rowcount > 0:
                ligacoes_feitas += 1

    # Salva as alterações e fecha a conexão
    conn.commit()
    conn.close()
    
    print(f"Carga concluída com sucesso!")
    print(f"- {novos_fornecedores} novos fornecedores cadastrados.")
    print(f"- {ligacoes_feitas} vínculos de segmentos criados.")

if __name__ == '__main__':
    importar_fornecedores()