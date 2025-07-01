
class ETL_boletos_base:
    def ETL_boletos():
        import pandas as pd
        import awswrangler as awr
        import openpyxl
        import os

        query_path = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos\sql\reissued_invoices.sql"
        with open(query_path, 'r') as file:
            query = file.read()

        df_reemitidos = awr.athena.read_sql_query(query, database='silver')
        df_reemitidos.drop_duplicates(subset='ponteiro', inplace=True)
        lista_reemitidos = df_reemitidos['ponteiro'].to_list()

        df_reemitidos['data_baixa'] = pd.NA
        df_reemitidos['valor_baixa'] = pd.NA
        df_reemitidos['situacao'] = 'REEMITIDO'

        query_path = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos\sql\paid_invoices.sql"
        with open(query_path, 'r') as file:
            query = file.read()

        df_pagos = awr.athena.read_sql_query(query, database='silver')
        df_pagos.drop_duplicates(subset='ponteiro', inplace=True)
        df_pagos['data_reemissao'] = pd.NA
        df_pagos['situacao'] = 'PAGO'
        df_reemitidos_pagos = df_pagos[df_pagos['ponteiro'].isin(lista_reemitidos)]

        df_final = pd.concat([df_reemitidos_pagos, df_reemitidos])

        colunas_ordenadas = [
            'codigo_cadastro',
            'ponteiro',
            'numero_documento',
            'numero_boleto',
            'nosso_numero',
            'sequencia_documento',
            'aplicacao_financeira',
            'valor_titulo',
            'valor_baixa',
            'situacao',
            'data_emissao',
            'data_reemissao',
            'data_baixa',
            'data_vencimento',
            'conjunto',
            'matricula',
            'unidade',
            'empresa',
            'associado',
            'vendedor',
            'grupo'
        ]

        df_final = df_final[colunas_ordenadas]
        df_final = df_final.sort_values(by=['ponteiro', 'data_reemissao'], ascending=False)

        df_final['conjunto'] = df_final['conjunto'].astype(str)
        df_final['matricula'] = df_final['matricula'].astype(str)
        df_final['unidade'] = df_final['unidade'].astype(str)
        df_final['numero_boleto'] = df_final['numero_boleto'].astype(str)
        df_final['nosso_numero'] = df_final['nosso_numero'].astype(str)

        df_final['valor_baixa'] = df_final['valor_baixa'].fillna(0)
        df_final['data_baixa'] = df_final['data_baixa'].fillna(pd.Timestamp('1900-01-01').date())
        df_final['conjunto'] = df_final['conjunto'].fillna('NULL')
        df_final['matricula'] = df_final['matricula'].fillna('NULL')
        df_final['unidade'] = df_final['unidade'].fillna('NULL')
        df_final['data_reemissao'] = df_final['data_reemissao'].fillna(pd.Timestamp('1900-01-01').date())
        df_final['numero_boleto'] = df_final['numero_boleto'].fillna('NULL')
        df_final['nosso_numero'] = df_final['nosso_numero'].fillna('NULL')

        today = pd.Timestamp.today().date()
        today_format = today.strftime('%Y%m%d')

        save_path_sharepoint = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Pagamento de Reemissões"
        save_path_new = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos"
        save_path_cache = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos\cache"

        arquivo_sharepoint = os.path.join(save_path_sharepoint, 'boletos_reemitidos.xlsx')
        arquivo_new = os.path.join(save_path_new, 'boletos_reemitidos.xlsx')
        arquivo_cache = os.path.join(save_path_cache, f'boletos_reemitidos_{today_format}.xlsx')

        df_final.to_excel(arquivo_new, engine='openpyxl', index=False, sheet_name='boletos_reemitidos_pagos')
        df_final.to_excel(arquivo_cache, engine='openpyxl', index=False, sheet_name='boletos_reemitidos_pagos')
        df_final.to_excel(arquivo_sharepoint, engine='openpyxl', index=False, sheet_name='boletos_reemitidos_pagos')

        print(f"Arquivo Excel salvo com sucesso em: {arquivo_sharepoint}")

if __name__ == '__main__':
    ETL_boletos_base.ETL_boletos()