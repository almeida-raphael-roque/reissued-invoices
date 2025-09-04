

class ETL_boletos_viz:
    def ETL_boletos_viz():

        import xlwings as xw
        import pandas as pd
        import shutil

        wb1_path = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos\boletos_reemitidos.xlsx"
        wb1 = xw.Book(wb1_path)
        ws1 = wb1.sheets['boletos_reemitidos_pagos']
        intervalo = ws1.range('A1').expand()
        dados = intervalo.value

        wb2_path = r"C:\Users\raphael.almeida\Documents\Processos\boletos_reemitidos\Boletos Reemitidos Pagos.xlsx"
        wb2 = xw.Book(wb2_path)
        ws2 = wb2.sheets['BASE']
        ws2.clear_contents()
        ws2.range('A1').value = dados
        ws3 = wb2.sheets['Boletos Reemitidos Pagos']

        today = pd.Timestamp.today().normalize()
        day_offset_1 = today - pd.Timedelta(days=1)
        day_offset_2 = today - pd.Timedelta(days=2)
        day_offset_3 = today - pd.Timedelta(days=3)
        day_offset_4 = today - pd.Timedelta(days=4)

        yesterday = day_offset_1.date()
        dbf_yesterday = day_offset_2.date()
        dbf_yesterday_2 = day_offset_3.date()
        dbf_yesterday_3 = day_offset_4.date()

        ws3.range('B16').value = day_offset_4.date()
        ws3.range('B12').value = day_offset_3.date()
        ws3.range('B8').value = day_offset_2.date()
        ws3.range('B4').value = day_offset_1.date()

        base_values = ws2.range('A1').expand().value

        if base_values:
            df = pd.DataFrame(base_values[1:], columns=base_values[0])
        else:
            df = pd.DataFrame()

        listas_ponteiros_4 = df.loc[df['data_reemissao'] == day_offset_4, 'ponteiro'].tolist()
        listas_ponteiros_3 = df.loc[df['data_reemissao'] == day_offset_3, 'ponteiro'].tolist()
        listas_ponteiros_2 = df.loc[df['data_reemissao'] == day_offset_2, 'ponteiro'].tolist()
        listas_ponteiros_1 = df.loc[df['data_reemissao'] == day_offset_1, 'ponteiro'].tolist()

        def calcular_pagamentos_4dias_4empresas(df, listas_ponteiros, empresas):
            resultados = {}
            for idx, ponteiros in enumerate(listas_ponteiros, 1):
                for empresa in empresas:
                    soma = df.loc[
                        (df['ponteiro'].isin(ponteiros)) &
                        (df['empresa'] == empresa),
                        'valor_baixa'
                    ].sum()
                    resultados[(empresa, idx)] = soma
            return resultados

        listas_ponteiros = [listas_ponteiros_1, listas_ponteiros_2, listas_ponteiros_3, listas_ponteiros_4]
        empresas = ['Segtruck', 'Stcoop', 'Viavante', 'Tag']
        pagamentos = calcular_pagamentos_4dias_4empresas(df, listas_ponteiros, empresas)
        colunas = ['C', 'D', 'E', 'F']

        datas_linhas = [
            (yesterday, 6),
            (dbf_yesterday, 10),
            (dbf_yesterday_2, 14),
            (dbf_yesterday_3, 18)
        ]
        
        for idx, (data, linha) in enumerate(datas_linhas, 1):
            for idx_col, (coluna, empresa) in enumerate(zip(colunas, empresas)):
                valor = pagamentos.get((empresa, idx), 0)
                ws3.range(f'{coluna}{linha}').value = valor

        # saving and closing excel files
        wb1.save()
        wb1.close()
        wb2.save()
        wb2.close()
        save_sharepoint = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Pagamento de Reemissões\Boletos Reemitidos Pagos.xlsx"
        
        shutil.copy(wb2_path, save_sharepoint)


        print(f"Arquivo Boletos Reemitidos Pagos salvo com sucesso em {save_sharepoint}")

if __name__ == '__main__':
    ETL_boletos_viz.ETL_boletos_viz()