import xlwings as xw
import pandas as pd
import shutil
import os
import datetime as dt  # Importação adicionada para os logs de tempo

class ETL_relat_boleto_reem_vis:
    
    @staticmethod
    def ETL_boleto_reem_vis():
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] Iniciando o processo de ETL (Visualização de Boletos Reemitidos)...")

        # EXTRACT - Arquivo Base
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 1/6 - Abrindo arquivo base via xlwings e extraindo dados...")
        wb1_path = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_boletos_reemitidos\boletos_reemitidos.xlsx"
        wb1 = xw.Book(wb1_path)
        ws1 = wb1.sheets['boletos_reemitidos_pagos']
        intervalo = ws1.range('A1').expand()
        dados = intervalo.value

        # LOAD - Transferindo para arquivo destino
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 2/6 - Abrindo arquivo destino e populando a aba BASE...")
        wb2_path = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_boletos_reemitidos\Boletos Reemitidos Pagos.xlsx"
        wb2 = xw.Book(wb2_path)
        ws2 = wb2.sheets['BASE']
        ws2.clear_contents()
        ws2.range('A1').value = dados
        ws3 = wb2.sheets['Boletos Reemitidos Pagos']

        # TRANSFORM - Manipulação de Datas
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 3/6 - Calculando o range de datas (D-1 a D-4)...")
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

        # TRANSFORM - Agrupamentos e Cálculos
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 4/6 - Processando DataFrame e calculando os pagamentos por empresa...")
        base_values = ws2.range('A1').expand().value

        if base_values:
            df = pd.DataFrame(base_values[1:], columns=base_values[0])
        else:
            df = pd.DataFrame()

        listas_ponteiros_4 = df.loc[df['data_reemissao'] == day_offset_4, 'ponteiro'].tolist()
        listas_ponteiros_3 = df.loc[df['data_reemissao'] == day_offset_3, 'ponteiro'].tolist()
        listas_ponteiros_2 = df.loc[df['data_reemissao'] == day_offset_2, 'ponteiro'].tolist()
        listas_ponteiros_1 = df.loc[df['data_reemissao'] == day_offset_1, 'ponteiro'].tolist()

        def calcular_pagamentos_4dias_4empresas(df_local, listas_pont, empresas_local):
            resultados = {}
            for idx, ponteiros in enumerate(listas_pont, 1):
                for empresa in empresas_local:
                    soma = df_local.loc[
                        (df_local['ponteiro'].isin(ponteiros)) &
                        (df_local['empresa'] == empresa),
                        'valor_baixa'
                    ].sum()
                    resultados[(empresa, idx)] = soma
            return resultados

        listas_ponteiros = [listas_ponteiros_1, listas_ponteiros_2, listas_ponteiros_3, listas_ponteiros_4]
        empresas = ['Segtruck', 'Stcoop', 'Viavante', 'Tag']
        pagamentos = calcular_pagamentos_4dias_4empresas(df, listas_ponteiros, empresas)
        
        # LOAD - Preenchendo as métricas
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 5/6 - Inserindo os valores calculados na aba de visualização...")
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

        # LOAD FINAL - Salvando e movendo
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] 6/6 - Salvando planilhas, fechando Excel e copiando para o SharePoint...")
        wb1.save()
        wb1.close()
        wb2.save()
        wb2.close()
        
        save_sharepoint = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Pagamento de Reemissões\Boletos Reemitidos Pagos.xlsx"
        shutil.copy(wb2_path, save_sharepoint)

        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] Arquivo 'Boletos Reemitidos Pagos' salvo com sucesso em {save_sharepoint}")
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] Processo finalizado com SUCESSO!")

if __name__ == '__main__':
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] Script acionado. Preparando execução...")
    # Corrigido a chamada da classe abaixo:
    ETL_relat_boleto_reem_vis.ETL_boleto_reem_vis()