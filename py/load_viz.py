import xlwings as xw
import pandas as pd

wb1_path = r"C:\Users\raphael.almeida\Documents\Projetos\Boletos Reemitidos\boletos_reemitidos.xlsx"
wb1 = xw.Book(wb1_path)
ws1 = wb1.sheets['boletos_reemitidos_pagos']
                 
intervalo = ws1.range('A1').expand()

dados = intervalo.value

wb2_path = r"C:\Users\raphael.almeida\Documents\Projetos\Boletos Reemitidos\Boletos Reemitidos Pagos.xlsx"
wb2 = xw.Book(wb2_path)
ws2 = wb2.sheets['BASE']

ws2.range('A1').value = dados


ws3 = wb2.sheets['Boletos Reemitidos Pagos']

#atualizando datas

today = pd.Timestamp.today().date()
day_offset_1 = today - pd.Timedelta(days=1)
day_offset_2 = today - pd.Timedelta(days=2)
day_offset_3 = today - pd.Timedelta(days=3)
day_offset_4 = today - pd.Timedelta(days=3)


ws3.range('G4').value = day_offset_1
ws3.range('F4').value = day_offset_2
ws3.range('E4').value = day_offset_3
ws3.range('D4').value = day_offset_4

#atualizando células de pagamento
base_values = ws2.range('A1').expand().value

# Ajuste: usar a primeira linha como cabeçalho
if base_values:
    df = pd.DataFrame(base_values[1:], columns=base_values[0])
else:
    df = pd.DataFrame()



def calcular_pagamentos_por_empresa_e_dia(df, empresas, dias):
    """
    Calcula o total de pagamentos por empresa e por dia de reemissão.

    Args:
        df (pd.DataFrame): DataFrame com os dados.
        empresas (list): Lista de nomes das empresas.
        dias (list): Lista de datas (datetime.date) para considerar.

    Returns:
        dict: {(empresa, dia): soma_pagamentos}
    """
    resultados = {}
    for dia in dias:
        lista_ponteiros = df.loc[df['data_reemissao'] == dia, 'ponteiro'].tolist()
        for empresa in empresas:
            soma = df.loc[
                (df['ponteiro'].isin(lista_ponteiros)) &
                (df['empresa'] == empresa),
                'valor_baixa'
            ].sum()
            resultados[(empresa, dia)] = soma
    return resultados

lista_empresas = ['Segtruck', 'Stcoop', 'Viavante']
dias_reemissao = [day_offset_1, day_offset_2, day_offset_3, day_offset_4]

pagamentos_por_empresa_e_dia = calcular_pagamentos_por_empresa_e_dia(df, lista_empresas, dias_reemissao)

# Mapeamento de colunas e linhas para cada empresa e dia
colunas = ['F', 'H', 'J', 'L']  # F: offset 4, H: offset 3, J: offset 2, L: offset 1
linhas = {'Segtruck': 6, 'Stcoop': 9, 'Viavante': 12}

for idx_dia, dia in enumerate(dias_reemissao):
    for empresa in lista_empresas:
        valor = pagamentos_por_empresa_e_dia.get((empresa, dia), 0)
        coluna = colunas[idx_dia]
        linha = linhas[empresa]
        ws3.range(f'{coluna}{linha}').value = valor

wb2.save()  
wb2.close()

wb1.close()

