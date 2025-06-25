import pandas as pd
import awswrangler as awr
import openpyxl

query_path = r"C:\Users\raphael.almeida\Documents\Projetos\Boletos Reemitidos\sql\reissued_invoices.sql"
with open (query_path, 'r') as file:
    query = file.read()

df = awr.athena.read_sql_query(query, database='silver')

save_path = r"C:\Users\raphael.almeida\Documents\Projetos\Boletos Reemitidos\boletos_reemitidos.xlsx"
df.to_excel(save_path, engine='openpyxl', index=False, sheet_name='boletos_reemitidos')