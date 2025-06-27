SELECT 
tm.codigo_cadastro,
tm.ponteiro,
tm.numero_documento,
tm.numero_boleto,
tm.nosso_numero,
tm.sequencia_documento,
a.descricao AS aplicacao_financeira,
bx.valor_baixa, 
CAST(CAST(tm.data_emissao AS TIMESTAMP) AS DATE) AS data_emissao,
CAST(bx.data_baixa AS date) AS data_baixa,
CAST(tm.data_vencimento AS date) AS data_vencimento,
irs.id AS conjunto,
irs.parent AS matricula,
cat.fantasia AS unidade,
'Segtruck' AS empresa,
cata.nome AS associado,
COALESCE(v.descricao,'OUTROS') AS vendedor,
ins.description AS status_conjunto,



FROM silver.titulo_movimento tm
LEFT JOIN silver.titulo t ON t.ponteiro = tm.ponteiro
INNER JOIN silver.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento 
LEFT JOIN silver.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento
LEFT JOIN silver.invoice i ON i.id = ii.parent
LEFT JOIN silver.insurance_reg_set irs ON irs.id = i.id_set
LEFT JOIN silver.insurance_reg_set_coverage irsc ON irsc.parent = irs.id--joins principais
LEFT JOIN insurance_status ins ON ins.id = irs.id_status

LEFT JOIN silver.representante r ON r.codigo = i.id_unity
LEFT JOIN silver.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf--unidade cat

LEFT JOIN silver.cliente cli ON cli.codigo = i.customer_id
LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = cli.cnpj_cpf--cliente cata
INNER JOIN silver.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo 
AND a.codigo_empresa = tm.codigo_empresa
AND a.codigo IN (166,1)
LEFT JOIN vendedor v ON v.codigo = ir.id_consultant

INNER JOIN (
    SELECT 
    MAX(data_lancamento) AS data_baixa,
    SUM(valor_baixa) AS valor_baixa,
    tb.ponteiro
    FROM silver.titulo_movimento tb
    INNER JOIN silver.situacao_documento stb ON stb.codigo = tb.codigo_situacao_documento
    WHERE tb.historico NOT IN (1,5)
    AND (tb.ponteiro_consolidado IS NULL OR tb.ponteiro_consolidado = 0 )
    AND stb.entra_fluxo_caixa ='S'
    AND tb.crc_cpg = 'R'
    GROUP BY tb.ponteiro 
) bx ON bx.ponteiro = tm.ponteiro and a.taxa_comissao > 0 AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado = 0)

