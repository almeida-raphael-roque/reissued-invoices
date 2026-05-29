> **⚠️ For confidentiality and information security reasons, this repository has had sensitive data, names, and details removed or changed. All confidential or strategic information has been intentionally suppressed or obfuscated. This material is for demonstration purposes only. ⚠️**

# Reissued Invoices Payment Report

Automated ETL pipeline that consolidates reissued bank slips (*boletos*) and their payment status across multiple business units, producing daily operational reports for finance and collections teams.

## Project Overview

**Purpose:** Generate a daily view of invoices reissued in the last four days, flag whether each was paid, and summarize payment amounts by company and reissue date.

**Business problem:** Reissued invoices are hard to track manually across several ERP schemas. Teams need a reliable, repeatable report to monitor collections performance on reissued slips and support cash-flow visibility.

**Outcome:** A two-stage pipeline—data extraction and consolidation, then dashboard refresh—replaces manual spreadsheet work with scheduled, auditable outputs (Excel base file + executive summary workbook).

## Technologies Used

| Area | Stack |
|------|--------|
| Language | **Python** |
| Query layer | **SQL** (Athena / Presto) on AWS data lake (`silver` + per-company schemas) |
| Data processing | **Pandas** |
| Cloud | **AWS Athena** via **AWS Data Wrangler** (`awswrangler`) |
| Reporting | **Microsoft Excel** (`openpyxl`, **xlwings**) |
| Exploration / prototyping | **Jupyter Notebook** |

## Key Libraries

| Library | Role in this project |
|---------|----------------------|
| **awswrangler** | Runs SQL files against Athena and returns results as DataFrames |
| **pandas** | Merges reissued vs. paid datasets, typing, null handling, aggregations |
| **openpyxl** | Writes the consolidated base Excel export |
| **xlwings** | Reads/writes Excel templates and refreshes the visualization workbook |
| **datetime** | Timestamps for logging and dated cache files |

## Data Processing & SQL Logic

Two SQL assets drive extraction (`sql/`):

**Reissued invoices** (`reissued_invoices.sql`)
- `UNION ALL` across four company schemas with identical column layout
- Joins: customer catalog, financial application, invoice/insurance chain, seller, unit
- Filters: active history (`historico = 1`), non-consolidated pointer, receivable type (`crc_cpg = 'R'`), reissue date in last 4 days, specific application codes
- Computes net title value: `valor_titulo_movimento + acrescimo - desconto`

**Paid invoices** (`paid_invoices.sql`)
- Same multi-schema union pattern
- Subquery `bx`: aggregates payment date (`MAX`) and amount (`SUM`) per `ponteiro` from settlement movements that enter cash flow
- Filters: payments in last 4 days, commission-bearing applications

**Python business rules** (`ETL_boleto_reem.py`)
- Deduplicate by `ponteiro`; tag rows as `REEMITIDO` or `PAGO`
- Keep paid rows only when they match a reissued pointer; union with full reissued set
- Standardize schema, sort by pointer/reissue date, fill nulls for downstream Excel/BI compatibility
- Export to project folder, dated `cache/`, and shared drive path

## Analysis & Notebook Logic

| Stage | Base ETL (`ETL_boleto_reem.py` / `ETL.ipynb`) | Visualization (`ETL_boleto_reem_vis.py` / `load_viz.ipynb`) |
|-------|-----------------------------------------------|-------------------------------------------------------------|
| **Extract** | Athena queries from SQL files | Reads base Excel via xlwings |
| **Transform** | Merge reissued + paid; status & null treatment | Rolling window D-1…D-4 by `data_reemissao`; sum `valor_baixa` by company |
| **Analyze** | Row-level reconciliation (paid vs. open reissues) | Matrix: 4 days × 4 companies |
| **Report** | `boletos_reemitidos.xlsx` + daily snapshot | Populates template `Boletos Reemitidos Pagos.xlsx` |

Notebooks mirror the production scripts and were used to prototype and validate logic before scripting.

## Project Structure

```
sql/                    # Athena extraction queries
python/
  ETL_boleto_reem.py    # Stage 1: extract, merge, export base file
  ETL_boleto_reem_vis.py# Stage 2: dashboard metrics & template refresh
  *.ipynb               # Prototyping / ad-hoc validation
cache/                  # Dated Excel snapshots (gitignored)
```

## Key Highlights

- **Multi-tenant data model:** Single pipeline unifies four ERP schemas via SQL `UNION` and consistent enrichment joins
- **Reconciliation logic:** Links reissued pointers to settlement movements, exposing paid vs. outstanding reissues in one dataset
- **Operational automation:** End-to-end run from lake query to SharePoint-ready workbook without manual copy-paste
- **Rolling 4-day window:** SQL and Python aligned on the same business calendar for daily monitoring
- **Layered reporting:** Raw detail export plus executive summary with company-level payment totals by reissue day
