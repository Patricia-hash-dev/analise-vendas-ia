"""
Script principal de análise de vendas com apoio de IA.

Autora: Patricia Pacheco 💜 com seu ChatGPT Lumi
Versão: 1.0

Gera:
- processed_data/insight_resumo_vendas_por_pais.csv
- processed_data/top3_produtos_por_pais.csv
"""

import pandas as pd

# 💾 Carregar os três arquivos CSV originais
aliexpress = pd.read_csv("data/raw_data/Meganium_Sales_Data_-_AliExpress.csv")
etsy = pd.read_csv("data/raw_data/Meganium_Sales_Data_-_Etsy.csv")
shopee = pd.read_csv("data/raw_data/Meganium_Sales_Data_-_Shopee.csv")

# 🏷 Adicionar a coluna "Plataforma" em cada DataFrame
aliexpress["Plataforma"] = "AliExpress"
etsy["Plataforma"] = "Etsy"
shopee["Plataforma"] = "Shopee"

# 🔗 Unir os três DataFrames em um só
df_total = pd.concat([aliexpress, etsy, shopee], ignore_index=True)

# 🗓️ Converter data de nascimento e calcular idade
df_total["buyer_birth_date"] = pd.to_datetime(df_total["buyer_birth_date"], errors="coerce")
df_total["idade"] = 2025 - df_total["buyer_birth_date"].dt.year

# 🔍 Filtrar apenas as colunas relevantes para a análise
dados_filtrados = df_total[[
    "delivery_country", "product_sold", "Plataforma",
    "idade", "invoice_id", "total_price"
]]

# 📊 Agrupar por país e plataforma
grouped = dados_filtrados.groupby(['delivery_country', 'Plataforma']).agg(
    Total_Vendas=('total_price', 'sum'),
    Numero_Pedidos=('invoice_id', 'nunique')
).reset_index()

# 🌎 Calcular totais por país (todas as plataformas juntas)
total_por_pais = grouped.groupby('delivery_country').agg(
    Total_Pais=('Total_Vendas', 'sum'),
    Total_Pedidos=('Numero_Pedidos', 'sum')
).reset_index()

# 🔄 Juntar os totais com os dados agrupados
merged = pd.merge(grouped, total_por_pais, on='delivery_country')

# 💰 Calcular ticket médio e percentual de cada plataforma
merged['Ticket_Medio'] = merged['Total_Vendas'] / merged['Numero_Pedidos']
merged['Percentual_Plataforma'] = (merged['Total_Vendas'] / merged['Total_Pais']) * 100

# 👑 Identificar a plataforma com maior venda por país
idx_max = merged.groupby('delivery_country')['Total_Vendas'].idxmax()
plataforma_top = merged.loc[idx_max, ['delivery_country', 'Plataforma']]
plataforma_top = plataforma_top.rename(columns={'Plataforma': 'Plataforma_Mais_Vendas'})

# 🧩 Unir ao DataFrame final
tabela_final = pd.merge(merged, plataforma_top, on='delivery_country')

# 📋 Selecionar colunas para exibir no relatório final
tabela_resumo = tabela_final[[
    'delivery_country', 'Total_Pais', 'Total_Pedidos', 'Ticket_Medio',
    'Plataforma', 'Total_Vendas', 'Percentual_Plataforma', 'Plataforma_Mais_Vendas'
]]

# 🧼 Organizar os dados
tabela_resumo = tabela_resumo.sort_values(by='Total_Pais', ascending=False).round(2)

# 💾 Exportar a Tabela Resumo para CSV
tabela_resumo.to_csv('data/processed_data/insight_resumo_vendas_por_pais.csv', index=False, encoding='utf-8-sig')

# 🌟 Mostrar os 10 primeiros países (só para conferência)
print("\nTop 10 países por volume de vendas:\n")
print(tabela_resumo.head(10))


# ----------------------------------------------------
# 🎯 Análise dos 3 Produtos Mais Vendidos por País
# ----------------------------------------------------

# 🧮 Agrupar por país, produto e plataforma
vendas_por_pais_produto = dados_filtrados.groupby(
    ["delivery_country", "product_sold", "Plataforma"]
).agg(
    total_vendas=("invoice_id", "nunique"),
    idade_media=("idade", "mean")
).reset_index()

# 🥇 Selecionar os 3 mais vendidos por país
top3_produtos_pais = vendas_por_pais_produto.sort_values(
    ["delivery_country", "total_vendas"], ascending=[True, False]
).groupby("delivery_country").head(3)

# 🎨 Arredondar idade média
top3_produtos_pais["idade_media"] = top3_produtos_pais["idade_media"].round(1)

# 💾 Exportar para CSV
top3_produtos_pais.to_csv("data/processed_data/top3_produtos_por_pais.csv", index=False, encoding="utf-8-sig")

# 👀 Exibir os 15 primeiros registros
print("\nTop 3 produtos por país:\n")
print(top3_produtos_pais.head(15))