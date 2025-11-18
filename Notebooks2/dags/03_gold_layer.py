#!/usr/bin/env python
# coding: utf-8

# # Camada Gold - Modelagem Dimensional e Métricas
# 
# Este notebook implementa a **terceira camada do pipeline** (Gold Layer), onde criamos um modelo dimensional (star schema) e calculamos métricas de negócio.
# 
# ## Objetivos:
# - Criar tabelas de dimensão (clientes, produtos, países, datas)
# - Criar tabelas fato separadas (vendas, tarifas, cancelamentos)
# - Calcular métricas agregadas e análises avançadas (RFM, produtos mais vendidos)
# - Preparar dados otimizados para análise e consultas
# 
# ## Modelo:
# Implementamos um modelo dimensional estrela (star schema) com dimensões desnormalizadas para melhor performance analítica.

# # 1. Separando dados em Fato e Dimensão 
# 


# In[1]:


import pandas as pd
import os
from datetime import datetime
import time
import csv

# medir tempo
inicio = time.time()

# Definindo o diretório base para os dados (deve ser o mesmo dos scripts anteriores)
BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"

# Carregar dados da camada silver
df_clean = pd.read_parquet(f'{DATA_PATH}/silver/dados_limpos.parquet')

# Linhas stockcode de tarifas
stockcode_fees = ['C2', 'DOT', 'POST','AMAZONFEE']
df_clean['total_value'] = df_clean['Quantity'] * df_clean['UnitPrice']

# =========================================
#               CRIAÇÃO DE TABELAS
# =========================================

# --------- Dimensao Country -----------
dim_country = (
    df_clean[['Country']]
    .drop_duplicates()
    .copy()
)
dim_country = dim_country.reset_index(drop=True)
dim_country['CountryID'] = dim_country.index + 1

# --------- Dimensao Date -----------
dim_date = (
    df_clean[['InvoiceDate']]
    .drop_duplicates(subset=['InvoiceDate'])
    .copy()
)
dim_date = dim_date.reset_index(drop=True)
dim_date['InvoiceDate'] = pd.to_datetime(dim_date['InvoiceDate'])
dim_date['DateID'] = dim_date.index + 1
dim_date['Year'] = dim_date['InvoiceDate'].dt.year
dim_date['Month'] = dim_date['InvoiceDate'].dt.month
dim_date['Day'] = dim_date['InvoiceDate'].dt.day
dim_date['Weekday'] = dim_date['InvoiceDate'].dt.day_name()
dim_date['Hour'] = dim_date['InvoiceDate'].dt.hour

# Mapeamentos para substituição
country_map = dict(zip(dim_country['Country'], dim_country['CountryID']))
date_map = dict(zip(dim_date['InvoiceDate'], dim_date['DateID']))

# --------- Fato Vendas -----------
fact_sales = df_clean[
    (~df_clean['InvoiceNo'].astype(str).str.startswith(('C', 'A'))) &
    (df_clean['Quantity'] > 0) &
    (df_clean['UnitPrice'] > 0) &
    (~df_clean['StockCode'].isin(stockcode_fees))
].copy()

# Converter CustomerID para string
fact_sales['CustomerID'] = fact_sales['CustomerID'].fillna('Unknown').astype(str)
fact_sales['CountryID'] = fact_sales['Country'].map(country_map)
fact_sales['DateID'] = pd.to_datetime(fact_sales['InvoiceDate']).map(date_map)

fact_sales = fact_sales[[
    'InvoiceNo', 'StockCode', 'CustomerID', 'DateID',
    'Quantity', 'UnitPrice', 'total_value', 'CountryID'
]]

# --------- Fato Tarifas -----------
fact_fees = df_clean[
    (df_clean['StockCode'].isin(stockcode_fees)) &
    (~df_clean['InvoiceNo'].astype(str).str.startswith(('C', 'A')))].copy()
fact_fees['CustomerID'] = fact_fees['CustomerID'].astype(str)
fact_fees['CountryID'] = fact_fees['Country'].map(country_map)
fact_fees['DateID'] = pd.to_datetime(fact_fees['InvoiceDate']).map(date_map)
fact_fees = fact_fees[[
    'InvoiceNo', 'StockCode', 'CustomerID', 'DateID',
    'Quantity', 'UnitPrice', 'total_value', 'CountryID'
]]

# --------- Fato Cancelamentos -----------
fact_cancellations = df_clean[
    (df_clean['InvoiceNo'].astype(str).str.startswith(('C', 'A'))) &
    (~df_clean['StockCode'].isin(['C2', 'DOT', 'POST']))
].copy()
fact_cancellations['CustomerID'] = fact_cancellations['CustomerID'].astype(str)
fact_cancellations['CountryID'] = fact_cancellations['Country'].map(country_map)
fact_cancellations['DateID'] = pd.to_datetime(fact_cancellations['InvoiceDate']).map(date_map)
fact_cancellations = fact_cancellations[[
    'InvoiceNo', 'StockCode', 'CustomerID', 'DateID',
    'Quantity', 'UnitPrice', 'total_value', 'CountryID'
]]

# =========================================
#               TABELAS DIMENSAO
# =========================================

# --------- Dim Clientes -----------
all_customers = pd.concat([
    fact_sales[['CustomerID', 'CountryID']],
    fact_fees[['CustomerID', 'CountryID']],
    fact_cancellations[['CustomerID', 'CountryID']]
], ignore_index=True)

dim_customer = (
    all_customers
    .drop_duplicates(subset=['CustomerID'])
    .copy()
)
dim_customer = dim_customer.reset_index(drop=True)

# Garante existencia do cliente 'Unknown'
if 'Unknown' not in dim_customer['CustomerID'].values:
    # A lógica original para encontrar 'Desconhecido' em dim_country foi removida
    # para simplificar, pois 'Unknown' não tem um país definido.
    dim_customer.loc[len(dim_customer)] = ['Unknown', None] 

# --------- Dim Produtos -----------
dim_product = (
    df_clean[['StockCode', 'Description']]
    .drop_duplicates(subset=['StockCode'])
    .rename(columns={'Description': 'ProductDescription'})
    .copy()
)

# --------- Dim Date -----------
# Já criada acima como dim_date, apenas reordenar colunas
dim_date = dim_date[[
    'DateID', 'InvoiceDate', 'Year', 'Month', 'Day', 'Weekday', 'Hour'
]]

# --------- Dim Country -----------
dim_country = dim_country[['CountryID', 'Country']]


# ### Criação das Tabelas Dimensionais e Fato
# 
# Nesta seção, transformamos os dados limpos em um modelo dimensional:
# 
# **Dimensões criadas:**
# - **dim_country**: Países únicos com IDs
# - **dim_date**: Datas com componentes (ano, mês, dia, dia da semana, hora)
# - **dim_customer**: Clientes únicos com referência ao país
# - **dim_product**: Produtos únicos com descrições
# 
# **Tabelas Fato criadas:**
# - **fact_sales**: Vendas normais (excluindo cancelamentos e tarifas)
# - **fact_fees**: Tarifas cobradas (POST, AMAZONFEE, etc)
# - **fact_cancellations**: Cancelamentos e ajustes
# 
# Cada tabela fato contém chaves estrangeiras para as dimensões e medidas numéricas (Quantity, UnitPrice, total_value).

# In[2]:


# Classificando vendas/tarifas/cancelamentos
fact_sales['TransactionType'] = 'Sale'
fact_fees['TransactionType'] = 'Fee'
fact_cancellations['TransactionType'] = 'Cancellation'

# Unindo tabelas fatos em uma só
fact_all = pd.concat(
    [fact_sales, fact_fees, fact_cancellations],
    ignore_index=True
)


# ### Consolidação das Tabelas Fato
# 
# Unificamos as três tabelas fato (vendas, tarifas e cancelamentos) em uma única tabela `fact_all` com uma coluna adicional `TransactionType` que identifica o tipo de transação. Isso facilita análises consolidadas mantendo a separação lógica dos dados.

# # 2. Agregando dados

# In[3]:


# =========================================
#            MÉTRICAS GOLD
# =========================================
# ----------- Análise RFM -----------

# Junte fact_sales com dim_date para obter a data real
fact_sales = fact_sales.merge(
    dim_date[['DateID', 'InvoiceDate']],
    on='DateID',
    how='left'
)

# Garanta que InvoiceDate está em datetime
fact_sales['InvoiceDate'] = pd.to_datetime(fact_sales['InvoiceDate'])

# Última data de compra
latest_date = fact_sales['InvoiceDate'].max()

# RFM com InvoiceDate
rfm = (
    fact_sales.groupby('CustomerID')
    .agg(
        Recency=('InvoiceDate', lambda x: (latest_date - x.max()).days),
        Frequency=('InvoiceNo', 'nunique'),
        Monetary=('total_value', 'sum')
    )
    .reset_index()
)

# ----------- Análise de produtos ----------- 

# Mais vendido por quantidade
most_quantity_product = (
    fact_sales.groupby(['CustomerID', 'StockCode'])
    .agg(UnitsSold=('Quantity', 'sum'))
    .reset_index()
    .sort_values(['CustomerID', 'UnitsSold'], ascending=[True, False])
    .drop_duplicates('CustomerID')
)

# Mais vendido por transação
most_transaction_product = (
    fact_sales.groupby(['CustomerID', 'StockCode'])
    .agg(Transactions=('InvoiceNo', 'nunique'))
    .reset_index()
    .sort_values(['CustomerID', 'Transactions'], ascending=[True, False])
    .drop_duplicates('CustomerID')
)

# Combinando resultados
most_purchased_products = most_quantity_product.merge(
    most_transaction_product,
    on=['CustomerID', 'StockCode']
)

# ----------- Metricas gerais -----------

# Vendas bruta
gross_sales = fact_sales['total_value'].sum()

# Vendas liquidas
net_sales = gross_sales + fact_cancellations['total_value'].sum() - fact_fees['total_value'].sum()

# Total de pedidos
metric_total_orders = fact_sales['InvoiceNo'].nunique()

# Clientes distintos
distinct_customers = dim_customer['CustomerID'].nunique()

# Produtos distintos
distinct_products = dim_product['StockCode'].nunique()

# Unidades vendidas
product_units_sold = fact_sales['Quantity'].sum()


# ### Cálculo de Métricas de Negócio
# 
# Criamos várias análises agregadas importantes para o negócio:
# 
# **Análise RFM (Recency, Frequency, Monetary):**
# - **Recency**: Há quantos dias o cliente fez sua última compra
# - **Frequency**: Quantas transações o cliente realizou
# - **Monetary**: Quanto o cliente gastou no total
# 
# **Análise de Produtos Mais Comprados:**
# - Produto mais comprado por quantidade
# - Produto mais comprado por número de transações
# 
# **Métricas Gerais:**
# - Vendas brutas e líquidas
# - Total de pedidos
# - Clientes e produtos distintos
# - Unidades vendidas

# In[4]:


# ------- visualizar metricas ------- 
import pandas as pd

# Exibir RFM e produtos mais comprados
# display(rfm) # Comentado para evitar erro de display em ambiente não-notebook
# display(most_purchased_products) # Comentado para evitar erro de display em ambiente não-notebook

# Exibir métricas gerais em tabela
metrics = pd.DataFrame({
    'Metrics': [
        'Gross Sales',
        'Net Sales',
        'Total Orders',
        'Distinct Customers',
        'Distinct Products',
        'Product Units Sold'
    ],
    'Value': [
        gross_sales,
        net_sales,
        metric_total_orders,
        distinct_customers,
        distinct_products,
        product_units_sold
    ]
})
# display(metrics) # Comentado para evitar erro de display em ambiente não-notebook


# ### Visualização das Métricas
# 
# Exibimos as métricas calculadas em formato tabular para validação e análise preliminar.

# # 3. Carregando dados na camada gold

# In[5]:
dim_country = dim_country.drop(columns=['CountryID'])
fact_all = fact_all.drop(columns=['CountryID'])
dim_country.columns = dim_country.columns.str.lower()
dim_date.columns = dim_date.columns.str.lower()
dim_customer.columns = dim_customer.columns.str.lower()
dim_product.columns = dim_product.columns.str.lower()
fact_all.columns = fact_all.columns.str.lower()
rfm.columns = rfm.columns.str.lower()
most_purchased_products.columns = most_purchased_products.columns.str.lower()
metrics.columns = metrics.columns.str.lower()

# Salvar tabelas finais (camada Gold)
gold_path = f'{DATA_PATH}/gold/'

dim_country.to_parquet(f'{gold_path}dim_country.parquet', index=False)
dim_date.to_parquet(f'{gold_path}dim_date.parquet', index=False)
dim_customer.to_parquet(f'{gold_path}dim_customer.parquet', index=False)
dim_product.to_parquet(f'{gold_path}dim_product.parquet', index=False)
fact_all.to_parquet(f'{gold_path}fact_all.parquet', index=False)
rfm.to_parquet(f'{gold_path}rfm.parquet', index=False)
most_purchased_products.to_parquet(f'{gold_path}most_purchased_products.parquet', index=False)
metrics.to_parquet(f'{gold_path}metrics.parquet', index=False)

print("Tabelas salvas na camada Gold.")

# =============================
# LOG DO PIPELINE (gold)
# =============================

fim = time.time()
duracao = fim - inicio
registros = df_clean.shape[0]

log_file = f"{BASE_DIR}/logs_pipeline.csv"

log = [
    datetime.now(),   # data_execucao
    "gold",         # camada
    "sucesso",        # status
    round(duracao, 2),
    registros
]

# salva o log
with open(log_file, "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(log)

print("Log registrado no arquivo logs_pipeline.csv")
# ### Salvamento na Camada Gold
# 
# Todas as tabelas dimensionais, fatos e métricas agregadas são salvas em formato Parquet na camada Gold. Estes arquivos estão prontos para serem carregados em um banco de dados ou ferramenta de visualização.

# In[6]:


# display(fact_sales['total_value'].sum()) # Comentado para evitar erro de display em ambiente não-notebook


# ### Validação
# 
# Verificação do total de vendas calculado para garantir a integridade dos dados.
