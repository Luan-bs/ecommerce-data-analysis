#!/usr/bin/env python
# coding: utf-8

# # Camada Bronze - Ingestão de Dados Brutos
# 
# Este notebook é responsável pela **primeira camada do pipeline de dados** (Bronze Layer), onde realizamos a ingestão dos dados brutos diretamente da fonte original.
# 
# ## Objetivos:
# - Baixar o dataset de e-commerce do Kaggle
# - Criar a estrutura de pastas do projeto (bronze, silver, gold)
# - Adicionar metadados de ingestão (data e fonte)
# - Salvar os dados brutos em formato Parquet para otimização de armazenamento
# 
# ## Sobre os dados:
# Dataset de transações de e-commerce contendo informações sobre vendas, produtos, clientes e países.

# In[5]:


#Execute instalação das bibliotecas 
# %pip install kagglehub pandas pyarrow

import pandas as pd
import kagglehub
import os 
from datetime import datetime

# Definindo o diretório base para os dados
# Este é o caminho onde o Airflow espera encontrar os dados
BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"

# Criar estrutura de pastas 
os.makedirs(f"{DATA_PATH}/bronze", exist_ok=True)
os.makedirs(f"{DATA_PATH}/silver", exist_ok=True)
os.makedirs(f"{DATA_PATH}/gold", exist_ok=True)

# Carregar dados da fonte original 
path = kagglehub.dataset_download("carrie1/ecommerce-data")
# O arquivo CSV está dentro da pasta baixada
csv_path = os.path.join(path, "data.csv")
df_raw = pd.read_csv(csv_path, encoding = "ISO-8859-1")

print(f"Dados carregados {df_raw.shape[0]} linhas, {df_raw.shape[1]} colunas")

 # Adicionar informações de quando os dados foram carregados
df_raw["data_ingestao"] = datetime.now()
df_raw["fonte_arquivos"] = "carrie1/ecommerce-data" 

# Salvando camada bronze em formato Parquet
df_raw.to_parquet(f"{DATA_PATH}/bronze/dados_brutos.parquet", index=False)
print("Dados salvos na camada bronze")

# visualizar primeiras linhas
# df_raw.head() # Comentado para evitar erro de display em ambiente não-notebook


# ## Resultado
# 
# Os dados foram carregados com sucesso e salvos na camada bronze. As primeiras linhas mostram:
# - **InvoiceNo**: Número da fatura
# - **StockCode**: Código do produto
# - **Description**: Descrição do produto
# - **Quantity**: Quantidade vendida
# - **InvoiceDate**: Data e hora da transação
# - **UnitPrice**: Preço unitário
# - **CustomerID**: ID do cliente
# - **Country**: País da transação
# - **data_ingestao**: Timestamp de quando os dados foram carregados
# - **fonte_arquivos**: Identificação da fonte original dos dados
