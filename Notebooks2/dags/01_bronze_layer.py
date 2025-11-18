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

import pandas as pd
import kagglehub
import os 
from datetime import datetime
import time
import csv

# medir tempo
inicio = time.time()

# Definindo o diretório base para os dados
BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"

# Criar estrutura de pastas 
os.makedirs(f"{DATA_PATH}/bronze", exist_ok=True)
os.makedirs(f"{DATA_PATH}/silver", exist_ok=True)
os.makedirs(f"{DATA_PATH}/gold", exist_ok=True)

# Carregar dados da fonte original 
path = kagglehub.dataset_download("carrie1/ecommerce-data")
csv_path = os.path.join(path, "data.csv")
df_raw = pd.read_csv(csv_path, encoding="ISO-8859-1")

print(f"Dados carregados {df_raw.shape[0]} linhas, {df_raw.shape[1]} colunas")

# Adicionar informações adicionais
df_raw["data_ingestao"] = datetime.now()
df_raw["fonte_arquivos"] = "carrie1/ecommerce-data"

# Salvando camada bronze
df_raw.to_parquet(f"{DATA_PATH}/bronze/dados_brutos.parquet", index=False)
print("Dados salvos na camada bronze")

# =============================
# LOG DO PIPELINE (bronze)
# =============================

fim = time.time()
duracao = fim - inicio
registros = df_raw.shape[0]

log_file = f"{BASE_DIR}/logs_pipeline.csv"

log = [
    datetime.now(),   # data_execucao
    "bronze",         # camada
    "sucesso",        # status
    round(duracao, 2),
    registros
]

# salva o log
with open(log_file, "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(log)

print("Log registrado no arquivo logs_pipeline.csv")
