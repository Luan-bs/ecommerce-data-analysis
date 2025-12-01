# Importar bibliotecas necessárias
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import os
from datetime import datetime
import time
import csv

#medir tempo
inicio = time.time()

warnings.filterwarnings('ignore')

# ==========================================
#     CONFIGURAÇÃO DA CONEXÃO
# ==========================================

BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"
GOLD_PATH = f"{DATA_PATH}/gold/"

DB_TYPE = 'postgresql'
DB_HOST = 'postgres_dw'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_PORT = '5432'
DB_NAME = 'ecommerce_dw'

if DB_TYPE == 'postgresql':
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
else:
    raise ValueError("DB_TYPE deve ser 'postgresql'.")

print(f"✓ Configuração definida para: {DB_TYPE.upper()}")
print(f"✓ String de conexão: {connection_string.replace(DB_PASSWORD, '***')}")

# ==========================================
#     TESTE DE CONEXÃO
# ==========================================

try:
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Conexão estabelecida com sucesso!")
except Exception as e:
    print("✗ Erro ao conectar ao banco:")
    print(str(e))
    raise


# ==========================================
#     CARREGAR DADOS DA CAMADA GOLD (LOAD)
# ==========================================

tables_to_load = {
    'dim_country': f'{GOLD_PATH}dim_country.parquet',
    'dim_date': f'{GOLD_PATH}dim_date.parquet',
    'dim_customer': f'{GOLD_PATH}dim_customer.parquet',
    'dim_product': f'{GOLD_PATH}dim_product.parquet',
    'fact_all': f'{GOLD_PATH}fact_all.parquet',
    'metrics': f'{GOLD_PATH}metrics.parquet'
}

# Ordem correta: dimensões primeiro, fatos depois
load_order = ['dim_country', 'dim_date', 'dim_customer', 'dim_product', 'fact_all', 'metrics']

try:
    with engine.connect() as conn:
        for table_name in load_order:
            file_path = tables_to_load.get(table_name)

            if not os.path.exists(file_path):
                print(f"AVISO: Arquivo {file_path} não encontrado. Pulando {table_name}.")
                continue

            print(f"Carregando {table_name} de {file_path}...")
            df = pd.read_parquet(file_path)
            df.columns = df.columns.str.lower()

            if table_name == 'metrics':
                df_metrics = pd.DataFrame({
                    'metric_name': df['metrics'],
                    'metric_value': df['value']
                })
                df_metrics.to_sql(table_name, conn, if_exists='append', index=False)
            else:
                df.to_sql(table_name, conn, if_exists='append', index=False)

            print(f"✓ {table_name} carregada com sucesso ({len(df)} linhas).")

        print("\n" + "="*60)
        print("✓✓✓ TODOS OS DADOS CARREGADOS NO BANCO DE DADOS! ✓✓✓")
        print("="*60)

except Exception as e:
    print(f"✗ Erro ao carregar dados: {str(e)}")
    raise


# ==========================================
#     LOG DO PIPELINE
# ==========================================

fim = time.time()
duracao = fim - inicio

fact_all_path = tables_to_load['fact_all']
registros = pd.read_parquet(fact_all_path).shape[0]

log_file = f"{BASE_DIR}/logs_pipeline.csv"

log = [
    datetime.now(),
    "load",
    "sucesso",
    round(duracao, 2),
    registros
]

with open(log_file, "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(log)

print("✓ Log registrado em logs_pipeline.csv")
