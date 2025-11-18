#!/usr/bin/env python
# coding: utf-8

# # Evolução do Banco de Dados - PostgreSQL/MySQL
# 
# 
# ## Objetivos:
# - **Etapa 1**: Configurar conexão com PostgreSQL ou MySQL
# - **Etapa 2**: Criar schema do banco de dados em SQL
# - Implementar estrutura de tabelas dimensionais e fatos
# - Criar índices e constraints para otimização
# 
# 
# # Etapa 1: Configuração do Banco de Dados
# 
# Nesta etapa, vamos configurar a conexão com o banco de dados.
# 
# ### Opções disponíveis:
# - **PostgreSQL** (recomendado para análises e data warehouse)
# - **MySQL** (alternativa popular)
# 
# ### Instalação das bibliotecas necessárias:
# ```bash
# 
# In[ ]:
# 
# 
# pip install pandas mysql-connector-python sqlalchemy psycopg2-binary
# 
# 
# In[ ]:
# 
# 
# Importar bibliotecas necessárias
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import os

warnings.filterwarnings('ignore')

# Definindo o diretório base para os dados (deve ser o mesmo dos scripts anteriores)
BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"
GOLD_PATH = f"{DATA_PATH}/gold/"

print("✓ Bibliotecas importadas com sucesso")


# ### Configuração de Conexão
# 
# **Escolha uma das opções abaixo:**
# 
# #### Opção 1: PostgreSQL (Recomendado)
# ```python
# # Configurações PostgreSQL
# DB_TYPE = 'postgresql'
# DB_USER = 'seu_usuario'
# DB_PASSWORD = 'sua_senha'
# DB_HOST = 'localhost'
# DB_PORT = '5432'
# DB_NAME = 'ecommerce_dw'
# ```
# 
# #### Opção 2: MySQL
# ```python
# # Configurações MySQL
# DB_TYPE = 'mysql'
# DB_USER = 'seu_usuario'
# DB_PASSWORD = 'sua_senha'
# DB_HOST = 'localhost'
# DB_PORT = '3306'
# DB_NAME = 'ecommerce_dw'
# ```

# In[ ]:


# ==========================================
#     CONFIGURAÇÃO DA CONEXÃO
# ==========================================

# Escolha o tipo de banco de dados: 'postgresql' ou 'mysql'
# NOTA: Se estiver usando o docker-compose padrão do Airflow, você precisará de um serviço de banco de dados extra.
# O nome do host deve ser o nome do serviço Docker.
DB_TYPE = 'postgresql'  # Altere para 'mysql' se preferir
DB_HOST = 'postgres_dw' # Alterado para o nome do serviço Docker (se estiver usando o docker-compose corrigido)
DB_USER = 'postgres'      # Usuário do banco
DB_PASSWORD = 'postgres'  # Senha do banco
DB_PORT = '5432'          # Porta (5432 para PostgreSQL, 3306 para MySQL)
DB_NAME = 'ecommerce_dw'  # Nome do banco de dados

# Criar string de conexão
if DB_TYPE == 'postgresql':
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    driver = 'psycopg2'
elif DB_TYPE == 'mysql':
    connection_string = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    driver = 'mysql-connector-python'
else:
    raise ValueError("DB_TYPE deve ser 'postgresql' ou 'mysql'")

print(f"✓ Configuração definida para: {DB_TYPE.upper()}")
print(f"✓ String de conexão: {connection_string.replace(DB_PASSWORD, '***')}")


# In[ ]:


# ==========================================
#     TESTAR CONEXÃO COM O BANCO
# ==========================================

try:
    # Criar engine de conexão
    engine = create_engine(connection_string)

    # Testar conexão
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✓ Conexão estabelecida com sucesso!")
        print(f"✓ Banco de dados: {DB_NAME}")
        print(f"✓ Tipo: {DB_TYPE.upper()}")

except Exception as e:
    print(f"✗ Erro ao conectar ao banco de dados:")
    print(f"  {str(e)}")
    print("\n💡 Dicas:")
    print("  1. Verifique se o servidor do banco está rodando")
    print("  2. Confirme usuário e senha")
    print("  3. Certifique-se que o banco de dados existe")
    if DB_TYPE == 'postgresql':
        print("  4. Para PostgreSQL, crie o banco com: CREATE DATABASE ecommerce_dw;")
    else:
        print("  4. Para MySQL, crie o banco com: CREATE DATABASE ecommerce_dw;")
    # Levantar o erro para que o Airflow marque a tarefa como falha
    raise


# ---
# 
# ## Etapa 2: Criar Schema do Banco de Dados
# 
# Nesta etapa, vamos criar a estrutura completa do Data Warehouse em SQL, incluindo:
# 
# - **Tabelas Dimensionais** (dim_country, dim_customer, dim_date, dim_product)
# - **Tabela Fato** (fact_sales)
# - **Tabela de Métricas** (metrics)
# - **Índices** para otimização de consultas
# - **Constraints** (chaves primárias e estrangeiras)
# 
# ### Modelo Dimensional (Star Schema)
# 
# # ![image.png](attachment:image.png) # Comentado para evitar erro de display em ambiente não-notebook

# ### 2.1 Script SQL - Tabelas Dimensionais

# In[ ]:


# ==========================================
#     SQL: CRIAR TABELAS DIMENSIONAIS
# ==========================================

# Ajustado para usar SERIAL/INT para chaves primárias e CASCADE para DROP TABLE
sql_create_dimensions = """
DROP TABLE IF EXISTS dim_country CASCADE;
CREATE TABLE dim_country (
    CountryID SERIAL PRIMARY KEY,
    Country VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CountryID INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_country_customer FOREIGN KEY (CountryID) REFERENCES dim_country(CountryID) ON DELETE SET NULL
);

DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    DateID SERIAL PRIMARY KEY,
    InvoiceDate TIMESTAMP NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    Weekday VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    StockCode VARCHAR(50) PRIMARY KEY,
    productdescription VARCHAR(500),
    UnitPrice DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

print("✓ Script SQL de criação das dimensões preparado")
print(f"✓ Total de linhas SQL: {len(sql_create_dimensions.split(chr(10)))}")


# ### 2.2 Script SQL - Tabela Fato

# In[ ]:


# ==========================================
#     SQL: CRIAR TABELA FATO
# ==========================================

sql_create_fact = """
DROP TABLE IF EXISTS fact_all CASCADE;
CREATE TABLE fact_all (
    fact_id SERIAL PRIMARY KEY,
    InvoiceNo VARCHAR(50) NOT NULL,
    StockCode VARCHAR(50) NOT NULL,
    CustomerID VARCHAR(50),
    CountryID INT,
    DateID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10, 2) NOT NULL,
    total_value DECIMAL(12, 2) NOT NULL,
    TransactionType VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_product FOREIGN KEY (StockCode) REFERENCES dim_product(StockCode) ON DELETE CASCADE,
    CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID) ON DELETE SET NULL,
    CONSTRAINT fk_country FOREIGN KEY (CountryID) REFERENCES dim_country(CountryID) ON DELETE CASCADE,
    CONSTRAINT fk_date FOREIGN KEY (DateID) REFERENCES dim_date(DateID) ON DELETE CASCADE
);

CREATE INDEX idx_fact_invoice ON fact_all(InvoiceNo);
CREATE INDEX idx_fact_stock_code ON fact_all(StockCode);
CREATE INDEX idx_fact_customer ON fact_all(CustomerID);
CREATE INDEX idx_fact_country ON fact_all(CountryID);
CREATE INDEX idx_fact_date ON fact_all(DateID);
CREATE INDEX idx_fact_total_price ON fact_all(total_value);
CREATE INDEX idx_fact_invoice_date ON fact_all(InvoiceNo, DateID);
"""

print("✓ Script SQL de criação da tabela fato preparado")
print(f"✓ Total de linhas SQL: {len(sql_create_fact.split(chr(10)))}")


# ### 2.3 Script SQL - Tabela de Métricas

# In[ ]:


# ==========================================
#     SQL: CRIAR TABELA DE MÉTRICAS
# ==========================================

sql_create_metrics = """
DROP TABLE IF EXISTS metrics CASCADE;
CREATE TABLE metrics (
    metric_name VARCHAR(100) PRIMARY KEY,
    metric_value DECIMAL(20, 2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_name ON metrics(metric_name);
"""

print("✓ Script SQL de criação da tabela de métricas preparado")
print(f"✓ Total de linhas SQL: {len(sql_create_metrics.split(chr(10)))}")


# ### 2.4 Executar Scripts SQL - Criar Schema Completo

# In[ ]:


# ==========================================
#     EXECUTAR SCRIPTS SQL
# ==========================================

try:
    with engine.connect() as conn:
        print("Executando criação das tabelas dimensionais...")
        # Executar a criação das dimensões
        conn.execute(text(sql_create_dimensions))
        print("✓ Tabelas dimensionais criadas com sucesso!")

        print("\nExecutando criação da tabela fato...")
        # Executar a criação da tabela fato
        conn.execute(text(sql_create_fact))
        print("✓ Tabela fato criada com sucesso!")

        print("\nExecutando criação da tabela de métricas...")
        # Executar a criação da tabela de métricas
        conn.execute(text(sql_create_metrics))
        print("✓ Tabela de métricas criada com sucesso!")

        print("\n" + "="*60)
        print("✓✓✓ SCHEMA DO BANCO DE DADOS CRIADO COM SUCESSO! ✓✓✓")
        print("="*60)

except Exception as e:
    print(f"✗ Erro ao criar schema:")
    print(f"  {str(e)}")
    # Levantar o erro para que o Airflow marque a tarefa como falha
    raise


# ### 2.5 Carregar Dados da Camada Gold para o Banco de Dados

# In[ ]:


# ==========================================
#     CARREGAR DADOS DA CAMADA GOLD
# ==========================================

tables_to_load = {
    'dim_country': f'{GOLD_PATH}dim_country.parquet',
    'dim_date': f'{GOLD_PATH}dim_date.parquet',
    'dim_customer': f'{GOLD_PATH}dim_customer.parquet',
    'dim_product': f'{GOLD_PATH}dim_product.parquet',
    'fact_all': f'{GOLD_PATH}fact_all.parquet',
    'metrics': f'{GOLD_PATH}metrics.parquet'
}

# A ordem de carregamento é importante: Dimensões antes da Fato
load_order = ['dim_country', 'dim_date', 'dim_customer', 'dim_product', 'fact_all', 'metrics']

try:
    with engine.connect() as conn:
        for table_name in load_order:
            file_path = tables_to_load.get(table_name)
            if not os.path.exists(file_path):
                print(f"AVISO: Arquivo {file_path} não encontrado. Pulando carregamento de {table_name}.")
                continue
                
            print(f"Carregando {table_name} de {file_path}...")
            df = pd.read_parquet(file_path)
            df.columns = df.columns.str.lower()
            
            # Para a tabela de métricas, o formato é diferente (chave-valor)
            if table_name == 'metrics':
                # Transforma o DataFrame de métricas para o formato chave-valor
                df_metrics = pd.DataFrame({
                    'metric_name': df['metrics'],
                    'metric_value': df['value']
                })
                df_metrics.to_sql(table_name, conn, if_exists='append', index=False)
            else:
                # Carregamento padrão para as demais tabelas
                df.to_sql(table_name, conn, if_exists='append', index=False)
            
            print(f"✓ {table_name} carregada com sucesso. {len(df)} linhas.")
        
        print("\n" + "="*60)
        print("✓✓✓ TODOS OS DADOS CARREGADOS NO BANCO DE DADOS! ✓✓✓")
        print("="*60)
        
except Exception as e:
    print(f"✗ Erro ao carregar dados: {str(e)}")
    # Levantar o erro para que o Airflow marque a tarefa como falha
    raise


# ### 2.6 Verificar Tabelas Criadas

# In[ ]:


# ==========================================
#     VERIFICAR TABELAS CRIADAS
# ==========================================

# SQL para listar tabelas (compatível com PostgreSQL e MySQL)
if DB_TYPE == 'postgresql':
    sql_list_tables = """
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """
else:  # MySQL
    sql_list_tables = f"""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = '{DB_NAME}'
    ORDER BY table_name;
    """

try:
    with engine.connect() as conn:
        result = pd.read_sql(sql_list_tables, conn)

        print("TABELAS CRIADAS NO BANCO DE DADOS:")
        print("="*60)
        print(result.to_string(index=False))
        print("="*60)
        print(f"\n✓ Total de tabelas: {len(result)}")

        # Verificar se todas as tabelas esperadas foram criadas
        expected_tables = ['dim_country', 'dim_customer', 'dim_date', 'dim_product', 
                          'fact_all', 'metrics']
        created_tables = result['table_name'].tolist()

        print("\nVERIFICAÇÃO DAS TABELAS:")
        for table in expected_tables:
            status = "✓" if table in created_tables else "✗"
            print(f"  {status} {table}")

except Exception as e:
    print(f"✗ Erro ao listar tabelas:")
    print(f"  {str(e)}")
    # Levantar o erro para que o Airflow marque a tarefa como falha
    raise


# ### 2.7 Visualizar Estrutura das Tabelas

# In[ ]:


# ==========================================
#     VISUALIZAR ESTRUTURA DAS TABELAS
# ==========================================

def describe_table(table_name):
    """Função para descrever a estrutura de uma tabela"""

    if DB_TYPE == 'postgresql':
        sql_describe = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
    else:  # MySQL
        sql_describe = f"DESCRIBE {table_name};"

    try:
        with engine.connect() as conn:
            result = pd.read_sql(sql_describe, conn)
            print(f"\nEstrutura da tabela: {table_name.upper()}")
            print("="*80)
            print(result.to_string(index=False))
            print("="*80)
    except Exception as e:
        print(f"✗ Erro ao descrever tabela {table_name}: {str(e)}")
        # Não levanta erro aqui, pois é apenas para visualização

# Descrever todas as tabelas
tables_to_describe = ['dim_country', 'dim_customer', 'dim_date', 'dim_product', 
                      'fact_all', 'metrics']

for table in tables_to_describe:
    describe_table(table)


# ---
# 
# ## Resumo da Evolução do Banco de Dados
# 
# ### ✓ Etapa 1 Concluída: Configuração
# - Conexão com PostgreSQL/MySQL estabelecida
# - Engine SQLAlchemy configurada
# - Teste de conexão realizado
# 
# ### ✓ Etapa 2 Concluída: Schema SQL
# - **4 Tabelas Dimensionais**: dim_country, dim_customer, dim_date, dim_product
# - **1 Tabela Fato**: fact_all (unificada)
# - **1 Tabela de Métricas**: metrics
# - **Constraints**: Chaves primárias e estrangeiras
# - **Índices**: Otimização de consultas
# 
# ### ✓ Etapa 3 Concluída: Carregamento de Dados
# - Dados da camada Gold carregados nas tabelas do Data Warehouse.
