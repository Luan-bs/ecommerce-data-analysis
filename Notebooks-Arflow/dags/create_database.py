from sqlalchemy import create_engine, text
import warnings
import time

warnings.filterwarnings('ignore')

# ==========================================
#     CONFIGURAÇÃO DA CONEXÃO
# ==========================================

DB_TYPE = 'postgresql'
DB_HOST = 'postgres_dw'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_PORT = '5432'
DB_NAME = 'ecommerce_dw'

connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(connection_string)

print("✓ Conectado ao banco para criar o SCHEMA")


# ==========================================
#     SQL PARA CRIAÇÃO DO SCHEMA
# ==========================================

sql_create_dimensions = """
CREATE TABLE IF NOT EXISTS dim_country (
    CountryID SERIAL PRIMARY KEY,
    Country VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CountryID INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_country_customer FOREIGN KEY (CountryID)
        REFERENCES dim_country(CountryID)
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    DateID SERIAL PRIMARY KEY,
    InvoiceDate TIMESTAMP NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    Weekday VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product (
    StockCode VARCHAR(50) PRIMARY KEY,
    productdescription VARCHAR(500),
    UnitPrice DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

sql_create_fact = """
CREATE TABLE IF NOT EXISTS fact_all (
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

    CONSTRAINT fk_product FOREIGN KEY (StockCode) REFERENCES dim_product(StockCode),
    CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES dim_customer(CustomerID),
    CONSTRAINT fk_country FOREIGN KEY (CountryID) REFERENCES dim_country(CountryID),
    CONSTRAINT fk_date FOREIGN KEY (DateID) REFERENCES dim_date(DateID)
);

CREATE INDEX IF NOT EXISTS idx_fact_invoice ON fact_all(InvoiceNo);
CREATE INDEX IF NOT EXISTS idx_fact_stock_code ON fact_all(StockCode);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_all(CustomerID);
CREATE INDEX IF NOT EXISTS idx_fact_country ON fact_all(CountryID);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_all(DateID);
"""

sql_create_metrics = """
CREATE TABLE IF NOT EXISTS metrics (
    metric_name VARCHAR(100) PRIMARY KEY,
    metric_value DECIMAL(20, 2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name);
"""


# ==========================================
#     EXECUTAR SQL
# ==========================================

with engine.connect() as conn:
    print("→ Criando tabelas dimensionais...")
    conn.execute(text(sql_create_dimensions))

    print("→ Criando tabela fato...")
    conn.execute(text(sql_create_fact))

    print("→ Criando tabela metrics...")
    conn.execute(text(sql_create_metrics))

print("\n✓✓✓ SCHEMA CRIADO COM SUCESSO ✓✓✓\n")
