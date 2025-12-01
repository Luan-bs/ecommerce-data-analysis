# 🧾 Pipeline Ecommerce Reino Unido

🔗 **DataSet:** [Link para o Data Set](https://www.kaggle.com/datasets/carrie1/ecommerce-data/data)

Este é um conjunto de dados transnacional que contém todas as transações ocorridas entre 01/12/2010 e 09/12/2011 para uma loja online de varejo sem loja física, sediada no Reino Unido e registrada. A empresa vende principalmente presentes exclusivos para todas as ocasiões. Muitos clientes da empresa são atacadistas.

---
## 📁 Colunas do Data Set

| Coluna          | Descrição                                         | Exemplo                            |
| --------------- | ------------------------------------------------- | ---------------------------------- |
| **InvoiceNo**   | Número da fatura ou transação comercial.          | 536365                             |
| **StockCode**   | Código único do produto no estoque.               | 85123A                             |
| **Description** | Nome ou descrição detalhada do produto.           | WHITE HANGING HEART T-LIGHT HOLDER |
| **Quantity**    | Quantidade de itens vendidos na transação.        | 6                                  |
| **InvoiceDate** | Data e hora em que a venda foi registrada.        | 12/1/2010 8:26                     |
| **UnitPrice**   | Preço unitário do produto (em libras esterlinas). | 2.55                               |
| **CustomerID**  | Identificação única do cliente.                   | 17850.0                            |
| **Country**     | País do cliente.                                  | United Kingdom                     |

📊 **Total de Linhas:** ~541.909
🧾 **Total de Colunas:** 8

---
## 🎯 Objetivos do Projeto
- Desenvolver um **pipeline de dados completo e funcional** que resolva um problema real de negócio.
- Realizar uma **análise exploratória de dados** voltada ao setor comercial.  
- Aplicar técnicas de **ETL** no Python (Pandas).
- Estrutura **Bronze - Silver - Gold**

---

## ⚙️ Ferramentas e Linguagens
- **Python (Pandas)**
- **SQLite**
- **Apache Airflow**
- **Streamlit** (Dashboard)
- GitHub Actions (Automação)

---

## 📂 Estrutura do Projeto

```
ecommerce-data-analysis/
│
├── pdf/                          # Instruções do professor para cada etapa
├── Dashboard/                    # Aplicação Streamlit para visualização
├── Notebooks-Airflow/            # Pipeline completo com Airflow
│   ├── docker-compose.yml        # Configuração do Airflow
│   └── dags/                     # DAGs com todas as camadas (Bronze, Silver, Gold)
│       ├── 01_bronze_layer.py
│       ├── 02_silver_layer.py
│       ├── 03_gold_layer.py
│       ├── 04_load_database.py
│       ├── 07_monitoring.py
│       ├── 08_create_database.py
│       └── pipeline_ecommerce.py
│
└── Documentacao.pdf               # Documentação completa do projeto
```

---

## 🚀 Como Rodar o Projeto

### 1️⃣ **Pré-requisitos**
- Docker e Docker Compose instalados
- Python 3.8+ (para o Dashboard)
- Git

### 2️⃣ **Clone o Repositório**
```bash
git clone https://github.com/seu-usuario/ecommerce-data-analysis.git
cd ecommerce-data-analysis
```

### 3️⃣ **Executar o Pipeline com Airflow**

Inicie o Airflow com Docker Compose:
```bash
docker-compose up -d
```

Acesse o Airflow UI em: `http://localhost:8080`
- **Usuário:** airflow
- **Senha:** airflow

Ative a DAG `pipeline_ecommerce` para executar todo o processo ETL.

Acesse o Dashboard em: `http://localhost:8501`

### 5️⃣ **Consultar a Documentação**

Para entender melhor o projeto, consulte o arquivo `Documentacao.pdf` na raiz do repositório.

Para ver as instruções originais de cada etapa, acesse a pasta `pdf/`.

---

## 📝 Camadas do Pipeline

- **Bronze Layer:** Ingestão dos dados brutos do CSV
- **Silver Layer:** Limpeza e transformação dos dados
- **Gold Layer:** Dados agregados e prontos para análise
- **Load Database:** Carregamento no banco de dados SQLite
- **Monitoring:** Monitoramento e logging do pipeline

---

## 📊 Resultados

O pipeline processa mais de 540 mil registros de transações, gerando insights sobre:
- Vendas por país
- Produtos mais vendidos
- Comportamento de clientes atacadistas
- Métricas de performance do negócio

---