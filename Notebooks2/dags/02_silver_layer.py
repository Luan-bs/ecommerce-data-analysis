# # Camada Silver - Limpeza e Transformação de Dados
# 
# Este notebook implementa a **segunda camada do pipeline** (Silver Layer), onde realizamos a limpeza e transformação dos dados brutos.
# 
# ## Objetivos:
# - Tratar valores faltantes (NaN)
# - Identificar e analisar duplicatas
# - Lidar com valores negativos, cancelamentos e tarifas
# - Detectar e avaliar outliers
# - Preparar dados limpos para a camada Gold
# 
# ## Processo:
# Os dados da camada Bronze são carregados, tratados e salvos na camada Silver em formato otimizado.


import pandas as pd
import os
from datetime import datetime
import time
import csv

# medir tempo
inicio = time.time()

# Definindo o diretório base para os dados (deve ser o mesmo do 01_bronze_layer.py)
BASE_DIR = "/opt/airflow/dags"
DATA_PATH = f"{BASE_DIR}/data"

# Carregar  dados da camada bronze
df = pd.read_parquet(f"{DATA_PATH}/bronze/dados_brutos.parquet")
print(f"Dados originais {df.shape}")
df_clean = df.copy()


# ### Carregamento dos Dados
# 
# Carregamos os dados da camada Bronze e criamos uma cópia para trabalhar, mantendo o arquivo original intacto.

# # Transformações

# ## 1. Valores Faltantes

# ### CustomerID

# *   CustomerID	| 135.080 Rows NaN

# Checando a possibilidade de tratar CustomerID NaN de acordo com InvoiceNo (Nº da fatura) iguais e com CustomerID preenchido


# Contar quantos InvoiceNo possuem pelo menos um CustomerID nulo
invoice_null = df.groupby('InvoiceNo')['CustomerID'].apply(lambda x: x.isnull().any())
invoice_notnull = df.groupby('InvoiceNo')['CustomerID'].apply(lambda x: x.notnull().any())

# Quantos têm mistura (nulo e não nulo)?
mixed_invoice = (invoice_null & invoice_notnull)

print("Total de InvoiceNo mistos:", mixed_invoice.sum())
print("Total de InvoiceNo com CustomerID nulo:", invoice_null.sum())
print("Total de InvoiceNo com CustomerID não nulo:", invoice_notnull.sum())
print(f"Total de linhas com CustomerID nulo: {df['CustomerID'].isnull().sum()}")


# #### Análise de CustomerID Nulos
# 
# Verificamos se é possível recuperar CustomerID usando o InvoiceNo (número da fatura) como referência. Se uma mesma fatura tiver registros com e sem CustomerID, podemos preencher os vazios.

#  - Total de InvoiceNo mistos: 0
# 
# 
# Significa que nao é possivel recuperar CustomerID pelo InvoiceNo

# ### Description

# *   Description | 1454 Rows NaN
# 
#  Muitos Description possuem erros de preenchimento também
# 

# Cada produto possui o seu StockCode. Muitos Description (nome do produto) estão NaN ou preenchidos de forma errada, porém possuem StockCode iguais

# Contar quantos StockCode possuem pelo menos um Description nulo
StockCode_null = df.groupby('StockCode')['Description'].apply(lambda x: x.isnull().any())
StockCode_notnull = df.groupby('StockCode')['Description'].apply(lambda x: x.notnull().any())

# Quantos têm mistura (nulo e não nulo)?
mixed_StockCode = (StockCode_null & StockCode_notnull)

print("Total de StockCode mistos:", mixed_StockCode.sum())
print("Total de StockCode com Description nulo:", StockCode_null.sum())
print("Total de StockCode com Description não nulo:", StockCode_notnull.sum())
print(f"Total de linhas com Description nulo: {df['Description'].isnull().sum()}")
print("==========================================")

#Exemplo:
# display(df[df["StockCode"] == "35965"]) # Comentado para evitar erro de display em ambiente não-notebook
print("\n")
print(f"Quantidade de Description NaN do StockCode 35965: {df[df['StockCode'] == '35965']['Description'].isnull().sum()}")


# #### Análise de Description (Descrição dos Produtos)
# 
# Verificamos se é possível recuperar descrições de produtos usando o StockCode como referência. Como cada StockCode representa um produto específico, podemos usar o primeiro valor válido encontrado para preencher os demais.


# Criar mapeamento de StockCode  Description válida
mapa_descricoes = df_clean.dropna(subset=['Description']).groupby('StockCode')['Description'].first().to_dict()

# Preencher Description de acordo com o primeiro valor
df_clean['Description'] = df_clean.apply(
    lambda row: mapa_descricoes.get(row['StockCode'], row['Description']),
    axis=1
)

print(f"Descriptions recuperados: {df['Description'].isnull().sum() - df_clean['Description'].isnull().sum()}")
print(f"Descriptions não recuperados: {df_clean['Description'].isnull().sum()}")
print(f"Quantidade de Descriptions erradas corrigidas: {df['Description'].nunique() - df_clean['Description'].nunique()}")


# #### Preenchimento de Descriptions
# 
# Criamos um mapeamento de StockCode para Description e usamos para preencher valores nulos e corrigir inconsistências. Isso também padroniza descrições do mesmo produto.

# ##  2. Duplicatas

#  Duplicatas são aceitáveis no modelo de negócio deste DataFrame. Cada linha representa um item em uma fatura, portanto, o mesmo `InvoiceNo` (número da fatura) pode aparecer várias vezes se uma fatura contiver múltiplos produtos. A venda em si é representada unicamente pelo `InvoiceNo`.



print(f"Quantidade de linhas duplicadas: {df_clean.duplicated().sum()}")
# display(df_clean[df_clean.duplicated()]) # Comentado para evitar erro de display em ambiente não-notebook


# #### Verificação de Duplicatas Completas
# 
# Identificamos linhas completamente duplicadas no dataset. Note que duplicatas parciais são esperadas, pois uma fatura pode conter múltiplos produtos.

# ## 3. Valores negativos, Cancelamentos e tarifas

# In[6]:


# Linhas de tarifas
stockcode_fees = ['C2', 'DOT', 'POST','AMAZONFEE']

# Filtrar DataFrame para mostrar Inconsistências:
#(InvoiceNo que começam com 'C','A)'| Quantity <= 0 | UnitPrice <= 0 e StockCode de tarifas
df_inconsistencias = df_clean[
   (df_clean['InvoiceNo'].astype(str).str.startswith('C')) |
    (df_clean['InvoiceNo'].astype(str).str.startswith('A')) |
     (df_clean['Quantity'] <= 0) | (df_clean['UnitPrice'] <= 0) | (df_clean['StockCode'].isin(stockcode_fees))
]

# display(df_inconsistencias) # Comentado para evitar erro de display em ambiente não-notebook
print("Dados serão divididos na camada gold")


# #### Identificação de Inconsistências
# 
# Filtramos registros que representam:
# - Faturas começando com 'C' (cancelamentos)
# - Faturas começando com 'A' (ajustes)
# - Quantidades ou preços negativos ou zero
# - Códigos de produtos relacionados a tarifas (C2, DOT, POST, AMAZONFEE)
# 
# Estas inconsistências serão tratadas separadamente na camada Gold.

# ## 4. Outliers

# In[7]:


df_clean.describe()


# #### Estatísticas Descritivas
# 
# Analisamos as estatísticas básicas dos dados para identificar possíveis outliers e entender a distribuição dos valores.

# In[8]:


# identificando Outliers
# display(df_clean[df_clean['Quantity'] > 5000]) # Comentado para evitar erro de display em ambiente não-notebook

print("\n")
print("-------------------------------------------------------")
print("Verificando padrão de compra do maior Outlier/Cliente")
print("-------------------------------------------------------")

# Trocar ID para visualizar todos
# display(df_clean[df_clean['CustomerID'] == 16446]) # Comentado para evitar erro de display em ambiente não-notebook

print("Foram feitos cancelamenos das compras Outliers")


# #### Análise de Outliers Extremos
# 
# Identificamos compras com quantidades muito altas (>5000 unidades) e verificamos o padrão de compra dos clientes. Muitos outliers são compras corporativas legítimas, com cancelamentos correspondentes.

# In[9]:


# Testar outliers usando IQR para a coluna 'Quantity'
Q1 = df_clean['Quantity'].quantile(0.25)
Q3 = df_clean['Quantity'].quantile(0.75)
IQR = Q3 - Q1

outliers_iqr = df_clean[(df_clean['Quantity'] < (Q1 - 1.5 * IQR)) | (df_clean['Quantity'] > (Q3 + 1.5 * IQR))]
print(f"Quantidade de outliers detectados pelo IQR em Quantity: {outliers_iqr.shape[0]}")
# display(outliers_iqr) # Comentado para evitar erro de display em ambiente não-notebook


# #### Teste de Outliers usando IQR (Intervalo Interquartil)
# 
# Aplicamos o método estatístico IQR para detectar outliers. No entanto, devido às características do negócio (compras corporativas, distribuição assimétrica), muitos valores identificados como outliers são na verdade transações legítimas. Por isso, optamos por não remover esses registros.
# 
# Resultado de 58.169 outliers não fazem sentido real.
# A maior parte deles são possiveis compras grandes legítimas.
# 
# variável Quantity tem estas características:
# 
# - Distribuição extremamente assimétrica (muitos valores baixos, poucos valores muito altos);
#  
# - Existem cancelamentos e devoluções com valores negativos;
# 
# - Existem compras corporativas (de 500, 1000, 2000 unidades) perfeitamente legítimas;
# 
# - E há linhas com erros humanos ou transações de teste, mas elas são exceções.

# ## 5. Carregando dados na camada Silver
# 


# In[10]:


df_clean.to_parquet(f"{DATA_PATH}/silver/dados_limpos.parquet",index=False)
print("Dados salvos na camada Silver")


# ### Salvamento dos Dados Limpos
# 
# Os dados tratados são salvos na camada Silver, prontos para serem modelados na camada Gold.
# =============================
# LOG DO PIPELINE (silver)
# =============================

fim = time.time()
duracao = fim - inicio
registros = df_clean.shape[0]

log_file = f"{BASE_DIR}/logs_pipeline.csv"

log = [
    datetime.now(),   # data_execucao
    "silver",         # camada
    "sucesso",        # status
    round(duracao, 2),
    registros
]

# salva o log
with open(log_file, "a", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(log)

print("Log registrado no arquivo logs_pipeline.csv")