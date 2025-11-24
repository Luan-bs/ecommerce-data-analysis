import pandas as pd
import matplotlib.pyplot as plt

print("="*50)
print("MONITORAMENTO DO PIPELINE")
print("="*50)

# lê o log real gerado pelas tasks
df_log = pd.read_csv(
    "C:\\airflow\\dags\\logs_pipeline.csv",
    header=None,
    names=[
        "data_execucao",
        "camada",
        "status",
        "tempo_segundos",
        "registros_processados",
    ]
)

# Taxa de sucesso
taxa_sucesso = (df_log['status'] == 'sucesso').sum() / len(df_log) * 100
print(f"\nTaxa de Sucesso: {taxa_sucesso:.2f}%")

# Tempo médio por camada
print("\nTempo Médio por Camada:")
print(df_log.groupby('camada')['tempo_segundos'].mean())

# Visualização
plt.figure(figsize=(12, 4))

# gráfico 1
plt.subplot(1, 2, 1)
df_log['status'].value_counts().plot(kind='bar')
plt.title('Status das Execuções')
plt.ylabel('Quantidade')

# gráfico 2
plt.subplot(1, 2, 2)
df_log.groupby('camada')['tempo_segundos'].mean().plot(kind='bar')
plt.title('Tempo Médio por Camada')
plt.ylabel('Segundos')

plt.tight_layout()
plt.savefig('pipeline_monitoring.png')

print("\nGráficos salvos em: pipeline_monitoring.png")
