import pandas as pd
import psycopg2
import matplotlib.pyplot as plt # type: ignore
from dotenv import load_dotenv
import os

load_dotenv()

try:
    connect = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    print("Conexão estabelecida com sucesso!")

    query = """
        SELECT
            datafaturamento,
            codigoproduto,
            precounitario,
            quantidadenegociada,
            (precounitario * quantidadenegociada) AS faturamento
        FROM
            dbo.bi_fato
        WHERE
            tipomovumento IN ('V', 'B')
            AND datafaturamento >= CURRENT_DATE - INTERVAL '3 months';
    """

    df = pd.read_sql(query, connect)

    # Agrupar e ordenar os produtos -- Vinicius
    df_produto = (
        df.groupby('codigoproduto', as_index=False)
          .agg({'faturamento': 'sum'})
          .sort_values(by='faturamento', ascending=False)
    )

    # Calcula valor acumulado -- Vinicius
    df_produto['PorcentAcumulado'] = (
        df_produto['faturamento'].cumsum() /
        df_produto['faturamento'].sum() * 100
    )

    # Função de classificação ABC
    def classificar(p):
        if p <= 80:
            return 'A'
        elif p <= 95:
            return 'B'
        else:
            return 'C'

    df_produto['Classificacao'] = df_produto['PorcentAcumulado'].apply(classificar)

    print(df_produto)

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

finally:
    if connect:
        connect.close()
        print("Conexão fechada")