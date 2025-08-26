import pandas as pd
import psycopg2
import matplotlib.pyplot as plt # type: ignore
from dotenv import load_dotenv
import os

#Config para vermos todas as informações na Saída do Terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', None)


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
            ft.datafaturamento,
            ft.codigoproduto,
            pt.marca,
            ft.precounitario,
            ft.quantidadenegociada,
            (ft.precounitario * ft.quantidadenegociada) AS faturamento
        FROM
            dbo.bi_fato AS ft
        INNER JOIN
            dbo.bi_produto AS pt
        ON
            ft.codigoproduto = pt.codigoproduto 
        WHERE
            ft.tipomovumento IN ('V', 'B')
            AND ft.datafaturamento >= CURRENT_DATE - INTERVAL '3 months';
    """

    df = pd.read_sql(query, connect)

    # Agrupar e ordenar os produtos -- Vinicius
    df_produto = (
        df.groupby(['codigoproduto', 'marca'], as_index=False)
          .agg({'faturamento': 'sum',
                'quantidadenegociada' : 'sum'})
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

    print(df_produto.head(20))

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

finally:
    if connect:
        connect.close()
        print("Conexão fechada")