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
            bf.datafaturamento,
            bf.codigoproduto,
            bp.nomeproduto,
            bp.marca,
            bf.precounitario,
            bf.quantidadenegociada,
            (bf.precounitario * bf.quantidadenegociada) AS faturamento
        FROM
            dbo.bi_fato AS bf
        INNER JOIN
            dbo.bi_produto AS bp
        ON
            bf.codigoproduto = bp.codigoproduto 
        WHERE
            bf.tipomovumento IN ('V', 'B')
            AND bf.datafaturamento >= CURRENT_DATE - INTERVAL '3 months';
    """



except psycopg2.Error as e:
    print("Erro ao conectar:", e)

finally:
    if connect:
        connect.close()
        print("Conexão fechada")