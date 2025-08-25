import pandas as pd
import psycopg2

try:
    connect = psycopg2.connect(
        database="CPlus5",
        user="consulta",
        password="vvs@2025@",
        host="192.168.10.225",
        port="5432"
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
            AND datafaturamento >= CURRENT_DATE - INTERVAL '12 months';
    """

    df = pd.read_sql(query, connect)
    print("Dados carregados com sucesso!")
    print(df.head())  # Mostra as 5 primeiras linhas para conferir

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

finally:
    if connect:
        connect.close()
        print("Conexão fechada")

