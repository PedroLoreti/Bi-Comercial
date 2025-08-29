import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Configurações do pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', None)

load_dotenv()

connect = None

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
        bf.datafaturamento AS data_compra,
        bc.codigocliente,
        bc.nomecliente,
        bc.uf,
        bf.codigovendedor,
        bv.nomerepresentante,  
        COUNT(DISTINCT bf.codigoproduto) AS mix_produtos,
        SUM(bf.precounitario * bf.quantidadenegociada) AS valor_total_compra,
        SUM(bf.quantidadenegociada) AS quantidade_total_itens
    FROM
        dbo.bi_fato AS bf
    INNER JOIN
        dbo.bi_cliente AS bc ON bf.codigocliente = bc.codigocliente
    INNER JOIN
        dbo.bi_produto AS bp ON bf.codigoproduto = bp.codigoproduto
    INNER JOIN
        dbo.bi_vendedor AS bv ON bf.codigovendedor = bv.codigovendedor  
    WHERE
        bf.tipomovumento IN ('V','B')
        AND bf.datafaturamento IN ('2025-08-27', '2025-08-28')
        AND bc.uf != 'RJ'
        AND bc.codigocliente IN (
                      --  Subquery: clientes que compraram pelo menos um dos produtos
            SELECT DISTINCT bf2.codigocliente
            FROM dbo.bi_fato bf2
            WHERE bf2.codigoproduto IN ('861-03985', '861-03992')
            AND bf2.datafaturamento IN ('2025-08-27', '2025-08-28')
            AND bf2.tipomovumento IN ('V','B')
        )
    GROUP BY
        bf.datafaturamento,
        bc.codigocliente, 
        bc.nomecliente, 
        bc.uf,
        bf.codigovendedor,
        bv.nomerepresentante
    ORDER BY
        bf.datafaturamento,
        valor_total_compra DESC;
"""

    df_vendas = pd.read_sql(query, connect)

    # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"AcaoForaDoEstado_{data_atual}.xlsx"

    df_vendas.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")
    print(f"📊 Total de clientes analisados: {len(df_vendas)}")
    
    # Mostrar as primeiras linhas para verificar
    print("\n📋 Primeiras linhas do resultado:")
    print(df_vendas.head())

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if connect:
        connect.close()
        print("Conexão fechada")