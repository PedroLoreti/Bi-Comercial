import pandas as pd
import psycopg2
import matplotlib.pyplot as plt # type: ignore
from dotenv import load_dotenv
import os
from datetime import datetime

# Configurações do pandas
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


    query_vendas = """
    SELECT
        bf.codigoproduto,
        bp.nomeproduto,
        bp.marca,
        SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento,
        SUM(bf.quantidadenegociada) AS Qtd_Itens,
        COUNT(bf.datafaturamento) AS Qtd_Pedidos,
        CEILING(SUM(bf.quantidadenegociada) / COUNT(bf.datafaturamento)) AS Itens_por_Pedidos,
        MAX(bf.datafaturamento) AS ultima_venda,

        -- Subquery para pegar o último estoque
        (SELECT be.estoquenadata 
         FROM dbo.bi_estoque be 
         WHERE be.codigoprincipal = bf.codigoproduto 
         ORDER BY be.data DESC 
         LIMIT 1) AS estoque_na_data,
        (SELECT MAX(be.data) 
         FROM dbo.bi_estoque be 
         WHERE be.codigoprincipal = bf.codigoproduto) AS data_estoque,

        CASE WHEN pb.permitecompra = TRUE THEN '[✓]' ELSE '[ ]' END AS P_Compra,
        CASE WHEN pb.permitevenda = TRUE THEN '[✓]' ELSE '[ ]' END AS P_Venda, 
        CASE WHEN pb.inativo = TRUE THEN '[✓]' ELSE '[ ]' END AS Inativo

    FROM
        dbo.bi_fato AS bf
    INNER JOIN
        dbo.bi_produto AS bp ON bf.codigoproduto = bp.codigoproduto
    INNER JOIN
        dbo.produtobase AS pb ON bf.codigoproduto = pb.codigoprincipal
    WHERE
        bf.tipomovumento IN ('V', 'B')
        AND bf.datafaturamento >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY
        bf.codigoproduto, bp.nomeproduto, bp.marca, 
        pb.permitecompra, pb.permitevenda, pb.inativo, pb.codigoprincipal
    ORDER BY
        faturamento DESC;
"""

    # Executar a query
    df_produto = pd.read_sql(query_vendas, connect)

  
    df_produto = df_produto.sort_values('faturamento', ascending=False)

    # Calcula valor acumulado
    df_produto['PorcentAcumulado'] = (
        df_produto['faturamento'].cumsum() /  
        df_produto['faturamento'].sum() * 100  
    )

    # Função de classificação ABC
    def classificar_abc(p):
        if p <= 80:
            return 'A'
        elif p <= 95:
            return 'B'
        else:
            return 'C'

    # ✅ ADICIONAR COLUNA DE CLASSIFICAÇÃO ABC
    df_produto['Classificacao'] = df_produto['PorcentAcumulado'].apply(classificar_abc)

    # 3. Verificar os dados
    print("📊 Dados de vendas carregados!")
    print(f"Total de produtos: {len(df_produto)}")
    print("\nPrimeiras linhas COM CLASSIFICAÇÃO ABC:")
    print(df_produto.head(10))

    # ✅ MOSTRAR DISTRIBUIÇÃO ABC
    print(f"\n🎯 Distribuição ABC:")
    print(df_produto['Classificacao'].value_counts())

            # Exportar para Excel
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nome_arquivo = f"Ultimas_Vendas{data_atual}.xlsx"

    df_produto.to_excel(nome_arquivo, index=False)
    print(f"✅ Arquivo exportado: {nome_arquivo}")
    print(f"📊 Total de clientes analisados: {len(df_produto)}")

except psycopg2.Error as e:
    print("Erro ao conectar:", e)

except Exception as e:
    print(f"Erro durante execução: {e}")

finally:
    if connect:
        connect.close()
        print("Conexão fechada")