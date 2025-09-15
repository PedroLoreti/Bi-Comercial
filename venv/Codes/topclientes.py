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

def main():
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
            bc.codigocliente,
            bc.nomecliente,
            bv.nomerepresentante,
            -- Faturamento por marca específica
            SUM(CASE WHEN UPPER(bp.marca) = 'MECTRONIC' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_mectronic,
            SUM(CASE WHEN UPPER(bp.marca) = 'TRAMONTINA ELETRIK' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_tramontina,
            SUM(CASE WHEN UPPER(bp.marca) LIKE '%OUROLUX%' THEN bf.precounitario * bf.quantidadenegociada ELSE 0 END) AS faturamento_ourolux,
            -- Faturamento total
            SUM(bf.precounitario * bf.quantidadenegociada) AS faturamento_total
        FROM
            dbo.bi_fato AS bf
        INNER JOIN dbo.bi_cliente AS bc 
            ON bf.codigocliente = bc.codigocliente
        INNER JOIN dbo.bi_produto AS bp 
            ON bf.codigoproduto = bp.codigoproduto
        INNER JOIN dbo.bi_vendedor AS bv 
            ON bc.codigovendedor = bv.codigovendedor
        WHERE
            bf.tipomovumento IN ('V', 'B')
            AND bf.datafaturamento >= CURRENT_DATE - INTERVAL '3 months'
            AND (UPPER(bp.marca) LIKE '%MECTRONIC%' 
                OR UPPER(bp.marca) LIKE '%TRAMONTINA%ELETRIK%'
                OR UPPER(bp.marca) LIKE '%OUROLUX%')
        GROUP BY
            bc.codigocliente, 
            bc.nomecliente, 
            bv.nomerepresentante
        ORDER BY
            faturamento_total DESC;
        """

        df_resultado = pd.read_sql(query, connect)

        # ✅ CORREÇÃO: Usar os nomes exatos das colunas
        print("📊 Faturamento por Cliente e Marca")
        print(f"Total de clientes: {len(df_resultado)}")
        print("\n📋 Primeiros resultados:")
        print(df_resultado.head(10))

        # ✅ CORREÇÃO: Nomes das colunas em minúsculo
        print(f"\n🎯 Total MECTRONIC: R$ {df_resultado['faturamento_mectronic'].sum():,.2f}")
        print(f"🎯 Total Tramontina: R$ {df_resultado['faturamento_tramontina'].sum():,.2f}")
        print(f"🎯 Total Ourolux: R$ {df_resultado['faturamento_ourolux'].sum():,.2f}")
        print(f"💰 Faturamento Total: R$ {df_resultado['faturamento_total'].sum():,.2f}")

        # Exportar para Excel
        data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nome_arquivo = f"faturamento_marcas_especificas_{data_atual}.xlsx"
        
        df_resultado.to_excel(nome_arquivo, index=False)
        print(f"\n✅ Arquivo exportado: {nome_arquivo}")

    except Exception as e:
        print(f"Erro durante execução: {e}")
    finally:
        if connect:
            connect.close()
            print("Conexão fechada")

if __name__ == "__main__":
    main()