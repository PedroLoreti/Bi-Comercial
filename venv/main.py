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

except psycopg2.Error as e:
    print("Erro ao conectar ao banco de dados", e)


cur = connect.cursor()


cur.execute("SELECT * FROM dbo.bi_fato WHERE datafaturamento = '2025-08-14';")

rows = cur.fetchall()

for row in rows[:100]:
    print(row)

