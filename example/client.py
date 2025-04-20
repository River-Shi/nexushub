import psycopg2
import os
import pandas as pd

TS_DB_NAME = os.getenv("TS_DB_NAME")
TS_DB_USER = os.getenv("TS_DB_USER")
TS_DB_PASSWORD = os.getenv("TS_DB_PASSWORD")
TS_DB_PORT = int(os.getenv("TS_DB_PORT"))


with psycopg2.connect(
    host="127.0.0.1",
    port=TS_DB_PORT,
    user=TS_DB_USER,
    password=TS_DB_PASSWORD,
    dbname=TS_DB_NAME,
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM kline_1h WHERE time >= '2025-01-01 00:00:00'")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

df_pivot = df.pivot(index="time", columns="symbol", values="close")
print(df_pivot)

