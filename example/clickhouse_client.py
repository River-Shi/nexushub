import clickhouse_connect
import os
import time
import polars as pl

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

def main():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    
    query = "SELECT * FROM kline_1h ORDER BY timestamp"
    
    time_start = time.time()
    data = client.query_arrow(query)
    df = pl.from_arrow(data)
    time_end = time.time()
    print(f"Query execution time: {time_end - time_start} seconds")
    
    print(df)


if __name__ == "__main__":
    main()




