# Cleaned up ingestion script based off of upload-data.ipynb
# Includes handling .gz, .csv, and .parquet file types

import argparse
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
 
    # Download file from url using wget
    file_name = url.split('/')[-1].strip()
    os.system(f'wget {url} -O {file_name}')

    # Create SQLAlchemy engine
    # postgresql://root:root@localhost:5432/ny_taxi
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read files into data frame according to format
    if file_name.endswith('.parquet'):
        pf = pq.read_table(file_name)
        df = pf.to_pandas()
    elif file_name.endswith('.csv') or file_name.endswith('.csv.gz'):
        df = pd.read_csv(file_name)
        if 'tpep_pickup_datetime' in df.columns:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        if 'tpep_dropoff_datetime' in df.columns:
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        if 'lpep_pickup_datetime' in df.columns:
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        if 'lpep_dropoff_datetime' in df.columns:
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    else:
        print(f'Unsupported file format: {file_name}')
        return

    # Generate SQL DDL statement to create table schema
    ddl = pd.io.sql.get_schema(df, name=table_name, con=engine)
    print(ddl)

    # Upload the data to the database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100000)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    # user, password, host, port, database name, table name, url of the file
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of table where data will be written')
    parser.add_argument('--url', required=True, help='url of the file file')

    args = parser.parse_args()
    main(args)
