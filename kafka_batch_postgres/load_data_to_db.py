import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, DateTime

# https://stackoverflow.com/questions/34484066/create-a-postgres-database-using-python

def create_db_connection(
        # use default db to connect to
        # then create a db of your choice
        db: str="postgres", 
        user: str="postgres", 
        pwd: str="postgres", 
        port: str="5432",
):
    try:
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, port=port)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        raise Exception(f"Connection to postgres db failed {e}")
    return conn

def create_db_engine(
        url: str
):
    return create_engine(url)

def check_database_exists(cursor, database: str="kafka_assignment"):
    try:
        qry = sql.SQL("select count(*) from pg_database where datname = '{}'").format(sql.Identifier(database))
    except Exception as e:
        return
        # raise Exception(f"Check database exists query failed {e}")
    cursor.execute(qry)
    return cursor.fetchone()

def create_database(cursor, database: str="kafka_assignment"):
    try:
        qry = sql.SQL("create database {}").format(sql.Identifier(database))
        cursor.execute(qry)
    except Exception as e:
        raise Exception(f"Failed to create database {database}")

def create_table(cursor, table_name: str="products"):
    qry = """
            create table if not exists {table_name} (
                id int,
                name varchar(50),
                category varchar(50),
                price decimal(5, 2),
                last_update timestamp
            )
            """.format(table_name=table_name)
    
    try:
        cursor.execute(qry)
    except Exception as e:
        raise Exception(f"Failed to create table {table_name}. {e}")
    

def load_data(engine, df: pd.DataFrame, table_name: str):
    try:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    except Exception as e:
        raise Exception(f"Failed to load data to database {e}")

def main():
    db_name = "kafka_assignment"
    user = "postgres"
    pwd = "postgres"
    conn = create_db_connection()
    cursor = conn.cursor()
    result = check_database_exists(cursor, db_name)
    if not result:
        create_database(cursor, db_name)
    url = f"postgresql+psycopg2://{user}:{pwd}@localhost/{db_name}"
    table_name = "products"
    engine = create_db_engine(url)
    create_table(cursor)
    df = pd.read_csv("mock_data.csv")
    df["last_update"] = pd.to_datetime(df["last_update"])
    load_data(engine, df, table_name)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()