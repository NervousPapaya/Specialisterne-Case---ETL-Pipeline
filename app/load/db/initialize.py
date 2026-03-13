# This module is for initializing the database
from app.load.db.connection import Connector
from app.load.schemas.table_schema import TABLES
from app.load.schemas.database_schema import database_schema
from psycopg2 import sql
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DatabaseInitializer:
    def __init__(self):
        self.connector = Connector(database_schema["database"], database_schema["user"], database_schema["password"], database_schema["host"])

    #This should maybe be in a separate class? So that we can open the connection once, and create all the tables
    def set_up_table(self,table_name: str, columns:dict, close:bool=True):
        """This method is designed to set up a table.
        table_name is a string being the name of the table
        columns is a dict with keys being column names and values being the data types"""

        column_defs = [
            sql.SQL("{} {} NOT NULL").format(
                sql.Identifier(col_name),
                sql.SQL(col_type)
            )
            for col_name, col_type in columns.items()
        ]

        query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
        {} SERIAL PRIMARY KEY,
        {}
        );""").format(
        sql.Identifier(table_name),
            sql.Identifier(f"{table_name}_id"),
            sql.SQL(",\n").join(column_defs)
        )

        self.connector.execute(query, close=close, commit=True)

    def initialize_db(self):
        self.connector.connect()
        for table in TABLES:
            print(f"Setting up table: {table}")
            self.set_up_table(table,TABLES[table],False)

        self.connector.close()



    # The following method is only used in testing on an external database.
    def create_db(self, db_name= "weather_db"):
        """This method creates the initial database"""
        # Connect to server (without specifying a database yet)
        conn = psycopg2.connect(
            dbname="postgres",
            user=self.connector.user,
            password=self.connector.password,
            host=self.connector.host,
            port=5432
        )

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Create database if it doesn't exist
        # the following returns a 1 if the database with db_name exists. pg_database is a general overview database of postgreSQL which stores all the databases.
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE {db_name};')
            print(f"Database {db_name} created!")
        else:
            print(f"Database {db_name} already exists.")

        cursor.close()
        conn.close()