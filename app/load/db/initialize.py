# This module is for initializing the database
from app.load.db.connection import Connector
from app.load.schemas.table_schema import TABLES
from app.config import local_database_schema,docker_database_schema
from psycopg2 import sql
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from pathlib import Path

class DatabaseInitializer:
    """This class handles the initial set up of the database.
    This includes creating it if necessary.
    The parameter 'docker' tells the class whether the database is in docker or a local database.
    Based on this parameter, the class pulls the relevant connection info from config.py"""

    def __init__(self, docker: bool = False):
        if docker:
            self.db_name = docker_database_schema["database"]
            self.db = Connector(docker_database_schema["database"], docker_database_schema["user"],
                                       docker_database_schema["password"], docker_database_schema["host"])
        else:
            self.db_name = local_database_schema["database"]
            self.db = Connector(local_database_schema["database"], local_database_schema["user"],
                                local_database_schema["password"], local_database_schema["host"])

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

        self.db.execute(query, close=close, commit=True)

    def set_up_view_tables(self,close: bool=True):
        base_path = Path(__file__).resolve().parent
        sql_folder = (base_path / ".." / ".." / "sql" / "tables").resolve()
        for filename in os.listdir(sql_folder):
            if filename.endswith(".sql"):
                filepath = os.path.join(sql_folder, filename)
                self.db.execute_sql_file(filepath,close=close,commit=True)


    def initialize_db(self):
        self.db.connect()
        for table in TABLES:
            print(f"Setting up table: {table}")
            self.set_up_table(table,TABLES[table],False)
        self.set_up_view_tables(False)

        self.db.close()

    # The following method only runs when working with an external database.
    def create_db(self):
        """This method creates the initial database"""
        # Connect to server (without specifying a database yet)
        conn = psycopg2.connect(
            dbname="postgres",
            user=self.db.user,
            password=self.db.password,
            host=self.db.host,
            port=5432
        )

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Create database if it doesn't exist
        # the following returns a 1 if the database with db_name exists. pg_database is a general overview database of postgreSQL which stores all the databases.
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{self.db_name}';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE {self.db_name};')
            print(f"Database {self.db_name} created!")
        else:
            print(f"Database {self.db_name} already exists.")

        cursor.close()
        conn.close()