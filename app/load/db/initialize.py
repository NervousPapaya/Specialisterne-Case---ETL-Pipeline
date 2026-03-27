# This module is for initializing the database
from app.load.db.connection import Connector
from app.load.schemas.table_schema import TABLES
from app.config import local_database_schema,docker_database_schema,database_schemas
from psycopg2 import sql
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from app.load.db.table_creator import TableCreator
from app.load.db.roles import RoleManager

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
        self.schemas=database_schemas
        self.RoleManager = RoleManager(db=self.db)
        self.TableCreator = TableCreator(db=self.db)


    def set_up_schemas(self,close=True):
        for schema in self.schemas:
            schema_name=self.schemas.get(schema)
            schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
            self.db.execute(schema_query, close=close,commit=True)

    def initialize_db(self):
        self.db.connect()
        self.set_up_schemas(close=False)
        for table in TABLES:
            print(f"Setting up table: {table}")
            self.TableCreator.set_up_table(table,TABLES[table]["columns"], TABLES[table]["schema"],close=False)
        self.TableCreator.set_up_view_tables(close=False)
        self.RoleManager.setup_roles()

        self.db.close()

    def create_db(self):
        """This method creates the initial database if none exists"""
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

