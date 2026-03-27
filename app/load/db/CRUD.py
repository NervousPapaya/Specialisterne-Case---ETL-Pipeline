from app.load.db.connection import Connector
from app.load.schemas.table_schema import TABLES
from psycopg2 import sql
from app.config import local_database_schema, docker_database_schema
import os

class CRUD:
    def __init__(self, user, password, docker:bool = False,):
        if docker:
            self.database = docker_database_schema["database"]
            self.host = docker_database_schema["host"]
        else:
            self.database = local_database_schema["database"]
            self.host = local_database_schema["host"]

        self.user = user
        self.password = password
        self.db = Connector(self.database,self.user,self.password,self.host)

    def create_mult_rows(self, table_name:str, rows: list[dict], schema_name:str='public', commit:bool = True, close:bool = True):
        """This method handles creating multiple new rows in a designated table of the database.
        rows must be a list of dictionaries, with keys being column names and values being, well... values.
        The close argument decides whether to close the connection after running the method. By default, it is true
        The commit argument decides whether a change should be commited to the database immediately. By default, it is true."""
        columns = TABLES.get(table_name).get("columns")
        if columns is None:
            raise ValueError(f"Unknown table: {table_name}")
        columns = list(columns.keys())

        #Check that all required columns are there
        for i, row in enumerate(rows):
            missing = [col for col in columns if col not in row]
            if missing:
                raise ValueError(f"Row {i} is missing columns for table '{table_name}': {missing}")

        column_names = [sql.Identifier(col_name) for col_name in columns]
        values = [[row[col] for col in columns] for row in rows]

        # Get the table-specific primary key
        pk_column = f"{table_name}_id"
        #Building the query
        query = sql.SQL("""INSERT INTO {}.{} ({})
        VALUES %s
        ON CONFLICT DO NOTHING
        RETURNING {}
        """).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_names),
            sql.Identifier(pk_column)
        )
        rows_created = self.db.execute_mult(query, values, commit=commit, close=close, table_name=table_name, returning_ids=True)
        return len(rows_created)

    def delete_all_rows(self, table_name:str,reset_id: bool = False):
        """This method deletes all rows of the given table.
        It is a nuclear option and should be handled with care."""
        query = sql.SQL("TRUNCATE TABLE {}").format(
        sql.Identifier(table_name)
        )
        if reset_id:
            query = sql.SQL("{} RESTART IDENTITY CASCADE").format(query)
        print(f"Deleting all rows from table {table_name}")
        self.db.execute(query, commit=True)

    def cleanse_db(self,reset_id: bool = False):
        """This method deletes every single row in every single table of the database.
        It is a nuclear option and should be handled with care.
        The optional variable reset_id tells the function whether to reset the ids in the tables."""
        for table in TABLES:
            self.delete_all_rows(table, reset_id)


    def reset_everything(self,reset_id: bool = False):
        self.cleanse_db(reset_id)
        file_path = "etl_times.json"

        # Check if the file exists first
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} has been deleted.")
        else:
            print(f"{file_path} does not exist.")

    def get_database(self):
        return self.database
    def get_host(self):
        return self.host