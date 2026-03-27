from app.load.db.connection import Connector
from psycopg2 import sql
from pathlib import Path
import os

class TableCreator:

    def __init__(self, db:Connector):
        self.db = db

    def set_up_table(self, table_name: str, columns: dict, schema="public", close: bool = True):
        """This method is designed to set up a table.
        table_name is a string being the name of the table
        columns is a dict with keys being column names and values being the data types"""

        column_defs = [
            sql.SQL("{} {}").format(
                sql.Identifier(col_name),
                sql.SQL(col_type)
            )
            for col_name, col_type in columns.items()
        ]

        query = sql.SQL("""
         CREATE TABLE IF NOT EXISTS {}.{} (
         {} SERIAL PRIMARY KEY,
         {}
         );""").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.Identifier(f"{table_name}_id"),
            sql.SQL(",\n").join(column_defs)
        )

        self.db.execute(query, close=close, commit=True)

    def set_up_view_tables(self, close: bool = True):
        base_path = Path(__file__).resolve().parent
        sql_folder = (base_path / ".." / ".." / "sql" / "view_tables").resolve()
        for filename in os.listdir(sql_folder):
            if filename.endswith(".sql"):
                filepath = os.path.join(sql_folder, filename)
                self.db.execute_sql_file(filepath, close=close, commit=True)