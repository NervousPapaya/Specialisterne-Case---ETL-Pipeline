import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from app.config import database_schemas
from app.load.db.logger import DBLogger

class Connector:
    """This class handles the connection to the database."""
    def __init__(self,database,user,password, host):
        #NOTE: As this is an exercise, the server password has been hardcoded in.
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.conn = None
        self.schemas = database_schemas
        self.logger = DBLogger(database=self.database,host=self.host)

    def connect(self):
        """This method handles opening the connection to the database"""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(host = self.host, dbname=self.database, user=self.user, password=self.password)
                print(f"Connection established to database {self.database}")
            except Exception as e:
                raise RuntimeError(f"Connection not established: {e}")

    def close(self):
        """This method handles closing the connection to the database"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print(f"Closed connection to database {self.database}")
        if self.logger.conn:
            self.logger.close()

    def query(self, query: str,parameters = None):
        """This method handles querying the database. It fetches the result as a list of tuples."""
        if self.conn is None:
            raise RuntimeError("Database connection must be established before running a query. Call connect() first")
        cur = self.conn.cursor()
        cur.execute(query,parameters)
        rows = cur.fetchall()

        return rows


    def query_as_df(self,query: str, parameters = None):
        """This is alternative query method. It fetches the result as a dataframe."""
        if self.conn is None:
            raise RuntimeError("Database connection must be established before running a query. Call connect() first")
        cur = self.conn.cursor()
        cur.execute(query, parameters)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        return df

    def execute(self, statement,parameters = None, *, commit: bool = False, close: bool = True, log = False, table_name: str = None, returning_ids: bool = False):
        """This method is to handle more general executions sent to the database.
        The parameters can be either a list or a dictionary.
        The commit argument decides whether a change is commited to the database automatically. It is False by default.
        As the use case is different from that of the query method, this method does not assume the connection is open.
        It opens the connection if need be and has an argument which determines whether to close the connection.
         By default, the connection is closed at the end."""
        if not self.conn:
            self.connect()
        ids = []
        with self.conn.cursor() as cur:
            cur.execute(statement, parameters)
            if returning_ids:
                ids = [row[0] for row in cur.fetchall()] if cur.rowcount > 0 else []
            if log:
                user = self.get_user()
                self.logger.tech_log_execute(user, statement, table_name, row_ids=ids, close=False)
        if commit is True:
            self.conn.commit()
        if close is True:
            self.close()
        return ids

    def execute_mult(self, statement,parameters = None, *, commit: bool = False, close: bool = True, log=True, table_name: str = None, returning_ids:bool=False):
        """This method is to handle general executions sent to the database.
        The parameters can be either a list or a dictionary.
        The commit argument decides whether a change is commited to the database automatically. It is False by default.
        As the use case is different from that of the query method, this method does not assume the connection is open.
        It opens the connection if need be and has an argument which determines whether to close the connection.
         By default, the connection is closed at the end."""
        if not self.conn:
            self.connect()

        ids = []
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, statement, parameters)
                if returning_ids:
                    ids = [row[0] for row in cur.fetchall()] if cur.rowcount > 0 else []
                if log:
                    user = self.get_user()
                    self.logger.tech_log_execute(user,statement,table_name,row_ids=ids,close=False)
            if commit:
                self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise Exception(f"{e}")
        finally:
            if close:
                self.close()
            return ids

    def execute_sql_file(self,filepath,* , commit: bool = False, close: bool = True):
        """This method executes a SQL statement fed from a SQL file"""
        with open(filepath, 'r') as f:
            sql_statement = f.read()
        self.execute(sql_statement, close=close, commit=commit)

    def get_user(self):
        return self.conn.get_dsn_parameters().get("user")

