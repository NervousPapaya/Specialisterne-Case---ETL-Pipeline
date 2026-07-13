import os
from psycopg2 import sql
import psycopg2
from app.config import database_schemas
import json

class DBLogger:
    """The purpose of this class is to log activity in the database. """
    def __init__(self, database, host):
        self.database = database
        self.host = host
        self.user = os.getenv("LOGGER_USER")
        self.password = os.getenv("LOGGER_PASSWORD")
        self.conn = None
        self.log_schema = database_schemas["log_schema"]

    def connect(self):
        """This method handles opening the connection to the database"""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(host = self.host, dbname=self.database, user=self.user, password=self.password)
            except Exception as e:
                raise RuntimeError(f"DBLogger failed to establish connection: {e}")

    def close(self):
        """This method handles closing the connection to the database"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print(f"Closed connection to database {self.database}")

    def tech_log_execute(self,user,sql_statement,table_name=None,row_ids=None,close=True):
        self.connect()
        try:
            with self.conn.cursor() as cur:
                if hasattr(sql_statement, "as_string"):
                    sql_string=sql_statement.as_string(self.conn)
                else:
                    sql_string = str(sql_statement)
                action = str(sql_string).strip().split()[0].upper()
                affected_rows, row_ids_to_store = self.check_row_ids(row_ids)
                table_name = table_name or "unknown"

                log_statement= sql.SQL("""
                INSERT INTO {}.tech_log (username,action,table_name,affected_rows,affected_row_ids,executed_at)
                VALUES (%s,%s,%s,%s,%s, CURRENT_TIMESTAMP)
                    """).format(sql.Identifier(self.log_schema))

                cur.execute(log_statement,(user,action,table_name, affected_rows,row_ids_to_store))
                self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"Logging failed: {e}")
        finally:
            if close:
                self.close()


    def check_row_ids(self,row_ids:list=None):
        if row_ids is not None:
            affected_rows = len(row_ids)
            if affected_rows > 100:
                row_ids_to_store = None
            else:
                row_ids_to_store = row_ids
        else:
            affected_rows = None
            row_ids_to_store = None
        return affected_rows, row_ids_to_store

    def log_business_event(self, user_or_process: str, event_type: str, message: str, metadata: dict = None, close=True):
        self.connect()
        try:
            with self.conn.cursor() as cur:
                log_statement = sql.SQL("""
                INSERT INTO {}.business_log (user_or_process, event_type, message, metadata, timestamp)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                """).format(sql.Identifier(self.log_schema))

                # Convert metadata dict to JSON if not None
                json_metadata = json.dumps(metadata or {})
                cur.execute(log_statement, (user_or_process, event_type, message, json_metadata))
                self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"Logging failed: {e}")
        finally:
            if close:
                self.close()