from psycopg2 import sql
from app.load.db.connection import Connector
from app.config import database_schemas
import os


class RoleManager:
    """This class manages the roles in the database. It has a method for setting up all the roles on initialization.
    Then there are methods for creating a role and inheriting a role in general.
    Other than that each role needs a bespoke method based on their access privileges."""
    def __init__(self, db: Connector):
        self.db = db
        self.raw_data = database_schemas["raw_data_schema"]
        self.logs = database_schemas["log_schema"]

    def setup_roles(self):
        self.setup_analyst()
        self.setup_auditor()
        self.setup_engineer()
        self.setup_inspector()
        self.setup_logger()
        self.setup_viewer()

    def setup_analyst(self, close=False):
        name = os.getenv("DATA_ANALYST_USER")
        password = os.getenv("DATA_ANALYST_PASSWORD")
        self.create_role(name, password)
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(name)), commit=True,
                        close=False)
        # Grant table-level select
        self.db.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public to {}").format(sql.Identifier(name)),
                        commit=True, close=False)
        # Grant access to future tables
        self.db.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to {}").format(
                sql.Identifier(name)), commit=True, close=False
        )

        # Give access to existing and future sequences
        self.grant_usage_select_sequence_access(name,"public",future=True)

        if close:
            self.db.close()

    def setup_auditor(self, close=False):
        name = os.getenv("AUDITOR_USER")
        password = os.getenv("AUDITOR_PASSWORD")
        self.create_role(name, password)
        logs = self.logs
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(logs),sql.Identifier(name)), commit=True,
                        close=False)
        # Grant table-level select
        self.db.execute(sql.SQL("GRANT SELECT ON {}.tech_log TO {}").format(sql.Identifier(logs),sql.Identifier(name)), commit=True,
                        close=False)
        self.db.execute(sql.SQL("GRANT SELECT ON {}.business_log TO {}").format(sql.Identifier(logs),sql.Identifier(name)), commit=True,
                        close=False)

        if close:
            self.db.close()

    def setup_engineer(self, close=False):
        name = os.getenv("DATA_ENGINEER_USER")
        password = os.getenv("DATA_ENGINEER_PASSWORD")
        self.create_role(name, password)
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(self.raw_data),
            sql.Identifier(name)
        ), commit=True, close=False)
        # Grant table-level permission
        grant_current_perm_query = sql.SQL("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {} to {}").format(
            sql.Identifier(self.raw_data),
            sql.Identifier(name)
        )
        self.db.execute(grant_current_perm_query, commit=True, close=False)
        # Grant access to future tables
        grant_future_perm_query = sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT, INSERT, UPDATE ON TABLES TO {}").format(
            sql.Identifier(self.raw_data),
            sql.Identifier(name)
        )
        self.db.execute(grant_future_perm_query, commit=True, close=False)

        # Give access to existing sequences
        self.db.execute(
            sql.SQL("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(self.raw_data),
                sql.Identifier(name)
            ),
            commit=True, close=False
        )

        # Give access to future sequences
        self.db.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {}").format(
                sql.Identifier(self.raw_data),
                sql.Identifier(name)
            ),
            commit=True, close=False
        )

        if close:
            self.db.close()


    def setup_inspector(self, close=False):
        name = os.getenv("DATA_INSPECTOR_USER")
        password = os.getenv("DATA_INSPECTOR_PASSWORD")
        inherit_name = os.getenv("data_analyst_user")
        self.create_role(name, password)
        # Inherit the data analyst role
        self.inherit_role(name,inherit_name)

        schema = self.raw_data
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema),
            sql.Identifier(name)
        ), commit=True, close=False)
        # Grant table-level select
        grant_current_perm_query = sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} to {}").format(
            sql.Identifier(schema),
            sql.Identifier(name)
        )
        self.db.execute(grant_current_perm_query, commit=True, close=False)
        # Grant access to future tables
        grant_future_perm_query = sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
            sql.Identifier(schema),
            sql.Identifier(name)
        )
        self.db.execute(grant_future_perm_query, commit=True, close=False)

        if close:
            self.db.close()


    def setup_logger(self):
        name = os.getenv("LOGGER_USER")
        password = os.getenv("LOGGER_PASSWORD")
        logs=sql.Identifier(self.logs)
        self.create_role(name, password)
        name = sql.Identifier(name)
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            logs,
            name
        ), commit=True, close=False)

        # Grant table-level permission
        grant_current_perm_query = sql.SQL("GRANT INSERT ON ALL TABLES IN SCHEMA {} TO {}").format(
            logs,
            name
        )
        self.db.execute(grant_current_perm_query, commit=True, close=False)

        # Give access to existing sequences
        self.db.execute(
            sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                logs,
                name
            ),
            commit=True, close=False
        )

        # Give access to future sequences
        self.db.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT USAGE, SELECT ON SEQUENCES TO {}").format(
                logs,
                name
            ),
            commit=True, close=False
        )



    def setup_viewer(self, close=False):
        name = os.getenv("VIEWER_USER")
        password = os.getenv("VIEWER_PASSWORD")
        self.create_role(name, password)
        # Grant schema usage
        self.db.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(name)), commit=True,
                        close=False)
        # Grant table-level access
        self.db.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public to {}").format(sql.Identifier(name)),
                        commit=True, close=close)


        if close:
            self.db.close()

    def create_role(self, name, password):
        create_role_query = sql.SQL("""
        DO $$
        BEGIN
        IF NOT EXISTS (
           SELECT 1 from pg_Roles WHERE rolname = {role_literal}
        ) THEN
        CREATE ROLE {role_ident} LOGIN PASSWORD {password};
        ELSE  
        ALTER ROLE {role_ident} LOGIN PASSWORD {password};
        END IF;
        END$$;""").format(
            role_literal=sql.Literal(name),
            role_ident=sql.Identifier(name),
            password=sql.Literal(password)
        )
        self.db.execute(create_role_query, commit=True)

    def inherit_role(self,name, inherit_name):
        self.db.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(inherit_name), sql.Identifier(name)),
                        commit=True, close=False)

    def create_user_from_role(self,name,password,inherit_name):
        self.create_role(name,password)
        self.inherit_role(name,inherit_name)


    def grant_usage_select_sequence_access(self,name,schema,future:bool=False):
        """This method grants access to sequences.
        The optional argument future decides whether this access should be for future as well"""
        # Give access to existing sequences
        self.db.execute(
            sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema),
                sql.Identifier(name)
            ),
            commit=True, close=False
        )
        if future:
            # Give access to future sequences
            self.db.execute(
                sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT USAGE, SELECT ON SEQUENCES TO {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(name)
                ),
                commit=True, close=False
            )