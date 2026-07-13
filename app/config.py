# This script is for setting up the variables used to connect to the database.
# They are used by the script initialize.py
#There are two schemas. One is for working with a local database in postgres.
#The other is for running in docker.
import os
from dotenv import load_dotenv

load_dotenv()

#Token for connecting to the old Specialisterne API
spec_token = os.getenv("SPEC_TOKEN")

#Token for connecting to the new Specialisterne API
new_spec_token = os.getenv("NEW_SPEC_TOKEN")

#Variable to change whether you are running in Docker or not
docker = False

#The name of the database schemas
database_schemas = {"raw_data_schema":os.getenv("RAW_DATA_SCHEMA"), "log_schema":os.getenv("LOG_SCHEMA")}


#local database connection details
local_database_schema = {"database": os.getenv("LOCAL_DB"),
                   "user":os.getenv("LOCAL_USER"),
                   "password":os.getenv("LOCAL_PASSWORD"),
                   "host": os.getenv("LOCAL_HOST")}

#Docker database connection details
docker_database_schema = {
    "database": os.getenv("DB_NAME"),
    "user":os.getenv("DB_USER"),
    "password":os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST")
}
