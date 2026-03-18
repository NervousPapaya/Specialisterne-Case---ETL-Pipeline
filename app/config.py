# This script is for setting up the variables used to connect to the database.
# They are used by the script initialize.py
#There are two schemas. One is for working with a local database in postgres.
#The other is for running in docker.
import os
from dotenv import load_dotenv

load_dotenv()

spec_token = os.getenv("SPEC_TOKEN")

#local database
local_database_schema = {"database": os.getenv("LOCAL_DB"),
                   "user":os.getenv("LOCAL_USER"),
                   "password":os.getenv("LOCAL_PASSWORD"),
                   "host": os.getenv("LOCAL_HOST")}

#Docker
docker_database_schema = {
    "database": os.getenv("DB_NAME"),
    "user":os.getenv("DB_USER"),
    "password":os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST")
}
