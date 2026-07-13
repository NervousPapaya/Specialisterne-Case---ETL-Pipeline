from app.config import database_schemas
raw_data_schema = database_schemas.get("raw_data_schema")
log_schema = database_schemas.get("log_schema")

TABLES = {
    "DMI": {
        "schema": raw_data_schema,
        "columns":{
            "dmi_id": "UUID UNIQUE NOT NULL",
            "parameter_id": "VARCHAR(50) NOT NULL",
            "value": "DOUBLE PRECISION NOT NULL",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE",
            "station_id": "VARCHAR(5)"
            }
        },
    "BME280":{
        "schema": raw_data_schema,
        "columns":{
            "reader_id": "UUID UNIQUE NOT NULL",
            "location": "VARCHAR(7) NOT NULL CHECK (location IN ('inside', 'outside'))",
            "humidity": "NUMERIC(20, 13) NOT NULL",
            "pressure": "NUMERIC(20, 13) NOT NULL",
            "temperature": "NUMERIC(20, 13) NOT NULL",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE"
            }
        },
    "DS18B20":{
        "schema": raw_data_schema,
        "columns":{
            "reader_id": "UUID UNIQUE NOT NULL",
            "location":"VARCHAR(7) NOT NULL CHECK (location IN ('inside', 'outside'))",
            "temperature": "NUMERIC(20, 13) NOT NULL",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE"
            }
        },
    "SCD41":{
        "schema": raw_data_schema,
        "columns":{
            "reader_id": "UUID UNIQUE",
            "co2": "INT",
            "humidity": "NUMERIC(20, 13)",
            "temperature": "NUMERIC(20, 13)",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE"
            }
    },
    "tech_log":{
        "schema": log_schema,
        "columns":{
        "username": "TEXT NOT NULL",
        "action": "TEXT NOT NULL",
        "table_name": "TEXT",
        "affected_rows": "INTEGER",
        "affected_row_ids": "INTEGER[]",
        "executed_at": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
        }
    },
    "business_log": {
        "schema": log_schema,
        "columns": {
            "user_or_process": "TEXT NOT NULL",
            "event_type": "TEXT NOT NULL",
            "message": "TEXT",
            "metadata": "JSONB",
            "timestamp": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
        }
    }
}