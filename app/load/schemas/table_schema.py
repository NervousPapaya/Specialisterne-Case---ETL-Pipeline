TABLES = {"DMI": {
            "dmi_id": "UUID",
            "parameter_id": "VARCHAR(50)",
            "value": "DOUBLE PRECISION",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE",
            "station_id": "INT"},
        "BME280":{
            "reader_id": "UUID",
            "location": "VARCHAR(7) NOT NULL CHECK (location IN ('inside', 'outside'))",
            "humidity": "NUMERIC(20, 13)",
            "pressure": "NUMERIC(20, 13)",
            "temperature": "NUMERIC(20, 13)",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE"
            },
        "DS18B20":{
            "location":"VARCHAR(7) NOT NULL CHECK (location IN ('inside', 'outside'))",
            "temperature": "NUMERIC(20, 13)",
            "observed_at": "TIMESTAMP WITH TIME ZONE",
            "pulled_at": "TIMESTAMP WITH TIME ZONE"
        }}