from sqlite3.dbapi2 import paramstyle

from extract.specialisterne import SpecAPI
from extract.dmi import DMIAPI
from load.db.initialize import DatabaseInitializer
from transform.transform import SpecDataTransformer, DMIDataTransformer
from load.db.CRUD import CRUD
import json
import time
from datetime import datetime, timedelta

def main():
    print("hej")
    initializer = DatabaseInitializer()
    initializer.create_db()
    initializer.initialize_db()
    crud = CRUD()
    crud.delete_all_rows("BME280")
    crud.delete_all_rows("DS18B20")
    spec_etl()


    #API = SpecAPI()
    #pull_time, records = API.pull_year_month_day(2,"2026","03","10")
    #for record in records:
        #print(record)

    #transformer = SpecDataTransformer()
    #db_dict = transformer.spec_data_to_db_dict(pull_time, records)
    #for table_name in db_dict:
        #crud.create_mult(table_name,db_dict[table_name])
    #spec_etl(max_pulls=2)
    stations = {
        "station_id_ballerup": "06181",
        "station_id_odense": "06126",
        "station_id_aarhus": "06072"
    }

    params = ["temp_dry","humidity","pressure"]

    for station in stations:
        for parameterId in params:
            dmi_etl(stations[station],parameterId)

def dmi_etl(station_id, parameter_id, from_time: str = "2026-03-09T00:00:00Z", max_pulls:int=None):
    API = DMIAPI()
    start_time = time.time()
    total_pulls = 0
    transformer = DMIDataTransformer()
    crud = CRUD()
    while True:
        while True:
            pull_time, records = API.pull_datetime(station_id=station_id, parameter_id=parameter_id, limit=5000, start_time=from_time)
            if not records:
                break
            # We create a new timestamp and will pull the next 5000 from there.



            from_time = advance_timestamp(max(r["properties"]["observed"] for r in records["features"]))
            db_dict = transformer.dmi_data_to_db_dict(pull_time, records)
            crud.create_mult("DMI", db_dict, commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"5000 records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            try:
                with open("etl_times.json", "r", encoding="utf-8") as f:
                    times_dict = json.load(f)
            except FileNotFoundError:
                times_dict = {"DMI": {"pull_time": pull_time, "from_time": from_time},
                              "spec": {"pull_time": None, "from_time": None}}

            times_dict["DMI"]["pull_time"] = pull_time
            times_dict["DMI"]["from_time"] = from_time
            with open("etl_times.json", "w", encoding="utf-8") as f:
                json.dump(times_dict, f, indent=4)
            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next 5000. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if max_pulls is not None and total_pulls >= max_pulls:
                elapsed_time = time.time() - start_time
                print(f"Reached maximum number of pulls. Aborting ETL. Elapsed time: {elapsed_time}")
                break
        crud.db.close()


def spec_etl(from_time: str = "2026-03-09T00:00:00Z", max_pulls:int=None):
    API = SpecAPI()
    start_time = time.time()
    total_pulls = 0
    transformer = SpecDataTransformer()
    crud = CRUD()
    while True:
        pull_time, records = API.pull_from(limit=5000,from_time=from_time)
        if not records:
            break
        #We create a new timestamp and will pull the next 5000 from there.
        # The timestamps of the BME280 and DS18B20 do not fully lign up. So we take the max of these.
        # This means we might miss a single record here or there, but we make sure not to get duplicates.
        #last_bme_index = -len(records) // 2 - 1
        #from_time = advance_timestamp(max(records[-1]["timestamp"], records[last_bme_index]["timestamp"]))
        from_time = advance_timestamp(max(r["timestamp"] for r in records))

        db_dict = transformer.spec_data_to_db_dict(pull_time,records)
        for table_name in db_dict:
            crud.create_mult(table_name,db_dict[table_name], commit=True, close=False)

        elapsed_time = time.time() - start_time
        print(f"5000 records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

        try:
            with open("etl_times.json", "r", encoding="utf-8") as f:
                times_dict = json.load(f)
        except FileNotFoundError:
            times_dict = {"DMI": {"pull_time": None, "from_time": None },
                          "spec": {"pull_time": pull_time, "from_time": from_time }}

        times_dict["spec"]["pull_time"] = pull_time
        times_dict["spec"]["from_time"] = from_time
        with open("etl_times.json", "w", encoding="utf-8") as f:
            json.dump(times_dict, f, indent=4)
        elapsed_time = time.time() - start_time
        print(f"json exported. Pulling next 5000. Elapsed time: {elapsed_time}")
        total_pulls += 1
        if max_pulls is not None and total_pulls >= max_pulls:
            elapsed_time = time.time() - start_time
            print(f"Reached maximum number of pulls. Aborting ETL. Elapsed time: {elapsed_time}")
            break
    crud.db.close()


def advance_timestamp(ts):
    """This function exists to increment timestamps by a single microsecond. This is to avoid overlapping, and """
    dt = datetime.fromisoformat(ts[:-1]) + timedelta(microseconds=1)
    return dt.isoformat(timespec='microseconds') + "Z"

if __name__ == "__main__":
    main()