from app.extract.specialisterne import SpecAPI
from app.extract.dmi import DMIAPI
from app.transform.transform import SpecDataTransformer, DMIDataTransformer
from app.load.db.CRUD import CRUD
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

class ETLProcess:
    def __init__(self, docker: bool = False):
        self.crud = CRUD(docker)

    def update_database(self):
        """This unnamed functions job will be to handle both ETLs"""
        times_dict = self.get_start_times()

        # This handles the specialisterne etl
        self.spec_etl(from_time=times_dict["spec"])

        # The rest handles the dmi etl
        stations_file = "station_ids.json"
        try:
            base_path = Path(__file__).resolve().parent
            file_path = (base_path / ".." / "load" / "schemas" / stations_file).resolve()
            with open(file_path, "r", encoding="utf-8") as f:
                stations = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"Required file missing: {stations_file} in path {file_path}")

        params = [
            "temp_dry",
            "humidity",
            "pressure"
        ]
        for station in stations:
            for parameterId in params:
                self.dmi_etl(stations[station], parameterId, from_time=times_dict["DMI"][parameterId])

    def dmi_etl(self, station_id, parameter_id, from_time: str = "2026-03-09T00:00:00Z", max_pulls: int = None,
                limit: int = 5000):
        API = DMIAPI()
        start_time = time.time()
        total_pulls = 0
        offset = 0
        transformer = DMIDataTransformer()

        print(f"Pulling {parameter_id} data with stationid {station_id} from the DMI API")
        while True:
            pull_time, records = API.pull_datetime(station_id=station_id, parameter_id=parameter_id, limit=limit,
                                                   start_time=from_time, offset=offset)
            if not records:
                print("No more new records.")
                break

            # We set an offset. The DMI api pulls newest record first,
            offset += limit
            # This from_time is to be stored in an external JSON. It will be used for telling the etl process where to start next time it is run.
            from_time = self.advance_timestamp(max(r["properties"]["observed"] for r in records))

            db_dict = transformer.dmi_data_to_db_dict(pull_time, records)
            self.crud.create_mult_rows("DMI", db_dict, commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"{limit} records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            self.export_start_times(from_time, "DMI", parameter_id)

            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next {limit}. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if self.check_max_vs_total_pulls(max_pulls,total_pulls,start_time):
                break
        self.crud.db.close()

    def spec_etl(self, from_time: str = "2026-03-09T00:00:00Z", max_pulls: int = None, limit: int = 5000):
        API = SpecAPI()
        start_time = time.time()
        total_pulls = 0
        avoid_ids = set()
        transformer = SpecDataTransformer()

        print("Pulling data from the Specialisterne API")
        while True:
            pull_time, records = API.pull_from(limit=limit, from_time=from_time)

            if avoid_ids:
                records = self.remove_rows_by_id(records, avoid_ids)
            if not records:
                print("No more new records.")
                break
            # We create a new timestamp and will pull the next entries from there.
            # The timestamps of the BME280 and DS18B20 do not fully line up.
            # So we take the min of these and increment by a millisecond.
            # This means we will get a duplicate row (one entry per reader) when making the next pull.

            last_bme, last_ds = self.get_last_bme_and_ds(records)
            from_time = self.advance_timestamp(min(last_bme["timestamp"],last_ds["timestamp"]))

            # We explicitly make sure to remove those duplicates.
            # This is not necessary for the database, as the create_mult_rows method skips duplicates.
            # HOWEVER, it is necessary to ensure the process stops.
            # Indeed, if we don't remove them, there records will always have at least two records, and the process will hang.
            avoid_ids = {last_bme["id"], last_ds["id"]}

            db_dict = transformer.spec_data_to_db_dict(pull_time, records)
            for table_name in db_dict:
                self.crud.create_mult_rows(table_name, db_dict[table_name], commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"5000 records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            self.export_start_times(from_time, "spec")

            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next 5000. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if self.check_max_vs_total_pulls(max_pulls,total_pulls,start_time):
                break
        self.crud.db.close()

    def advance_timestamp(self, ts):
        """This function exists to increment timestamps by a single microsecond.
        This is to avoid duplicates in our pull requests."""
        dt = datetime.fromisoformat(ts[:-1]) + timedelta(microseconds=1)
        return dt.isoformat(timespec='microseconds') + "Z"

    def get_last_bme_and_ds(self,records):
        last_bme = None
        last_ds = None

        for r in records:
            if "BME280" in r["reading"]:
                last_bme = r
            else:
                last_ds = r

        return last_bme, last_ds

    def check_max_vs_total_pulls(self,max_pulls,total_pulls,start_time):
        if max_pulls is not None and total_pulls >= max_pulls:
            elapsed_time = time.time() - start_time
            print(f"Reached maximum number of pulls. Aborting ETL. Elapsed time: {elapsed_time}")
            return True
        return False

    def remove_rows_by_id(self, data, row_ids):
        """This function is specifically set up to remove multiple rows from the data from the Specialisterne Api."""
        return [row for row in data if row['id'] not in row_ids]

    def get_start_times(self):
        """This function grabs the start time from the file etl_times.json if they exist.
        Otherwise, it outputs a dictionary with default start time.
        The default time is the start date of this project.
        The keys of the dict
        """
        try:
            with open("etl_times.json", "r", encoding="utf-8") as f:
                times_dict = json.load(f)
        except FileNotFoundError:
            times_dict = {
                "DMI": {
                    "temp_dry": "2026-03-09T00:00:00Z",
                    "humidity": "2026-03-09T00:00:00Z",
                    "pressure": "2026-03-09T00:00:00Z"},
                "spec": "2026-03-09T00:00:00Z"
            }

        return times_dict

    def export_start_times(self, from_time, api: str, parameter_id=None):
        """This function handles exporting the time metadata to a JSON file.
        The times exported are used as starting points for any new pull requests sent to the two APIs we are working with.
        from_time is the time label
        api is the api used. the function expects 'DMI' or 'spec'. """

        times_dict = self.get_start_times()

        if api == "DMI":
            if parameter_id not in ["temp_dry", "humidity", "pressure"]:
                raise ValueError("""No or incorrect parameterId provided. 
            You must provide a parameter id from the list 'temp_dry, humidity, pressure' when the API is DMI.""")
            times_dict["DMI"][parameter_id] = from_time
        elif api == "spec":
            times_dict["spec"] = from_time
        else:
            raise ValueError("Incorrect API name given. The API must be 'DMI' or 'spec'.")

        with open("etl_times.json", "w", encoding="utf-8") as f:
            json.dump(times_dict, f, indent=4)
            print("etl_times_json exported")

