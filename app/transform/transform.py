
class DMIDataTransformer:
    def __init__(self):
        pass

    def dmi_data_to_db_dict(self, pull_time, data):
        """This method transforms the data recorded from the DMI API.
        It pulls out the data we want, and outputs a list of dictionaries.
        Each dictionary corresponds to a row in the database."""
        filtered_data = [{
            "dmi_id": feature["id"],
            "parameter_id": feature["properties"]["parameterId"],
            "value": feature["properties"]["value"],
            "observed_at": feature["properties"]["observed"],
            "pulled_at": pull_time,
            "station_id": feature["properties"]["stationId"]
        } for feature in data]
        return filtered_data


class SpecDataTransformer:
    def __init__(self):
        pass

    def bme_record_to_dict(self,record):
        """This method transforms the data recorded from the BME280 data in the Specialisterne's API.
        It pulls out the data we want, and changes temperature from millicelsius to Celsius.
        The method outputs a dictionary."""
        db_dict = {}
        db_dict["reader_id"] = record['id']
        db_dict["location"] = "outside"
        read_dict = record.get("reading").get("BME280")
        for col in ["humidity","temperature"]:
            db_dict[col] = read_dict[col]
        db_dict["pressure"] = read_dict["pressure"]/100
        db_dict["observed_at"] = record["timestamp"]

        return db_dict

    def ds_record_to_dict(self,record):
        """This method transforms the data recorded from the DS18B20 data in the Specialisterne's API.
        It pulls out the data we want, and changes temperature from millicelsius to Celsius.
        The method outputs a dictionary."""
        db_dict = {}
        db_dict["reader_id"] = record['id']
        if record["reading"]["DS18B20"]["device_name"] == "28-0000003e33d5":
            db_dict["location"] = "outside"
        read_dict = record.get("reading").get("DS18B20")
        db_dict["temperature"] = read_dict["raw_reading"]/1000
        db_dict["observed_at"] = record["timestamp"]

        return db_dict

    def spec_data_to_db_dict(self,pull_time, data):
        """This method takes the recorded data from the specialisterne API, and transforms it to a dict of two lists.
        The keys of the dict are the table names for the lists to be put into.
        It returns a dict with lists of dicts (a dict per record) where the parent dict is keyed by the names of the readers (which are also the target table names)."""
        bme_db_list = []
        ds_db_list = []
        db_dict = {"BME280": bme_db_list, "DS18B20": ds_db_list}
        for record in data:
            device = list(record.get("reading").keys())[0]
            if device == "BME280":
                bme_dict = self.bme_record_to_dict(record)
                bme_dict["pulled_at"] = pull_time
                bme_db_list.append(bme_dict)
            if device =="DS18B20":
                ds_dict = self.ds_record_to_dict(record)
                ds_dict["pulled_at"] = pull_time
                ds_db_list.append(ds_dict)
        return db_dict


    def new_bme_record_to_dict(self,record):
        """This method transforms the data recorded from the BME280 data in the Specialisterne's API.
        It pulls out the data we want, and changes temperature from millicelsius to Celsius.
        The method outputs a dictionary."""
        db_dict = {}
        db_dict["reader_id"] = record['id']
        if record['location']['value'] == "000000005b900eb3-percepter-ballerup-out":
            db_dict["location"] = "outside"
        elif record['location']['value'] == '00000000adae116e-percepter-ballerup-in':
            db_dict["location"] = "inside"
        read_dict = record.get("reading").get("BME280")
        for col in ["humidity","temperature"]:
            db_dict[col] = read_dict[col]
        db_dict["pressure"] = read_dict["pressure"]/100
        db_dict["observed_at"] = record["timestamp"]

        return db_dict

    def new_ds_record_to_dict(self,record):
        """This method transforms the data recorded from the DS18B20 data in the Specialisterne's API.
        It pulls out the data we want, and changes temperature from millicelsius to Celsius.
        The method outputs a dictionary."""
        db_dict = {}
        db_dict["reader_id"] = record['id']
        if record['location']['value'] == "000000005b900eb3-percepter-ballerup-out":
            db_dict["location"] = "outside"
        elif record['location']['value'] == '00000000adae116e-percepter-ballerup-in':
            db_dict["location"] = "inside"
        read_dict = record.get("reading").get("DS18B20")
        db_dict["temperature"] = read_dict["raw_reading"]/1000
        db_dict["observed_at"] = record["timestamp"]

        return db_dict


    def scd_record_to_dict(self, record):
        """This method transforms the data recorded from the SCD41 in the Specialisterne's API.
        It pulls out the data we want, and converts temperature and humidity from raw readings to humane values.
        The method outputs a dictionary."""
        db_dict = {}
        db_dict["reader_id"] = record['id']
        read_dict = record.get("reading").get("SCD41")
        db_dict["co2"] = read_dict["co2"]
        db_dict["humidity"] = read_dict["humidity"]/65535*100
        db_dict["temperature"] = 175*read_dict["temperature"]/65535 -45
        db_dict["observed_at"] = record["timestamp"]

        return db_dict
    def new_spec_data_to_db_dict(self,pull_time, data):
        """This method takes the recorded data from the specialisterne API, and transforms it to a dict of two lists.
        The keys of the dict are the table names for the lists to be put into.
        It returns a dict with lists of dicts (a dict per record) where the parent dict is keyed by the names of the readers (which are also the target table names)."""
        bme_db_list = []
        ds_db_list = []
        scd_db_list = []
        db_dict = {"BME280": bme_db_list, "DS18B20": ds_db_list, "SCD41": scd_db_list}
        for record in data:
            device = list(record.get("reading").keys())[0]
            if device == "BME280":
                bme_dict = self.new_bme_record_to_dict(record)
                bme_dict["pulled_at"] = pull_time
                bme_db_list.append(bme_dict)
            if device == "DS18B20":
                ds_dict = self.new_ds_record_to_dict(record)
                ds_dict["pulled_at"] = pull_time
                ds_db_list.append(ds_dict)
            if device == "SCD41":
                scd_dict = self.scd_record_to_dict(record)
                scd_dict["pulled_at"] = pull_time
                scd_db_list.append(scd_dict)
        return db_dict