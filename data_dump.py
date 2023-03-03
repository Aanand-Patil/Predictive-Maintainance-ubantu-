import pymongo
import pandas as pd
import json

from sensor.config import mongo_client

# DATA_FILE_PATH="/config/workspace/aps_failure_training_set1.csv"
DATA_FILE_PATH = "/media/anand/ECD68F63D68F2D40/project/Predictive Maintainance(ubantu)/train_FD001.txt"
DATABASE_NAME = "NASA_Turbojet"
COLLECTION_NAME = "sensor"

if __name__ == "__main__":
    # columns names for data injection based on given txt file.
    index_names = ["unit_number", "time_cycles"]
    setting_names = ["setting_1", "setting_2", "setting_3"]
    sensor_names = ["s_{}".format(i + 1) for i in range(0, 21)]
    col_names = index_names + setting_names + sensor_names

    df = pd.read_csv(
        DATA_FILE_PATH, sep="\s+", header=None, index_col=False, names=col_names
    )

    def add_RUL_column(df):
        train_grouped_by_unit = df.groupby(by="unit_number")
        max_time_cycles = train_grouped_by_unit["time_cycles"].max()
        merged = df.merge(
            max_time_cycles.to_frame(name="max_time_cycle"),
            left_on="unit_number",
            right_index=True,
        )
        merged["RUL"] = merged["max_time_cycle"] - merged["time_cycles"]
        merged = merged.drop("max_time_cycle", axis=1)
        return merged

    df = add_RUL_column(df)

    print(f"Rows and columns: {df.shape}")

    # Convert dataframe to json so that we can dump these record in mongo db
    df.reset_index(drop=True, inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])
    # insert converted json record to mongo db
    mongo_client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
