import pandas as pd
from pandas import DataFrame
import logging

logging.basicConfig(filename="logs.log", format="%(asctime)s %(message)s", filemode="a")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def read_data() -> tuple[DataFrame, DataFrame]:
    """
    Reads the dataset from the csv files and returns them.
    :return: tuple of (dataset1, dataset2)
    """
    logger.info("Start task using Pandas Framework")
    dataset1 = pd.read_csv("Input/dataset1.csv")
    dataset2 = pd.read_csv("Input/dataset2.csv")
    logger.info("Dropped the 'invoice_id' column")
    dataset1 = dataset1.drop("invoice_id", axis=1)
    return dataset1, dataset2


def transform_data(dataset: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Apply data transformations
    """
    logger.info("Applying data transformation")
    arap_data = dataset[dataset["status"] == "ARAP"]
    accr_data = dataset[dataset["status"] == "ACCR"]
    arap_grouped = (
        arap_data.groupby(["legal_entity", "counter_party"])
        .agg({"value": "sum", "rating": "max"})
        .rename(columns={"value": "arap_value"})
    )
    accr_grouped = (
        accr_data.groupby(["legal_entity", "counter_party"])
        .agg({"value": "sum", "rating": "max"})
        .rename(columns={"value": "accr_value"})
    )
    return arap_grouped, accr_grouped


def merge_data(
    arap_data: DataFrame, accr_data: DataFrame, dataset2: DataFrame
) -> DataFrame:
    """
    Merge dataframes
    """
    merged_data = pd.merge(
        arap_data, accr_data, on=["legal_entity", "counter_party"], how="outer"
    )
    merged_data = merged_data.rename(
        columns={
            "rating_x": "max_rating_x",
            "arap_value_x": "arap_value",
            "arap_value_y": "arap_value",
            "rating_y": "max_rating_y",
        }
    )
    merged_data = merged_data.reset_index()
    final_data = merged_data.merge(dataset2, on=["counter_party"])
    return final_data


def process_data(final_data: DataFrame) -> DataFrame:
    """
    Perform final processing
    """
    logger.info("Final processing")
    final_data["ratings"] = final_data[["max_rating_x", "max_rating_y"]].max(axis=1)
    final_data.fillna(0, inplace=True)
    final_data_1 = final_data.drop(["max_rating_x", "max_rating_y"], axis=1)
    final_data_1 = final_data_1[
        ["legal_entity", "counter_party", "tier", "ratings", "arap_value", "accr_value"]
    ]
    return final_data_1


def main():
    logger.info("\n\nPandas Solution Logs")
    dataset1, dataset2 = read_data()
    arap_data, accr_data = transform_data(dataset1)
    final_data = merge_data(arap_data, accr_data, dataset2)
    final_data_1 = process_data(final_data)
    final_data_1.to_csv("Output/pandas_sol.csv")
    logger.info("Process completed using Pandas Framework")


if __name__ == "__main__":
    main()
