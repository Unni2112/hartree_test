import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import logging

# Configure the logger
logging.basicConfig(filename="logs.log", format="%(asctime)s %(message)s", filemode="a")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def process_data(df_arap, df_accr, df_beam):
    """
    Process and merge dataframes using Apache Beam
    """
    logger.info("Processing and merging data")
    arap_grouped = df_arap.pivot_table(
        values=["value", "rating"],
        index=["legal_entity", "counter_party"],
        aggfunc={"value": "sum", "rating": "max"},
    ).rename(columns={"value": "arap_value", "rating": "rating_x"})
    accr_grouped = df_accr.pivot_table(
        values=["value", "rating"],
        index=["legal_entity", "counter_party"],
        aggfunc={"value": "sum", "rating": "max"},
    ).rename(columns={"value": "accr_value", "rating": "rating_y"})
    df_merged = arap_grouped.join(
        accr_grouped, on=["legal_entity", "counter_party"], how="outer"
    ).reset_index()
    df_final = df_merged.join(df_beam.set_index("counter_party"), on="counter_party")
    df_final["ratings"] = df_final[["rating_x", "rating_y"]].max(axis=1)
    df_final.fillna(0, inplace=True)
    df_result = df_final.drop(["rating_x", "rating_y"], axis=1)
    df_result = df_result[
        ["legal_entity", "counter_party", "tier", "ratings", "arap_value", "accr_value"]
    ]
    return df_result


def main():
    logger.info("\n\nApache Beam Solution Logs")
    logger.info("Start task using Apache-beam Framework")

    # Create pipelines
    pipeline1 = beam.Pipeline(InteractiveRunner())
    pipeline2 = beam.Pipeline(InteractiveRunner())
    pipeline3 = beam.Pipeline(InteractiveRunner())

    df_arap = (
        pipeline1
        | "Read CSV" >> beam.dataframe.io.read_csv("Input/dataset1.csv")
        | "Filter values" >> beam.Filter(lambda record: record[4] == "ARAP")
    )

    df_accr = (
        pipeline2
        | "Read CSV" >> beam.dataframe.io.read_csv("Input/dataset1.csv")
        | "Filter values" >> beam.Filter(lambda record: record[4] == "ACCR")
    )

    df_beam = pipeline3 | "Read CSV" >> beam.dataframe.io.read_csv("Input/dataset2.csv")

    # Collect dataframes
    df1 = ib.collect(df_arap)
    df2 = ib.collect(df_accr)
    df3 = ib.collect(df_beam)

    # Process and merge data
    df_result = process_data(df1, df2, df3)
    df_result.to_csv("Output/apache_beam_sol.csv")
    logger.info("Process completed using Apache-beam Framework")


if __name__ == "__main__":
    main()
