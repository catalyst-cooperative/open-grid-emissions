import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.distributed import Client

# # Tell python where to look for modules.
import sys

sys.path.append("../../hourly-egrid/")

# import local modules
import src.data_cleaning as data_cleaning


def main():
    """
    Dask tester: load intermediate files from a --small run, then run data combining and BA aggregation steps on small workers.
    Replicates unmanaged memory worker failures in much faster time, allowing for quick debugging. 
    """

    # Start client so can see worker mem use

    client = Client(
        n_workers=2, threads_per_worker=2, memory_limit="1GB"
    )  # limit worker size to create issues even on --small dataset
    print(f"Dash dashboard at {client.dashboard_link}")

    o_shaped_eia_data = pd.read_csv(
        "../data/outputs/small/shaped_eia923_data_2020.csv",
        parse_dates=["datetime_utc"],
    )
    o_partial_cems_scaled = pd.read_csv(
        "../data/outputs/small/partial_cems_scaled_2020.csv",
        parse_dates=["datetime_utc"],
    )  # NOT FINAL VERSION
    o_cems = pd.read_csv(
        "../data/outputs/small/cems_2020.csv", parse_dates=["datetime_utc"]
    )  # NOT FINAL VERSION

    plant_static_attributes = pd.read_csv(
        "../data/results/small/plant_data/plant_static_attributes.csv"
    )

    o_partial_cems_scaled = o_partial_cems_scaled.merge(
        plant_static_attributes, how="left", on="plant_id_eia"
    )
    o_cems = o_cems.merge(plant_static_attributes, how="left", on="plant_id_eia")

    # Problem data_pipeline steps: on small data, these are fine with worker size 16GB (break with worker size 1GB)
    # On full dataset, these break with worker size 16GB (ie Gailin's laptop)

    combined_plant_data = data_cleaning.combine_subplant_data(
        o_cems, o_partial_cems_scaled, o_shaped_eia_data
    )

    print(combined_plant_data.index)
    print(combined_plant_data.groupby("datetime_utc").sum().compute().head())

    # # 12. Aggregate CEMS data to BA-fuel and write power sector results
    # ba_fuel_data = data_cleaning.aggregate_plant_data_to_ba_fuel(
    #     combined_plant_data, plant_static_attributes
    # )

    # print(ba_fuel_data.head())


if __name__ == "__main__":
    main()
