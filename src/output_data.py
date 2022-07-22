import pandas as pd
import numpy as np

import src.load_data as load_data
import src.column_checks as column_checks

GENERATED_EMISSION_RATE_COLS = [
    "generated_co2_rate_lb_per_mwh_for_electricity",
    "generated_ch4_rate_lb_per_mwh_for_electricity",
    "generated_n2o_rate_lb_per_mwh_for_electricity",
    "generated_co2e_rate_lb_per_mwh_for_electricity",
    "generated_nox_rate_lb_per_mwh_for_electricity",
    "generated_so2_rate_lb_per_mwh_for_electricity",
    "generated_co2_rate_lb_per_mwh_for_electricity_adjusted",
    "generated_ch4_rate_lb_per_mwh_for_electricity_adjusted",
    "generated_n2o_rate_lb_per_mwh_for_electricity_adjusted",
    "generated_co2e_rate_lb_per_mwh_for_electricity_adjusted",
    "generated_nox_rate_lb_per_mwh_for_electricity_adjusted",
    "generated_so2_rate_lb_per_mwh_for_electricity_adjusted",
]

UNIT_CONVERSIONS = {"lb": ("kg", 0.453592), "mmbtu": ("GJ", 1.055056)}

TIME_RESOLUTIONS = {"hourly": "H", "monthly": "M", "annual": "A"}


def output_intermediate_data(df, file_name, path_prefix, year, skip_outputs):
    column_checks.check_columns(df, file_name)
    if not skip_outputs:
        print(f"    Exporting {file_name} to data/outputs")
        df.to_csv(f"../data/outputs/{path_prefix}{file_name}_{year}.csv", index=False)


def output_to_results(df, file_name, subfolder, path_prefix, skip_outputs):
    if not skip_outputs:
        print(f"    Exporting {file_name} to data/results/{path_prefix}{subfolder}")

        metric = convert_results(df)

        df.to_csv(
            f"../data/results/{path_prefix}{subfolder}us_units/{file_name}.csv",
            index=False,
        )
        metric.to_csv(
            f"../data/results/{path_prefix}{subfolder}metric_units/{file_name}.csv",
            index=False,
        )


def output_data_quality_metrics(df, file_name, path_prefix, skip_outputs):
    if not skip_outputs:
        print(
            f"    Exporting {file_name} to data/results/{path_prefix}data_quality_metrics"
        )

        df.to_csv(
            f"../data/results/{path_prefix}data_quality_metrics/{file_name}.csv",
            index=False,
        )


def output_plant_data(df, path_prefix, resolution, skip_outputs):
    """
    Helper function for plant-level output.
    Output for each time granularity, and output separately for real and synthetic plants

    Note: plant-level does not include rates, so all aggregation is summation.
    """
    if not skip_outputs:
        if resolution == "hourly":
            # output hourly data
            # Separately save real and aggregate plants
            output_to_results(
                df[df.plant_id_eia > 900000],
                "synthetic_plant_data",
                "plant_data/hourly/",
                path_prefix,
                skip_outputs,
            )
            output_to_results(
                df[df.plant_id_eia < 900000],
                "CEMS_plant_data",
                "plant_data/hourly/",
                path_prefix,
                skip_outputs,
            )
        elif resolution == "monthly":
            # output monthly data
            output_to_results(
                df,
                "plant_data",
                "plant_data/monthly/",
                path_prefix,
                skip_outputs,
            )
        elif resolution == "annual":
            # output annual data
            df = df.groupby(["plant_id_eia"], dropna=False).sum().reset_index()
            # Separately save real and aggregate plants
            output_to_results(
                df,
                "plant_data",
                "plant_data/annual/",
                path_prefix,
                skip_outputs,
            )


def convert_results(df):
    """
    Take df in US units (used throughout pipeline).
    Return a df with metric units.

    ASSUMPTIONS:
        * Columns to convert have names of form
            `co2_lb_per_mwh_produced` (mass),
            `co2_lb_per_mwh_produced_for_electricity` (rate),
            `fuel_consumed_mmbtu` (mass)
          meaning that unit to convert is ALWAYS in numerator
    """
    converted = df.copy(deep=True)
    for column in converted.columns:
        unit = ""
        for u in UNIT_CONVERSIONS.keys():  # What to convert?
            if u in column.split("_"):
                unit = u
                break
        if unit == "":
            continue  # nothing to convert, next column
        new_unit, factor = UNIT_CONVERSIONS[unit]
        new_col = column.replace(unit, new_unit)
        converted.rename(columns={column: new_col}, inplace=True)
        converted.loc[:, new_col] = converted.loc[:, new_col] * factor
    return converted


def write_generated_averages(ba_fuel_data, year, path_prefix, skip_outputs):
    if not skip_outputs:
        avg_fuel_type_production = (
            ba_fuel_data.groupby(["fuel_category"]).sum().reset_index()
        )
        # Add row for total before taking rates
        total = avg_fuel_type_production.mean(numeric_only=True).to_frame().T
        total.loc[0, "fuel_category"] = "total"
        avg_fuel_type_production = pd.concat([avg_fuel_type_production, total], axis=0)

        # Find rates
        for emission_type in ["_for_electricity", "_for_electricity_adjusted"]:
            for emission in ["co2", "ch4", "n2o", "co2e", "nox", "so2"]:
                avg_fuel_type_production[
                    f"generated_{emission}_rate_lb_per_mwh{emission_type}"
                ] = (
                    (
                        avg_fuel_type_production[f"{emission}_mass_lb{emission_type}"]
                        / avg_fuel_type_production["net_generation_mwh"]
                    )
                    .fillna(0)
                    .replace(np.inf, np.NaN)
                    .replace(-np.inf, np.NaN)
                    .replace(
                        np.NaN, 0
                    )  # TODO: temporary placeholder while solar is broken. Eventually there should be no NaNs.
                )
        output_intermediate_data(
            avg_fuel_type_production,
            "annual_generation_averages_by_fuel",
            path_prefix,
            year,
            skip_outputs,
        )


def write_plant_metadata(
    cems, partial_cems, shaped_eia_data, path_prefix, skip_outputs
):
    """Outputs metadata for each subplant-hour."""

    KEY_COLUMNS = [
        "plant_id_eia",
        "subplant_id",
        "report_date",
    ]

    METADATA_COLUMNS = [
        "data_source",
        "hourly_profile_source",
        "net_generation_method",
    ]

    if not skip_outputs:
        # identify the source
        cems["data_source"] = "CEMS"
        partial_cems["data_source"] = "partial CEMS/EIA"
        shaped_eia_data["data_source"] = "EIA"

        # identify net generation method
        cems = cems.rename(columns={"gtn_method": "net_generation_method"})
        shaped_eia_data["net_generation_method"] = shaped_eia_data["profile_method"]
        partial_cems["net_generation_method"] = "partial_cems"

        # identify hourly profile method
        cems["hourly_profile_source"] = "CEMS"
        partial_cems["hourly_profile_source"] = "partial CEMS"
        shaped_eia_data = shaped_eia_data.rename(
            columns={"profile_method": "hourly_profile_source"}
        )

        # only keep one metadata row per plant/subplant-month
        cems_meta = cems.copy()[KEY_COLUMNS + METADATA_COLUMNS].drop_duplicates(
            subset=KEY_COLUMNS
        )
        partial_cems_meta = partial_cems.copy()[
            KEY_COLUMNS + METADATA_COLUMNS
        ].drop_duplicates(subset=KEY_COLUMNS)
        shaped_eia_data_meta = shaped_eia_data.copy()[
            ["plant_id_eia", "report_date"] + METADATA_COLUMNS
        ].drop_duplicates(subset=["plant_id_eia", "report_date"])

        # concat the metadata into a one file and export
        metadata = pd.concat(
            [cems_meta, partial_cems_meta, shaped_eia_data_meta], axis=0
        )

        metadata.to_csv(f"../data/results/{path_prefix}plant_data/plant_metadata.csv")

        # drop the metadata columns from each dataframe
        cems = cems.drop(columns=METADATA_COLUMNS)
        partial_cems = partial_cems.drop(columns=METADATA_COLUMNS)
        shaped_eia_data = shaped_eia_data.drop(columns=METADATA_COLUMNS)

    return cems, partial_cems, shaped_eia_data


def write_power_sector_results(ba_fuel_data, path_prefix, skip_outputs):
    """
    Helper function to write combined data by BA
    """

    data_columns = [
        "net_generation_mwh",
        "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu",
        "co2_mass_lb",
        "ch4_mass_lb",
        "n2o_mass_lb",
        "co2e_mass_lb",
        "nox_mass_lb",
        "so2_mass_lb",
        "co2_mass_lb_for_electricity",
        "ch4_mass_lb_for_electricity",
        "n2o_mass_lb_for_electricity",
        "co2e_mass_lb_for_electricity",
        "nox_mass_lb_for_electricity",
        "so2_mass_lb_for_electricity",
        "co2_mass_lb_adjusted",
        "ch4_mass_lb_adjusted",
        "n2o_mass_lb_adjusted",
        "co2e_mass_lb_adjusted",
        "nox_mass_lb_adjusted",
        "so2_mass_lb_adjusted",
        "co2_mass_lb_for_electricity_adjusted",
        "ch4_mass_lb_for_electricity_adjusted",
        "n2o_mass_lb_for_electricity_adjusted",
        "co2e_mass_lb_for_electricity_adjusted",
        "nox_mass_lb_for_electricity_adjusted",
        "so2_mass_lb_for_electricity_adjusted",
    ]

    if not skip_outputs:
        for ba in list(ba_fuel_data.ba_code.unique()):
            if type(ba) is not str:
                print(
                    f"Warning: not aggregating {sum(ba_fuel_data.ba_code.isna())} plants with numeric BA {ba}"
                )
                continue

            # filter the data for a single BA
            ba_table = ba_fuel_data[ba_fuel_data["ba_code"] == ba].drop(
                columns="ba_code"
            )

            # convert the datetime_utc column back to a datetime
            ba_table["datetime_utc"] = pd.to_datetime(
                ba_table["datetime_utc"], utc=True
            )

            # calculate a total for the BA
            ba_total = (
                ba_table.groupby(["datetime_utc"], dropna=False)
                .sum()[data_columns]
                .reset_index()
            )
            ba_total["fuel_category"] = "total"

            # concat the totals to the fuel-specific totals
            ba_table = pd.concat([ba_table, ba_total], axis=0, ignore_index=True)

            # round all values to one decimal place
            ba_table = ba_table.round(2)

            def add_generated_emission_rate_columns(df):
                for emission_type in ["_for_electricity", "_for_electricity_adjusted"]:
                    for emission in ["co2", "ch4", "n2o", "co2e", "nox", "so2"]:
                        df[f"generated_{emission}_rate_lb_per_mwh{emission_type}"] = (
                            (
                                df[f"{emission}_mass_lb{emission_type}"]
                                / df["net_generation_mwh"]
                            )
                            .fillna(0)
                            .replace(np.inf, np.NaN)
                            .replace(-np.inf, np.NaN)
                        )
                return df

            # output the hourly data
            ba_table_hourly = add_generated_emission_rate_columns(ba_table)

            # create a local datetime column
            try:
                local_tz = load_data.ba_timezone(ba, "local")
                ba_table_hourly["datetime_local"] = ba_table_hourly[
                    "datetime_utc"
                ].dt.tz_convert(local_tz)
            # TODO: figure out what to do for missing ba
            except ValueError:
                ba_table_hourly["datetime_local"] = pd.NaT

            # re-order columns
            ba_table_hourly = ba_table_hourly[
                ["fuel_category", "datetime_local", "datetime_utc"]
                + data_columns
                + GENERATED_EMISSION_RATE_COLS
            ]

            # export to a csv
            output_to_results(
                ba_table_hourly,
                ba,
                "power_sector_data/hourly/",
                path_prefix,
                skip_outputs,
            )

            # aggregate data to monthly
            ba_table_monthly = (
                ba_table.groupby(["fuel_category", "report_date"], dropna=False)
                .sum()
                .reset_index()
            )
            ba_table_monthly = add_generated_emission_rate_columns(ba_table_monthly)
            # re-order columns
            ba_table_monthly = ba_table_monthly[
                ["fuel_category", "report_date"]
                + data_columns
                + GENERATED_EMISSION_RATE_COLS
            ]
            output_to_results(
                ba_table_monthly,
                ba,
                "power_sector_data/monthly/",
                path_prefix,
                skip_outputs,
            )

            # aggregate data to annual
            ba_table_annual = (
                ba_table.groupby(["fuel_category"], dropna=False).sum().reset_index()
            )
            ba_table_annual = add_generated_emission_rate_columns(ba_table_annual)
            # re-order columns
            ba_table_annual = ba_table_annual[
                ["fuel_category"] + data_columns + GENERATED_EMISSION_RATE_COLS
            ]
            output_to_results(
                ba_table_annual,
                ba,
                "power_sector_data/annual/",
                path_prefix,
                skip_outputs,
            )
