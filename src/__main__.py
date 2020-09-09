import argparse
import datetime as dt
import logging
import os
from typing import Any

# from typing import List
# from gooey import Gooey
import foos  # noqa


# INITIALIZE ARGPARSER


arg_parser = argparse.ArgumentParser(
    description=(
        "Create XLSX Druckfiles for PKZ by passing a campaign name "
        " and the path containing the initial CSV zipfiles."
    )
)

arg_parser.add_argument(
    "-c", "--campaign", help="Campaign name (str)", type=str, nargs=1
)
arg_parser.add_argument(
    "-p",
    "--path",
    help="Path to folder containing the zipfiles (str)",
    type=str,
    nargs=1,
)

# INITIALIZE LOGGING


def initialize_logger(path):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create console handler
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    # Create file handler
    fh = logging.FileHandler(
        os.path.join(path, "log.log"), "w", encoding=None, delay="true"
    )
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


# DEFINE MAIN


def main(campaign_name: str, path: str, logger: Any):

    logger.debug(f"{campaign_name}".upper())
    logger.debug(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d, %H-%M-%S')}\n")

    out_path = foos.create_output_folder(campaign_name, path)
    (
        df_city_no_zip,
        df_zip_no_city,
        df_zipCity_no_address,
        df_address_no_zipCity,
        df_no_address_at_all,
        df_invalid_matrices,
        df_employees,
    ) = foos.initialize_output_dfs()

    df_dict = foos.create_dict_with_all_df(path)
    df_summary = foos.create_df_summary(df_dict)

    logger.info(f"Success loading {len(df_dict)} segment files.")

    for name, df in df_dict.items():

        logger.info(f"Processing segment {name} ...")

        df = foos.clean_email_column(df)

        df_address = foos.create_temp_df_for_address_handling(df)
        df_matrix = foos.create_temp_df_for_datamatrix_check(df)

        df_city_no_zip = foos.append_to_df_city_no_zip(df_address, name, df_city_no_zip)
        df_zip_no_city = foos.append_to_df_zip_no_city(df_address, name, df_zip_no_city)
        df_zipCity_no_address = foos.append_to_df_zipCity_no_address(
            df_address, name, df_zipCity_no_address
        )
        (
            df_address_no_zipCity,
            members_no_zipCity,
        ) = foos.append_to_df_address_no_zipCity(
            df_address, name, df_address_no_zipCity
        )
        (
            df_no_address_at_all,
            members_no_address_at_all,
        ) = foos.append_to_df_no_address_at_all(df_address, name, df_no_address_at_all)
        (
            df_invalid_matrices,
            members_with_invalid_matrices,
        ) = foos.append_to_df_invalid_matrices(df_matrix, name, df_invalid_matrices)
        df_employees = foos.append_to_df_employees(df, name, df_employees)

        df = foos.delete_problematic_entries(
            df,
            members_no_zipCity,
            members_no_address_at_all,
            members_with_invalid_matrices,
        )

        foos.save_df_to_excel(df, name, out_path)

    foos.save_feedback_xlsx(
        df_summary,
        df_city_no_zip,
        df_zip_no_city,
        df_zipCity_no_address,
        df_address_no_zipCity,
        df_no_address_at_all,
        df_invalid_matrices,
        df_employees,
        path,
    )

    logging.info("\nAll complete!")


if __name__ == "__main__":
    args = arg_parser.parse_args()
    campaign_name = args.campaign[0]
    path = args.path[0]
    logger = initialize_logger(path)

    main(campaign_name, path, logger)
