import os

import numpy as np

# import pandas as pd

from src import foos  # noqa

campaign_name = "INM_unittest"
path = r"tests/data/"


def test_create_output_folder(campaign_name, path):
    out_path = foos.create_output_folder(campaign_name, path)
    assert os.path.exists(out_path)


def test_initialize_output_dfs():
    (
        df_city_no_zip,
        df_zip_no_city,
        df_zipCity_no_address,
        df_address_no_zipCity,
        df_no_address_at_all,
        df_invalid_matrices,
        df_employees,
    ) = foos.initialize_output_dfs()
    assert df_city_no_zip.shape[1] == 3
    assert df_zip_no_city.shape[1] == 3
    assert df_zipCity_no_address.shape[1] == 3
    assert df_address_no_zipCity.shape[1] == 3
    assert df_no_address_at_all.shape[1] == 3
    assert df_invalid_matrices.shape[1] == 4
    assert df_employees.shape[1] == 5


def test__load_csv_into_df():
    df_pytest = foos._load_csv_into_df("/data/df_pytest.csv", "df_pytest.csv")
    assert df_pytest.shape[0] == 8


def test_clean_email_column(df_pytest):
    df_pytest = foos.clean_email_column(df_pytest)
    assert df_pytest["Email"].values == np.array(
        [
            "lucamanes@yahoo.fr",
            np.NaN,
            "bruno.truessel@bluewin.ch",
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            "sybille.theubet@bluewin.ch",
            "Mon.e.mail@gmx.com",
        ]
    )


def test_create_temp_df_for_address_handling(df_pytest):
    df_address = foos.create_temp_df_for_address_handling(df_pytest)
    assert df_address.columns() == [
        "memberid",
        "ZipCity",
        "AddressLine1",
        "PostBox",
        "Street",
        "zip",
        "city",
    ]
    assert df_address.loc[8, "zip"] == 1203
    assert df_address.loc[8, "city"] == "Genève"


def test_append_to_df_city_no_zip(df_pytest):
    df = foos.append_to_df_city_no_zip(df_pytest)
    assert df["memberid"].values == np.array([683415])
