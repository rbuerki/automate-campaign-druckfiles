import datetime as dt
import glob
import os
import re
from typing import Any, Dict, Set, Tuple
from zipfile import ZipFile

import numpy as np
import pandas as pd

campaign_name = "INM_TEST"
path = r"tests/data/"


def create_output_folder(campaign_name: str, path: str) -> str:
    """Create a new folder for the resulting xlsx-files, using the same
    location where the input zip folders are stored and named using the
    campaign_name. Return the created output path.
    """
    folder_name = "".join([campaign_name, "_druckfiles"])
    path = os.path.split(path)[0]
    out_path = os.path.join(path, folder_name)
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    return out_path


def initialize_output_dfs() -> Tuple[pd.DataFrame]:
    """Return a tuple of empty dataframes where problematic records
    will be stored for feedback.
    """
    df_city_no_zip = pd.DataFrame(columns=["memberid", "source", "action"])
    df_zip_no_city = pd.DataFrame(columns=["memberid", "source", "action"])
    df_zipCity_no_address = pd.DataFrame(columns=["memberid", "source", "action"])
    df_address_no_zipCity = pd.DataFrame(columns=["memberid", "source", "action"])
    df_no_address_at_all = pd.DataFrame(columns=["memberid", "source", "action"])
    df_invalid_matrices = pd.DataFrame(
        columns=["memberid", "DataMatrix", "source", "action"]
    )
    df_employees = pd.DataFrame(
        columns=["memberid", "MemberName", "MemberStatus", "source", "action"]
    )
    return (
        df_city_no_zip,
        df_zip_no_city,
        df_zipCity_no_address,
        df_address_no_zipCity,
        df_no_address_at_all,
        df_invalid_matrices,
        df_employees,
    )


def create_dict_with_all_df(path: str) -> Dict[str, pd.DataFrame]:
    """Gobble up all zip folders in the given path and combine all
    their csv files in a dictionary with filename as key and dataframe
    as value."""
    all_zips = glob.glob(os.path.join(path, "*.zip"))
    df_dict = {}
    for zip_ in all_zips:
        dfs = _return_dfs_from_zipfolder(zip_)
        #     df_dict |= dfs  # will work with Python 3.9
        df_dict = {**df_dict, **dfs}
    return df_dict


def _return_dfs_from_zipfolder(zip_path: str) -> Dict[str, pd.DataFrame]:
    """Return a dictionary of filenames and dataframes for csv files
    inside a zip_folder. Pass the path to the zip folder as input. This
    function is called within `create_dict_with_all_df`.
    """
    zipfolder = ZipFile(zip_path)
    df_dict = {}
    for csv_info in zipfolder.infolist():
        csv_name = csv_info.filename
        unzipped = zipfolder.open(csv_name)
        df = _load_csv_into_df(unzipped, csv_name)
        df_dict[csv_name] = df

    assert len(df_dict) == len(zipfolder.infolist())  # TODO: maybe check / log function

    return df_dict


def _load_csv_into_df(csv_file: Any, csv_name: str) -> pd.DataFrame:
    """Load data from a csv file and return a dataframe. This function
    is called within `return_dfs_from_zipfolder`.
    """
    try:
        df = pd.read_csv(csv_file, sep="|", header=0, dtype=str, encoding="UTF-8")
    except ValueError as e:
        print(f"ERROR! Could not read the file {csv_name}: {e}")
        raise
    return df


def create_df_summary(df_dict):
    """Return a dataframe with overview of segments and member count."""
    summary_list = []
    for name, df in df_dict.items():
        summary = {}
        summary["name"] = name.split(".")[0]
        summary["n_members_at_load"] = df.shape[0]
        summary_list.append(summary)
    df_summary = pd.DataFrame(summary_list, columns=["name", "n_members_at_load"])

    super_summary = {
        "name": "Total",
        "n_members_at_load": df_summary["n_members_at_load"].sum(),
    }
    df_summary = df_summary.append(super_summary, ignore_index=True)
    return df_summary


def clean_email_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned `Email` column where all entries with an
    invalid pattern are replaced by np.NaN.
    """
    try:
        df["Email"] = df["Email"].apply(_clean_email_strings)
        return df
    except ValueError:
        print("'Email' column not found, please check the input file structures.")


def _clean_email_strings(mail: str) -> str:
    """Checks entries to `Email` column for pattern validity, if
    they are invalid the entry is replaced by np.NaN. This funcion
    is called within `clean_email_column`.
    """
    MAIL_PATTERN = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"
    regex_mail = re.compile(MAIL_PATTERN, flags=re.IGNORECASE)

    try:
        return regex_mail.findall(mail)[0]
    except IndexError:  # set invalid values (return empty list) to NaN
        return np.NaN
    except TypeError:  # handle missing values (findall() fails)
        return np.NaN


def create_temp_df_for_address_handling(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with address columns only, split `ZipCity`
    into two columns `zip` and `city` using regex patterns.
    """
    try:
        df_address = df[
            ["memberid", "ZipCity", "AddressLine1", "PostBox", "Street"]
        ].copy()
    except ValueError:
        print("Some address columns not found, please check the input file structures.")

    df_address["zip"] = df_address["ZipCity"].apply(_get_zips)
    df_address["city"] = df_address["ZipCity"].apply(_get_cities)
    df_address[["zip", "city"]] = df_address[["zip", "city"]].replace("", np.NaN)

    # Make sure all white-space only strings are set to np.nan
    return df_address.applymap(lambda x: np.nan if str(x).isspace() else x)


def _get_zips(x):
    """Return numeric values from a sring."""
    try:
        zip_ = re.sub("[^0-9]", "", x)
        return zip_
    except TypeError:
        return np.NaN


def _get_cities(x):
    """Return non-numeric values from a sring."""
    try:
        city = re.sub(r"[^[\u00C0-\u017FA-Za-z\-\.\'\s]", "", x)
        return city
    except TypeError:
        return np.NaN


def append_to_df_city_no_zip(
    df_address: pd.DataFrame, name: str, df_city_no_zip: pd.DataFrame
) -> pd.DataFrame:
    """Append members with City but no Zip to the respective output df.
    (These members will NOT be deleted later on.)
    """
    city_no_zip = df_address.loc[
        (df_address["zip"].isnull()) & df_address["city"].notnull()
    ][["memberid"]]
    city_no_zip["source"] = name
    city_no_zip["action"] = "not deleted"
    df_city_no_zip = pd.concat([df_city_no_zip, city_no_zip], ignore_index=True)
    return df_city_no_zip.drop_duplicates()


def append_to_df_zip_no_city(
    df_address: pd.DataFrame, name: str, df_zip_no_city: pd.DataFrame
) -> pd.DataFrame:
    """Append members with Zip but no City to the respective output df.
    (These members will NOT be deleted later on.)
    """
    zip_no_city = df_address.loc[
        (df_address["city"].isnull()) & df_address["zip"].notnull()
    ][["memberid"]]
    zip_no_city["source"] = name
    zip_no_city["action"] = "not deleted"
    df_city_no_zip = pd.concat([df_zip_no_city, zip_no_city], ignore_index=True)
    return df_city_no_zip.drop_duplicates()


def append_to_df_zipCity_no_address(
    df_address: pd.DataFrame, name: str, df_zipCity_no_address: pd.DataFrame
) -> pd.DataFrame:
    """Append members with Zip & City but no other address parts to the
    respective output df. (These members will NOT be deleted later on.)
    """
    zipCity_no_address = df_address.loc[
        (df_address["city"].notnull())
        & (df_address["zip"].notnull())
        & (df_address["AddressLine1"].isnull())
        & (df_address["PostBox"].isnull())
        & (df_address["Street"].isnull())
    ][["memberid"]]
    zipCity_no_address["source"] = name
    zipCity_no_address["action"] = "not deleted"
    df_zipCity_no_address = pd.concat(
        [df_zipCity_no_address, zipCity_no_address], ignore_index=True
    )
    return df_zipCity_no_address.drop_duplicates()


def append_to_df_address_no_zipCity(
    df_address: pd.DataFrame, name: str, df_address_no_zipCity: pd.DataFrame
) -> pd.DataFrame:
    """Append members without Zip & City but other address parts to the
    respective output df. (These members will be DELETED later on. That's
    why we also return a set of the respective member ids.)
    """
    address_no_zipCity = df_address.loc[
        (df_address["ZipCity"].isnull())
        & (
            (df_address["AddressLine1"].notnull())
            | (df_address["PostBox"].notnull())
            | (df_address["Street"].notnull())
        )
    ][["memberid"]]
    address_no_zipCity["source"] = name
    address_no_zipCity["action"] = "DELETED"
    df_address_no_zipCity = pd.concat(
        [df_address_no_zipCity, address_no_zipCity], ignore_index=True
    )
    return (
        df_address_no_zipCity.drop_duplicates(),
        set(address_no_zipCity["memberid"].tolist()),
    )


def append_to_df_no_address_at_all(
    df_address: pd.DataFrame, name: str, df_no_address_at_all: pd.DataFrame
) -> pd.DataFrame:
    """Append members with no address info at all to the respective
    output df. (These members will be DELETED later on. That's why
    we also return a set of the respective member ids.)
    """
    no_address_at_all = df_address.loc[
        (df_address["ZipCity"].isnull())
        & (df_address["AddressLine1"].isnull())
        & (df_address["PostBox"].isnull())
        & (df_address["Street"].isnull())
    ][["memberid"]]
    no_address_at_all["source"] = name
    no_address_at_all["action"] = "DELETED"
    df_no_address_at_all = pd.concat(
        [df_no_address_at_all, no_address_at_all], ignore_index=True
    )
    return (
        df_no_address_at_all.drop_duplicates(),
        set(no_address_at_all["memberid"].tolist()),
    )


def create_temp_df_for_datamatrix_check(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with datamatrix-relevant columns only."""
    try:
        df_matrix = df[["memberid", "DeviceID", "DataMatrix"]].copy()
    except ValueError:
        print(
            (
                "Some matrix-relevant columns not found, ",
                "please check the input file structures.",
            )
        )
    return df_matrix


def append_to_df_invalid_matrices(
    df_matrix: pd.DataFrame, name: str, df_invalid_matrices: pd.DataFrame
) -> pd.DataFrame:
    """Append members with invalid datamatrix to the respective output df.
    (These members will be DELETED later on. That's why we also return a
    set of the respective member ids.)
    """
    members_with_invalid_matrices = _get_members_with_invalid_matrices(df_matrix)
    invalid_matrices = df_matrix.loc[
        df_matrix["memberid"].isin(members_with_invalid_matrices)
    ][["memberid", "DataMatrix"]]
    invalid_matrices["source"] = name
    invalid_matrices["action"] = "DELETED"
    df_invalid_matrices = pd.concat(
        [df_invalid_matrices, invalid_matrices], ignore_index=True
    )
    return (
        df_invalid_matrices.drop_duplicates(),
        members_with_invalid_matrices,
    )


def _get_members_with_invalid_matrices(df_matrix: pd.DataFrame) -> Set:
    """Check if `memberid` and `DeviceID` are in datamatrix and that
    the datamatrix contains only numeric characters. Return a set of
    `memberid` where the matrix is invalid. This function is called
    within `append_to_df_invalid_matrices`.
    """
    members_with_invalid_data = []
    for row in df_matrix.itertuples(index=False):
        if not row[0] in row[2] or not row[1] in row[2] or not row[2].isnumeric():
            members_with_invalid_data.append(row[0])
    return set(members_with_invalid_data)


def append_to_df_employees(
    df: pd.DataFrame, name: str, df_employees: pd.DataFrame
) -> pd.DataFrame:
    """Append members with employee status to the respective output df.
    (These members will NOT be deleted later on.)
    """
    employees = df.loc[
        df["MemberStatus"].str.endswith("Employee") == True  # noqa E712
    ][["memberid", "MemberName", "MemberStatus"]]
    employees["source"] = name
    employees["action"] = "not_deleted"
    df_employees = pd.concat([df_employees, employees], ignore_index=True)
    return df_employees.drop_duplicates()


def delete_problematic_entries(
    df: pd.DataFrame,
    members_no_address_at_all: Set,
    members_no_zipCity: Set,
    members_with_invalid_matrices: Set,
) -> pd.DataFrame:
    """Return dataframe where all members that have to be deleted
    because of invalid addresses or datamatrices are eliminated.
    """
    members_to_delete = members_no_address_at_all.union(members_no_zipCity).union(
        members_with_invalid_matrices
    )

    df = df.loc[~df["memberid"].isin(members_to_delete)]
    return df


def save_df_to_excel(df: pd.DataFrame, name: str, out_path: str):
    """Save transformed dataframe to excel, with all values to string."""
    df = df.applymap(lambda x: str(x))
    df = df.replace("nan", "")
    sheetname = name.rpartition(".")[0]
    filename = name.replace("csv", "xlsx")
    full_path = os.path.join(out_path, filename)
    writer = pd.ExcelWriter(full_path, engine="xlsxwriter")
    df.to_excel(
        writer,
        sheet_name=sheetname,
        index=False,
        engine="xlsxwriter",
        encoding="UTF-8",
    )

    # Setting col witdh to max_len of col values + 1, with a min of 15
    sheet = writer.sheets[sheetname]  # for
    for pos, col in enumerate(df):
        max_len = df[col].astype(str).map(len).max()
        sheet.set_column(pos, pos, max([15, max_len + 1]))

    writer.save()


def save_feedback_xlsx(
    df_summary: pd.DataFrame,
    df_city_no_zip: pd.DataFrame,
    df_zip_no_city: pd.DataFrame,
    df_zipCity_no_address: pd.DataFrame,
    df_address_no_zipCity: pd.DataFrame,
    df_no_address_at_all: pd.DataFrame,
    df_invalid_matrices: pd.DataFrame,
    df_employees: pd.DataFrame,
    path: str,
):
    """Create and save an excel file with all problematic entries, one
    sheet per dataframe.
    """
    full_path = os.path.join(
        path,
        f"feedback_{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d-%H-%M-%S')}.xlsx",
    )
    writer = pd.ExcelWriter(full_path, engine="xlsxwriter")
    df_summary.to_excel(writer, sheet_name="SUMMARY", index=False)
    df_invalid_matrices.to_excel(writer, sheet_name="invalid_matrices", index=False)
    df_address_no_zipCity.to_excel(writer, sheet_name="address_no_zipCity", index=False)
    df_no_address_at_all.to_excel(writer, sheet_name="no_address_at_all", index=False)
    df_zipCity_no_address.to_excel(writer, sheet_name="zipCity_no_address", index=False)
    df_zip_no_city.to_excel(writer, sheet_name="zip_no_city", index=False)
    df_city_no_zip.to_excel(writer, sheet_name="city_no_zip", index=False)
    df_employees.to_excel(writer, sheet_name="employees", index=False)

    for sheet in writer.sheets.values():
        sheet.set_column("A:E", 35)

    writer.save()
