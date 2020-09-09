# Application for generation of Campaign-"Druckfiles"

Aug 2020, Version 0.1

## Introduction

App to convert zip files containing CSVs into XLSX-"Druckfiles", cleaning and validating the data during the process.
Additionally a "feedback" excel file is generated, documenting the problematic data entries and if they have been deleted or not.

## Run

The app can be run from the main folder (containing this README file) by typing the following exemplary command in the CLI.

```python
python src -c "TEST_1" -p "data/"
```

The command takes two parameters:

- `-c` / `--campaign`: a string with the name of the campaign (used to create output_folders).
- `-p` / `--path`:  a string containing the path to the directory containing the input zip-files.

(Note: multiple use of already existing path-campaign-combinations will overwrite the existing data.)

## Output

In the `path` directory:

- A new folder called `[campaign]_druckfiles` containing the processed XLSX-files for all input CSV files
- A `feedback_[timestamp].xlsx` with overall count summary and a list of validated / cleaned data entries (on separate worksheet each)
- A (for the moment) quite useless `log.log` (that could be further fleshed out in the future)

## What has to be true?

For the app to work, following conditions have to be met:

- The input files have to be stored as csv files within zip files in one directory (no matter how many zip files)
- All input files have to be in csv format, you can not have other file formats within the zip files
- All input files need to have at least the following columns:
  - `memberid`
  - `MemberName`
  - `MemberStatus`
  - `DeviceID`
  - `DataMatrix`
  - `AddressLine1`
  - `Street`
  - `PostBox`
  - `ZipCity`
  - `Email`

## What get's done?

1) Reading all csv files from all zip files into a dictionary of pd.DataFrames
2) Initializing some output stuff, creating the output folder
3) Iterating through each csv
   1) Cleaning mail addressess using a regEX pattern and dropping all invalid addresses
   2) Handle problematic data, listing members with ...
      1) City but no Zip
      2) Zip but no City
      3) Zip and / or City but no address (--> street / post box / address line 1)
      4) Address but no Zip and City --> ARE DELETED
      5) No Adress, Zip and City --> ARE DELETED
      6) Invalid DataMatrices (--> `memberid` / `DeviceID` not in string or non-numeric chars in string) --> ARE DELETED
      7) Any kind of `employee` status
   3) Saving each dataframe to XLSX in the `druckfiles` folder
4) Saving the `feedback.xlsx`

## Build

The application is built with Python 3.8 and only requires the following third-party libraries:

- `numpy`
- `pandas`
- `xlsxwriter`

## Testing and development

A `tests` folder is set-up for unittesting with `pytest` but not fully implemented. Instead the testing has been performed with the `dev_notebook.ipynb` and towards completion of the project with the `test_notebook.ipynb`

There is a testfile `df_pytest.csv` in the `tests/data` folder containing 9 rows of data, prepared as follows:

    Mail cleaning:
    - 1 invalid email (row 4)

    Address checks:
    - 1 city_no_zip (row 0)
    - 1 zip_no_city (row 1)
    - 1 zipCity_no_address (row 2)
    - 1 address_no_zipCity (row 3) -> DELETE
    - 1 no_address_at_all (row 4) -> DELETE

    Matrix checks, 2 invalid matrices
    - 1 non-numeric (row 7) -> DELETE
    - 1 memberid not in matrix (row 8) -> DELETE

    Employee count:
    - 2 employees (rows 0, 1)
