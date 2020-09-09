# import numpy as np
# import pandas as pd
# import pytest

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
# sys.path.append(os.path.abspath("../src"))


"""
The file `df_pytest.csv` on which the unit tests are based,
contains 9 rows of data, prepared as follows:

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
"""
