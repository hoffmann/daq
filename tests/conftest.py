# content of conftest.py
import pytest
import os

@pytest.fixture(scope="module")
def azure_dataset():
    account_name = os.environ.get("ACCOUNT_NAME")
    account_key = os.environ.get("ACCOUNT_KEY")
    url = "    https://{}.dfs.core.windows.net/daq-test/testdata/".format(account_name)
    return url
