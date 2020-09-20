# content of conftest.py
import pytest
import os
import numpy as np
import pandas as pd
from storefact import get_store
from kartothek.io.dask.dataframe import update_dataset_from_ddf
import dask.dataframe as dd

@pytest.fixture(scope="session")
def account_name():
    return os.environ.get("ACCOUNT_NAME")


@pytest.fixture(scope="session")
def account_key():
    return os.environ.get("ACCOUNT_KEY")

@pytest.fixture(scope="session")
def container():
    return 'daq-test'

@pytest.fixture(scope="session")
def dataset_uuid():
    return 'uuid'

@pytest.fixture(scope="session")
def store(account_name, account_key, container):
    store = get_store(
        "hazure",
        account_name=account_name,
        account_key=account_key,
        container=container,
        create_if_missing=True
    )
    return store

@pytest.fixture(scope="session")
def dataset_url(account_name, container, dataset_uuid):
    return "https://{}.dfs.core.windows.net/{}/{}/table".format(account_name, container, dataset_uuid)


@pytest.fixture(scope="session")
def dataset(store, dataset_uuid):
    df = pd.DataFrame(
        {
            "A": np.array([1,2,3,4], dtype="int32"),
            "B": [
                pd.Timestamp("2002-01-01"),
                pd.Timestamp("2002-01-02"),
                pd.Timestamp("2002-01-03"),
                pd.Timestamp("2002-01-04"),
            ],
            "C": pd.Series(1, index=list(range(4)), dtype="double"),
            "D": ["test", "train", "test", "prod"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=2)
    delayed = update_dataset_from_ddf(
            ddf,
            store=lambda: store,
            dataset_uuid=dataset_uuid,
            table='table',
            partition_on=["B"]
        )
    delayed.compute()
    yield
    for k in store.keys(prefix=dataset_uuid):
        store.delete(k)
