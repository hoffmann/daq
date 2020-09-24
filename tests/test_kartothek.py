import pytest
from daq.kartothek import Dataset
from daq.stats import describe


def test_kartothek_dataset(dataset, dataset_url, account_name, account_key, container, dataset_uuid):
    Dataset.account_keys = {account_name: account_key}
    ds = Dataset(dataset_url)

    #import IPython; IPython.embed()
    assert ds.container == container
    assert ds.dataset_uuid == dataset_uuid
    assert ds.table == 'table'
    assert ds.schema == {'A': 'int64', 'B': 'timestamp[ns]', 'C': 'double', 'D': 'string'}
    assert 'uuid.by-dataset-metadata.json'  in ds.storage_keys()

    df = ds.read_dataset_as_ddf()
    assert df.shape[1] == 4

    dm = ds.dataset_metadata
    assert dm.index_columns == set('B')

def test_readonly(dataset, dataset_url, account_name, account_key):
    Dataset.account_keys = {account_name: account_key}
    ds = Dataset(dataset_url)
    with pytest.raises(Exception):
        s = ds.writable_store

def test_stats(dataset, dataset_url, account_name, account_key):
    Dataset.account_keys = {account_name: account_key}
    ds = Dataset(dataset_url)
    s = describe(ds)
    assert dataset_url in s
