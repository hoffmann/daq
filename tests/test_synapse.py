from daq.kartothek import Dataset
from daq.synapse import table_definition


def test_table_definition(dataset, dataset_url, account_name, account_key, container, dataset_uuid):
    Dataset.account_keys = {account_name: account_key}
    ds = Dataset(dataset_url)

    s = table_definition(ds)
    assert dataset_url in s
