import urllib
from datetime import datetime, timedelta

import kartothek.core.dataset

from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContainerPermissions
from kartothek.io.dask.dataframe import read_dataset_as_ddf
from kartothek.io.dask.dataframe import update_dataset_from_ddf

from daq.store import get_sas_token, parse_dataset

from storefact import get_store




class Dataset:
    """
    Dataset.account_keys = {'account1': 'key1'}
    """

    account_keys = None

    def __init__(self, url, writable=False):
        self.writable = writable
        self.url = url
        (
            self.account_name,
            self.container,
            self.dataset_uuid,
            self.table,
        ) = parse_dataset(self.url)

    @property
    def store(self):
        account_key = self.account_keys[self.account_name]
        readonly_sas_token = get_sas_token(
            self.account_name, account_key, self.container, readonly=True
        )
        store = get_store(
            "hazure",
            account_name=self.account_name,
            account_key=readonly_sas_token,
            container=self.container,
            create_if_missing=False,
            use_sas=True,
        )
        return store

    @property
    def writable_store(self):
        if not self.writable:
            raise Exception("try to write to readonly dataset")
        account_key = self.account_keys[self.account_name]
        writable_sas_token = get_sas_token(
            self.account_name, account_key, self.container, readonly=False
        )
        store = get_store(
            "hazure",
            account_name=self.account_name,
            account_key=writable_sas_token,
            container=self.container,
            create_if_missing=False,
            use_sas=True,
        )
        return store

    def read_dataset_as_ddf(self, **kwargs):
        """
        # when setting dates_as_object=False
        import pandas as pd
        predicates = [[("c_date", "==", pd.to_datetime('2020-01-01'))]]

        # with dates_as_object=True or if querying partition key
        from datetime import date
        predicates = [[("c_date", "==", date(2020,1,1))]]

        columns=['col1', 'col2'],
        predicates=predicates,
        dates_as_object=True,
        """
        return read_dataset_as_ddf(
            dataset_uuid=self.dataset_uuid,
            store=lambda: self.store,
            table=self.table,
            **kwargs
        )

    def update_dataset_from_ddf(self, ddf, **kwargs):
        """
        partition_on=["c_date"],
        num_buckets=num_buckets,
        shuffle=True,
        delete_scope=delete_scope
        """
        return update_dataset_from_ddf(
            ddf,
            store=lambda: self.writable_store,
            dataset_uuid=self.dataset_uuid,
            table=self.table,
            **kwargs
        )

    def storage_keys(self):
        return kartothek.core.dataset.DatasetMetadata.storage_keys(
            self.dataset_uuid, self.store
        )

    @property
    def dataset_metadata(self):
        """
        dm = ds.dataset_metadata
        # DatasetMetadata(uuid=someid, tables=['table'], partition_keys=['c_date'], metadata_version=4, indices=['c_date'], explicit_partitions=True)
        dm.index_columns
        # c_date
        list(dm.indices['c_date'].observed_values())
        """
        metadata = kartothek.core.dataset.DatasetMetadata.load_from_store(
            self.dataset_uuid, self.store
        )
        return metadata.load_all_indices(store=self.store)

    @property
    def schema(self):
        schema_wrapper = self.dataset_metadata.table_meta[self.table]
        return {field.name: str(field.type) for field in schema_wrapper}
