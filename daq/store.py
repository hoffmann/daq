import urllib
from datetime import datetime, timedelta
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContainerPermissions

def get_sas_token(account_name, account_key, container, readonly=True):
    service = BlockBlobService(account_name=account_name, account_key=account_key)
    # res = service.create_container(container)
    if readonly:
        permission = ContainerPermissions(
            read=True, list=True, write=False, delete=False
        )
    else:
        permission = ContainerPermissions(read=True, list=True, write=True, delete=True)
    expiry = (datetime.utcnow() + timedelta(days=7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start = (datetime.utcnow() - timedelta(days=7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    token = service.generate_container_shared_access_signature(
        container, permission=permission, expiry=expiry, start=start
    )
    return token


def parse_dataset(url):
    """
    https://<accountname>.dfs.core.windows.net/<container>/<file.path>/
    """
    r = urllib.parse.urlparse(url)
    _, container, dataset_uuid, table = r.path.split("/", 3)
    account_name = r.hostname.split(".")[0]
    return account_name, container, dataset_uuid, table



class StoreManager:
    def __init__(self, account_keys):
        self.account_keys = account_keys

    def get_store(self, url, readonly=True):
        account_name, container, dataset_uuid = parse_dataset(url)
        account_key = self.account_keys[account_name]
        sas_token = get_sas_token(account_name, account_key, container, readonly)
        store = get_store(
            "hazure",
            account_name=account_name,
            account_key=writable_sas_token,
            container=container,
            create_if_missing=False,
            use_sas=True,
        )
        return dataset_uuid, store


    def print_store_config(self, url, readonly=True):
        account_name, container, dataset_uuid, table = parse_dataset(url)
        account_key = self.account_keys[account_name]
        sas_token = get_sas_token(account_name, account_key, container, readonly)
        print(f"""from storefact import get_store
store = get_store(
    "hazure",
    account_name='{account_name}',
    account_key='{sas_token}',
    container='{container}',
    create_if_missing=False,
    use_sas=True,
)
dataset_uuid = '{dataset_uuid}'
table = '{table}'
""")
