import numpy as np
import functools
import dask
import dask.dataframe as dd
import pyarrow.parquet as pq
import re
from urllib.parse import urlparse
from urllib.parse import unquote
from storefact import get_store
from operator import and_, or_

def df_condition(df, condition):
    key, op, value = condition
    if op == "==":
        return df[key] == value
    elif op == "!=":
        return df[key] != value
    elif op == "<=":
        return df[key] <= value
    elif op == ">=":
        return df[key] >= value
    elif op == "<":
        return df[key] < value
    elif op == ">":
        return df[key] > value
    #elif op == "in":
    else:
        raise NotImplementedError("op not supported")

def filter_df(df, predicates=None):
    if predicates is None:
        return df
    return df[functools.reduce(np.logical_or, [
                   functools.reduce(np.logical_and, [
                       df_condition(df, pred) for pred in pred_and]
                                   ) for pred_and in predicates])]


def translate(pat):
    """Translate a shell PATTERN to a regular expression.
    There is no way to quote meta-characters.
    """
    i, n = 0, len(pat)
    res = ''
    while i < n:
        c = pat[i]
        i = i+1
        if c == '*':
            res = res + '(.*)'
        else:
            res = res + re.escape(c)
    return res + '\Z(?ms)'

def translate_named(pat):
    """Translate a shell PATTERN to a regular expression.
    There is no way to quote meta-characters.
    """
    i, n = 0, len(pat)
    res = ''
    while i < n:
        c = pat[i]
        i = i+1
        if c == '*':
            name = pat[:i-2].rsplit('/', 1)[1]
            res = res + '(?P<{}>.*)'.format(name)
        else:
            res = res + re.escape(c)
    return res + '\Z(?ms)'

class Pattern:
    def __init__(self, pat):
        self.pat = pat
        self.re_pat = re.compile(translate(pat))
        self.re_pat_named = re.compile(translate_named(pat))

    def match(self, s):
        return self.re_pat.match(s) is not None

    def filepath(self, s, part):
        """Return the matching part of a pattern

        - If part = 0 the filename is return
        - If part >0 the n'th wildcard part is removed.

        For compatability with the Azure Synapse filename-function
        """
        match = self.re_pat.match(s)
        if match:
            return match.group(part)

    def partitions(self, s):
        match = self.re_pat_named.match(s)
        if match:
            return {unquote(k): unquote(v) for k,v in match.groupdict().items()}


def partitions(s):
    """
    Take a list of encoded column-value strings and decode them to tuples
    input: `quote(column)=quote(value)`
    output `(column, value)`
    Parameters
    ----------
    indices: list of tuple
        A list of tuples where each list entry is (column, value)
    Returns
    -------
    list
        List with column value pairs
    """
    parts = s.split("/")
    partitions_ = []
    for part in parts:
        split = part.split("=")
        if len(split) == 2:
            column, value = split
            partitions_.append((unquote(column), unquote(value)))
    return partitions_



def evaluate(d, condition):
    key, op, value = condition
    res = True
    if key in d:
        if op == "==":
            res = d[key] == value
        elif op == "!=":
            res = d[key] != value
        elif op == "<=":
            res = d[key] <= value
        elif op == ">=":
            res = d[key] >= value
        elif op == "<":
            res = d[key] < value
        elif op == ">":
            res = d[key] > value
        #elif op == "in":
        else:
            raise NotImplementedError("op not supported")
    return res


def filter_partition(key, predicates=None):
    if predicates is None:
        return True
    d = dict(partitions(key))
    return functools.reduce(or_, [
                   functools.reduce(and_, [
                       evaluate(d, pred) for pred in pred_and]
                                   ) for pred_and in predicates])


def get_key(account):
    import os
    k = "STORAGE_KEY".format(account)
    if k in os.environ:
        return os.environ[k]
    raise KeyError("account not found")

def parse_dataset(url):
    """
    abfs://<containername>@<accountname>.dfs.core.windows.net/<file.path>/

    https://<accountname>.dfs.core.windows.net/<container>/<file.path>/
    """
    r = urlparse(url)
    if r.scheme == 'abfs':
        container = r.username
        path = r.path
    elif r.scheme == 'https':
        _, container, path = r.path.split('/', 2)
    else:
        raise Exception(f"unknown protocol: {r.schema}")
    account_name = r.hostname.split(".")[0]
    return account_name, container, path




class Dataset:
    def __init__(self, url, account_key=None, globstring=None):
        account_name, container, prefix = parse_dataset(url)
        self.url = url
        self.account_name = account_name
        if account_key is None:
            self.account_key = get_key(account_name)
        self.container = container
        self.prefix = prefix
        self.globstring = globstring
        self.store = get_store(**
            {'type': 'hazure',
            'account_name': self.account_name,
            'account_key': self.account_key,
            'container': self.container
            })
        self._cached_keys = None

    def keys(self, filters=None, suffix=".parquet"):
        if self._cached_keys is None:
            self._cached_keys = keylist = [k[len(self.prefix):] for k in self.store.keys(self.prefix)]
        else:
            keylist = self._cached_keys
        return [k for k in keylist if k.endswith(suffix) and filter_partition(k, filters)]

    def read_parquet(self, filters=None, columns=None):
        """
        filters: [[(key, op, value)]]
        columns: restrict to columns
        """
        def load(k, filters, partitions=None):
            table = pq.read_table(self.store.open("{}{}".format(self.prefix, k)), columns=columns, filters=filters)
            df = table.to_pandas(date_as_object=True)
            for (key, value) in partitions:
                df[key] = value
            return filter_df(df, filters)
        dfs = [dask.delayed(load)(k, filters, partitions(k)) for k in self.keys(filters)]
        return dd.from_delayed(dfs)
