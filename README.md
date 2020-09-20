daq - dataset query
===================

author: Peter Hoffmann

Overview
--------

daq is a tool to query hive partitioned datasets in the azure blob store/datalake gen2 with dask.

Release
-------

    git tag -a 0.1 -m 0.1

    pip install twine wheel
    python setup.py bdist_wheel
    twine upload dist/daq-0.1-py2.py3-none-any.whl
