def synapse_columns(table_meta, index_col):
    """map arrow types to synapse types"""
    mapping = {
        "string": "varchar(100)",
        "double": "float",
        "int64": "bigint",
        "timestamp[ns]": "datetime",
        "date32[day]": "date",
        "bool": "bit",
    }

    result = ""
    for col in table_meta:
        if col.name == index_col:
            continue
        col_name = f"[{col.name}]"
        col_type = mapping.get(str(col.type))
        result += f"            {col_name:<35} {col_type},\n"
    return result[:-2]  # remove last ,


def table_definition(dataset):
    """print an azure synapse table definition for a kartothek dataset"""

    index_col = list(dataset.dataset_metadata.index_columns)[
        0
    ]  ##works only with one index column
    cols = synapse_columns(
        dataset.dataset_metadata.table_meta[dataset.table], index_col
    )

    template = """
with {dataset.dataset_uuid} as (
    SELECT
        result.filepath(1) as [{index_col}],
        *
    FROM
        OPENROWSET(
            BULK '{dataset.url}/{index_col}=*/*.parquet',
            FORMAT='PARQUET'
        ) with(
{cols}
        ) as [result]
)

select top 100 * from {dataset.dataset_uuid};
    """
    return template.format(dataset=dataset, cols=cols, index_col=index_col)
