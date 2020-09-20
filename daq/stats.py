

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "{:3.1f}{}{}".format(num, unit, suffix)
        num /= 1024.0
    return "{:.1f}{}{}".format(num, 'Yi', suffix)

def describe(dataset):

    schema = "\n".join(f"    {name:30} {type}" for (name,type) in dataset.schema.items())
    index_values = ""
    for i in dataset.dataset_metadata.index_columns:
        vals = sorted(list(dataset.dataset_metadata.indices[i].observed_values()))
        index_values += "{i} {count} {min} {max}".format(i=i, count=len(vals), min=min(vals), max=max(vals))


    s = f"""dataset url: {dataset.url}

index stats (name, count, min, max):

    {index_values}

schema:

{schema}
    """
    return s
