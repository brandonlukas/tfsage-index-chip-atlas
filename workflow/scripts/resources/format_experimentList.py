# https://github.com/inutano/chip-atlas/wiki#experimentList_schema
import dask.dataframe as dd
from snakemake.script import snakemake


def format_experimentList(input_file, output_file):
    df = load_basic(input_file)
    df = df.dropna(axis=1, how="all").reset_index(drop=True)
    df = format_table(df)
    df.to_parquet(output_file, index=False)


def load_basic(file_path, max_cols=70):
    column_names = list(range(max_cols))
    ddf = dd.read_csv(
        file_path,
        sep="\t",
        header=None,
        names=column_names,
        dtype=str,
        on_bad_lines="warn",
        assume_missing=True,
    )

    ddf = ddf[ddf[1] == "hg38"]
    ddf = ddf[ddf[2].isin(["TFs and others", "Histone", "ATAC-Seq"])]
    ddf = ddf[(ddf[2] != "Histone") | (ddf[3] == "H3K27ac")]
    df = ddf.compute()
    return df


def format_table(
    df,
    schema_columns=[
        "ID",
        "Assembly",
        "TrackClass",
        "TrackType",
        "CellClass",
        "CellType",
        "Description",
        "ProcessingLogs",
        "Title",
        "MetaData",
    ],
):
    num_cols = df.shape[1]

    if num_cols > 9:
        # Join columns 9+ into a single MetaData string
        metadata_cols = df.columns[9:]
        df["MetaData"] = (
            df[metadata_cols]
            .astype(str)
            .apply(
                lambda row: "\t".join(
                    [val for val in row if val not in ["", "nan", "<NA>", "None"]]
                ),
                axis=1,
            )
        )
        # Drop the original metadata columns
        df = df.drop(columns=metadata_cols)
    else:
        df["MetaData"] = ""

    df.columns = schema_columns
    df["MetaData"] = df["MetaData"].astype("string[python]")
    return df


format_experimentList(snakemake.input[0], snakemake.output[0])
