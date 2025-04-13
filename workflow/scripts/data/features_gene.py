from tfsage.utils import write_10x_h5, read_10x_h5
from snakemake.script import snakemake


def features_gene(input_file, output_file):
    # Aggregate gene-level data
    df = (
        read_10x_h5(input_file)
        .sparse.to_dense()
        .assign(gene=lambda x: x.index.str.split(":").str[1])
        .groupby("gene")
        .mean()
    )

    # Save aggregated data
    write_10x_h5(df, output_file)


features_gene(snakemake.input[0], snakemake.output[0])
