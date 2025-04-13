from pathlib import Path
from tfsage.features import extract_features_parallel, load_region_set
from tfsage.utils import write_10x_h5
from snakemake.script import snakemake


def features_tss(input_files, output_file, threads):
    # Load reference gene locations
    gene_loc_set = load_region_set("hg38")

    # Extract features
    df = extract_features_parallel(input_files, gene_loc_set, max_workers=threads)
    df.columns = [f"{Path(f).stem}" for f in input_files]

    # Save results
    write_10x_h5(df, output_file)


features_tss(snakemake.input, snakemake.output[0], snakemake.threads)
