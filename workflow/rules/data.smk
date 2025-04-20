import os
import dask.dataframe as dd


def collect_experiments(wildcards):
    path = checkpoints.format_experimentList.get().output[0]
    experiments = dd.read_parquet(path)["ID"].compute()
    template = rules.download.output[0]
    return collect(template, threshold=wildcards.threshold, experiment=experiments)


rule features_tss:
    input:
        collect_experiments,
    output:
        os.path.join(config["data_dir"], "{threshold}/rp_matrix/tss.h5"),
    threads: workflow.cores
    resources:
        mem_mb=96000,
    script:
        "../scripts/data/features_tss.py"


rule features_gene:
    input:
        rules.features_tss.output[0],
    output:
        os.path.join(config["data_dir"], "{threshold}/rp_matrix/gene.h5"),
    resources:
        mem_mb=96000,
    script:
        "../scripts/data/features_gene.py"


rule embeddings:
    input:
        rp_matrix=rules.features_tss.output[0],
        metadata=rules.format_experimentList.output[0],
    output:
        os.path.join(config["data_dir"], "{threshold}/embeddings.parquet"),
    params:
        align_key="TrackClass",
    script:
        "../scripts/data/embeddings.py"
