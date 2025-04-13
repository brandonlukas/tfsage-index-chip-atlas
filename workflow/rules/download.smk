import os
from tfsage.download import download_chip_atlas


rule download:
    output:
        os.path.join(config["downloads_dir"], "{threshold}/{experiment}.bed"),
    retries: 5
    run:
        download_chip_atlas(wildcards.experiment, output[0], wildcards.threshold)
