checkpoint format_experimentList:
    input:
        config["resources"]["experimentList"],
    output:
        config["resources"]["experimentList_parquet"],
    script:
        "../scripts/resources/format_experimentList.py"
