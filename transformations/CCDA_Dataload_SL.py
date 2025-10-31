from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, from_xml, current_timestamp


@dp.table(
    name="ccda_variant_silver",
    comment="Convert CCDA data from text to variant",
    table_properties={
        'quality': 'silver',
        'delta.enableChangeDataFeed': 'true',
        'delta.enableDeletionVectors': 'true',
        "delta.feature.variantType-preview": "supported"
    }
)
def ccda_variant_silver():
    # Create corrupt record detection expression
    corrupt_check = from_xml(
        col("value"),
        "_corrupt_record STRING",
        {
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record"
        }
    ).getField("_corrupt_record")
    
    df = (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("ccda_ingest_bronze")
        .withColumn(
            "parsed_xml",
            when(
                corrupt_check.isNull(),
                from_xml(
                    col("value"),
                    "variant",
                    {
                        "mode": "PERMISSIVE",
                        "columnNameOfCorruptRecord": "_corrupt_record",
                        "ignoreMalformed": "true",
                        "multiline": "true"
                    }
                )
            ).otherwise(None)
        )
        .withColumn("_corrupt_record", corrupt_check)
        .withColumnRenamed("load_datetime", "bz_load_datetime")
        .withColumn("load_datetime", current_timestamp())
        .drop("value")
    )
    return df