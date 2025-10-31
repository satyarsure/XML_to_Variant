from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, when, lit, current_timestamp
from pyspark.sql.types import StringType

inputPath = spark.conf.get("inputPath")

@dp.table(
    name="ccda_ingest_bronze",
    comment="Ingested CCDA data as whole text",
    table_properties={
        'quality': 'bronze',
        'delta.enableChangeDataFeed': 'true',
        'delta.enableDeletionVectors': 'true'
    }
)
def ccda_ingest_bronze():
    """
    Reads the raw CCDA data xml files as whole text and creates a streaming source using Auto Loader.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.allowOverwrites", "true")
        .option("wholeText", "true")
        .load(inputPath)
        .select("*",
            col("_metadata.file_path").alias("file_path"),
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_size").alias("file_size"),
            col("_metadata.file_block_start").alias("file_block_start"),
            col("_metadata.file_block_length").alias("file_block_length"),
            col("_metadata.file_modification_time").alias("file_modification_time"))
        .withColumn("load_datetime", current_timestamp())
    )
