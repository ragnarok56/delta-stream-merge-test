import pyspark
from delta.tables import DeltaTable
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.getOrCreate()

raw_delta_table_path = '/tmp/delta'
raw_content_table_path = '/tmp/delta-content'
enriched_content_table_path = '/tmp/delta-content-enriched'
delta_gold_table_path = '/tmp/delta-gold'



def raw_data_job():
    """
    simulate a flow of data where processing raw is an expensive operation.

    write _all_ records to the `delta` table
    write only unique "content" to the `delta-content` table
    """

    (DeltaTable.createIfNotExists(spark)
        .addColumn('value', 'LONG')
        .addColumn('timestamp', 'TIMESTAMP')
        .location(raw_content_table_path)
        .execute()
    )

    df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 10) \
        .load()

    df = df.withColumn('value', F.col('value') % 10)

    def _batch_write(batchDF, id):
        df_cached = batchDF.cache()
        # all records
        df_cached.repartition(1).write.mode("append").format("delta").save(raw_delta_table_path)

        # only values > 0 and no dupes in the batch
        df_cached_no_dupes = (df_cached
            .filter(F.col('value') > 0)
            .dropDuplicates(['value'])
        )

        no_dupe_table = DeltaTable.forPath(spark, raw_content_table_path)
        (
            no_dupe_table
                .alias('target')
                .merge(
                    df_cached_no_dupes.alias('source'), 'target.value = source.value'
                )
                .whenNotMatchedInsertAll()
                .execute()
        )

    df = (df.writeStream
        .queryName("raw")
        .foreachBatch(_batch_write)
        .start()
    )

    return df

def enrich_content_job():
    """
    "enrichment" job that processes unique "content" from the `delta-content` table

    results are merged into the `delta-content-enriched` table
    """

    (DeltaTable.createIfNotExists(spark)
        .addColumn('value', 'LONG')
        .addColumn('value_enriched', 'LONG')
        .location(enriched_content_table_path)
        .execute()
    )

    def _batch_write(batchDF, id):
        target_table = DeltaTable.forPath(spark, enriched_content_table_path)
        (
            target_table
                .alias('target')
                .merge(
                    batchDF.alias('source'), 'target.value = source.value'
                )
                # insert all non matching records as new
                .whenNotMatchedInsertAll()
                # update existing records with new enriched values from source
                .whenMatchedUpdateAll()
                .execute()
        )

    df = (spark.readStream
        .format("delta")
        .load(raw_content_table_path)
        .withColumn('value_enriched', F.col('value') * 100)
        .drop('timestamp')
        .writeStream
        .format("delta")
        .queryName("content-enrich")
        .foreachBatch(_batch_write)
        .option('checkpointLocation', '/tmp/checkpoints/delta-content-enriched')
        .start()
    )

    return df

def delta_gold_job():
    """reformat table so value is in a struct"""

    (DeltaTable.createIfNotExists(spark)
        .addColumn('gold', 'struct<value:LONG, value_enriched:LONG, timestamp:TIMESTAMP>')
        .addColumn('raw', 'struct<value:LONG, timestamp:TIMESTAMP>')
        .location(delta_gold_table_path)
        .execute()
    )

    df = (spark.readStream
        .format("delta")
        .load(raw_delta_table_path)
        .select(F.struct('value', 'timestamp').alias('gold'), F.struct('value', 'timestamp').alias('raw'))
        .writeStream
        .format("delta")
        .trigger(processingTime='10 seconds')
        .queryName("gold")
        .outputMode('append')
        .option('checkpointLocation', '/tmp/checkpoints/delta-gold')
        .start(delta_gold_table_path)
    )

    return df

def update_gold_with_enrichment_job():
    """
    this streams from the enrichment table but that only updates basically once
    so the gold table never gets updated again after this executes the first time :|
    """
    def _batch_write(batchDF, id):
        target_table = DeltaTable.forPath(spark, delta_gold_table_path)
        (
            target_table
                .alias('target')
                .merge(
                    batchDF.alias('source'), 'target.gold.value = source.value'
                )
                # insert all non matching records as new
                .whenNotMatchedInsertAll()
                # update existing records with new enriched values from source
                .whenMatchedUpdate(set = {
                    "gold.value_enriched": "source.value_enriched"
                })
                .execute()
        )

    df = (spark.readStream
        .format("delta")
        .load(enriched_content_table_path)
        # force single partition to maybe avoid write conflicts with itself?
        # update: this didnt work :(
        .repartition(1)
        .writeStream
        .option("mergeSchema", "true")
        .format("delta")
        # set trigger time to be slightly offset from the delta-gold job
        # so they dont concurrently write to the unpartitioned table
        # update: this worked but feels not great
        .trigger(processingTime='15 seconds')
        .queryName("gold-enrichment")
        .foreachBatch(_batch_write)
        .outputMode('append')
        .option('checkpointLocation', '/tmp/checkpoints/gold-enrichment')
        .start()
    )

    return df

all_dfs = [
    raw_data_job(),
    enrich_content_job(),
    delta_gold_job(),
    update_gold_with_enrichment_job()
]

for df in all_dfs:
    df.awaitTermination()
