import pyspark
from delta.tables import DeltaTable
import pyspark.sql.functions as F

spark = (pyspark.sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel('error')

spark.createDataFrame([
    ["a", ([("aaa","file_a")], ("aaa", "file_a"), "1")],
    ["b", ([("bbb","file_b")], ("bbb", "file_b"), "1")],
    ["c", ([("ccc","file_c")], ("ccc", "file_c"), "1")]
], "old: string, cd: struct<m: array<struct<hash: string, filename: string>>, d: string>").write.format("delta").mode("overwrite").save("/tmp/merge-test")

df = spark.createDataFrame([
    ["aaa", ([("aaa", 1000)], ("aaa", 1000), "1")],
    ["bbb", ([("bbb", 2000)], ("bbb", 1000), "1")],
    ["ccc", ([("ccc", 3000)], ("ccc", 1000), "1")]
], "hash:string, cd:struct<m:array<struct<hash:string, filesize:int>>, d: string>")

dt = DeltaTable.forPath(spark, "/tmp/merge-test")

(dt.alias("target")
    .merge(
        df.alias("source"),
        F.element_at('target.cd.m', 1).getField('hash') == F.element_at('source.cd.m', 1).getField('hash'))
    .whenMatchedUpdate(set = {
        # this works as expected
        # "cd.m": "source.cd.m"

        # this fails with
        # Caused by: org.apache.spark.SparkException: [INTERNAL_ERROR] Cannot evaluate expression: update_fields(lambda x_0#906, WithField(filesize, 1000000))
        "cd.m": F.transform("target.cd.m", lambda x: x.withField('filesize', F.lit(1000000)))
    })
    .execute()
)

dt = DeltaTable.forPath(spark, "/tmp/merge-test")
updated_df = dt.toDF()
updated_df.printSchema()
print(updated_df.show(truncate=False))