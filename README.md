# Delta Stream Merge Test

```
docker-compose run --rm spark
```

Spark UI: http://localhost:4040

## test in container

### launch pyspark
```
/opt/spark/bin/pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"
```

```
import pyspark
from delta.tables import DeltaTable
import pyspark.sql.functions as F

DeltaTable.forPath(spark, "/tmp/delta-content-enriched").toDF().count()
                                                                         
DeltaTable.forPath(spark, "/tmp/delta-content").toDF().count()

DeltaTable.forPath(spark, "/tmp/delta-content").toDF().show(truncate=False)

DeltaTable.forPath(spark, "/tmp/delta-gold").toDF().show(truncate=False)
```