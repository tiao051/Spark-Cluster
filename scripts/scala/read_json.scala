import org.apache.spark.sql.SparkSession

val multiline_df = spark.read.option("multiline","true").json("/workspace/data/test.json")
multiline_df.show(false)
