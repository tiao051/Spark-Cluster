import org.apache.spark.sql.SparkSession

val multiline_df = spark.read.option("multiline","true").json("/workspace/test.json")
multiline_df.show(false)