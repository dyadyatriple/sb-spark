import org.apache.spark.sql.types.{StringType, StructType,TimestampType,LongType,DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object filter {
  def main()={
    val spark = SparkSession.builder()
      .appName("filter")
      .getOrCreate()
    val offset = spark.sparkContext.getConf.get("offset")
    val topicName = spark.sparkContext.getConf.get("topic_name")
    val output_dir_prefix = spark.sparkContext.getConf.get("output_dir_prefix")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topicName)
      .option("startingOffsets",
        if (offset.contains("earliest"))
          offset
        else {
          "{\"" + topicName + "\":{\"0\":" + offset + "}}"
        }
      ).load()

    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", DoubleType, true)
      .add("uid", StringType, true)
      .add("timestamp", LongType, true)

    val ParsedValues = df.select(from_json(col("value").cast("string"), schema).alias("value"))
      .select(col("value.*"))
      .withColumn("date", date_format(($"timestamp" / 1000).cast(TimestampType), "yyyymmdd"))
      .withColumn("p_date", date_format(($"timestamp" / 1000).cast(TimestampType), "yyyymmdd"))

    val buyValues = ParsedValues.filter($"event_type" === "buy")
    val viewValues = ParsedValues.filter($"event_type" === "view")


    buyValues.write
      .partitionBy("p_date")
      .mode("append")
      .json(s"$output_dir_prefix/buy")

    viewValues.write
      .partitionBy("p_date")
      .mode("append")
      .json(s"$output_dir_prefix/view")
  }
}
