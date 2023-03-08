import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, LongType, DoubleType}


object user_items {
  def main(args: Array[String]) = {


    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", DoubleType, true)
      .add("uid", StringType, true)
      .add("timestamp", LongType, true)
      .add("date", StringType, true)

    def NewName(name: String, prefix: String): String = {
      name match {
        case "uid" => "uid";
        case _ => s"${prefix}_$name"
      }
    }

    val spark = SparkSession.builder()
      .appName("user_items")
      .getOrCreate()

    def preproc(df: DataFrame, kind: String): DataFrame = {
      import spark.implicits._
      val tmp = df.select($"uid",
        lower(regexp_replace($"item_id", """[" ",-]""", "_")).as("item_id"))
        .where($"uid".isNotNull)
        .groupBy("uid")
        .pivot("item_id")
        .agg(count("item_id"))

      val newCols = tmp.columns.map(x => NewName(x, kind))
      return tmp.toDF(newCols.toSeq: _*).withColumnRenamed("uid", s"uid_$kind")
    }

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._
    val input_dir = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    val output_dir = spark.sparkContext.getConf.get("spark.users_items.output_dir")
    val update = spark.sparkContext.getConf.get("spark.users_items.update")

    val maxDateExist = "20200429"
    val maxDateRewrite = "20200430"


    val existingParquet = if (update == "1")
      spark.read.option("mergeSchema", "true")
        .parquet(s"$output_dir/users-items/$maxDateExist")
    else spark.emptyDataFrame


    val viewData = preproc(spark.read.option("mergeSchema", "true").schema(schema).json(s"$input_dir/view/*"), "view")
    val buyData = preproc(spark.read.option("mergeSchema", "true").schema(schema).json(s"$input_dir/buy/*"), "buy")

    val userMatrix = viewData
      .join(buyData, viewData("uid_view") === buyData("uid_buy"), "full")
      .withColumn("uid", coalesce($"uid_view", $"uid_buy"))
      .drop("uid_view", "uid_buy")
    if (!existingParquet.isEmpty) {
      userMatrix.withColumn("test_test", lit(1)).write.mode("overwrite").parquet("/user/daniil.dudochkin/test")
      val newParquet = spark.read.option("mergeSchema", "true").parquet("/user/daniil.dudochkin/test")
      val res = newParquet.unionAll(existingParquet)
      val aggs = res.columns.filter(x => x != "uid").map(x => sum(x).as(s"$x"))
      res.groupBy("uid")
        .agg(aggs.head, aggs.tail: _*)
        .where($"uid".isNotNull)
        .write.mode("overwrite")
        .parquet(s"$output_dir/users-items/$maxDateRewrite")
    }
    else {
      userMatrix.write.mode("overwrite").parquet(s"$output_dir/users-items/$maxDateExist")
    }
  }
}