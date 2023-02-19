class data_mart {
  def main()= {
    import org.apache.spark.sql.{DataFrame, SparkSession}
    import org.apache.spark.sql.functions._
    import java.sql.{Connection, DriverManager, Statement}
    import org.apache.spark.sql.expressions.UserDefinedFunction
    import org.apache.spark.sql.functions.udf

    import java.net.{URL, URLDecoder}
    import scala.util.Try
    //...
    //внутри класса/объекта:

    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()


    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load().cache()

    val ageBins: String =
      """case when age>=18 and age<=24 then '18-24'
      when age>=18 and age<=24 then '18-24'
      when age>=25 and age<=34 then '25-34'
      when age>=35 and age<=44 then '35-44'
      when age>=45 and age<=54 then '45-54'
      when age>=55 then '>=55'
      else null end as age_cat"""
    val clientsProcessed: DataFrame = clients.selectExpr("uid", ageBins, "gender")


    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")
    val visitsCleaned = visits.filter("uid is not null").withColumn("cat", lower(regexp_replace($"category", lit("""[" ",-]"""), lit("_"))))
      .drop("category").withColumnRenamed("cat", "category")


    val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    val transformedLogs: DataFrame = logs.select($"uid", explode($"visits.url").as("url"), $"visits.timestamp".as("timestamp"))

    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        new URL(URLDecoder.decode(url, "UTF-8")).getHost
      }.getOrElse("")
    })

    val logsProcessed: DataFrame = transformedLogs.select($"uid", decodeUrlAndGetDomain($"url").alias("domain"), $"timestamp")
      .select($"uid", regexp_replace($"domain", lit("www."), lit("")).as("domain"), $"timestamp")
      .filter($"domain".isNotNull && $"uid".isNotNull)

    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "daniil_dudochkin")
      .option("password", "X6TIT7bL")
      .option("driver", "org.postgresql.Driver")
      .load()

    val logsWithCats = logsProcessed.join(cats, Seq("domain"), "left")
      .filter($"category".isNotNull)
      .groupBy("uid").pivot("category").agg(count($"domain")) //.agg(first(size($"timestamp")))

    def NewName(name: String, prefix: String): String = {
      name match {
        case "uid" => "uid";
        case _ => s"${prefix}_$name"
      }
    }

    val newNamesCats = logsWithCats.columns.map(x => NewName(x, "web"))
    val renamedLogsPivoted = logsWithCats.toDF(newNamesCats.toSeq: _*).withColumnRenamed("uid", "uid_t")

    val visitsPivoted = visitsCleaned.withColumn("n", lit(1)).groupBy("uid").pivot("category").agg(sum("n"))
    val newNamesVisits = visitsPivoted.columns.map(x => NewName(x, "shop"))
    val renamedVisitsPivoted = visitsPivoted.toDF(newNamesVisits.toSeq: _*)

    val LogsNVisits = renamedVisitsPivoted
      .join(renamedLogsPivoted, renamedLogsPivoted("uid_t") === renamedVisitsPivoted("uid"), "full")
      .withColumn("uid_new", coalesce($"uid", $"uid_t")).drop("uid").drop("uid_t")
      .withColumnRenamed("uid_new", "uid").where($"uid".isNotNull)
    val res = clientsProcessed.join(LogsNVisits, Seq("uid"), "left").filter($"uid".isNotNull)
    res.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/daniil_dudochkin")
      .option("dbtable", "clients")
      .option("user", "daniil_dudochkin")
      .option("password", "X6TIT7bL")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true) //позволит не терять гранты на таблицу
      .mode("overwrite") //очищает данные в таблице перед записью
      .save()
  }
}
