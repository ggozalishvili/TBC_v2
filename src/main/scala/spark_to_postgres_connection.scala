import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit,avg}

object spark_to_postgres_connection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(name = "spark_to_postgres_connection")
      .master(master = "local")
      .config("spark.sql.warehouse.dir", "C:\\apps\\opt\\spark-3.1.1-bin-hadoop2.7\\warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val clientdf = spark.read
      .format(source = "jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/TBC")
      .option("dbtable", "\"client\"")
      .option("password", "123456")
      .load()

    val currencydf = spark.read
      .format(source = "jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/TBC")
      .option("dbtable", "\"currency\"")
      .option("password", "123456")
      .load()

    val df = spark.read
      .format(source = "jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/TBC")
      .option("dbtable", "\"transaction\"")
      .option("password", "123456")
      .load()

    val transactiondf = df.filter(df("date").gt(lit("2020-09-22"))).filter(df("date").lt(lit("2020-09-29")))

    val joined_df = transactiondf.join(currencydf, transactiondf("currencyid") === currencydf("id"), "left")

    val final_joined = joined_df.join(clientdf, joined_df("iban") === clientdf("iban") && joined_df("date") === clientdf("date"), "inner").drop(clientdf("iban")).drop(clientdf("date"))

    final_joined.write.option("path", "C:\\").option("encoding", "UTF-8").saveAsTable("t")

    val byIban = Window.partitionBy("iban")
    val final_version = final_joined.withColumn("avg amount", avg("amount") over byIban) //withColumn("count", expr("count(amount") over byIban)

    final_version.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv("file:///C:/out.csv")
  }
}

