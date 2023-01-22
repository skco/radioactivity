
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("fundament-sparka")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val radioActivityDF = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .option("inferschema", true)
      .csv("src/main/scala/RADIO2.csv")
      .withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd hh:mm"))
      .withColumn("pulses_per_minute",col("pulses_per_minute").cast("integer"))

    radioActivityDF.printSchema()
    radioActivityDF.show()

    radioActivityDF.select(mean(radioActivityDF("pulses_per_minute"))).show()
    radioActivityDF.select(max(radioActivityDF("pulses_per_minute"))).show()
    radioActivityDF.select(min(radioActivityDF("pulses_per_minute"))).show()
  }

}
