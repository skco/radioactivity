
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object Radioactivity {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("radioactivity")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawData = spark.read
                       .option("header", value = true)
                       .option("delimiter", ";")
                       .option("inferschema", value = true)
                       .csv("radioactivitySampleData.csv")


    val radioActivityDF     =  rawData.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd hh:mm"))
                                      .withColumn("cpm",col("cpm").cast("integer"))
                                      .withColumn("avg10",avg(col("cpm")).over(Window.rowsBetween(-10,0))) // moving average len 10
                                      .withColumn("avg10",round(col("avg10"),2))

    // total max,min, avg
      radioActivityDF.select(mean(radioActivityDF("cpm"))).show()
      radioActivityDF.select(max(radioActivityDF("cpm"))).show()
      radioActivityDF.select(min(radioActivityDF("cpm"))).show()

      //average cpm per hour
      val radioactivityHourDF:Dataset[Row] = radioActivityDF.groupBy(hour(col("datetime")).alias("hour"))
                                                            .agg(mean("cpm")
                                                            .alias("cpmByHour"))
                                                            .withColumn("cpmByHour",round(col("cpmByHour"),1))
                                                            .sort("hour")
      radioactivityHourDF.show(100)

      //radioActivityDF.show(1300)
      //println(s"Records count:${radioActivityDF.count()}")
      //radioActivityDF.printSchema()
  }

}
