package DataProcess
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext

  import spark.implicits._
  def getTriples(triplesFile: String): DataFrame = {
    val triDF = spark.sqlContext.read.parquet(triplesFile).toDF()
    triDF.show(false)
    triDF
  }
  // print pred info
  def getPreds(predsFile: String): DataFrame = {
    val predsDF = spark.sqlContext.read.parquet(predsFile).toDF()
    predsDF.show(false)
    predsDF
  }
}
