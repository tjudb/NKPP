
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

 
object NCIBuilder {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    //     todo: read triple (get data)
   val inputTriFile = DataProcess.DataReader.getTriples(args(0))
   val inputPredFile = Da0taProcess.DataReader.getPreds(args(1))
   val outputDIR = args(2)
   //  Configuration.Configuration.loadUserSettings(args(0), args(1), args(2))
 
    Configuration.Configuration.loadUserSettings(inputTriFile, inputPredFile, outputDIR)
    val triDF = DataProcess.DataReader.getTriples(inputTriFile)
    val predsDF = DataProcess.DataReader.getPreds(inputPredFile)

    // todo: DataAnalysis (get pred, group by pred, join)
 
    val st = System.currentTimeMillis()
    DataAnalysis.DataAnalysis.processData(triDF, predsDF)
    val end = System.currentTimeMillis()
    println("[DONE] " + (end - st) + "ms")

  }
}
