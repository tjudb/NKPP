package Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
object Configuration {
  val sparkSession = loadSparkSession(loadSparkConf())
  val sparkContext = loadSparkContext(sparkSession)
  var inputTriFile = ""
  var inputPredFile = ""
  var outputDIR = ""
  def loadUserSettings(inputTriFile:String,
                       inputPredFile:String,
                       outputDIR: String) = {
    this.inputTriFile = inputTriFile
    this.inputPredFile = inputPredFile
    this.outputDIR = outputDIR
  }

  /**
   * Create SparkContext.
   * The overview over settings:
   * http://spark.apache.org/docs/latest/programming-guide.html
   */
  def loadSparkConf(): SparkConf = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("NCI_INDEX")
   
    conf
  }

  def loadSparkSession(conf: SparkConf): SparkSession  = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    spark
  }
  def loadSparkContext(sparkSession: SparkSession): SparkContext = {
    sparkSession.sparkContext
  }
}
