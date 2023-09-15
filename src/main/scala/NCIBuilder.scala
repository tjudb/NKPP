
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/*
索引生成，2023-04-10
 */
object NCIBuilder {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val inputTriFile = "F:\\dbproject\\NCI_MAPPING\\data\\uobm100\\uobm100.tri"
    val inputPredFile = "F:\\dbproject\\NCI_MAPPING\\data\\uobm100\\uobm100.p"
    val outputDIR = "F:\\dbproject\\NCI_INDEX\\RESULT\\UOBM100"
    //     todo: read triple (get data)
//    val triDF = DataProcess.DataReader.getTriples(args(0))
//    val predsDF = Da0taProcess.DataReader.getPreds(args(1))
//    val outputDIR = args(2)
//    Configuration.Configuration.loadUserSettings(args(0), args(1), args(2))

    // =============
    Configuration.Configuration.loadUserSettings(inputTriFile, inputPredFile, outputDIR)
    val triDF = DataProcess.DataReader.getTriples(inputTriFile)
    val predsDF = DataProcess.DataReader.getPreds(inputPredFile)

    // todo: DataAnalysis (get pred, group by pred, join)
    // 目前的方法：处理图+路径的会存在冗余
    val st = System.currentTimeMillis()
    DataAnalysis.DataAnalysis.processData(triDF, predsDF)
    val end = System.currentTimeMillis()
    println("[DONE] " + (end - st) + "ms")



    // todo: Index Save
//    val stsave = System.currentTimeMillis()
//    DataStorage.IndexSave.indexSave(Configuration.Configuration.outputDIR)
//    val endsave = System.currentTimeMillis()
//    println("[SAVE TIME] " + (endsave - stsave) + "ms")
  }


  def addData(triple: sql.DataFrame): sql.DataFrame ={
    val schema = StructType(List(
      StructField("sub", LongType, nullable = true),
      StructField("pred",LongType, nullable = true),
      StructField("obj", LongType, nullable = true)
    ))

    val rdd = _sc.parallelize(Seq(
      Row(100L, 1L, 13L) //入环边
//      Row(11L, 1L, 101L)// 出环边
//      Row(101L, 1L, 102L),
//      Row(103L, 1L, 104L), // 无连接边
//      Row(104L, 1L, 105L),
//      Row(106L, 1L, 107L)
    ))
    val df = spark.createDataFrame(rdd, schema)

    triple.union(df)
  }
}
