package DataStorage

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File
import scala.util.control.Breaks.break
 
object IndexSave {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext

  import spark.implicits._
  def read(_path: String): DataFrame = {
    spark.read.parquet(_path).toDF()
  }
  def indexSave(_path: String): Unit = {
    val statisDF = read(_path).toDF()
    statisDF.show(false)
    statisDF.select("pred").map(_.toString()).distinct().collect().foreach(_pred => {
      val pp = _pred.replace("[", "").replace("]", "")
      val predDF = statisDF
        .select("pred", "stPath", "maxPath", "cirExist", "cirMinPath")
        .where(statisDF("pred") === pp)
      predDF.show(false)
      val cirDF = predDF.select("stPath", "cirMinPath").where(predDF("cirExist") === true).toDF()
   
      cirDF.select("stPath").distinct().toDF().collect().foreach(row => {
        val filename = pp + "_" + row.get(0) + "_1_NCI"
        cirDF.select("cirMinPath").distinct().toDF().write.parquet(Configuration.Configuration.outputDIR + File.separator + "NCI" + File.separator + filename)
      })
      val nocirDF = predDF.select("stPath", "maxPath").where(predDF("cirExist") === false).toDF()
     
      var count = 0
      nocirDF.select("stPath").distinct().toDF().collect().foreach(row => {
        count += 1
        val filename = pp + "_" + row.get(0) + "_0_NCI"
        nocirDF.select("maxPath").distinct().toDF().write.parquet(Configuration.Configuration.outputDIR + File.separator + "NCI" + File.separator + filename)
        if(count >= 100) break
      })
      println("[processing done...] " + pp)
    })

  }
}
