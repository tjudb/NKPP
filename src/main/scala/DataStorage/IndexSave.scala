package DataStorage

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File
import scala.util.control.Breaks.break

// 文件名
// st_min_max_cnt
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
      // 保存数据
      // 得到起点
      cirDF.select("stPath").distinct().toDF().collect().foreach(row => {
        val filename = pp + "_" + row.get(0) + "_1_NCI"
        cirDF.select("cirMinPath").distinct().toDF().write.parquet(Configuration.Configuration.outputDIR + File.separator + "NCI" + File.separator + filename)
      })
      val nocirDF = predDF.select("stPath", "maxPath").where(predDF("cirExist") === false).toDF()
      // 保存数据
      var count = 0
      nocirDF.select("stPath").distinct().toDF().collect().foreach(row => {
        count += 1
        val filename = pp + "_" + row.get(0) + "_0_NCI"
        nocirDF.select("maxPath").distinct().toDF().write.parquet(Configuration.Configuration.outputDIR + File.separator + "NCI" + File.separator + filename)
        if(count >= 100) break
      })
      println("[processing done...] " + pp)
    })
//    spark.read.text(Configuration.Configuration.outputDIR + File.separator + "NCI" + File.separator + "4_11_0_NCI").show(false)
    // 现在：
    // -- NCI
    // ------ PP_ST_TYPE_NCI
    // ------ PATH

    // --文件名:　pred_type_NCI: 3_type_NCI ---> type 1:cir 0:non-cir
    // ----st_min_max_cnt 3786952_2979_2177869_358
    // -----可达节点 2177869 3473503 [路径]
    // -----可达节点 2161008 3473503 【路径】
    // 文件名
    // 根据起点，类型分类
    // 然后保存到一个文件夹

  }
}
