package DataAnalysis

import org.apache.spark.sql.functions.{col, collect_list, collect_set, count, explode, split, struct}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SPARK_BUILD_DATE, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import java.io.File
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object DataAnalysis {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._

  /*
  pred: 当前谓词
  predCount: 当前谓词的三元组数量
  stCount: 起点个数
  endCount: 终点个数
  stPath: 路径起点
  maxPath: 最长路径
  maxPathLen: 最长路径占比
  lenRatioBasePred: 在当前谓词的范围内，最长路径占比
  lenRatioBaseTri: 在整个数据的范围内，最长路径占比
  cirExist: 是否存在环
  cirNum: 环的个数
  cirMinPath: 环路径
  cirMinPathLen: 环的最小长度
  cirlenRatioBasePred：在当前谓词的范围内，环路径占比
  cirlenRatioBaseTri：在整个数据的范围内，环路径占比
   */
  case class RDFAnalysis(pred: String, predCount: Long, stCount: Long, endCount: Long,
                         stPath: String,
                         maxPath: String, maxPathLen: Long, lenRatioBasePred: Double, lenRatioBaseTri: Double,
                         cirExist: Boolean, cirNum: Int = 0, cirMinPath: String, cirMinPathLen: Long, cirlenRatioBasePred: Double, cirlenRatioBaseTri: Double)


  class MyAccumulator extends AccumulatorV2[Row, util.ArrayList[Row]] {
    val list = new util.ArrayList[Row]()
    override def isZero: Boolean = {
      list.isEmpty
    }

    override def copy(): AccumulatorV2[Row, util.ArrayList[Row]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      list.clear()
    }

    override def add(v: Row): Unit = {
      list.add(v)
    }

    override def merge(other: AccumulatorV2[Row, util.ArrayList[Row]]): Unit = {
      list.addAll(other.value)
    }

    override def value: util.ArrayList[Row] = {
      list
    }
  }
 
  def stPointEndPoint(tri: DataFrame): Unit = {
 
    val preds = tri.select("pred").distinct().collect()
    preds.foreach(row => {
      val curpp = row.get(0).toString
      val predDF = getTripleByPred(tri, curpp)
      val predCount = predDF.count()
      val st = predDF.select("sub").except(predDF.select("obj")).count()
      val end = predDF.select("obj").except(predDF.select("sub")).count()
      if(st != predDF.select("sub").distinct().count() || end != predDF.select("obj").distinct().count()) {
        println("[Processing] " + curpp)
        println("--[predCount] " + predCount)
        println("----[start point] " + st + " --> " + predDF.select("sub").except(predDF.select("obj")).distinct().count())
        println("----[end point] " + end + " --> " + predDF.select("obj").except(predDF.select("sub")).distinct().count())
      }
 
    })
  }

 

 
  var expreList: ListBuffer[ListBuffer[String]] = new ListBuffer[ListBuffer[String]]()
 
  var _expre: ListBuffer[String] = new ListBuffer[String]()

  def processData(tri: DataFrame, predsDF: DataFrame): Unit = {
    val rdfAnalysisList = new MyAccumulator
    _sc.register(rdfAnalysisList)
 
    val encoderSchema = Encoders.product[RDFAnalysis].schema

    val triCount = tri.count()
    val preds = tri.select("pred").distinct().collect()
 

    preds.foreach(row => {
      val curpp = row.get(0).toString
      println("[Processing] " + curpp)
   
      if (!check(curpp)) {
        val predDF = getTripleByPred(tri, curpp)
        if (checkStar(predDF)) {
       
          val predDF = getTripleByPred(tri, curpp)
          val predCount = predDF.count()
          println("[START]============================")
          println("|---[PRED] " + curpp + ": star")
          println("|---[count curDF] " + predCount)

          val starSt = System.currentTimeMillis()
          val starRES = getKleeneStar(predDF, curpp)
          val starEnd = System.currentTimeMillis()
          println("[KLEENE STAR TIME] " + (starEnd - starSt) + "ms")
          println("|------[STAR RESULT] GET ")
          println("|------[JOIN COUNT] " + starRES._2)
          println("|------[KLEENE STAR TYPE] " + starRES._3)
  
          saveNCI(starRES._1, starRES._2, starRES._3, curpp)
          println("|------[Single-SAVE DONE] ")
          println("[END]============================")

        } else {
         
          println("|---[PRED] " + curpp + ": no star")
        }
      }else {
 
        println("|---[found]")
      }
    })

 
    val _preds = predsDF.select("predID").distinct().map(_.toString().replace("[","").replace("]", "")).collect()
 
    val _pathpool: mutable.Set[String] = mutable.Set("-")
    experGenera(_preds.toSet)
    println("[exper] " + expreList.size)
    expreList.foreach(row => {
      var pre = ""
      var after = ""
      if (row.size == 3) {
        pre = row.apply(0) + "_" + row.apply(1)
        after = row.apply(1) + "_" + row.apply(2)
      } else {
        pre = row.apply(0) + "_" + row.apply(1)
        after = ""
      }
      val curpp = row.mkString("_")
      println("[Processing] " + curpp)
      if (_pathpool.contains(pre) || _pathpool.contains(after)) {
     
        println("|---[EXPER] " + curpp + ": no star")
      } else {
        val predDF = getTripleByPredEXPER(tri, curpp)
 
        if (checkStar(predDF)) {
  
          val predCount = predDF.count()
          println("[START]============================")
          println("|---[EXPER] " + curpp + ": star")
          val starSt = System.currentTimeMillis()
          val starRES = getKleeneStar(predDF, curpp)
          val starEnd = System.currentTimeMillis()
          println("[KLEENE STAR TIME] " + (starEnd - starSt) + "ms")
          println("|------[STAR RESULT] GET ")
          println("|------[JOIN COUNT] " + starRES._2)
          println("|------[KLEENE STAR TYPE] " + starRES._3)
          saveNCI(starRES._1, starRES._2, starRES._3, curpp, true)
          println("|------[EXPER-SAVE DONE] ")
        }else {
          _pathpool += pre
          _pathpool += after
          println("|---[EXPER] " + curpp + ": no star")
        }
      }
    })
  }

  
  def check(str: String): Boolean = {
 
    val indexDIR = new File(Configuration.Configuration.outputDIR)
    val files = indexDIR.listFiles()
    if(files.map( file => file.getName.split("_").apply(0)).contains(str)){
      true
    }else false
  }

  def saveNCI(res: DataFrame, len: Long, cir: Boolean, pp: String, exper: Boolean = false): Unit = {
    if(!exper) {
      res.select("value").write.parquet(Configuration.Configuration.outputDIR + File.separator
        + pp + "_" + cir + "_" + len + File.separator + "NCI")
    }else {
      res.select("value").write.parquet(Configuration.Configuration.outputDIR + File.separator
        + pp + "-" + cir + "-" + len + File.separator + "NCI")
    }

    println("|------[Path] " + res.count())
  }

  def dataStatis(starRES:(DataFrame, Int, Boolean, Long, Long, DataFrame, DataFrame),
                 curpp: String,
                 predCount: Long,
                 triCount: Long,
                 rdfAnalysisList: MyAccumulator): Unit = {
  
    val analySt = System.currentTimeMillis()
    val starPath = starRES._1
    starPath.show(false)
    val _cir = starRES._3
   
    val tempStarDF = starPath.select("value", "cir").where(col("cir") === "1").toDF()
  
    if (tempStarDF.count() != 0) {  
      val mergePath = tempStarDF.select("value")
        .map(_.toString().replace("[", "").replace("]", "").split(",").sorted.toSet.toSeq).distinct().collect()
      cirCount = mergePath.size 
      var count = 0
      mergePath.foreach(path => {
    
        count += 1
        val _path = path.mkString(",")
        var _length = path.size.toLong
        if (_length == 1) _length += 1  
        rdfAnalysisList.add(Row(curpp, predCount, starRES._4, starRES._5,
          path.apply(0),
          "", 0L, 0.0, 0.0,
          _cir, cirCount, _path, _length - 1, ((_length - 1) * 1.0) / predCount * 1.0, ((_length - 1) * 1.0) / triCount * 1.0
        ))
        if (count >= 100) break
      })
    }
 
    val tempStarDF1 = starPath.select("value", "cir").where(col("cir") === "0").toDF()
    if (tempStarDF1.count() != 0) {
      val mergePath1 = tempStarDF1.select("value").map(_.toString().replace("[", "").replace("]", "").split(",").toSeq).collect()
      var count = 0
      mergePath1.foreach(path => {
        count += 1
        val _path = path.mkString(",")
        rdfAnalysisList.add(Row(curpp, predCount, starRES._4, starRES._5,
          path.apply(0),
          _path, path.length.toLong - 1, ((path.length.toLong - 1) * 1.0) / predCount * 1.0, ((path.length.toLong - 1) * 1.0) / triCount * 1.0,
          _cir, cirCount, "", 0L, 0.0, 0.0
        ))
        if (count >= 100) break
      })
    }
    // ========================================
    val analyEnd = System.currentTimeMillis()
    println("[ANALYSIS TIME] " + (analyEnd - analySt) + "ms")
  }

 
  def experGenera(preds: collection.Set[String]): ListBuffer[ListBuffer[String]] = {

    preds.foreach(pred => {
      _expre.clear()
      experDFS(preds, pred)
    })
    expreList
  }

 
  def experDFS(preds: collection.Set[String], curPred: String): Unit = {
    _expre.append(curPred)
    if (_expre.length >= 2) {
      val temp: ListBuffer[String] = _expre.clone()  
      expreList.append(temp)
    }
    if (_expre.length >= 3) return
    preds.foreach(pred => {
      if (!_expre.contains(pred)) {
        experDFS(preds, pred)
        _expre = _expre.dropRight(1)
      }
    })
  }
  
  def checkStar(predTri: DataFrame): Boolean = {

 
    if(predTri.select("sub")
      .except(predTri.select("obj")).count() != predTri.select("sub").distinct().count() ) {
      true
    }else if(predTri.select("obj")
    .except(predTri.select("sub")).count() != predTri.select("obj").distinct().count()){
      true
    }else false

  }

 
  def getTripleByPred(tri: DataFrame, pp: String): DataFrame = {
    val ptri = tri.select("sub", "obj").where($"pred" === pp)
    ptri
  }
 
  def getTripleByPredEXPER(tri: DataFrame, pp: String): DataFrame = {
    val pstr = pp.split("_")
    if (pstr.size == 2) {
      val left = tri.select("sub", "obj").where($"pred" === pp.charAt(0))
      val right = tri.select("sub", "obj").where($"pred" === pp.charAt(1))
      left.createOrReplaceTempView("left")
      right.createOrReplaceTempView("right")
      //      spark.sql("select * from left").show(false)
      spark.sql("select left.sub as sub, right.obj as obj from left inner join right on left.obj = right.sub").toDF()
    } else {
      val left = tri.select("sub", "obj").where($"pred" === pp.charAt(0))
      val mid = tri.select("sub", "obj").where($"pred" === pp.charAt(1))
      val right = tri.select("sub", "obj").where($"pred" === pp.charAt(2))
      left.createOrReplaceTempView("left")
      mid.createOrReplaceTempView("mid")
      right.createOrReplaceTempView("right")
      //      spark.sql("select * from left").show(false)
      spark.sql("select left.sub as sub, right.obj as obj from left, mid, right where left.obj = mid.sub and mid.obj = right.sub").toDF()
    }
 
  }
 
  def getKleeneStar(curTri: DataFrame, pp: String): (DataFrame, Int, Boolean, Long, Long, DataFrame, DataFrame)  = {
    val joincnt = 1
    var cir = false
    curTri.createOrReplaceTempView("init")
  
    val stTable = curTri.select("sub").except(curTri.select("obj")).distinct()
    stTable.createOrReplaceTempView("stTable")
    val stCount = stTable.count()
 
    val endTable = curTri.select("obj").except(curTri.select("sub")).distinct()
    endTable.createOrReplaceTempView("endTable")
    val endCount = endTable.count()
    println("---[get stTable, endTable...] " + stCount + " " + endCount)
 
    if(stCount == 0 || endCount == 0) {
      cir = true  
    }

    var res = curTri
    var join = 0  
    if(cir) {
      val tuple = cirProcess(joincnt, pp, res)
      res = tuple._1
      join = tuple._2
    }
 
    if(!cir) {
      val tuple = maybeCirProcess(joincnt, pp, res, stTable, endTable, cir)
      res = tuple._1
      join = tuple._2
      cir = tuple._3
    }
    (res, join, cir, stCount, endCount, stTable, endTable)
  }

  def filterRes(res: DataFrame, st: DataFrame): DataFrame = {
//    st.show()
    res.createOrReplaceTempView("_res")
    st.createOrReplaceTempView("_st")
    if (spark.sql("select sub from _st").toDF().count() != 0) {
      val _sql = "select _res.* from _st left join _res on _st.sub == _res.sub"
      spark.sql(_sql).toDF()
    }else {
     res
    }
  }
 
  def cirProcess(join: Int, pp: String, curTri: DataFrame): (DataFrame, Int) = {
    println("[CIR PROCESS...] " + pp)
    val schema1 = StructType(
      Seq(
        StructField("sub", LongType, true),
        StructField("value", StringType, true),
        StructField("obj", LongType, true),
        StructField("cir", StringType, true)  
      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    var res = curTri
    var len = join
    res.createOrReplaceTempView("_init")
    res.createOrReplaceTempView("_left")
 
    val df = spark.sql("select '1' as id, _left.sub from _left ").toDF()
    df.createOrReplaceTempView("_left")

    val mergeRes = spark.sql("select id, concat_ws(',', collect_set(sub)) as value from _left group by id").toDF()
    mergeRes.createOrReplaceTempView("_left")
 
    val cirStr = mergeRes.select("value")
      .head(1)
      .map(_.toString().replace("[", "").replace("]", ""))
    len = cirStr.apply(0).split(",").length
    finalRes = spark.sql("select sub, '" + cirStr + "' as value, obj, '1' as cir from _init").toDF()
    finalRes.createOrReplaceTempView("_left")
    (finalRes, len)
  }

 

  def maybeCirProcess(cur: Int, _pre: String, curTri: DataFrame, stTable: DataFrame, _cir: Boolean): (DataFrame, Int, Boolean) = {
    println("|------[MAYBE CIRCLE PROCESS]")
    var joincnt = cur
    var pre = _pre
    var cir = _cir
    var res = curTri
    val row = curTri.count()
    val stCnt = spark.sql("select distinct sub from stTable").count()
    val endCnt = spark.sql("select distinct obj from endTable").count()
    import scala.util.control.Breaks._

    val schema1 = StructType(
      Seq(
        StructField("sub", LongType, true),
        StructField("value", StringType, true),
        StructField("obj", LongType, true),
        StructField("cir", StringType, true)  
      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)

    println("[GET ST, END TALBE]")
    spark.sql("select * from _left where sub in (select sub from stTable)").createOrReplaceTempView("_stTable")
    spark.sql("select * from _left where obj in (select obj from endTable)").createOrReplaceTempView("_endTable")

    println("[REMOVE BEFORE] " + row)

    res = spark.sql("select * from _left where sub not in (select sub from stTable) and obj not in (select obj from endTable)").toDF()

    val temp = res.count()
    println("[REMOVE AFTER] " + temp)
    if(temp != 0 ) res.createOrReplaceTempView("_left")
 
    val tempsub = spark.sql("select distinct sub from _left").toDF()

    val tempobj = spark.sql("select distinct obj from _left").toDF()
 
    var tempstDF = tempsub.select("sub").except(tempobj.select("obj")).toDF()
    var tempst = tempstDF.count()
    var tempendDF = tempobj.select("obj").except(tempsub.select("sub")).toDF()
    var tempend = tempendDF.count()

 
    if(temp == 0 && row == 2) {
    
      res = curTri
      res.createOrReplaceTempView("_left")
      tempstDF = stTable
      tempst = tempstDF.count()
      tempendDF = spark.sql("select obj from endTable").toDF()
      tempend = tempendDF.count()
    }
    if (tempst == 0 && tempend == 0 && temp != 0 && stCnt != 0 && endCnt != 0) {
      println("[CIR] True")
      curTri.show(false)
      //cur: Int, _pre: String, curTri: DataFrame
      val tuple = cirProcess(cur, pre, curTri)
      finalRes = tuple._1
      joincnt = tuple._2
      cir = true  
    }else {

      if(tempst != 0 && tempend != 0){
        // initial
      tempendDF.createOrReplaceTempView("tempendDF")
      tempstDF.createOrReplaceTempView("tempstDF")
      spark.sql("select * from _left where sub in (select sub from tempstDF)")
        .toDF().createOrReplaceTempView("_left")
 
      res.createOrReplaceTempView("_right")
      breakable{
        while(true) {
          println("[CIR] False")
          cir = false  
          println("[BIG Table] " + res.count())
          res = spark.sql(joinsql(joincnt, pre)).toDF().distinct()
          res.show(false)
          res.cache()
          println("[JOIN] " + joincnt)
          res.createOrReplaceTempView("_left")
 
          var _sql = ""
          val colName: ListBuffer[String] = ListBuffer("sub", "','", "obj")

          val lastNullDF = spark.sql("select _left.* from _left where obj" + joincnt + " is null").toDF()
          lastNullDF.createOrReplaceTempView("_lastNullDF")

          for (x <- 1 to (joincnt - 1)) {
            colName += "','"
            colName += "obj" + x
          }

          if ((joincnt - 1) == 0) {
            _sql = "select sub, concat(" + colName.mkString(",") + ") as value, obj, '0' as cir from _lastNullDF"
          } else {
            _sql = "select sub, concat(" + colName.mkString(",") + ") as value, obj" + (joincnt) + " as obj, '0' as cir from _lastNullDF"
          }
          val doneDF = spark.sql(_sql).toDF()
          finalRes = finalRes.union(doneDF).distinct()
          finalRes.cache()

          val lastEndDF = spark.sql("select _left.* from _left where obj" + joincnt + " in (select obj from tempendDF)").toDF()
          lastEndDF.createOrReplaceTempView("_lastEndDF")

          colName += "','"
          colName += "obj" + joincnt

          if ((joincnt) == 0) {
            _sql = "select sub, concat(" + colName.mkString(",") + ") as value, obj, '0' as cir from _lastEndDF"
          } else {
            _sql = "select sub, concat(" + colName.mkString(",") + ") as value, obj" + (joincnt) + " as obj, '0' as cir from _lastEndDF"
          }

          val doneDF1 = spark.sql(_sql).toDF()
          // 更新 finalRes
          finalRes = finalRes.union(doneDF1).distinct()
          finalRes.cache()
          finalRes.createOrReplaceTempView("_finalRes")
          println("[::next]")
          res = res.except(lastNullDF).except(lastEndDF).dropDuplicates("obj" + joincnt).toDF()
          res.createOrReplaceTempView("_left")

      
          if (res.count() == 0 || (row) == joincnt || joincnt >= 5) {
            if (temp == 0) {
              finalRes = finalRes
              break
            }
        
            if (stCnt != 0 && temp != 0) {
              // stTable　left
              val stJoin = spark.sql("select _stTable.sub as sub, _finalRes.value as value, _finalRes.obj as obj, cir from _stTable, _finalRes where _stTable.obj = _finalRes.sub").toDF()
              stJoin.createOrReplaceTempView("_finalRes")
              joincnt += 1
            }
         
            if (endCnt != 0 && temp != 0) {

              val endJoin = spark.sql("select _finalRes.sub as sub, _finalRes.value as value, _endTable.obj as obj , cir from _finalRes, _endTable where _finalRes.obj = _endTable.sub").toDF()
              endJoin.createOrReplaceTempView("_finalRes")
              joincnt += 1  
            }
            if(temp != 0) {
              finalRes = spark.sql("select sub, concat(sub, ',', value, ',', obj) as value, obj, cir from _finalRes").toDF()
            }
            println("[RES.....]")
            finalRes.show(false)
            break
          }
          pre = joincnt.toString
          joincnt += 1
        }
      }

    }
      else if(stCnt == 0) {
        cir = true
        res.createOrReplaceTempView("_init")
      
        val df = spark.sql("select '1' as id, _left.obj from _left where obj not in (select obj from endTable)").toDF()
        df.createOrReplaceTempView("_left")
 
        finalRes = spark.sql("select id, concat_ws(',', collect_set(obj)) as value from _left group by id").toDF()
        finalRes.createOrReplaceTempView("_left")

        val cirStr = finalRes.select("value")
          .head(1)
          .map(_.toString().replace("[", "").replace("]", ""))

        finalRes = spark.sql("select sub, '" + cirStr(0) + "' as value , obj from _init").toDF()
        finalRes.createOrReplaceTempView("_left")

       
        finalRes = spark.sql("select _left.sub, concat( _left.value , ',', _endTable.obj) as value, _endTable.obj, '0' as cir from _endTable, _left where _endTable.sub = _left.obj").toDF()

     
        finalRes = finalRes.union(spark.sql("select _left.*, '1' as cir from _left where _left.obj not in (select sub from _endTable) limit 1")).toDF()


      }else if(endCnt == 0) {
        cir = true
        res.createOrReplaceTempView("_init")

        
        val df = spark.sql("select '1' as id, _left.sub from _left where sub not in (select sub from stTable)").toDF()
        df.createOrReplaceTempView("_left")
 
        finalRes = spark.sql("select id, concat_ws(',', collect_set(sub)) as value from _left group by id").toDF()
        finalRes.createOrReplaceTempView("_left")

        val cirStr = finalRes.select("value")
          .head(1)
          .map(_.toString().replace("[", "").replace("]", ""))

        finalRes = spark.sql("select sub, '" + cirStr(0) + "' as value , obj from _init").toDF()
        finalRes.createOrReplaceTempView("_left")

        finalRes = spark.sql("select _stTable.sub, concat( _stTable.sub , ',', _left.value) as value, _left.obj, '0' as cir from _stTable, _left where _stTable.obj = _left.sub").toDF()
 
        finalRes = finalRes.union(spark.sql("select _left.*, '1' as cir from _left where _left.sub not in (select obj from _stTable) limit 1")).toDF()

      }
    }

    (finalRes, joincnt, cir)
  }

  def joinsql(cur: Int, pre: String): String = {

     "select _left.*, _right.obj as obj" + cur + ",_right.sub as joinpoint from _left left join _right on _left.obj" + pre + " == _right.sub"
//    "select _left.*, _right.obj as obj" + cur + " from _left, _right where _left.obj" + pre + " in (select sub from _right)"
  }
  def nullsql(cur: Int): String = {
    "select * from _last where obj" + cur + " is not null"
  }

}


