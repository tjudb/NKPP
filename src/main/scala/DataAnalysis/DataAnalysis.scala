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

  /**
   * 起点，终点 分析
   * @param tri
   */
  def stPointEndPoint(tri: DataFrame): Unit = {
    // 起点 终点
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
//      else {
//        println("--[NO KLEENE]")
//      }
    })
  }

//  if (predDF.select("sub")
//    .except(predTri.select("obj")).count() != predTri.select("sub").distinct().count()) {
//    true
//  } else if (predTri.select("obj")
//    .except(predTri.select("sub")).count() != predTri.select("obj").distinct().count()) {
//    true
//  } else false

  // todo 定义表达式列表
  var expreList: ListBuffer[ListBuffer[String]] = new ListBuffer[ListBuffer[String]]()
  // todo 当前谓词的表达式
  var _expre: ListBuffer[String] = new ListBuffer[String]()

  def processData(tri: DataFrame, predsDF: DataFrame): Unit = {
    val rdfAnalysisList = new MyAccumulator
    _sc.register(rdfAnalysisList)
    // * data statis
    val encoderSchema = Encoders.product[RDFAnalysis].schema

    val triCount = tri.count()
    val preds = tri.select("pred").distinct().collect()
    // 必须把preds collect 才能避免并行

    preds.foreach(row => {
      val curpp = row.get(0).toString
      println("[Processing] " + curpp)
      val predDF = getTripleByPred(tri, curpp)
      if(checkStar(predDF)) {
        // todo: 存在
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
        // 【已经得到克林闭包结果了】
//        dataStatis(starRES, curpp, predCount, triCount, rdfAnalysisList)
        // todo 保存
        saveNCI(starRES._1, starRES._2, starRES._3, curpp)
        println("|------[Single-SAVE DONE] ")
        println("[END]============================")

      }else {
        // todo: 不存在
        println("|---[PRED] " + curpp + ": no star")
      }
    })

    // exper
    val _preds = predsDF.select("predID").distinct().map(_.toString().replace("[","").replace("]", "")).collect()
    // path pool , 存储无法连接的谓词对
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
        // todo: 不存在
        println("|---[EXPER] " + curpp + ": no star")
      } else {
        val predDF = getTripleByPredEXPER(tri, curpp)
        //      predDF.show(false)
        if (checkStar(predDF)) {
          // todo: 存在
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
//    val statisInfo = spark.createDataFrame(rdfAnalysisList.value, encoderSchema)
//    statisInfo.orderBy("pred").show(false)
//    statisInfo.write.parquet(Configuration.Configuration.outputDIR)
//    println("[SAVE DONE] ")
//    println("[KLEENE STAR PREDS] ")
//    statisInfo.select("pred").distinct().show(false)
//    println("[KLEENE STAR PREDS CIRCLE] ")
//    statisInfo.select("pred", "cirExist", "cirNum").filter(statisInfo.col("cirExist")).toDF().distinct().show(false)
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
    // todo：数据分析【非环的最大长度，起点，终点个数，非环的全部路径，最大长度占比，环的个数，环的最短路径】
    val analySt = System.currentTimeMillis()
    val starPath = starRES._1
    starPath.show(false)
    val _cir = starRES._3
    // 环
    val tempStarDF = starPath.select("value", "cir").where(col("cir") === "1").toDF()
    //        tempStarDF.show(false)
    var cirCount = 0
    if (tempStarDF.count() != 0) { // 有环
      val mergePath = tempStarDF.select("value")
        .map(_.toString().replace("[", "").replace("]", "").split(",").sorted.toSet.toSeq).distinct().collect()
      cirCount = mergePath.size // 环的个数
      var count = 0
      mergePath.foreach(path => {
        //            println("[path] " + path)
        count += 1
        val _path = path.mkString(",")
        var _length = path.size.toLong
        if (_length == 1) _length += 1 // zi huan
        //            println("[_path] " + _path)
        rdfAnalysisList.add(Row(curpp, predCount, starRES._4, starRES._5,
          path.apply(0),
          "", 0L, 0.0, 0.0,
          _cir, cirCount, _path, _length - 1, ((_length - 1) * 1.0) / predCount * 1.0, ((_length - 1) * 1.0) / triCount * 1.0
        ))
        if (count >= 100) break
      })
    }

    // path
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

  /**
   * 生成谓词表达式生成，表达式长度 <=3
   */
  def experGenera(preds: collection.Set[String]): ListBuffer[ListBuffer[String]] = {

    preds.foreach(pred => {
      _expre.clear()
      experDFS(preds, pred)
    })
    expreList
  }

  /**
   * 从当前谓词出发，递归得到表达式
   *
   * @param preds
   * @return
   */
  def experDFS(preds: collection.Set[String], curPred: String): Unit = {
    _expre.append(curPred)
    if (_expre.length >= 2) {
      val temp: ListBuffer[String] = _expre.clone() // 深拷贝
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
  def checkPred(pred: String): Boolean = {
    // uobm
//    val lis: List[String] = List("5") // long path
    val lis: List[String] = List("25") // shortpath 300 400 500
//    val lis: List[String] = List("1") // circle

    // dbpedia
//    val lis: List[String] = List("4894", "26", "53721", "37261", "54039", "23492", "35484"
//    , "56905", "34340", "9233", "7279", "40415", "36077", "33090", "40415", "44925", "41420", "51734", "51363", "53734")
    // 短 4894 26
    // 长 54039 23492
    // 环 35484 37261 51734
//    val lis: List[String] = List("4894", "26", "37261", "54039", "23492", "35484", "51363")
//    val lis: List[String] = List("4894") // 短 【占内容】
//    val lis: List[String] = List("26") // 短 【占内容】
    //    val lis: List[String] = List("54039", "23492") // 长
//    val lis: List[String] = List("35484", "37261", "51734") // 环
    // watdiv

    // yago
    if(lis.contains(pred)) {
      true
    }else false
  }
  // 判断是否存在kleene
  def checkStar(predTri: DataFrame): Boolean = {

//    println("======left - right")
//    predTri.select("sub")
//      .except(predTri.select("obj")).show()
//    println("======right - left")
//    predTri.select("obj")
//      .except(predTri.select("sub")).show()
    if(predTri.select("sub")
      .except(predTri.select("obj")).count() != predTri.select("sub").distinct().count() ) {
      true
    }else if(predTri.select("obj")
    .except(predTri.select("sub")).count() != predTri.select("obj").distinct().count()){
      true
    }else false

  }

  // 得到当前谓词的triple表
  def getTripleByPred(tri: DataFrame, pp: String): DataFrame = {
    val ptri = tri.select("sub", "obj").where($"pred" === pp)
    ptri
  }

  // 得到当前谓词的triple表
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
    //    val ptri = tri.select("sub", "obj").where($"pred" === pp)
    //    ptri
  }
  // 2023-08-14
  // curTri: 当然谓词的三元组 [sub, obj]
  // pp: 谓词 [id]
  def getKleeneStar(curTri: DataFrame, pp: String): (DataFrame, Int, Boolean, Long, Long, DataFrame, DataFrame)  = {
    val joincnt = 1
    var cir = false
    curTri.createOrReplaceTempView("init")
    // 得到起点
    val stTable = curTri.select("sub").except(curTri.select("obj")).distinct()
    stTable.createOrReplaceTempView("stTable")
    val stCount = stTable.count()
    // 得到终点
    val endTable = curTri.select("obj").except(curTri.select("sub")).distinct()
    endTable.createOrReplaceTempView("endTable")
    val endCount = endTable.count()
    println("---[get stTable, endTable...] " + stCount + " " + endCount)
    // 判断是否有环
    if(stCount == 0 || endCount == 0) {
      cir = true // 0 0 纯环 （单环/多环） 0 + / + 0 不是纯环
    }

    var res = curTri
    var join = 0 // join次数
    // 有环 cir = true
    if(cir) {
      val tuple = cirProcess(joincnt, pp, res)
      res = tuple._1
      join = tuple._2
    }
    // 无环
    if(!cir) {
      val tuple = maybeCirProcess(joincnt, pp, res, stTable, endTable, cir)
      res = tuple._1
      join = tuple._2
      cir = tuple._3
    }
    (res, join, cir, stCount, endCount, stTable, endTable)
  }

  // old
  def getKleeneStar1(curTri: DataFrame, pp: String): (DataFrame, Int, Boolean, Long, Long, DataFrame, DataFrame) = {
    var cir = false
    val joincnt = 1
    val pre = ""
    // 得到起点
    val stTable = curTri.select("sub").except(curTri.select("obj"))
    val stCount = stTable.count()
    // 得到终点
    val endTable = curTri.select("obj").except(curTri.select("sub"))
    val endCount = endTable.count()
    // =========================================
    // 判断是否有环 【环 跟 非环路径 分开处理】
    // 判断去掉起点之后的行 做差集 之后 是否为0
    stTable.createOrReplaceTempView("stTable")
    endTable.createOrReplaceTempView("endTable")
    curTri.createOrReplaceTempView("init")

    // todo: 判断是否有环这部分很重要。判断不对就是进入死循环
    if(stCount == 0 && endCount == 0) {
      cir = true // 纯环 （单环/多环）
    }

    // init
    curTri.createOrReplaceTempView("_left")
    curTri.createOrReplaceTempView("_right")

    var res = curTri
    var join = 0 // join次数

    if(!cir && pp != "1") {
      // 可能存在环
      val tuple = maybeCirProcess1(joincnt, pre, res, stTable, cir)
      res = tuple._1
      join = tuple._2
      cir = tuple._3
    }else {
      // 一定存在环
      val tuple = cirProcess(joincnt, pre, res)
      // 存在环，但是不存在非环路径，但是可能会存在多环
      if(stCount == 0 && endCount == 0) {
        res = tuple._1
        join = tuple._2
      }else {
        // 存在环，但是存在非环路径
        // todo：需要过滤
        // 这里暂时没有过滤，因为我不会
        // 但是正确性是可以保证的
        res = tuple._1
        join = tuple._2
      }
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

  // 20230814
  def cirProcess(join: Int, pp: String, curTri: DataFrame): (DataFrame, Int) = {
    println("[CIR PROCESS...] " + pp)
    val schema1 = StructType(
      Seq(
        StructField("sub", LongType, true),
        StructField("value", StringType, true),
        StructField("obj", LongType, true),
        StructField("cir", StringType, true) // 1 有环，0 无环
      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    var res = curTri
    var len = join
    res.createOrReplaceTempView("_init")
    res.createOrReplaceTempView("_left")
    // get sub (all)
    val df = spark.sql("select '1' as id, _left.sub from _left ").toDF()
    df.createOrReplaceTempView("_left")

    val mergeRes = spark.sql("select id, concat_ws(',', collect_set(sub)) as value from _left group by id").toDF()
    mergeRes.createOrReplaceTempView("_left")

    // [] 消除
    val cirStr = mergeRes.select("value")
      .head(1)
      .map(_.toString().replace("[", "").replace("]", ""))
    len = cirStr.apply(0).split(",").length
    finalRes = spark.sql("select sub, '" + cirStr + "' as value, obj, '1' as cir from _init").toDF()
    finalRes.createOrReplaceTempView("_left")
    (finalRes, len)
  }

  // 纯环
  def cirProcess1(cur: Int, _pre: String, curTri: DataFrame): (DataFrame, Int) = {
    // 什么时候终止？单环/多环的情况。
    // 当路径长度 == 行数（当前表总三元组数） 时 停止。【行数跟总结点数那个好呢？】
    // 如果多环里面环的路径长度不一样，就会存在路径冗余，
    println("|------[CIRPROCESS] circle")
    val schema1 = StructType(
      Seq(
        StructField("sub", LongType, true),
        StructField("value", StringType, true),
        StructField("obj", LongType, true),
        StructField("cir", StringType, true) // 1 有环，0 无环
      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    var joincnt = cur
    var pre = _pre
    var res = curTri
    val row = curTri.count()
    res.createOrReplaceTempView("_init")
    res.createOrReplaceTempView("_left")
    // add id
    val df = spark.sql("select '1' as id, _left.sub from _left").toDF()
    df.createOrReplaceTempView("_left")
    // 合并所有点 [single]
    val mergeRes = spark.sql("select id, concat_ws(',', collect_set(sub)) as value from _left group by id").toDF()
    mergeRes.createOrReplaceTempView("_left")
    val cirStr = mergeRes.select("value")
      .head(1)
      .map(_.toString().replace("[", "").replace("]", ""))
    joincnt = cirStr.apply(0).split(",").length
    finalRes = spark.sql("select sub, '" + cirStr(0) + "' as value , obj , '1' as cir from _init").toDF()
    finalRes.createOrReplaceTempView("_left")

    (finalRes, joincnt)
  }

  def maybeCirProcess(join: Int, pp: String, curTri: DataFrame, stTable: DataFrame, endTable: DataFrame, _cir: Boolean): (DataFrame, Int, Boolean)  = {

    stTable.createOrReplaceTempView("stTable")
    endTable.createOrReplaceTempView("endTable")
    curTri.createOrReplaceTempView("init")
    curTri.createOrReplaceTempView("_left")
    curTri.createOrReplaceTempView("_right")
    import scala.util.control.Breaks._
    var res = curTri
    var pre = ""
    var joincnt = join
    val schema1 = StructType(
      Seq(
        StructField("sub", LongType, true),
        StructField("value", StringType, true),
        StructField("obj", LongType, true),
        StructField("cir", StringType, true) // 1 有环，0 无环
      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)

    // 连接点 初始化
    val schema2 = StructType(Seq(StructField("jp", LongType, true)))
    var joinPoint = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

    // 合并 右表，使得join的时候 是多对一
    curTri.groupBy("sub").agg(collect_set("obj").alias("obj")).toDF()
      .createOrReplaceTempView("_right")
//    spark.sql("select sub, concat_ws('|', collect_set(obj)) as obj from _right group by sub").toDF()
//     .createOrReplaceTempView("_right")
//    println("---[_right] ")
//    spark.sql("select * from _right").toDF().show(false)
    // in 会导致数据倾斜
    res = spark.sql("select _left.sub, _left.obj from _left join stTable on _left.sub = stTable.sub").toDF()
    res.createOrReplaceTempView("_left")
    res = spark.sql("select _left.sub, _left.obj from _left where _left.obj not in (select obj from endTable)").toDF()
    res.createOrReplaceTempView("_left")

    println("---[init _left...] " + res.count())
    println("---[init _right...] " + curTri.count())
    breakable {
      while(true) {
        println("---[JOINCNT...] " + joincnt)
        // join + 最后一列展开。
        val lastcol = "obj" + joincnt
        res = spark.sql(joinsql(joincnt, pre)).distinct()
        joinPoint = joinPoint.union(res.select("joinpoint").toDF()).distinct().toDF()
        joinPoint.createOrReplaceTempView("jp")
        res = res.withColumn("obj" + joincnt, explode(col(lastcol))).drop("joinpoint").toDF()
        res.createOrReplaceTempView("_left")
        res.show()
        println("---[JOIN DONE...] " + res.count())
        // update/filter _right
        spark.sql("select * from _right where _right.sub not in (select jp from jp)").toDF()
          .createOrReplaceTempView("_right")

        // filter last col  , last col in endTable --> path
        val lastEndDF = spark.sql("select _left.* from _left where obj" + joincnt + " in (select obj from endTable)").toDF()
        lastEndDF.createOrReplaceTempView("_lastEndDF")
        println("---[lastEndDF count] " + lastEndDF.count())
        // merge lastEndDF
        var _sql = ""
        val colName: ListBuffer[String] = ListBuffer("sub", "','", "obj")
        for (x <- 1 to joincnt) {
          colName += "','"
          colName += "obj" + x
        }
        if(joincnt != 0) {
          _sql = "select sub, concat(" + colName.mkString(",") + ") as value, obj" + joincnt + " as obj, '0' as cir from _lastEndDF"
        }
        val margeDF = spark.sql(_sql).toDF()
        margeDF.show()
        // union finalRES
        finalRes = finalRes.union(margeDF).distinct()
        println("---[finalRes Count] " + finalRes.count())
        // update left
        res = res.except(lastEndDF).dropDuplicates("obj" + joincnt).toDF()
        res.createOrReplaceTempView("_left")
        pre = joincnt.toString
        joincnt += 1
        if(res.count() == 0) break
        println("---[::next]")
      }
    }
    // 根据 起点和终点 得到相应的 三元组
    (finalRes, joincnt, _cir)
  }

  def maybeCirProcess1(cur: Int, _pre: String, curTri: DataFrame, stTable: DataFrame, _cir: Boolean): (DataFrame, Int, Boolean) = {
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
        StructField("cir", StringType, true) // 1 有环，0 无环
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
    // 判断是不是环
    val tempsub = spark.sql("select distinct sub from _left").toDF()

    val tempobj = spark.sql("select distinct obj from _left").toDF()
    // 移除原数据之后的起点和终点
    var tempstDF = tempsub.select("sub").except(tempobj.select("obj")).toDF()
    var tempst = tempstDF.count()
    var tempendDF = tempobj.select("obj").except(tempsub.select("sub")).toDF()
    var tempend = tempendDF.count()

    // 特殊
    if(temp == 0 && row == 2) {
      // 纯路径
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
      cir = true // 去掉终点、起点剩下的是纯环 （单环/多环）
    }else {

      if(tempst != 0 && tempend != 0){
        // initial
      tempendDF.createOrReplaceTempView("tempendDF")
      tempstDF.createOrReplaceTempView("tempstDF")
      spark.sql("select * from _left where sub in (select sub from tempstDF)")
        .toDF().createOrReplaceTempView("_left")
      // 一定不要忘了更新_right _left
      res.createOrReplaceTempView("_right")
      breakable{
        while(true) {
          println("[CIR] False")
          cir = false // 去掉终点、起点剩下是非环
          // 让非环的进行 左连接join，然后跟起点和终点进行合并
          println("[BIG Table] " + res.count())
          res = spark.sql(joinsql(joincnt, pre)).toDF().distinct()
          res.show(false)
          res.cache()
          println("[JOIN] " + joincnt)
          res.createOrReplaceTempView("_left")

          // ==========得到每次join之后的结果（通过最后一列筛选）
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

          // trick joincnt
          if (res.count() == 0 || (row) == joincnt || joincnt >= 5) {
            if (temp == 0) {
              finalRes = finalRes
              break
            }
            // [替换 起点和终点]
            // 跟起点连接，
            if (stCnt != 0 && temp != 0) {
              // stTable　left
              val stJoin = spark.sql("select _stTable.sub as sub, _finalRes.value as value, _finalRes.obj as obj, cir from _stTable, _finalRes where _stTable.obj = _finalRes.sub").toDF()
              stJoin.createOrReplaceTempView("_finalRes")
              joincnt += 1
            }
            // 跟终点连接
            if (endCnt != 0 && temp != 0) {

              val endJoin = spark.sql("select _finalRes.sub as sub, _finalRes.value as value, _endTable.obj as obj , cir from _finalRes, _endTable where _finalRes.obj = _endTable.sub").toDF()
              endJoin.createOrReplaceTempView("_finalRes")
              joincnt += 1 // 与终点表join了一次
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
        // 只存在终点连接
        // 去除终点
        val df = spark.sql("select '1' as id, _left.obj from _left where obj not in (select obj from endTable)").toDF()
        df.createOrReplaceTempView("_left")

        // 合并所有点 [single]
        finalRes = spark.sql("select id, concat_ws(',', collect_set(obj)) as value from _left group by id").toDF()
        finalRes.createOrReplaceTempView("_left")

        val cirStr = finalRes.select("value")
          .head(1)
          .map(_.toString().replace("[", "").replace("]", ""))

        finalRes = spark.sql("select sub, '" + cirStr(0) + "' as value , obj from _init").toDF()
        finalRes.createOrReplaceTempView("_left")

        // 与终点连接
        finalRes = spark.sql("select _left.sub, concat( _left.value , ',', _endTable.obj) as value, _endTable.obj, '0' as cir from _endTable, _left where _endTable.sub = _left.obj").toDF()

        // 纯环
        finalRes = finalRes.union(spark.sql("select _left.*, '1' as cir from _left where _left.obj not in (select sub from _endTable) limit 1")).toDF()


      }else if(endCnt == 0) {
        cir = true
        res.createOrReplaceTempView("_init")

        // 只存在起点连接
        // 去除起点
        val df = spark.sql("select '1' as id, _left.sub from _left where sub not in (select sub from stTable)").toDF()
        df.createOrReplaceTempView("_left")

        // 合并所有点 [single]
        finalRes = spark.sql("select id, concat_ws(',', collect_set(sub)) as value from _left group by id").toDF()
        finalRes.createOrReplaceTempView("_left")

        val cirStr = finalRes.select("value")
          .head(1)
          .map(_.toString().replace("[", "").replace("]", ""))

        finalRes = spark.sql("select sub, '" + cirStr(0) + "' as value , obj from _init").toDF()
        finalRes.createOrReplaceTempView("_left")

        finalRes = spark.sql("select _stTable.sub, concat( _stTable.sub , ',', _left.value) as value, _left.obj, '0' as cir from _stTable, _left where _stTable.obj = _left.sub").toDF()

        // 纯环
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

// 如果不是环，就不断join，每次筛去终点，同时去重
//    breakable {
//      while (true) {
//        // [一定要限制表的数量]
//        println("[BIG Table] " + res.count())
//        res = spark.sql(joinsql(joincnt, pre)).toDF().distinct()
//        res.show(false)
//        res.cache()
//        println("[JOIN] " + joincnt)
//        // 先不管起点，终点，就把最后一列为null的列提取出来就行 【为null肯定不是环】
//        res.createOrReplaceTempView("_left")
//        // join 完之后 把 最后一列属于 终点的提取出来
//        //  where _left.obj"+ joincnt +" in (select obj from endTable)
//        val lastNullDF = spark.sql("select _left.* from _left where obj" + joincnt + " in (select obj from endTable)").toDF()
////        println("[remove end row]")
//
//        if(lastNullDF.count() != 0) {
//
//          lastNullDF.createOrReplaceTempView("_lastNullDF")
//          // 处理成：sub、value、obj的形式 [int, string, int]
//          val colName = new StringBuilder
//          colName ++= ("sub, ',', obj")
//          for (x <- 1 to (joincnt)) {
//            colName ++= (", ',', obj" + x)
//          }
//          var _sql = ""
//          if ((joincnt) == 0) {
//            _sql = "select sub, concat(" + colName + ") as value, obj, '0' as cir from _lastNullDF"
//          } else {
//            _sql = "select sub, concat(" + colName + ") as value, obj" + (joincnt) + " as obj, '0' as cir from _lastNullDF"
//          }
//          val doneDF = spark.sql(_sql).toDF()
//          finalRes = finalRes.union(doneDF).distinct()
//          finalRes.cache()
//
//          println("[::next]")
//        }
//
////        val lastNotNullDF = spark.sql("select _left.* from _left where obj" + joincnt + " is not null").toDF()
//        val lastNotNullDF = spark.sql("select _left.* from _left where obj" + joincnt + " not in (select obj from endTable)").toDF()
//        // 没有环干扰的情况 【没有环的情况】
//        if(lastNotNullDF.count() == 0) {
//          println("[CIR] False")
//          cir = false
//          finalRes = filterRes(finalRes, stTable) // DONE (筛起点的边)
//          finalRes.cache()
//          break
//        }
//
//        lastNotNullDF.dropDuplicates("obj" + joincnt).createOrReplaceTempView("_left")
////        lastNotNullDF.createOrReplaceTempView("_left")
//        pre = joincnt.toString
//
//        if ((row) == joincnt) {
//          // select _right.* from _right where _right.sub not in (select sub from _left)
//          val colName = new StringBuilder
//          colName ++= ("sub, ',', obj")
//          for (x <- 1 to (joincnt)) {
//            colName ++= (", ',', obj" + x)
//          }
//
//          // 得到从起点开始的环，
//          //把它当路径存储
//          val stCirDF = spark.sql("select _left.* from _left where _left.sub in (select sub from stTable) ")
//          stCirDF.createOrReplaceTempView("_stCirDF")
//
//          var _sql = ""
//          if ((joincnt) == 0) {
//            _sql = "select sub, concat(" + colName + ") as value, obj, '0' as cir from _stCirDF"
//          } else {
//            _sql = "select sub, concat(" + colName + ") as value, obj" + joincnt + " as obj, '0' as cir from _stCirDF"
//          }
//          val doneDF1 = spark.sql(_sql).toDF()
//          finalRes = finalRes.union(doneDF1).distinct()
//
//          // 得到到终点终止的环
//          //select _right.* from _right where _right.obj" + (starRES._2 - 1) + " not in (select obj from _end) and _right.obj" + (starRES._2 - 1) + " != -1
//          val endCirDF = spark.sql("select _left.* from _left where _left.obj"+ joincnt +" in (select obj from endTable) ")
//          endCirDF.createOrReplaceTempView("_endCirDF")
//
//          if ((joincnt) == 0) {
//            _sql = "select sub, concat(" + colName + ") as value, obj, '0' as cir from _endCirDF"
//          } else {
//            _sql = "select sub, concat(" + colName + ") as value, obj"+ joincnt +" as obj, '0' as cir from _endCirDF"
//          }
//          val doneDF3 = spark.sql(_sql).toDF()
//          finalRes = finalRes.union(doneDF3).distinct()
//
//          // 得到不从起点开始的环
//          // 当环 [处理]
//          val notStCir = spark.sql("select _left.* from _left where _left.sub not in (select sub from stTable)")
//          notStCir.createOrReplaceTempView("_cir")
//
//          if ((joincnt) == 0) {
//            _sql = "select sub, concat(" + colName + ") as value, obj, '1' as cir from _cir"
//          } else {
//            _sql = "select sub, concat(" + colName + ") as value, obj"+ joincnt +" as obj, '1' as cir from _cir"
//          }
//          val doneDF2 = spark.sql(_sql).toDF()
//          finalRes = finalRes.union(doneDF2).distinct()
//          cir = true
//          break
//        }
//        joincnt += 1
//      }
//    }

/**
 * 出现这个报错是因为map、filter等的参数使用了外部的变量，但是这个变量不能序列化。特别是当引用了某个类（经常是当前类）的成员函数或变量时，会导致这个类的所有成员（整个类）都需要支持序列化
 */

//  def rdfAnalysis(pred: String, predCount: Long, tuple: (sql.DataFrame, Int, Boolean, Long, Long, sql.DataFrame, sql.DataFrame)
//                  , triCount: Long): Unit = {
//    //    encoderSchema.printTreeString()
//    var starPath = tuple._1
//    println("||----[FILL NULL] ")
//
//    // 必须是同类型的才可以填充
//    starPath = starPath.na.fill(-1)
//    starPath.show()
//    var cir = tuple._3
//    if (!cir) {
//      // 不存在环
//      val mergePath = starPath.map(_.toSeq.foldLeft("")(_ + "," + _).substring(1))
//      mergePath.foreach(path => {
//        rdfAnalysisList.add(Row(pred, predCount, tuple._4, tuple._5,
//          path, path.split(",").length.toLong - 1, (path.split(",").length * 1.0 - 1) / predCount * 1.0, (path.split(",").length * 1.0 - 1) / triCount * 1.0,
//          cir, 0, "", 0L, 0.0, 0.0
//        ))
//      })
//
//    } else {
//      println("||----[ST] " + tuple._4)
//      println("||----[END] " + tuple._5)
//      // 存在环 + 路径
//      if (tuple._4 != 0 || tuple._5 != 0) {
//        // 环 + 路径
//        tuple._6.createOrReplaceTempView("_left") // st
//        starPath.createOrReplaceTempView("_right") // star
//        tuple._7.createOrReplaceTempView("_end") // end
//        // 去掉终点和起点路径 可以得到环
//        var tempStarDF: sql.DataFrame = starPath
//        var stStarDF: sql.DataFrame = starPath
//        var endStarDF: sql.DataFrame = starPath
//        println("||-----[JOIN POINT] " + tuple._2 + " -> " + (tuple._2 - 1))
//        // 起点筛一次、终点筛一次
//        if (tuple._4 != 0) {
//
//          stStarDF = spark.sql("select _right.*  from _left left join _right on _right.sub = _left.sub").toDF()
//
//          tempStarDF = spark.sql("select _right.* from _right where _right.sub not in (select sub from _left)").toDF().distinct()
//          println("||-----[FIRST FILTER]")
//          //          tempStarDF.show(false)
//          tempStarDF.createOrReplaceTempView("_right") // cover
//        }
//        if (tuple._5 != 0) {
//          // 有终点
//          // 没有起点，只有终点，根据终点进行筛选
//          // 终点一定在最长路径里面
//          //          println("end")
//          endStarDF = spark.sql("select distinct _right.*  from _right join _end where _right.obj" + (tuple._2 - 1) + " == _end.obj").toDF()
//          //          endStarDF.show()
//          //          tempStarDF = spark.sql("select _right.*  from _end left join _right on _right.obj"+ (tuple._2 - 1) +" == _end.obj and _right.obj"+ (tuple._2 - 1) +" is not null").toDF()
//          //          endStarDF.show()
//          //【第二次筛选 对_right筛选】把终点的边筛选掉，且把objx为 - 1的边筛选掉
//          // [前]
//          //          tempStarDF.show(false)
//          //          spark.sql("select * from _end").toDF().show()
//          println("||-----[SECOND FILTER]")
//          //select _right.* from _right where _right.sub not in (select sub from _left)
//          tempStarDF = spark.sql("select _right.* from _right where _right.obj" + (tuple._2 - 1) + " not in (select obj from _end) and _right.obj" + (tuple._2 - 1) + " != -1 ").toDF().distinct()
//          //          println(":<<<<<")
//        }
//
//        // 如果两次筛选完，不为空，就说明有环
//        if (!tempStarDF.isEmpty) { // 有环
//          println("||------[Kleene Star Again] " + cir)
//          // 环：：：：
//          val mergePath = tempStarDF.map(_.toSeq.filter(_ != -1).map(_.toString.toInt)
//            .toSet.toSeq.sorted
//            .foldLeft("")(_ + "," + _).substring(1)).distinct()
//          val cirCount = mergePath.count().toInt // 环的个数
//          mergePath.foreach(path => {
//            rdfAnalysisList.add(Row(pred, predCount, tuple._4, tuple._5,
//              "", 0L, 0.0, 0.0,
//              cir, cirCount, path, path.split(",").length.toLong, (path.split(",").length * 1.0) / predCount * 1.0, (path.split(",").length * 1.0) / triCount * 1.0
//            ))
//          })
//          //          mergePath.show(false)
//          // 路径：：：
//          // 终点、起点处理筛选完，就只是路径了
//          tempStarDF = stStarDF.union(endStarDF).distinct()
//          // 终点、起点处理之后，就只是路径了
//          val mergePath1 = tempStarDF.map(_.toSeq.filter(_ != -1).foldLeft("")(_ + "," + _).substring(1))
//          //          mergePath1.show(false)
//          // 但是在统计数据信息时，需要考虑去重 【环的情况】
//          mergePath1.foreach(path => {
//            rdfAnalysisList.add(Row(pred, predCount, tuple._4, tuple._5,
//              path + " *", path.split(",").toSet.size.toLong - 1, (path.split(",").toSet.size.toLong - 1) * 1.0 / predCount * 1.0, (path.split(",").toSet.size.toLong - 1) * 1.0 / triCount * 1.0,
//              cir, cirCount, "", 0L, 0.0, 0.0
//            ))
//          })
//
//        } else {
//          //           无环的情况
//          cir = false
//          println("||------[Kleene Star Again] " + cir)
//          // 不存在环
//          val mergePath = starPath.map(_.toSeq.filter(_ != -1).foldLeft("")(_ + "," + _).substring(1))
//          //          mergePath.show(false)
//          mergePath.foreach(path => {
//            rdfAnalysisList.add(Row(pred, predCount, tuple._4, tuple._5,
//              path, path.split(",").length.toLong - 1, (path.split(",").length * 1.0 - 1) / predCount * 1.0, (path.split(",").length * 1.0 - 1) / triCount * 1.0,
//              cir, 0, "", 0L, 0.0, 0.0
//            )
//            )
//          })
//        }
//
//      } else if (tuple._4 == 0 && tuple._5 == 0) {
//        // 环 跟 非环 没有相交点
//        // 只有环 排序，去重 【多环，单环都可以】
//        // to int 型！！！ 【bug所在地]
//        val mergePath = starPath.map(_.toSeq.filter(_ != -1).map(_.toString.toInt)
//          .toSet.toSeq.sorted
//          .foldLeft("")(_ + "," + _).substring(1)).distinct()
//
//        val cirCount = mergePath.count().toInt
//        mergePath.foreach(path => {
//          rdfAnalysisList.add(Row(pred, predCount, tuple._4, tuple._5,
//            "", 0L, 0.0, 0.0,
//            cir, cirCount, path, path.split(",").length.toLong, (path.split(",").length * 1.0) / predCount * 1.0, (path.split(",").length * 1.0) / triCount * 1.0
//          ))
//        })
//      }
//    }
//  }
