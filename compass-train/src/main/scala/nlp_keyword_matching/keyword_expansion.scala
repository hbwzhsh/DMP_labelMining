package nlp_keyword_matching

import org.apache.spark.{SparkConf, SparkContext}

import scala.math._
/**
 * Created by JinBinbin on 2016/3/21.
 *
 * 简介：关键词扩展
 * 输入参数：input1 input2 output k threshold
 * input1格式：输入文件路径，表示词向量，格式与Word2Vec.scala的输出格式一致。
 * input2格式：输入文件路径，文件由多行组成，每行表示一个领域和该领域的核心词。形式为 “field：words”，words之间用 ‘，’ 或 ‘。’ 隔开。
 * output格式：输出文件路径，文件由多行组成，每行表示一个领域和该领域的扩展词。形式为 “field~words”，words之间用空格隔开。
 * k：类型为整型，即为算法第3步提到的k。
 * threshold：类型为浮点型，即为算法第3步提到的threshold。
 *
 * 提交任务命令示例：
 * spark-submit --class com.iflytek.compass.CDD --master yarn-cluster --num-executors 100 --driver-memory 4g --executor-memory 2g --executor-cores 4 --queue tool ./MLlib-1.0-SNAPSHOT-jar-with-dependencies.jar input1 input2 output k threshold
 *
 */
object keyword_expansion {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("CDD").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)

    val src1 = args(0)
    val src2 = args(1)
    val dest = args(2)
    val corpus = sc.textFile(src1)
    val wordVectorRDD =
      corpus
        .map(line => line.split('~'))
        .filter(_.length == 2)
        .map(s => (s(0),s(1).split('\t').map(_.toDouble)))  //RDD[(String,Array[Double])] [词，向量]
        .cache()


    val effectiveLine =
      sc
        .textFile(src2)
        .map(line => line.split("："))
        .filter(_.length == 2)
        .cache()

    val classNum =  effectiveLine.count().toInt
    val classInd = effectiveLine.map(s => s(0)).zipWithIndex().collect().toMap  //Map[String,long] [类名，索引]
    val classIndBroad = sc.broadcast(classInd)

    val coreWordsRDD =
      effectiveLine
        .map(s => (s(0),s(1)))
        .flatMapValues(s => s.split(Array('，','。')))
        .map(_.swap) //RDD[(String,String)] [词，类]
    //找到核心词在语料库中的向量
    val wordInfo = coreWordsRDD.join(wordVectorRDD).collect() //Array[(String,(String,Array(Double)))] Array[词，(类名，向量)]
    val wordInfoBroad = sc.broadcast(wordInfo)

    val k = args(3).toInt //在每一个核心词类中选取与目标词相似度最高的k个
    val threshold = args(4).toDouble  //当平均相似度大于threshold时将目标词加入
    //计算每个词向量在各个类中的平均相似度
    val wordScore = wordVectorRDD.map({case (word,vector) =>
        val topK = Array.fill(classNum,k)(0.0)
        for ((word,(cluster,vector2)) <- wordInfoBroad.value ){
          val ind = classIndBroad.value(cluster).toInt
          var i = 0
          val cos = distance(vector,vector2)
          while (i<k && topK(ind)(i) >= cos){ i += 1}
          if (i < k){
            for (j <- Range(k-1,i,-1)){
              topK(ind)(j) = topK(ind)(j-1)
            }
            topK(ind)(i) = cos
          }
        }
        val aveTopK = Array.fill(classNum)(0.0)
        for (i <- 0 until classNum){
          var sum = 0.0
          var total = 0
          for (j <- 0 until k){
            if (topK(i)(j) > 0){
              sum += topK(i)(j)
              total += 1
            }
          }
          aveTopK(i) = sum / total
        }
        (word,aveTopK)
      })
    //仅当词在一个领域内的平均相似度大于threshold时，扩展
    val res = wordScore
      .mapValues(aveTopK => {
        var set:Set[String] = Set.empty
        for ((cluster, ind) <- classIndBroad.value) {
          if (aveTopK(ind.toInt) >= threshold) {
            set = set+cluster
          }
        }
        set
      })
      .filter(s=>s._2.size == 1)
      .flatMapValues(set => set)
      .map(_.swap)
      .groupByKey()
      .map(s => s._1+"~"+s._2.mkString(" "))
      .repartition(1)
    res.saveAsTextFile(dest+k.toString+"_"+threshold.toString)
    sc.stop()
  }

  def distance(vec1:Array[Double],vec2:Array[Double]):Double = {
    val size = vec1.size
    var dot:Double = 0.0
    var x,y:Double = 0.0
    for (i<-0 until size) {
      dot += vec1(i) * vec2(i)
      x += vec1(i) * vec1(i)
      y += vec2(i) * vec2(i)
    }
    dot*1.0/(sqrt(x)*sqrt(y))
  }
}