package nlp_keyword_matching

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by Linxiao Bai on 2016/5/27.
  * This object utillizes class regx
 */
object RegxRunner {

  def main(args: Array[String]) {
    if(args.length <5){
      System.err.println("Must Have 3 inPut")
      System.exit(1)
    }
    val sc = ShareSparkMethod.getSparkContext("filterByDisease")
    val myRegx= new RegX()
    myRegx.setBound(args(2),args(3))

    if(args.length==5) {
      val inPart1 = sc.broadcast(sc.textFile(args(4)).collect()).value
      myRegx.setSubjectArray(inPart1)
    }

    if(args.length==6) {
      val inPart1 = sc.broadcast(sc.textFile(args(4)).collect()).value
      val inPart2 = sc.broadcast(sc.textFile(args(5)).collect()).value
      myRegx.setSubjectArray(inPart1)
      myRegx.setVerbArray(inPart2)
    }

    if(args.length==7) {
      val inPart1 = sc.broadcast(sc.textFile(args(4)).collect()).value
      val inPart2 = sc.broadcast(sc.textFile(args(5)).collect()).value
      val inPart3 = sc.broadcast(sc.textFile(args(6)).collect()).value
      myRegx.setSubjectArray(inPart1)
      myRegx.setVerbArray(inPart2)
      myRegx.setObjectArray(inPart3)
    }

/* All Set and Ready to Go!!! */
    myRegx.ready()

    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .map(x=> (x._2.fields.data.get("dvc").toString
        ,x._2.fields.data.get("recResult").toString
        )
      )
      .filter(x=> myRegx.goMatch(x._2))

    val infile1=infile0.groupByKey().cache()
    println(infile1.count())

    infile1.map(x=>x._1+","+x._2.mkString("~")).repartition(1).saveAsTextFile(args(1))
  }
}

