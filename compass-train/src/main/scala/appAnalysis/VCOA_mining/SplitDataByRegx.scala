package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/5/19.
 */
object SplitDataByRegx {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }
    val sc = ShareSparkMethod.getSparkContext("SplitDataRegx")
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
        && x._2.fields.data.get("recResult").toString!="EMPTY")
      .map(x=> (x._2.fields.data.get("dvc").toString
        ,(x._2.fields.data.get("timestamp").toString//timeStamp filtering here
        ,{val temp=x._2.fields.data.get("clientIp").toString.split('.')
        temp(0)+'.'+temp(1)+'.'+temp(2)
      },x._2.fields.data.get("recResult").toString
        ))
      ).cache()

    def myTest(candidate:String): Boolean ={
      val pat1=Pattern.compile("我.{0,6}上班")
      val pat2=Pattern.compile("我.{0,6}下班")
      val pat3=Pattern.compile("我.{0,6}加班")
      val pat4=Pattern.compile("我.{0,6}去公司")
      val pat5=Pattern.compile("我.{0,6}在公司")
      val pat6=Pattern.compile("我.{0,6}出差")
      val pat7=Pattern.compile("我.{0,6}在办公室")
      val pat8=Pattern.compile("我.{0,6}去办公室")


      val ret=((pat1.matcher(candidate).matches())
        ||(pat2.matcher(candidate).matches())
        ||(pat3.matcher(candidate).matches())
        ||(pat4.matcher(candidate).matches())
        ||(pat5.matcher(candidate).matches())
        ||(pat6.matcher(candidate).matches())
        ||(pat7.matcher(candidate).matches())
        ||(pat8.matcher(candidate).matches())

        )
      return ret
    }

    val infile1 = ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
        && myTest(x._2.fields.data.get("recResult").toString)
        && x._2.fields.data.get("recResult").toString!="EMPTY"
      )
      .map(x=>(x._2.fields.data.get("dvc").toString,""))
      .groupByKey()
      .cache()

    println(infile1.count())

    val infile11=infile1
     .join(infile0)
     .map(x=>x._1+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3).repartition(100)


//    val infile2 = ShareSparkMethod
//      .readExtractPair(args(0),sc)
//      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
//        && x._2.fields.data.get("recResult").toString!="EMPTY"
//        && !myTest(x._2.fields.data.get("recResult").toString))
//      .map(x=>(x._2.fields.data.get("dvc").toString,""))
//      .groupByKey()
//      .cache()
//      .join(infile0)
//      .map(x=>x._1+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3).repartition(100)


    infile11.saveAsTextFile(args(1))
//    infile2.saveAsTextFile(args(2))





  }
}

