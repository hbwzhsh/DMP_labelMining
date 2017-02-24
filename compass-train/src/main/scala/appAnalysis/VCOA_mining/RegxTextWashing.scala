package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/5/20.
 */
object RegxTextWashing {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }
    val sc = ShareSparkMethod.getSparkContext("SplitDataRegx")


    def myTest(in:String): Boolean ={
      val candidateArray=in.split(Array('，','。','？','！','.',',','?','!',' '))


      val pat1=Pattern.compile(".{0,}我.{0,6}上班.{0,}")
      val pat2=Pattern.compile(".{0,}我.{0,6}下班.{0,}")
      val pat3=Pattern.compile(".{0,}我.{0,6}加班.{0,}")
      val pat4=Pattern.compile(".{0,}我.{0,6}去公司.{0,}")
      val pat5=Pattern.compile(".{0,}我.{0,6}在公司.{0,}")
      val pat6=Pattern.compile(".{0,}我.{0,6}出差.{0,}")
      val pat7=Pattern.compile(".{0,}我.{0,6}在办公室.{0,}")
      val pat8=Pattern.compile(".{0,}我.{0,6}去办公室.{0,}")

      //Has Child
//      val pat1=Pattern.compile(".{0,6}我.{0,6}孩.{0,6}上.{0,6}学.{0,6}")
//      val pat2=Pattern.compile(".{0,6}我.{0,6}孩.{0,6}放.{0,6}学.{0,6}")
//      val pat3=Pattern.compile(".{0,6}我.{0,6}儿子.{0,6}放.{0,6}学.{0,6}")
//      val pat4=Pattern.compile(".{0,6}我.{0,6}女儿.{0,6}上.{0,6}学.{0,6}")
//      val pat5=Pattern.compile(".{0,6}我.{0,6}儿子.{0,6}放.{0,6}学.{0,6}")
//      val pat6=Pattern.compile(".{0,6}我.{0,6}女儿.{0,6}上.{0,6}学.{0,6}")
    //version 1
      //      val pat1=Pattern.compile(".{0,6}我.{0,6}送.{0,6}爸.{0,6}")
      //      val pat2=Pattern.compile(".{0,6}我.{0,6}送.{0,6}妈.{0,6}")
      //      val pat3=Pattern.compile(".{0,6}我.{0,6}送.{0,6}长辈.{0,6}")
      //      val pat4=Pattern.compile(".{0,6}我.{0,6}送.{0,6}母.{0,6}")
      //      val pat5=Pattern.compile(".{0,6}我.{0,6}送.{0,6}父.{0,6}")
      //      val pat6=Pattern.compile(".{0,6}我.{0,6}送.{0,6}娘.{0,6}")
      //      val pat7=Pattern.compile(".{0,6}我.{0,6}送.{0,6}叔.{0,6}")
      //      val pat8=Pattern.compile(".{0,6}我.{0,6}送.{0,6}姨.{0,6}")
      //      val pat9=Pattern.compile(".{0,6}我.{0,6}送.{0,6}婶.{0,6}")
      //      val pat10=Pattern.compile(".{0,6}我.{0,6}送.{0,6}姑.{0,6}")
      //      val pat11=Pattern.compile(".{0,6}我.{0,6}送.{0,6}伯.{0,6}")
      //      val pat12=Pattern.compile(".{0,6}我.{0,6}送.{0,6}舅.{0,6}")

      //version 2
//            val pat1=Pattern.compile(".{0,6}送.{0,6}爸.{0,6}")
//            val pat2=Pattern.compile(".{0,6}送.{0,6}妈.{0,6}")
//            val pat3=Pattern.compile(".{0,6}送.{0,6}长辈.{0,6}")
//            val pat4=Pattern.compile(".{0,6}送.{0,6}母.{0,6}")
//            val pat5=Pattern.compile(".{0,6}送.{0,6}父.{0,6}")
//            val pat6=Pattern.compile(".{0,6}送.{0,6}娘.{0,6}")
//            val pat7=Pattern.compile(".{0,6}送.{0,6}叔.{0,6}")
//            val pat8=Pattern.compile(".{0,6}送.{0,6}姨.{0,6}")
//            val pat9=Pattern.compile(".{0,6}送.{0,6}婶.{0,6}")
//            val pat10=Pattern.compile(".{0,6}送.{0,6}姑.{0,6}")
//            val pat11=Pattern.compile(".{0,6}送.{0,6}伯.{0,6}")
//            val pat12=Pattern.compile(".{0,6}送.{0,6}舅.{0,6}")


//      val pat1=Pattern.compile(".{0,6}去.{0,6}看.{0,6}您.{0,6}")
//      val pat2=Pattern.compile(".{0,6}去.{0,6}看.{0,6}你们.{0,6}")
//      val pat3=Pattern.compile(".{0,6}去.{0,6}看.{0,6}你.{0,6}")
//      val pat4=Pattern.compile(".{0,6}到.{0,6}看.{0,6}您.{0,6}")
//      val pat5=Pattern.compile(".{0,6}到.{0,6}看.{0,6}你们.{0,6}")
//      val pat6=Pattern.compile(".{0,6}到.{0,6}看.{0,6}你.{0,6}")
//      val pat7=Pattern.compile(".{0,6}去.{0,6}探望.{0,6}您.{0,6}")
//      val pat8=Pattern.compile(".{0,6}去.{0,6}探望.{0,6}你们.{0,6}")
//      val pat9=Pattern.compile(".{0,6}去.{0,6}探望.{0,6}你.{0,6}")
//      val pat10=Pattern.compile(".{0,6}到.{0,6}探望.{0,6}您.{0,6}")
//      val pat11=Pattern.compile(".{0,6}到.{0,6}探望.{0,6}你们.{0,6}")
//      val pat12=Pattern.compile(".{0,6}到.{0,6}探望.{0,6}你.{0,6}")
//      val pat13=Pattern.compile(".{0,6}去.{0,6}医院.{0,6}看.{0,6}")
//      val pat14=Pattern.compile(".{0,6}到.{0,6}医院.{0,6}看.{0,6}")
//      val pat15=Pattern.compile(".{0,6}来.{0,6}医院.{0,6}看.{0,6}")


      for(candidate<-candidateArray) {
        val ret = ((pat1.matcher(candidate).matches())
          || (pat2.matcher(candidate).matches())
          || (pat3.matcher(candidate).matches())
          || (pat4.matcher(candidate).matches())
          || (pat5.matcher(candidate).matches())
          || (pat6.matcher(candidate).matches())
          || (pat7.matcher(candidate).matches())
          || (pat8.matcher(candidate).matches())
//          || (pat9.matcher(candidate).matches())
//          || (pat10.matcher(candidate).matches())
//          || (pat11.matcher(candidate).matches())
//          || (pat12.matcher(candidate).matches())
//          || (pat13.matcher(candidate).matches())
//          || (pat14.matcher(candidate).matches())
//          || (pat15.matcher(candidate).matches())
          )
        if (ret==true)
          return true
      }
      return false
    }
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> myTest(x._2.fields.data.get("recResult").toString))
      .map(x=>(x._2.fields.data.get("dvc").toString,1))
      .reduceByKey((a,b)=>a+b)
      .map(x=>x._1).cache()

    println(infile0.count())

    infile0.repartition(100).saveAsTextFile(args(1))






  }
}

