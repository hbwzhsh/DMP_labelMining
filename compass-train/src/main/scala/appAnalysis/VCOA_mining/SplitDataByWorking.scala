package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/5/16.
 */
object SplitDataByWorking {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }




    val sc = ShareSparkMethod.getSparkContext("IP")
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
        && x._2.fields.data.get("recResult").toString!="EMPTY")
      .map(x=> (x._2.fields.data.get("dvc").toString
            ,(x._2.fields.data.get("timestamp").toString//timeStamp filtering here
            ,{val temp=x._2.fields.data.get("clientIp").toString.split('.')
            temp(0)+'.'+temp(1)+'.'+temp(2)
          }
            ))
          )


    val infile1 = ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
        &&(x._2.fields.data.get("recResult").toString.contains("上班")
        || x._2.fields.data.get("recResult").toString.contains("下班")))
      .map(x=>(x._2.fields.data.get("dvc").toString,""))
      .groupByKey()
      .cache()
      .join(infile0)
      .map(x=>x._1+"/t"+x._2._2._1+"\t"+x._2._2._2).repartition(100)


    val infile2 = ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> x._2.fields.data.get("clientIp").toString.split('.').size==4
        && x._2.fields.data.get("recResult").toString!="EMPTY"
        && !(x._2.fields.data.get("recResult").toString.contains("上班")
        || x._2.fields.data.get("recResult").toString.contains("下班")))
      .map(x=>(x._2.fields.data.get("dvc").toString,""))
      .groupByKey()
      .cache()
      .join(infile0)
      .map(x=>x._1+"\t"+x._2._2._1+"\t"+x._2._2._2).repartition(100)


    infile1.saveAsTextFile(args(1))
    infile2.saveAsTextFile(args(2))





  }
}

