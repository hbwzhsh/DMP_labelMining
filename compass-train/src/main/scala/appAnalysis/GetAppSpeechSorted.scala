package appAnalysis

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/3/7.
 */
/*
* This object group UV by app, collect input result and sort by timestamp.
* */
object GetAppSpeechSorted {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("getspeechwithappsorted")


    val infile1 = ShareSparkMethod
      .readExtractPair(args(1),sc)
      .filter{x=>
        x._2.fields.data.get("inputName")==args(0) &&
          x._2.fields.data.get("recResult")!="EMPTY"
      }
      .map{x=>
        (x._2.fields.data.get("timestamp").toString,
          (x._2.fields.data.get("timestamp").toString.toDouble,x._2.fields.data.get("recResult").toString))
      }
      .groupByKey()
      .map{x=>
        (x._1,x._2.toList.sortBy(_._1).map(x=>x._2))
      }
      .map(x=>x._1+"\t"+x._2.mkString("~"))
      .repartition(1)


    infile1.saveAsTextFile("/user/lxbai/project3/inputNameSorted/"+args(0)+"/")


  }
}
