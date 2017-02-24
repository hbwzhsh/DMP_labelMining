package appAnalysis

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/2/7.
 */

/*
* This object collect input result from target app
*/
object GetAppSpeech {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("getspeechwithapp")

    val infile1 = ShareSparkMethod
      .readExtractPair(args(1),sc)
      .filter{x=>
        x._2.fields.data.get("appname")==args(0)&&
          x._2.fields.data.get("recResult")!="EMPTY"
      }
      .map(x =>x._2.fields.data.get("recResult").toString).repartition(1)

    infile1.saveAsTextFile("/user/lxbai/project3/"+args(0)+"/")


  }
}
