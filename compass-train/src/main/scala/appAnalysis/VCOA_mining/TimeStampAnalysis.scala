package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/5/12.
 */
object TimeStampAnalysis {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }



    val sc = ShareSparkMethod.getSparkContextLocal("TimeStamp")
    val infile1 = ShareSparkMethod
      .readExtractPair(args(0),sc)
      .map(x=>
        (x._2.fields.data.get("valat").toString,x._2.fields.data.get("valong").toString)).filter(x=>x._1!="-1.0")

    val infile2=infile1.take(10)
    infile2.foreach(x=>println(x))
  }
}

