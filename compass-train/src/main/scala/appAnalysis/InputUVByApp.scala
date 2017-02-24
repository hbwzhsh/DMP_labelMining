package appAnalysis

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/2/12.
  * This object does uv analysis by app.
 */

object InputUVByApp {

  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("UV")


    val infile1 = ShareSparkMethod
      .readExtractPair(args(0),sc).filter(x=>x._2.fields.data.get("recResult")!="EMPTY")
      .map{x =>
        (x._2.fields.data.get("dvc").toString
          ,x._2.fields.data.get("appname").toString)
      }
      .groupByKey()
      .map(x=>(x._1,x._2.toSet))
      .flatMapValues(x=>x)
      .groupBy(x=>x._2)
      .map(x=>(x._1,x._2.size))
      .repartition(1)
      .sortBy(x=>x._2,false)


    infile1.saveAsTextFile(args(1))

  }
}
