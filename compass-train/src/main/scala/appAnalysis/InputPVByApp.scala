package appAnalysis

/**
 * Created by LinxiaoBai on 2016/4/7.
  * This object does pv analysis by app.
 */
object InputPVByApp {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("Use days tongji")

    val infile1 = ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter{x =>
        x._2.fields.data.get("appname") != "EMPTY" &&
          x._2.fields.data.get("recResult") != "EMPTY"
      }
      .map(x =>(x._2.fields.data.get("appname").toString,1))
      .reduceByKey((a,b)=>a+b)
      .repartition(1)
      .sortBy(x=>x._2,false)

    infile1.saveAsTextFile(args(1))

  }
}
