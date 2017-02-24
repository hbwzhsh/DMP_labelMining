package mlClassify.LongTuCampaign

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/4/1.
  * This Object finds inner join of payed user in VCOA data
 */
object PayUserFinder {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("[Usage]:<infile1><infile2><infile3><outfile>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("get pay user ")
    val sc = new SparkContext(conf)

    val user_app = sc.textFile(args(0)).repartition(100).map(x => x.split("\t")).map(x => (x(0), x(1)))
    val payusers = sc.textFile(args(1)).repartition(1).map(x => ("imei_" + x, ""))

    val gameapp = sc.textFile(args(2)).map(x => x.split("\t")).map(x => x(0)).collect().toSet

    val payusers_app = user_app.join(payusers).map(x =>(x._1,x._2._1 + x._2._2) ).map(x =>(x._1,x._2.split(",")))
      .map(x =>(x._1, GetGameApp(x._2, gameapp))).map(x =>(x._1,x._2.split(",").length)).map(x =>(x._2,x._1)).groupByKey()
      .map(x =>(x._1,x._2.size)).reduceByKey((a,b) => a+b).map(x =>"appnum:" + x._1 + "\t" +"usernum:" + x._2)
    payusers_app.repartition(1).saveAsTextFile(args(3))

  }

  def GetGameApp(x: Array[String], appSet: Set[String]): String = {
    var applist = ""
    for (value: String <- x) {
      if (appSet.contains(value))
        applist += value + ","
    }
    applist + ","
  }
}
