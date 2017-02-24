package appAnalysis.mailGuy_app

import com.iflytek.huidu.utils.ShareSparkMethod
/**
 * Created by LinxiaoBai on 2016/4/7.
  * Helper function Cleaning invalid phone #
 */
object PhoneNumberCleaning {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContextLocal("")

    val infile1 = sc.textFile(args(0))
      .cache()

    println(infile1.count())

    val infile2=infile1
      .map(x=>x.split("\t"))
      .sortBy(x=>x(1).toInt,false)
      .map(x=>x.mkString("\t"))

    infile2.saveAsTextFile(args(1))


  }
}
