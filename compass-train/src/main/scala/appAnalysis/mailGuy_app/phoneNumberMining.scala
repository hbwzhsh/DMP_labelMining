package appAnalysis.mailGuy_app

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/2/7.
 */
object phoneNumberMining {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("getspeechwithapp")

    val p = Pattern.compile("1\\d{10}")
    val infile1 = ShareSparkMethod
    .readExtractPair(args(1),sc).filter(x=>x._2.fields.data.get("appname")==args(0)
      &&x._2.fields.data.get("recResult")!="EMPTY"&&p.matcher(x._2.fields.data.get("recResult").toString).matches())
      .map(x =>(x._2.fields.data.get("recResult").toString,(1,x._2.fields.data.get("dvc").toString)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+","+y._2)).map(x=>(x._1,(x._2._1,x._2._2.split(",").groupBy(x=>x).map(x=>x._2.size).toArray.mkString(","))))
      .repartition(1).sortBy(x=>x._2._1.toInt,false).map(x=>x._1+"\t"+x._2._1.toString+"\t"+x._2._2.toString)


    infile1.saveAsTextFile("/user/lxbai/project3/"+args(0)+"/")


  }
}
