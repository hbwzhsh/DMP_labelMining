package mlClassify.LongTuCampaign

import tools.ShareSparkMethod

/**
 * Created by Linxiao Bai on 2016/4/11.
  * subSet payed User to different class.
 */
object PayLevelUser {
  def main(args: Array[String]) {
    if(args.length != 4){
      System.err.println("[Usage]:<infile1><infile2><infile3><outfile>")
      System.exit(1)
    }

    val sc = ShareSparkMethod.getSparkContext("payleveluser")
    val gameuser = sc.textFile(args(0)).repartition(10).map(x =>x.replace("(","").replace(")","").split(","))
    .map(x =>(x(0),""))
    val genderuser = sc.textFile(args(1)).repartition(100).map(x =>x.split("\t")).map(x =>(x(0),x(1)))

    val male_gameuser = gameuser.join(genderuser).map(x =>(x._1,x._2._1 + x._2._2)).filter(x =>x._2.equals("MALE"))
    male_gameuser.map(x =>x._1).repartition(1).saveAsTextFile(args(3) + "/male_gameuser")

    val serious_gameuser = sc.textFile(args(2)).map(x =>x.split("\t")).map(x =>(x(0),""))
    val male_serious_gameuser = serious_gameuser.join(male_gameuser).map(x =>x._1)
    male_serious_gameuser.repartition(1).saveAsTextFile(args(3) + "/male_serious_gameuser")

    val no_serious_gameuser = gameuser.subtractByKey(serious_gameuser).map(x =>x._1)
    no_serious_gameuser.repartition(1).saveAsTextFile(args(3) + "/noerious_gameuser")
  }
}
