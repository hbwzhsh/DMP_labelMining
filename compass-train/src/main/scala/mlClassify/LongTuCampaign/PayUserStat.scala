package mlClassify.LongTuCampaign

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/3/31.
  * This object collects UV statistics of payed user In VCOA Data
 */

object PayUserStat {

  def main(args: Array[String]) {
    if(args.length < 5){
      System.err.println("[Usage]:<infile1><infile2><infile3><infile4><outfile>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("get pay user appinstall")
    val sc = new SparkContext(conf)

    val user_app = sc.textFile(args(0))
      .repartition(100)
      .map(x => x.split("\t"))
      .map(x => (x(0),x(1)))

    val payusers = sc.textFile(args(1))
      .repartition(1)
      .map(x =>("imei_" + x,""))

    val user_gender = sc.textFile(args(2))
      .repartition(10).map(x =>x.split("\t"))
      .map(x =>(x(0),x(1)))

    val game_type = sc.textFile(args(3))
      .map(x => x.split("\t"))
      .map(x =>(x(0),x(1)))
      .collect()
      .toMap

    val payusers_app = user_app
      .join(payusers)
      .map(x =>x._2._1 + x._2._2)

    val payusers_gender = user_gender
      .join(payusers)
      .map(x =>x._2._1 + x._2._2)
      .map(x =>(x,1))
      .reduceByKey(_+_)
      .map(x =>x._1 + "\t" + x._2)

    val payusers_gameapp = payusers_app
      .flatMap(x =>x.split(","))
      .map{x =>
        if(game_type.contains(x))
          (x,game_type.get(x).toString)
        else
          (" "," ")
      }

    val payusers_game = payusers_gameapp
      .map(x =>(x._1,1))
      .reduceByKey(_+_)
      .map(x =>x._1 + "\t" + x._2)

    val payusers_type = payusers_gameapp
      .map(x =>(x._2,1))
      .reduceByKey(_+_)
      .map(x =>x._1 + "\t" + x._2)

    val outfile1 = payusers_app
      .flatMap(x =>x.split(","))
      .map(x =>(x,1))
      .reduceByKey(_+_)
      .sortBy(x =>x._2)
      .map(x =>x._1 + "\t" + x._2)


    val num = payusers_app.count()
    println(s"join num: $num")
    outfile1.repartition(1).saveAsTextFile(args(4) + "/app")
    payusers_gender.repartition(1).saveAsTextFile(args(4) + "/gender")
    payusers_game.repartition(1).saveAsTextFile(args(4) + "/game")
    payusers_type.repartition(1).saveAsTextFile(args(4) + "/type")


  }


}
