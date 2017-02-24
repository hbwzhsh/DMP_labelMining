package mlClassify.LongTuCampaign

import com.iflytek.huidu.utils.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import tools.HdfsFileRW

/**
 * Created by Linxiao on 2016/4/7.
  * This Object does prediction on heavy of a game. The
 */


object HeavyGameUsersPred {
  def main(args: Array[String]) {
    if(args.length != 9){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }


    def FileWrite(fs: FileSystem, filename: String) : BufferedWriter= {
      var outputStream = fs.create (new Path (filename) )
      var bw = new BufferedWriter (new OutputStreamWriter (outputStream) )
      return bw
    }

    val sc = ShareSparkMethod.getSparkContext("Use days stat")

/*Read game package name as Set*/

    val gamepkg = sc.textFile(args(0)).filter(x=>x.contains("刀塔传奇")||x.contains("星际传奇")||x.contains("古剑奇谭")||x.contains("鬼武三国志")||x.contains("尸兄"))
      .map(_.split("\t")).map(x =>(x(0),x(1))).collect().toMap

/*Pull package name, active days, none-active date. Filter-out None-game app, using (imei, appname) as key*/

    val infile1 = ShareSparkMethod
      .readExtractPair(args(1),sc).repartition(args(5).toInt)
      .map{x =>
        ((x._1,x._2.fields.data.get("packagename").toString),
          (x._2.fields.data.get("appUse").asInstanceOf[util.ArrayList[Long]].toArray().mkString(","),
            x._2.fields.data.get("appNotUse").asInstanceOf[util.ArrayList[Long]].toArray().mkString(","))
          )
      }
      .filter(x =>gamepkg.contains(x._1._2))
      .persist()


/*UV analysis*/
    val infile2 = infile1
      .groupByKey()
      .map{x =>
        (x._1,
          x._2.reduce((a,b)=>(a._1 + "," + b._1,a._2 + "," + b._2))
          )
      }
      .map{x =>
        (x._1,
          (x._2._1.split(",").toSet.size,
            x._2._2.split(",").toSet.size)
          )
      }

    val thres_days = args(3).toInt
    val thres_ratio = args(4).toDouble

/*Adding filtering condition*/
    val frequentUser = infile2
      .filter(x => x._2._1 > thres_days)
      .map{x =>
        (x._1,x._2._1.toDouble / (x._2._1.toDouble + x._2._2.toDouble))
      }
      .filter(x => x._2 > thres_ratio)
      .map(x =>(x._1._1,1))
      .reduceByKey(_+_)
      .map(x =>x._1 + "\t" + x._2)


/*StatKicksIn. Analyze goodness of fit. Print TF,TP,FF,FP*/
    val pay=  sc.textFile(args(6)).map(x => ("imei_"+x,""))
    val nonePay=  sc.textFile(args(7)).map(x => ("imei_"+x,""))

    val heavyUser = frequentUser.map(s=>s.split("\t")).map(s=>(s(0),s(1)))
    val noneHeavyUser=sc.textFile(args(8))
      .map(s=>s.replace("(",""))
      .map(s=>s.replace(")",""))
      .map(s=>s.split(","))
      .map(s=>(s(0),s(1)))
      .subtractByKey(heavyUser)


    val HP=pay.join(heavyUser).count()
    val HNP=nonePay.join(heavyUser).count()
    val NHP=pay.join(noneHeavyUser).count()
    val NHNP=nonePay.join(noneHeavyUser).count()

/*Generating confusion Table*/
    val ret="\n\n\n"+thres_days.toString+thres_ratio.toString+"\n\t\tHeavyUser\t\t\tNoneHeavyUser\nPay\t"+HP.toString+"\t\t\t"+NHP.toString+"\n"+"NPay\t"+HNP.toString+"\t\t\t"+NHNP.toString+"\n"+
      "\t\t"+(HP.toDouble/HNP).toString+"\t\t\t"+(NHP.toDouble/NHNP).toString+"\t"+(HP.toInt+HNP.toInt+NHP.toInt+NHNP.toInt).toString

    val conf = new Configuration()
    val fs=org.apache.hadoop.fs.FileSystem.get(conf)
    val out= new HdfsFileRW()
      .FileWrite(fs,args(2)+thres_days.toString+thres_ratio.toString+".txt")
    out.write(ret)
    out.close()



/*Spitting result in log*/
    println("thresRatio is ====>"+thres_ratio)
    println("thresDays is ====>"+thres_days)
    println("====>overall population in string is" +HP+HNP+NHP+NHNP)
    println("====>overall population in num is" +HP.toInt+HNP.toInt+NHP.toInt+NHNP.toInt)
    println("====>left ratio is" + HP.toDouble/HNP)
    println("====>right ratio is"+ NHP.toDouble/NHNP)
    println("====>odds ratio is"+(HP.toDouble/HNP)/(NHP.toDouble/NHNP))


    println("HP =====>"+ HP)
    println("NHP =====>"+ NHP)
    println("HNP =====>"+ HNP)
    println("NHNP =====>"+ NHNP)
  }
}
