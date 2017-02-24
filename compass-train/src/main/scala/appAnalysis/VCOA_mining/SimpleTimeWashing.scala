package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

/**
 * Created by LinxiaoBai on 2016/5/17.
 */
object SimpleTimeWashing {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }
    val sc = ShareSparkMethod.getSparkContext("IP")
    val infile0=sc.textFile(args(0))
          .map(x=>
            {val temp=x.replace("/t","\t")
              .split("\t")
              (temp(0),(temp(1),temp(2)))
            }//(dvc,(time,ip))
          )
          .groupByKey()
          .cache()

    val allUV=infile0.count()
    println("All UV is ====>"+allUV)

    //time filtering begins here
    val sdf = new SimpleDateFormat("EEEE")
    val asd = new SimpleDateFormat("hh")
    //Use on more than 7 days? and User Average days
    val infile1=infile0
      .map(x=>(x._1,x._2
        .map(x=>sdf
          .format(new Date(x._1.toDouble.toLong)))
          .toSet
          .size
        )
      )
    val averageUseDay= infile1.map(x=>x._2).reduce((x,y)=>x+y)
    println("averageUseDay is ====>"+averageUseDay.toDouble/allUV)

    val infile2=infile1
      .filter(x=>x._2>6)

    println("7days UV is ====>"+infile2.count())

      // How many users use on weekdays only
    val infile3= infile0
      .map(x=>(x._1,x._2
        .map(x=> (sdf.format(new Date(x._1.toDouble.toLong))
                ,asd.format(new Date(x._1.toDouble.toLong)).toInt
              )
        )
        .map(x=>{
          if((x._1=="Sunday"||x._1=="Saturday")|| (x._2<9||x._2>18)) "weekend"
          else "weekdays"
          }
        )
        .toSet
        )
      )
      .cache()

    val weekdays=infile3.filter(x=> x._2.contains("weekdays")&& !x._2.contains("weekend")).count()
    println("weekdaysOnly UV is ====>"+weekdays)

    val weekend=infile3.filter(x=> x._2.contains("weekend")&& !x._2.contains("weekdays")).count()
    println("weekendOnly UV is ====>"+weekend)


    // Move On to PV


    val infile4=sc.textFile(args(0))
      .map(x=>
      {val temp=x.replace("/t","\t")
        .split("\t")
        (temp(0),sdf.format(new Date(temp(1).toDouble.toLong)),asd.format(new Date(temp(1).toDouble.toLong)),temp(2))
      }//(dvc,eeee,hh,ip)
      )
    val allPV=infile4.count()
    println("allPV is ====>"+allPV)
    val weekendPV=infile4.filter(x=> ((x._2== ("Saturday")||x._2.==("Sunday")) || (x._3.toInt<9 ||x._3.toInt>18))).count()
    println("weekend PV is ====>"+weekendPV)
    val weekdaysPV=infile4.filter(x=> ((x._2!= ("Saturday")&& x._2.!=("Sunday")) && (x._3.toInt>9 &&x._3.toInt<17))).count()
    println("weekdays PV is ====>"+weekdaysPV)





  }
}

