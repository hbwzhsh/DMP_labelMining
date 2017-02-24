package appAnalysis.VCOA_mining

import com.iflytek.huidu.utils.ShareSparkMethod

import scala.collection.mutable.Map

/**
 * Created by LinxiaoBai on 2016/5/18.
  * This object is a helper object that sort user IP changes and occupation time trend.
 */
object SimpleIPAnalysis {

  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }
    val sc = ShareSparkMethod.getSparkContextLocal("IP")
    val infile1 = sc
      .textFile(args(0))
      .map(x=>x.replace("/t","\t").split("\t"))
      .map(x=> (x(0),(x(1),x(2))))
      .cache()//checkpoint
      .groupByKey()
      .map { x =>
        (x._1, {
          val sortArray = x._2.toArray.sortBy(x => x._1)
          val iter = sortArray.length
          var counter = 0
          var ipTimeMap: scala.collection.mutable.Map[String, Long] = Map()
          var prevItem = ("fakeTime", "fakeIp")
          var currLastTime = 60000L
          for (item <- sortArray) {
            //ip=(time,ip)
            counter += 1

            var continue = 0 //to mark continue point. 1 is it

            /* If IP address is first appearance, push into map with occupation of 10min*/
            if (!ipTimeMap.contains(item._2)) {
              ipTimeMap += (item._2 -> 60000L)
              prevItem = item
              continue = 1
            }


            if (continue == 0) {
              /* If ip is different from prev? */
              if (item._2 != prevItem._2 || iter == counter) {
                if (iter == counter) {
                  currLastTime += (item._1.toDouble.toLong - prevItem._1.toDouble.toLong)
                }
                ipTimeMap(prevItem._2) += currLastTime
                currLastTime = 60000L //reset curLastTime
                prevItem = item
              }

              else {
                currLastTime += (item._1.toDouble.toLong - prevItem._1.toDouble.toLong)
                prevItem = item
              }
            }
          }
          ipTimeMap.toArray.sortBy(x => x._2)
        })
      }
      .map(x=>(x._1,x._2(0)._1,(x._2(0)._2/60000).toString))

    infile1.saveAsTextFile(args(0))




  }
}

