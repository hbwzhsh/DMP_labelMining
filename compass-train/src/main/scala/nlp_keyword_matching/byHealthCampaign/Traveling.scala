package nlp_keyword_matching.byHealthCampaign

import com.iflytek.huidu.utils.ShareSparkMethod
import nlp_keyword_matching.RegX

/**
 * Created by LinxiaoBai on 2016/5/27.
 */

object Traveling {

  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }


    val myRegx= new RegX()
    myRegx.setSubjectArray((Array("到","去","来","回")))
    myRegx.setVerbArray(Array("看","探望","医院"))
    myRegx.setObjectArray(Array("爸","妈","长辈","母","父","娘","叔","姨","婶","舅","舅","公","你","你们","您","爸"))
    myRegx.ready()

    val sc = ShareSparkMethod.getSparkContext("filterByDisease")
        val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .map{x=> (x._2.fields.data.get("dvc").toString
        ,x._2.fields.data.get("recResult").toString
        )
      }
      .filter(x=> myRegx.goMatch(x._2))


    val infile1=infile0
      .groupByKey()
      .map(x=>x._1)
      .cache()

    println(infile1.count())
    infile1.repartition(1).saveAsTextFile(args(1))

  }
}

