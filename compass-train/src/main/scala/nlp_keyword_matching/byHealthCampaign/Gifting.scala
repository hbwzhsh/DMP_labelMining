package nlp_keyword_matching.byHealthCampaign

import com.iflytek.huidu.utils.ShareSparkMethod
import nlp_keyword_matching.RegX

/**
 * Created by LinxiaoBai on 2016/5/27.
 */
object Gifting {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    var myRegx= new RegX()
    myRegx.setVerbArray(Array("送"))
    myRegx.setObjectArray(Array("爸","妈","母","父","娘","叔","姨","婶","姑","舅","公","您","他","她","老板","上司","老师","长","主管","总监","经理"))
    myRegx.ready()

    val sc = ShareSparkMethod.getSparkContext("filterByDisease")
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .map(x=> (x._2.fields.data.get("dvc").toString
        ,x._2.fields.data.get("recResult").toString
        )
      )
      .filter(x=> myRegx.goMatch(x._2))
      .filter(x=> !x._2.contains("到") && !x._2.contains("去") && !x._2.contains("来") && !x._2.contains("回"))


    val infile1= infile0.map(x=>x._2)
    println(infile1.count())

  }
}

