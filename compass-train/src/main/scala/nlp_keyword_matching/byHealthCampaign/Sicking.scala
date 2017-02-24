package nlp_keyword_matching.byHealthCampaign

import com.iflytek.huidu.utils.ShareSparkMethod
import nlp_keyword_matching.RegX

/**
 * Created by LinxiaoBai on 2016/5/20.
 */
object Sicking {
  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    val myRegx= new RegX()
    myRegx.setSubjectArray(Array("我"))
    myRegx.setVerbArray(Array("得","查","染","害","有"))
    myRegx.setObjectArray(Array("冠心病","高血压","糖尿病","高血脂","癌","高血糖","心脏","肺","肝","脾","肾","内分泌","消化","血糖"))
    myRegx.ready()

    val sc = ShareSparkMethod.getSparkContext("filterByDisease")
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .filter(x=> myRegx.goMatch(x._2.fields.data.get("recResult").toString))
      .map(x=> (x._2.fields.data.get("dvc").toString
        ,1
        )
      )

    val infile1=infile0.groupByKey().cache()
    println(infile1.count())

    infile1.map(x=>x._1.toString).repartition(1).saveAsTextFile(args(1))

  }
}

