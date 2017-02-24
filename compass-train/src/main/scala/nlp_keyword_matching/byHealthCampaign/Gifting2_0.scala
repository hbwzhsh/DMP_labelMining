package nlp_keyword_matching.byHealthCampaign

import com.iflytek.huidu.utils.ShareSparkMethod
import nlp_keyword_matching.RegX

import scala.collection.mutable.ArrayBuffer

/**
 * Created by LinxiaoBai on 2016/5/27.
  * 2.0 version of Gifting remove some Ambiguity.
 */
object Gifting2_0 {

  def takeContext(in:Array[(String,String)], regX: RegX) :Array[String]={
    var i=0
    var ret = new ArrayBuffer[String]
    while (i>=0 && i<in.length){
      if(regX.goMatch(in(i)._1) && !in(i)._1.contains("来") && !in(i)._1.contains("去") && !in(i)._1.contains("到") && !in(i)._1.contains("回")){

        var k=i-3
        while (k<=i+3){
          if(k==i){
            ret+="====>"
          }
          if(k<in.length && k<=i+3 && k>=0 && in(k)._2==in(i)._2){
            ret+=in(k)._1
          }

          k+=1
        }
        ret+="|"
      }
      i+=1
    }
    return ret.toArray
  }


  def main(args: Array[String]) {
    if(args.length <1){
      System.err.println("[Usage]:<infile1><infile2><outfile><thres_days><thres_ratio><partitions>")
      System.exit(1)
    }

    var myRegx= new RegX()
    myRegx.setVerbArray(Array("送"))
    myRegx.setObjectArray(Array("爸","妈","母","父","娘","叔","姨","婶","姑","舅","公","您","他","她","老板","上司","老师","校长","部长","处长","科长","司长","厅长","主管","总监","经理"))
    myRegx.ready()

    val sc = ShareSparkMethod.getSparkContext("filterByDisease")
    val infile0=ShareSparkMethod
      .readExtractPair(args(0),sc)
      .map(x=> (x._2.fields.data.get("dvc").toString
        ,(x._2.fields.data.get("recResult").toString
        ,x._2.fields.data.get("timestamp").toString
        ,x._2.fields.data.get("appname").toString)
        )
      )
      .filter(x=>x._2._1!="EMPTY")


    val infile1= infile0
      .groupByKey()
      .repartition(100)
      .cache()

    val infile2=infile1
      .map(x=>x._1+"\t"+takeContext(x._2.toArray.sortBy(x=>x._2).map(x=>(x._1,x._3)),myRegx).mkString("~"))


    infile2.repartition(1).saveAsTextFile(args(1))

  }
}

