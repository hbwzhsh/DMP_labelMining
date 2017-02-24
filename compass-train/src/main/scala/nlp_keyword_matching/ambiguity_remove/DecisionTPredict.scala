package nlp_keyword_matching.ambiguity_remove

import com.iflytek.huidu.utils.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import tools.HdfsFileRW

import scala.collection.immutable.HashMap

/**
 * Created by Linxiao Bai on 2016/6/30.
 */

object DecisionTPredict{


  def main(args: Array[String]) {
    val sc=ShareSparkMethod.getSparkContextLocal("decisionTreePredict")
    val indata=sc.textFile(args(0)).cache()
    val conf = new Configuration()
    val fs=org.apache.hadoop.fs.FileSystem.get(conf)
    var map= new HashMap[String,Int]
    val tempmap=  new HdfsFileRW().FileRead(fs,args(1)+"Keymap/map.txt")
    var s="fakeData"

    while (s!=null){
      s=tempmap.readLine()
      if(s!=null) {
        val temp = s.split(':')
        map += (temp(0) -> temp(1).toInt)
      }
    }
    tempmap.close()

    val model=DecisionTreeModel.load(sc,args(1)+"model")

    val data=PredictionHelper.transform(indata,map,model).filter(x=> x._1== 1).map(x=>x._1+","+x._2)

    data.saveAsTextFile(args(2))

  }

}
