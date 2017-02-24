package nlp_keyword_matching.ambiguity_remove

import com.iflytek.huidu.utils.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import tools.{HdfsFileRW, HttpRequest}

import scala.collection.immutable.{HashMap, HashSet}

/**
 * Created by LinxiaoBai on 2016/6/28.
 */

object DecisionTTrain{

  def main(args: Array[String]) {
    val sc=ShareSparkMethod.getSparkContextLocal("DecisionTTrain")

    val time0=System.currentTimeMillis()

    var helpmap=new HashMap[String,Int]
    var k=1
    def func(in:String) :Array[Int]={
      var result=""
      var toggle=0
      var toggle2=0
      val target='送'
      for(i<- in){
        if(i==target){
          toggle=1
        }
        if(i>='A' && i<='Z' && toggle==1){
          toggle2=1
          result+=i
        }
        else{
          if(i==target){
            toggle=1
            result+=','
          }
          else if(toggle2==1){
            toggle=0
            result+=','
          }
          toggle2=0

        }
      }

      var ret=new HashSet[Int]
      //i is a string

      for(i<- result.split(',')){
        if (i!=""){
          if(!helpmap.contains(i)){
            helpmap+=(i->k)
            k+=1
          }
          ret+=helpmap(i)
        }
      }
      ret.toArray.distinct
    }

    /*
    action here
     */
    val infile0=sc.textFile(args(0))
      .collect()
      .map(x=>{
        val temp1=x.trim.split(',')
        val text=temp1(1)
        val input=  URLEncoder.encode(text,"utf-8")
        val s=HttpRequest.sendGet("http://ltpapi.voicecloud.cn/analysis/?api_key=A104Z6d7v020L9B6b9x6UnBU5TqmaQ7NdRcWXJ1x&text="+input+"&pattern=dp&format=plain","")

        val ret=func(s).sortBy(x=>x)
        if(ret.length==0){
          temp1(0)+" "
        }
        else{
          temp1(0)+" "+ret.mkString(":1 ")+":1"
        }

      })


    /*
    write reprocessed data to args(1)
     */


    val conf = new Configuration()
    val fs=org.apache.hadoop.fs.FileSystem.get(conf)
    if (fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    val infile1=sc.parallelize(infile0,1)
    infile1.saveAsTextFile(args(1))

    /*
    write keySet to hdfs
    */

    val out= new HdfsFileRW().FileWrite(fs,args(1)+"Keymap/map.txt")
    val i=helpmap.keysIterator

    while(i.hasNext){
      val key=i.next()
      out.write(key+':'+helpmap(key).toString+"\n")
      out.flush()
    }
    out.close()


    /*
    deleting unnecessary file to support trainning
     */

    if (fs.exists(new Path(args(1)+"._SUCCESS.crc"))) fs.delete(new Path(args(1)+"._SUCCESS.crc"),true)
    if (fs.exists(new Path(args(1)+".part-00000.crc"))) fs.delete(new Path(args(1)+".part-00000.crc"),true)
    if (fs.exists(new Path(args(1)+"_SUCCESS"))) fs.delete(new Path(args(1)+"_SUCCESS"),true)

    /*
    Generating model
     */
    val data = MLUtils.loadLibSVMFile(sc, args(1)+"part-00000")



    val splits = data.randomSplit(Array(0.5, 0.5))
    val (trainingData, testData) = (splits(0), splits(1))



    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 3
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    //    model.save(sc, args(1)+"model/")


    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    val t1Error= labelAndPreds.filter(r=>r._1 != r._2 && r._1==1D).count().toDouble / testData.count()/testErr
    val t2Error= labelAndPreds.filter(r=>r._1 != r._2 && r._1==0D).count().toDouble / testData.count()/testErr

    println("Test Error = " + testErr)
    //    println("Learned classification tree model:\n" + model.toDebugString)
    println("Type1 Error = " + t1Error)
    println("Type2 Error = " + t2Error)
    println("Running Time is ===>"+(System.currentTimeMillis() - time0))
  }
}