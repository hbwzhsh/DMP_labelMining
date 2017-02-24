package nlp_keyword_matching.ambiguity_remove

import com.iflytek.huidu.utils.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

import scala.collection.immutable.HashMap


/**
 * Created by LinxiaoBai on 2016/5/27.
 */
object DecisionTTrain2_0 {
  def main(args: Array[String]) {
    if(args.length <0){
      System.err.println("Must Have 3 inPut")
      System.exit(1)
    }

    val conf = new Configuration()
    val fs=org.apache.hadoop.fs.FileSystem.get(conf)
    val sc = ShareSparkMethod.getSparkContextLocal("filterByDisease")

    /*load maps */
    val time0=System.currentTimeMillis()
    val dpmap= PredictionHelper.mapLoader(args(1))
    val posmap= PredictionHelper.mapLoader(args(2))
    val infile= sc.textFile(args(0))
      .map(x=>x.trim.split(",",2))
      .map(x=> x(0)+" "+PredictionHelper.buildTreeHelper(x(1),dpmap,posmap))

    /*save text file for loading libsvm*/
    if (fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)),true)
    infile.saveAsTextFile(args(3))





    /*load data to libsvm*/
    if (fs.exists(new Path(args(3)+"._SUCCESS.crc"))) fs.delete(new Path(args(3)+"._SUCCESS.crc"),true)
    if (fs.exists(new Path(args(3)+".part-00000.crc"))) fs.delete(new Path(args(3)+".part-00000.crc"),true)
    if (fs.exists(new Path(args(3)+"_SUCCESS"))) fs.delete(new Path(args(3)+"_SUCCESS"),true)
    val data = MLUtils.loadLibSVMFile(sc, args(3)+"part-00000")


    /*Form Tree Here*/

    var newMap= new HashMap[Int,Int]
    for (i<- 1 to 15){
      newMap+=(i->29)
    }


    val splits = data.randomSplit(Array(0.5, 0.5))

    val trainingData=MLUtils.loadLibSVMFile(sc, args(4))
    val testData=MLUtils.loadLibSVMFile(sc, args(5))

//    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = newMap
    val impurity = "gini"
    val maxDepth = 3
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    val t1Error = labelAndPreds.filter(r=>r._1 != r._2 && r._1==1D).count().toDouble / testData.count()/testErr
    val t2Error = labelAndPreds.filter(r=>r._1 != r._2 && r._1==0D).count().toDouble / testData.count()/testErr

    /*print result*/
    println("Test  Error = " + testErr)
    println("Type1 Error = " + t1Error)
    println("Type2 Error = " + t2Error)
    println("Running Time is ===>"+(System.currentTimeMillis() - time0))
  }
}