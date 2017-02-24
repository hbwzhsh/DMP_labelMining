package nlp_keyword_matching.ambiguity_remove

/**
 * Created by Administrator on 2016/6/29.
 */

import com.iflytek.huidu.utils.ShareSparkMethod
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

object DecisionTrees{
  def main(args: Array[String]) {
    val sc=ShareSparkMethod.getSparkContextLocal("decsion trees")
    val data = MLUtils.loadLibSVMFile(sc, args(0))
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
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
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)
    model.save(sc, args(1))



  }

}
