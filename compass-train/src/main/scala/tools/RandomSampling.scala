package tools

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/3/8.
 * Random Sampling Testing/ Training
 */
object RandomSampling {
  def main(args: Array[String]) {
    if(args.length < 3){
      System.err.println("[Usage]:<input_file><Train><Test>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Split File")
    val sc = new SparkContext(conf)
    val in = sc.textFile(args(0))
    val split = in.randomSplit(Array(0.8,0.2))
    split(0).saveAsTextFile(args(1))
    split(1).saveAsTextFile(args(2))

  }
}
