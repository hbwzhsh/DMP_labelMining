package tools

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/3/4.
 * Sort Unsorted Libsvm data.
 */

object SortLibsvm {
  def main(args: Array[String]) {
    if(args.length < 2){
      System.err.println("[Usage]:<input><output>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("File Norm")
    val sc = new SparkContext(conf)
    val in_file = sc.textFile(args(0))
    val out_file = in_file.map(line => (line.substring(0,1), line.substring(2,line.length - 1))).map(x=>(x._1, x._2.split(" ").sortBy(s=>s.substring(0,s.indexOf(":")).toInt))).map(x=>x._1 + " " + x._2.mkString(" "))

    out_file.saveAsTextFile(args(1))
  }
}
