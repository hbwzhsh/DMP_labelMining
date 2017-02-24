package nlp_keyword_matching.ambiguity_remove

import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import tools.{HdfsFileRW, HttpRequest}

import scala.collection.immutable.{HashMap, HashSet}
import scala.util.parsing.json.JSON

/**
 * Created by Linxiao Bai on 2016/6/30.
 */
object PredictionHelper{

  def transform(in: RDD[String],map:HashMap[String,Int],model:DecisionTreeModel):RDD[(Int,String)]={
    /*
    Def func here
    */

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
        if(map.contains(i)){
          ret+=map(i)
        }
      }
      ret.toArray.distinct
    }

    //func ends here


    val temp=in.map(x=>{
      val text = x.split("\t")(0)
      val input = URLEncoder.encode(text, "utf-8")
      val url="http://ltpapi.voicecloud.cn/analysis/?api_key=A104Z6d7v020L9B6b9x6UnBU5TqmaQ7NdRcWXJ1x&text=" + input + "&pattern=dp&format=plain"
      val s=HttpRequest.sendGet(url, "")
      val temp=func(s).sortBy(x=>x).mkString(":1 ")
      if(temp!="")
        ("0 "+func(s).sortBy(x=>x).mkString(":1 ")+":1",text)
      else
        ("0 ",text)
      })
      .map(x=>(x._1.trim,x._2))
      .filter(line => !(line._1.isEmpty || line._1.startsWith("#")))
      .map { line =>
        val items = line._1.split(' ')
        val label = items.head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
          val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
          val current = indices(i)
          require(current > previous, "indices should be one-based and in ascending order" )
          previous = current
          i += 1
        }
        ((label, indices.toArray, values.toArray),line._2)
      }

    val d= {
      temp.persist(StorageLevel.MEMORY_ONLY)
      temp.map { case ((label, indices, values),text) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    temp.map { case ((label, indices, values),text) =>
     val feat= LabeledPoint(label, Vectors.sparse(d, indices, values)).features
     val pred=model.predict(feat).toInt
      (pred,text)
    }




  }


  def buildTreeHelper(in:String, dpmap:Map[String,Int],posmap:Map[String,Int]):String={

    var ret=new HashMap[Int,Int]()
    val target="送"
    val input=  URLEncoder.encode(in,"utf-8")
    val s=HttpRequest.sendGet("http://ltpapi.voicecloud.cn/analysis/?api_key=A104Z6d7v020L9B6b9x6UnBU5TqmaQ7NdRcWXJ1x&text="+input+"&pattern=dp&format=json","")

    val b = JSON.parseFull(s)


    var list:List[Map[String,String]]= List()
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(list1: List[List[List[Map[String,String]]]]) => list=list1(0)(0)
      case None =>
      case other =>
    }


    var id= -1
    for(i<- list ){
      if(i("cont").contains(target)){
        id=i("id").asInstanceOf[Double].toInt
      }
    }

    if(id==(-1)){
      return ""
    }

    ret+=(16->posmap(list(id)("pos")))

    for(i<-list){
      if(i("id").asInstanceOf[Double].toInt!=id){

        val parentid=i("parent").asInstanceOf[Double].toInt
        if(parentid==id) {

          val key = dpmap(i("relate"))
          val value = posmap(i("pos"))
          if(!ret.keySet.contains(key)){
            ret+= (key->value)
          }
        }
      }
    }
    return ret.toArray.sortBy(x=>x._1).map(x=>x._1.toString+":"+x._2.toString).mkString(" ")
  }


  def buildTreeHelperCount(in:String, dpmap:Map[String,Int],posmap:Map[String,Int]):String={

    var ret=new collection.mutable.HashMap[Int,Int]()
    val target="送"
    val input=  URLEncoder.encode(in,"utf-8")
    val s=HttpRequest.sendGet("http://ltpapi.voicecloud.cn/analysis/?api_key=A104Z6d7v020L9B6b9x6UnBU5TqmaQ7NdRcWXJ1x&text="+input+"&pattern=dp&format=json","")
    val b = JSON.parseFull(s)


    var list:List[Map[String,String]]= List()
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(list1: List[List[List[Map[String,String]]]]) => list=list1(0)(0)
      case None =>
      case other =>
    }


    var id= -1
    for(i<- list ){
      if(i("cont").contains(target)){
        id=i("id").asInstanceOf[Double].toInt
      }
    }

    if(id==(-1)){
      return ""
    }

    ret+=(16->posmap(list(id)("pos")))

    for(i<-list){
      if(i("id").asInstanceOf[Double].toInt!=id){

        val parentid=i("parent").asInstanceOf[Double].toInt
        if(parentid==id) {

          val key = dpmap(i("relate"))
          if(!ret.keySet.contains(key)){
            ret+= (key->0)
          }
          val value = ret(key)+1
          ret(key)=value
        }
      }
    }
    return ret.toArray.sortBy(x=>x._1).map(x=>x._1.toString+":"+x._2.toString).mkString(" ")
  }


  def mapLoader(dir:String): HashMap[String,Int] ={
    var ret= new HashMap[String,Int]
    var s="fakeData"
    val conf = new Configuration()
    val fs=org.apache.hadoop.fs.FileSystem.get(conf)
    val tempmap=  new HdfsFileRW().FileRead(fs,dir)

    while (s!=null){
      s=tempmap.readLine()
      if(s!=null) {
        val temp = s.split(':')
        ret += (temp(0) -> temp(1).toInt)
      }
    }
    tempmap.close()
    return ret
  }

}


